package broker

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orka-agents/agent-runtime-foundry/internal/brokerapi"
	"github.com/orka-agents/agent-runtime-foundry/internal/foundry"
)

func TestBrokerStoragePoisonCancelsDetachedMutations(t *testing.T) {
	for _, phase := range []string{"create", "stop", "delete"} {
		t.Run(phase, func(t *testing.T) {
			mode := "success"
			if phase == "stop" {
				mode = "hold-known"
			}
			f := newBrokerFixture(t, mode)
			cfg := brokerTestConfig(t, f)
			cfg.operationTimeout = 5 * time.Second
			entered := make(chan context.Context, 1)
			release := make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			defer unblock()
			client := &http.Client{Transport: brokerFixtureTransport(func(r *http.Request) (*http.Response, error) {
				selected := phase == "create" && r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/endpoint/sessions") ||
					phase == "stop" && r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, ":stop") ||
					phase == "delete" && r.Method == http.MethodDelete
				if selected {
					entered <- r.Context()
					select {
					case <-r.Context().Done():
						return nil, r.Context().Err()
					case <-release:
					}
				}
				return http.DefaultTransport.RoundTrip(r)
			})}
			b, server := startBrokerTestWithClient(t, cfg, client)
			c := brokerTestContext(cfg)
			done := brokerAsyncInference(context.Background(), server.URL, c, brokerTestBody(""))
			if phase == "stop" {
				brokerAwait(t, func() bool { return brokerInvocationState(b, c) == "accepted" })
				b.closePrompt(foundry.JSONDigest(c.Owner), c.promptKey())
				_ = brokerWaitInference(t, done)
			} else if phase == "delete" {
				result := brokerWaitInference(t, done)
				if result.err != nil || result.status != http.StatusOK {
					t.Fatal("original invocation did not complete")
				}
				brokerPendingControl(t, server.URL, brokerapi.RetirePath, c, false, 0)
			}
			var mutationCtx context.Context
			select {
			case mutationCtx = <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("selected original mutation did not enter transport")
			}
			retained, restore := brokerReviewFaultStore(t, cfg.stateDir)
			before, err := os.ReadFile(filepath.Join(retained, "state.json"))
			if err != nil {
				t.Fatal("could not read original intent")
			}
			other := c
			other.Owner.RuntimeSessionUID = "other-owner"
			other, body := brokerTestControlContext(brokerapi.RenewPath, other)
			status, _, err := brokerTestHTTP(context.Background(), server.URL, brokerapi.RenewPath, other, body)
			if err != nil || status != http.StatusServiceUnavailable {
				t.Fatal("storage poison was not observed")
			}
			cancelled := mutationCtx.Err() != nil
			unblock()
			if phase == "create" {
				result := brokerWaitInference(t, done)
				if result.err == nil && result.status == http.StatusOK {
					t.Fatal("poisoned broker exposed successful output")
				}
			}
			brokerAwait(t, func() bool {
				b.mu.Lock()
				defer b.mu.Unlock()
				return len(b.workers) == 0
			})
			b.close()
			server.Close()
			after, err := os.ReadFile(filepath.Join(retained, "state.json"))
			if err != nil || !bytes.Equal(before, after) || !errors.Is(b.storageError, errBrokerStorage) {
				t.Fatal("storage poison changed durable ownership or disappeared")
			}
			restore()
			creates, inferences, stops, deletes := f.counts()
			if !cancelled || stops != 0 || deletes != 0 || phase == "create" && creates+inferences != 0 {
				t.Fatalf("detached mutation continued after poison: cancelled=%v creates=%d inferences=%d stops=%d deletes=%d", cancelled, creates, inferences, stops, deletes)
			}
		})
	}
}
