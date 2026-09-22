package broker

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orka-agents/agent-runtime-foundry/internal/brokerapi"
	"github.com/orka-agents/agent-runtime-foundry/internal/foundry"
)

// Exercise cancellation while the real hosted AgentKit is awaiting a model.
// Azure's gateway and session lifecycle remain fixtures; a lost acknowledgement
// must stay unresolved even when the fixture reports that compute is idle.
func TestBrokerAgentKitHostedCancellation(t *testing.T) {
	source := os.Getenv("AGENTKIT_SOURCE_DIR")
	if source == "" {
		t.Skip("set AGENTKIT_SOURCE_DIR and AGENTKIT_PYTHON to test an AgentKit checkout")
	}
	for _, mode := range []string{"disconnect", "delayed_ack_disconnect", "lease_expiry", "acknowledgement_lost", "native_disconnect"} {
		t.Run(mode, func(t *testing.T) {
			native := mode == "native_disconnect"
			if native && os.Getenv("AGENTKIT_MAF_PYTHON") == "" {
				t.Skip("set AGENTKIT_MAF_PYTHON to test the native Microsoft Agent Framework runtime")
			}
			modelStarted, modelCancelled := make(chan struct{}), make(chan struct{})
			var models, inferences atomic.Int32
			model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				if err != nil || r.Method != http.MethodPost || r.URL.Path != "/v1/chat/completions" ||
					bytes.Contains(body, []byte(brokerAgentKitFixtureProof)) || r.Header.Get(brokerAgentKitProofHeader) != "" {
					t.Error("model request used the wrong route or exposed continuation credentials")
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				if models.Add(1) != 1 {
					t.Error("cancelled inference was replayed")
					w.WriteHeader(http.StatusConflict)
					return
				}
				close(modelStarted)
				// Never finish the model response. Socket closure, rather than a
				// mocked CancelledError, must interrupt AgentKit's model request.
				<-r.Context().Done()
				close(modelCancelled)
			}))
			t.Cleanup(model.Close)
			hostedURL, stateFile := brokerStartAgentKit(t, source, model.URL, native)
			f := newBrokerFixture(t, "success")
			cfg := brokerTestConfig(t, f)
			cfg.agentKitProof = brokerAgentKitFixtureProof
			acknowledgementLost := make(chan struct{})
			acknowledgementHeld, releaseAcknowledgement := make(chan struct{}), make(chan struct{})
			release := sync.OnceFunc(func() { close(releaseAcknowledgement) })
			t.Cleanup(release)
			client := &http.Client{Transport: brokerFixtureTransport(func(r *http.Request) (*http.Response, error) {
				if !strings.HasSuffix(r.URL.Path, "/endpoint/protocols/openai/responses") {
					return http.DefaultTransport.RoundTrip(r)
				}
				if inferences.Add(1) != 1 {
					t.Error("cancelled hosted request was resubmitted")
					return nil, errBrokerConflict
				}
				request := r.Clone(r.Context())
				request.URL, _ = url.Parse(hostedURL + "/responses")
				request.Host = request.URL.Host
				response, err := http.DefaultTransport.RoundTrip(request)
				if err == nil && mode == "delayed_ack_disconnect" {
					close(acknowledgementHeld)
					select {
					case <-releaseAcknowledgement:
						return response, nil
					case <-request.Context().Done():
						_ = response.Body.Close()
						return nil, request.Context().Err()
					}
				}
				if err != nil || mode != "acknowledgement_lost" {
					return response, err
				}
				// Simulate a gateway losing the first frame. An acknowledgement
				// accepted only by the gateway does not prove broker ownership.
				reader := bufio.NewReader(response.Body)
				var frame bytes.Buffer
				for {
					line, readErr := reader.ReadString('\n')
					if readErr != nil || frame.Len()+len(line) > 1<<20 {
						_ = response.Body.Close()
						return nil, errBrokerAmbiguous
					}
					frame.WriteString(line)
					if strings.TrimSpace(line) == "" {
						break
					}
				}
				if !strings.HasPrefix(response.Header.Get("Content-Type"), "text/event-stream") ||
					!bytes.Contains(frame.Bytes(), []byte(`"response.created"`)) {
					t.Error("hosted server did not send an early response acknowledgement")
					_ = response.Body.Close()
					return nil, errBrokerAmbiguous
				}
				response.Body = struct {
					io.Reader
					io.Closer
				}{reader, response.Body}
				close(acknowledgementLost)
				return response, nil
			})}
			b, server := startBrokerTestWithClient(t, cfg, client)
			c := brokerTestContext(cfg)
			if mode == "lease_expiry" {
				c.LeaseExpiresAt = time.Now().Add(2 * time.Second).UTC().Format(time.RFC3339Nano)
			}
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			done := brokerAsyncInference(ctx, server.URL, c, brokerTestBody(""))
			select {
			case <-modelStarted:
			case <-time.After(4 * time.Second):
				t.Fatal("hosted model request did not start")
			}
			if mode == "acknowledgement_lost" {
				select {
				case <-acknowledgementLost:
				case <-time.After(4 * time.Second):
					t.Fatal("gateway did not receive the early acknowledgement")
				}
				if brokerInvocationState(b, c) != "intent" {
					t.Fatal("gateway-only acknowledgement was admitted as broker evidence")
				}
			} else if mode == "delayed_ack_disconnect" {
				select {
				case <-acknowledgementHeld:
				case <-time.After(4 * time.Second):
					t.Fatal("gateway did not hold the original acknowledgement")
				}
			} else {
				brokerAwait(t, func() bool { return brokerInvocationState(b, c) == "accepted" })
			}
			if mode != "lease_expiry" {
				cancel()
			}
			if mode == "delayed_ack_disconnect" {
				brokerAwait(t, func() bool {
					b.mu.Lock()
					defer b.mu.Unlock()
					return b.ledger.Sessions[foundry.JSONDigest(c.Owner)].Prompts[c.promptKey()].Closing
				})
				release()
			}
			if result := brokerWaitInference(t, done); result.err == nil && result.status == http.StatusOK {
				t.Fatal("cancelled hosted inference exposed a successful terminal response")
			}
			if mode == "acknowledgement_lost" {
				brokerAwait(t, func() bool { return brokerInvocationState(b, c) == "uncertain" })
				brokerPendingControl(t, server.URL, brokerapi.SettlePath, c, false, 1)
				brokerPendingControl(t, server.URL, brokerapi.RetirePath, c, false, 1)
			} else {
				proof := brokerTestControl(t, server.URL, brokerapi.SettlePath, c)
				if !proof.SettlementProven || proof.ActiveInvocations != 0 || proof.AmbiguousInvocations != 0 {
					t.Fatal("acknowledged hosted cancellation did not settle")
				}
				if proof := brokerTestControl(t, server.URL, brokerapi.RetirePath, c); !proof.RetirementProven {
					t.Fatal("acknowledged hosted owner did not retire")
				}
			}
			select {
			case <-modelCancelled:
			case <-time.After(4 * time.Second):
				t.Fatal("hosted client disconnect left the model request running")
			}
			brokerAwait(t, func() bool { _, _, stops, _ := f.counts(); return stops > 0 })
			creates, _, _, deletes := f.counts()
			wantDeletes := 1
			if mode == "acknowledgement_lost" {
				wantDeletes = 0
			}
			if creates != 1 || inferences.Load() != 1 || models.Load() != 1 || deletes != wantDeletes {
				t.Fatal("hosted cancellation replayed work or deleted an unresolved owner")
			}
			for _, path := range []string{stateFile, filepath.Join(cfg.stateDir, "state.json")} {
				data, err := os.ReadFile(path)
				// An initial cancelled request has no hosted continuation to
				// cache. The broker must always retain its ownership ledger.
				if path == stateFile && os.IsNotExist(err) {
					continue
				}
				if err != nil || bytes.Contains(data, []byte(cfg.agentKitProof)) {
					t.Fatal("durable state was missing or retained continuation credentials")
				}
			}
		})
	}
}
