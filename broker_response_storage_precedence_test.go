package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type brokerGatedResponseBody struct {
	io.ReadCloser
	onRead, onClose func()
}

func (b *brokerGatedResponseBody) Read(p []byte) (int, error) {
	if b.onRead != nil {
		b.onRead()
	}
	return b.ReadCloser.Read(p)
}

func (b *brokerGatedResponseBody) Close() error {
	if b.onClose != nil {
		b.onClose()
	}
	return b.ReadCloser.Close()
}

func TestBrokerStoragePoisonResponsePrecedence(t *testing.T) {
	for _, phase := range []string{"response-identity", "finish-after-durable-completion"} {
		t.Run(phase, func(t *testing.T) {
			f := newBrokerFixture(t, "success")
			cfg := brokerTestConfig(t, f)
			entered, release := make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			defer unblock()
			gate := sync.OnceFunc(func() { close(entered); <-release })
			client := newBrokerHTTPClient()
			transport := client.Transport.(*http.Transport)
			defer transport.CloseIdleConnections()
			client.Transport = brokerFixtureTransport(func(r *http.Request) (*http.Response, error) {
				response, err := transport.RoundTrip(r)
				if err == nil && r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/protocols/openai/responses") {
					body := &brokerGatedResponseBody{ReadCloser: response.Body}
					if phase == "response-identity" {
						body.onRead = gate
					} else {
						body.onClose = gate
					}
					response.Body = body
				}
				return response, err
			})
			b, server := startBrokerTestWithClient(t, cfg, client)
			c := brokerTestContext(cfg)
			done := brokerAsyncInference(context.Background(), server.URL, c, brokerTestBody(""))
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("original response did not reach the selected persistence boundary")
			}
			before, err := os.ReadFile(filepath.Join(cfg.stateDir, "state.json"))
			if err != nil {
				t.Fatal(err)
			}
			state := brokerInvocationState(b, c)
			wantState := "intent"
			if phase == "finish-after-durable-completion" {
				wantState = "completed"
			}
			if state != wantState {
				t.Fatalf("wrong selected boundary: state %q, want %q", state, wantState)
			}
			retained, restore := brokerReviewFaultStore(t, cfg.stateDir)
			unblock()
			result := brokerWaitInference(t, done)
			b.mu.Lock()
			poisoned := errors.Is(b.storageError, errBrokerStorage)
			cancelled := b.ctx.Err() != nil
			active := len(b.active)
			b.mu.Unlock()
			b.close()
			server.Close()
			after, err := os.ReadFile(filepath.Join(retained, "state.json"))
			restore()
			if err != nil || !bytes.Equal(before, after) || !poisoned || !cancelled || active != 0 {
				t.Fatal("storage failure did not retain the exact durable owner and remove active authority")
			}
			creates, inference, stops, deletes := f.counts()
			if creates != 1 || inference != 1 || stops != 0 || deletes != 0 {
				t.Fatal("storage failure replayed work or claimed cleanup")
			}
			t.Logf("phase=%s HTTP=%d storagePoisoned=%v originalDurableState=%s cancelled=%v", phase, result.status, poisoned, state, cancelled)
			if result.err != nil || result.status != http.StatusServiceUnavailable {
				t.Errorf("poisoned response HTTP = %d, want 503", result.status)
			}
		})
	}
}
