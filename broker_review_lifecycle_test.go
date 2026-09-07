package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func brokerReviewFaultStore(t *testing.T, dir string) (string, func()) {
	t.Helper()
	retained := dir + "-retained"
	if err := os.Rename(dir, retained); err != nil {
		t.Fatal("could not retain fixture ledger before storage fault")
	}
	restored := false
	restore := func() {
		if restored {
			return
		}
		if err := os.Remove(dir); err != nil && !os.IsNotExist(err) {
			t.Fatal("could not remove fixture storage fault")
		}
		if err := os.Rename(retained, dir); err != nil {
			t.Fatal("could not restore retained fixture ledger")
		}
		restored = true
	}
	t.Cleanup(restore)
	// A regular file at the directory path makes every attempted save fail
	// deterministically, including when the tests run with root privileges.
	if err := os.WriteFile(dir, []byte("fixture storage unavailable"), 0o600); err != nil {
		t.Fatal("could not install fixture storage fault")
	}
	return retained, restore
}

func TestBrokerStorageFailureCancelsActiveRequests(t *testing.T) {
	for _, trigger := range []string{"expiry", "settle", "other-owner-renewal"} {
		t.Run(trigger, func(t *testing.T) {
			f := newBrokerFixture(t, "hold-known")
			cfg := brokerTestConfig(t, f)
			b, server := startBrokerTest(t, cfg)
			c := brokerTestContext(cfg)
			if trigger == "expiry" {
				c.LeaseExpiresAt = time.Now().Add(800 * time.Millisecond).UTC().Format(time.RFC3339Nano)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := brokerAsyncInference(ctx, server.URL, c, brokerTestBody(""))
			brokerAwait(t, func() bool { return brokerInvocationState(b, c) == "accepted" })
			before, err := os.ReadFile(filepath.Join(cfg.stateDir, "state.json"))
			if err != nil {
				t.Fatal("could not read durable fixture ownership")
			}
			retained, restore := brokerReviewFaultStore(t, cfg.stateDir)
			if trigger != "expiry" {
				path, closing := brokerSettlePath, c
				if trigger == "other-owner-renewal" {
					path = brokerRenewPath
					closing.Owner.RuntimeSessionUID = "fixture-other-session"
				}
				closing, body := brokerTestControlContext(path, closing)
				status, _, err := brokerTestHTTP(context.Background(), server.URL, path, closing, body)
				if err != nil || status != http.StatusServiceUnavailable {
					t.Fatal("control did not report the storage failure")
				}
			}
			brokerAwait(t, func() bool {
				response, err := http.Get(server.URL + "/healthz")
				if err != nil {
					return false
				}
				_ = response.Body.Close()
				return response.StatusCode == http.StatusServiceUnavailable
			})
			select {
			case result := <-done:
				if result.err == nil && result.status == http.StatusOK {
					t.Fatal("storage failure exposed a successful response")
				}
			case <-time.After(time.Second):
				t.Fatal("storage failure left accepted inference running")
			}
			after, err := os.ReadFile(filepath.Join(retained, "state.json"))
			if err != nil || !bytes.Equal(before, after) {
				t.Fatal("storage fault changed the last durable ownership record")
			}
			b.mu.Lock()
			active, poisoned := len(b.active), b.storageError != nil
			b.mu.Unlock()
			creates, inferences, stops, deletes := f.counts()
			if active != 0 || !poisoned || creates != 1 || inferences != 1 || stops != 0 || deletes != 0 {
				t.Fatal("poisoned storage kept active authority or claimed remote cleanup")
			}
			closing, body := brokerTestControlContext(brokerSettlePath, c)
			status, _, err := brokerTestHTTP(context.Background(), server.URL, brokerSettlePath, closing, body)
			if err != nil || status != http.StatusServiceUnavailable {
				t.Fatal("poisoned storage exposed settlement proof")
			}
			b.close()
			server.Close()
			restore()
			_, restarted := startBrokerTest(t, cfg)
			proof := brokerTestControl(t, restarted.URL, brokerSettlePath, c)
			if !proof.SettlementProven || proof.AmbiguousInvocations != 0 {
				t.Fatal("recovery lost the original acknowledged owner")
			}
			proof = brokerTestControl(t, restarted.URL, brokerRetirePath, c)
			creates, inferences, stops, deletes = f.counts()
			if !proof.RetirementProven || creates != 1 || inferences != 1 || stops == 0 || deletes != 1 {
				t.Fatal("storage recovery replayed work or skipped acknowledged cleanup")
			}
		})
	}
}

func TestBrokerRetirementRejectsNewSettlementPrompt(t *testing.T) {
	for _, state := range []string{"retiring", "retired"} {
		t.Run(state, func(t *testing.T) {
			f := newBrokerFixture(t, "success")
			cfg := brokerTestConfig(t, f)
			deleteStarted, releaseDelete := make(chan struct{}), make(chan struct{})
			var deleteOnce, releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(releaseDelete) }) }
			defer release()
			client := &http.Client{Transport: brokerFixtureTransport(func(r *http.Request) (*http.Response, error) {
				if state == "retiring" && r.Method == http.MethodDelete {
					deleteOnce.Do(func() { close(deleteStarted) })
					select {
					case <-releaseDelete:
					case <-r.Context().Done():
						return nil, r.Context().Err()
					}
				}
				return http.DefaultTransport.RoundTrip(r)
			})}
			b, server := startBrokerTestWithClient(t, cfg, client)
			c := brokerTestContext(cfg)
			status, _, err := brokerTestHTTP(context.Background(), server.URL, brokerResponsesPath, c, brokerTestBody(""))
			if err != nil || status != http.StatusOK {
				t.Fatal("initial fixture inference failed")
			}
			_ = brokerTestControl(t, server.URL, brokerSettlePath, c)
			if state == "retiring" {
				retire, body := brokerTestControlContext(brokerRetirePath, c)
				status, _, err := brokerTestHTTP(context.Background(), server.URL, brokerRetirePath, retire, body)
				if err != nil || status != http.StatusConflict {
					t.Fatal("retirement did not wait for deletion acknowledgement")
				}
				select {
				case <-deleteStarted:
				case <-time.After(time.Second):
					t.Fatal("retirement did not reach the held deletion")
				}
			} else {
				_ = brokerTestControl(t, server.URL, brokerRetirePath, c)
			}
			before, err := os.ReadFile(filepath.Join(cfg.stateDir, "state.json"))
			if err != nil {
				t.Fatal("could not read fixture retirement ownership")
			}
			later := c
			later.TaskUID, later.PromptID = "fixture-task-after-retirement", "fixture-prompt-after-retirement"
			later, body := brokerTestControlContext(brokerSettlePath, later)
			status, _, err = brokerTestHTTP(context.Background(), server.URL, brokerSettlePath, later, body)
			if err != nil || status != http.StatusGone {
				t.Errorf("new settlement identity was admitted after retirement began: status=%d", status)
			}
			after, err := os.ReadFile(filepath.Join(cfg.stateDir, "state.json"))
			if err != nil || !bytes.Equal(before, after) {
				t.Error("rejected settlement identity changed durable retirement ownership")
			}
			b.mu.Lock()
			valid := brokerLedgerValid(b.ledger, cfg.configDigest)
			prompts := len(b.ledger.Sessions[brokerJSONDigest(c.Owner)].Prompts)
			b.mu.Unlock()
			if !valid || prompts != 1 {
				t.Errorf("retirement accepted new prompt or invalidated ledger: valid=%v prompts=%d", valid, prompts)
			}
			proof := brokerTestControl(t, server.URL, brokerSettlePath, c)
			if !proof.SettlementProven {
				t.Fatal("existing settlement lost idempotent proof during retirement")
			}
			release()
			_ = brokerTestControl(t, server.URL, brokerRetirePath, c)
			b.close()
			server.Close()
			_, restarted := startBrokerTest(t, cfg)
			proof = brokerTestControl(t, restarted.URL, brokerRetirePath, c)
			if !proof.RetirementProven {
				t.Fatal("retired ledger did not reopen with the original proof")
			}
		})
	}
}

func TestBrokerCancelledResponseRetainsAcceptanceEvidence(t *testing.T) {
	for _, shape := range []string{"json", "sse-terminal-only", "sse-after-created"} {
		t.Run(shape, func(t *testing.T) {
			media := "application/json"
			if shape != "json" {
				media = "text/event-stream"
			}
			f := newBrokerEvidenceFixture(t, func(request foundryResponseRequest) (string, []byte) {
				response := map[string]any{"id": "provider-cancelled", "status": "cancelled", "agent_session_id": request.AgentSessionID, "output": []any{}}
				var value any = response
				if media == "text/event-stream" {
					value = map[string]any{"type": "response.cancelled", "response": response}
				}
				data, _ := json.Marshal(value)
				if media == "text/event-stream" {
					data = []byte("data: " + string(data) + "\n\n")
				}
				if shape == "sse-after-created" {
					response["status"] = "in_progress"
					created, _ := json.Marshal(map[string]any{"type": "response.created", "response": response})
					data = append([]byte("data: "+string(created)+"\n\n"), data...)
				}
				return media, data
			})
			cfg := brokerTestConfig(t, f)
			b, server := startBrokerTest(t, cfg)
			c := brokerTestContext(cfg)
			status, _, err := brokerTestHTTP(context.Background(), server.URL, brokerResponsesPath, c, brokerTestBody(""))
			if err != nil || status == http.StatusOK {
				t.Fatal("cancelled provider response was exposed as success")
			}
			if state := brokerInvocationState(b, c); state != "accepted" && state != "settled" {
				t.Fatalf("coherent cancellation lost acceptance evidence: state=%s", state)
			}
			proof := brokerTestControl(t, server.URL, brokerSettlePath, c)
			if !proof.SettlementProven || proof.AmbiguousInvocations != 0 {
				t.Fatal("acknowledged cancellation could not prove stop containment")
			}
			proof = brokerTestControl(t, server.URL, brokerRetirePath, c)
			creates, inferences, stops, deletes := f.counts()
			if !proof.RetirementProven || creates != 1 || inferences != 1 || stops == 0 || deletes != 1 {
				t.Fatal("acknowledged cancellation replayed work or skipped cleanup")
			}
		})
	}
}

func TestBrokerCancelledSSERequiresCoherentEvidence(t *testing.T) {
	for _, failure := range []string{"status-mismatch", "response-error", "wrong-session"} {
		t.Run(failure, func(t *testing.T) {
			f := newBrokerEvidenceFixture(t, func(request foundryResponseRequest) (string, []byte) {
				response := map[string]any{"id": "provider-cancelled", "status": "cancelled", "agent_session_id": request.AgentSessionID, "output": []any{}}
				switch failure {
				case "status-mismatch":
					response["status"] = "in_progress"
				case "response-error":
					response["error"] = map[string]string{"code": "server_error", "message": "fixture-error-do-not-persist"}
				case "wrong-session":
					response["agent_session_id"] = "fixture-other-session"
				}
				data, _ := json.Marshal(map[string]any{"type": "response.cancelled", "response": response})
				return "text/event-stream", []byte("data: " + string(data) + "\n\n")
			})
			cfg := brokerTestConfig(t, f)
			b, server := startBrokerTest(t, cfg)
			c := brokerTestContext(cfg)
			status, _, err := brokerTestHTTP(context.Background(), server.URL, brokerResponsesPath, c, brokerTestBody(""))
			if err != nil || status == http.StatusOK || brokerInvocationState(b, c) != "uncertain" {
				t.Fatal("incoherent cancellation acknowledged an invocation")
			}
			brokerPendingControl(t, server.URL, brokerSettlePath, c, false, 1)
			brokerPendingControl(t, server.URL, brokerRetirePath, c, false, 1)
			_, _, _, deletes := f.counts()
			if deletes != 0 {
				t.Fatal("incoherent cancellation authorized deletion")
			}
		})
	}
}
