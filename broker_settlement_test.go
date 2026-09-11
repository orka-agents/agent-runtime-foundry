package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestBrokerCompletedSettlementPreservesRemoteConversation(t *testing.T) {
	for _, restart := range []bool{false, true} {
		name := "same-broker"
		if restart {
			name = "recovered-broker"
		}
		t.Run(name, func(t *testing.T) {
			f := newBrokerFixture(t, "success")
			f.server.Close()
			var historyLost atomic.Bool
			// Foundry stop terminates compute. Model a backend that retains
			// response history in RAM and rejects continuation after a stop.
			f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, ":stop") {
					historyLost.Store(true)
				}
				if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/protocols/openai/responses") {
					raw, err := io.ReadAll(r.Body)
					var request foundryResponseRequest
					if err != nil || json.Unmarshal(raw, &request) != nil {
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					r.Body = io.NopCloser(bytes.NewReader(raw))
					if request.PreviousResponseID != "" && historyLost.Load() {
						w.WriteHeader(http.StatusBadRequest)
						return
					}
				}
				f.serve(w, r)
			}))
			cfg := brokerTestConfig(t, f)
			b, server := startBrokerTest(t, cfg)
			c := brokerTestContext(cfg)
			status, data, err := brokerTestHTTP(context.Background(), server.URL, brokerResponsesPath, c, brokerTestBody(""))
			var first foundryResponse
			if err != nil || status != http.StatusOK || json.Unmarshal(data, &first) != nil {
				t.Fatalf("seed response failed: status=%d", status)
			}
			if restart {
				b.close()
				server.Close()
				b, server = startBrokerTest(t, cfg)
			}
			proof := brokerTestControl(t, server.URL, brokerSettlePath, c)
			if !proof.SettlementProven || proof.ActiveInvocations != 0 || !brokerDigestValid(proof.ProofDigest) {
				t.Fatal("completed request lacked durable settlement")
			}
			c.TaskUID, c.PromptID, c.OperationID = "continuation-task", "continuation-prompt", "continuation-request"
			status, _, err = brokerTestHTTP(context.Background(), server.URL, brokerResponsesPath, c, brokerTestBody(first.ID))
			if err != nil || status != http.StatusOK {
				t.Fatalf("completed settlement destroyed remote conversation: status=%d", status)
			}
			_ = brokerTestControl(t, server.URL, brokerSettlePath, c)
			proof = brokerTestControl(t, server.URL, brokerRetirePath, c)
			if !proof.RetirementProven {
				t.Fatal("completed session retirement lacked deletion proof")
			}
			creates, inferences, stops, deletes := f.counts()
			if creates != 1 || inferences != 2 || stops != 0 || deletes != 1 {
				t.Fatalf("unexpected lifecycle operations: %d %d %d %d", creates, inferences, stops, deletes)
			}
			b.mu.Lock()
			valid := brokerLedgerValid(b.ledger, cfg.configDigest)
			b.mu.Unlock()
			if !valid {
				t.Fatal("completed settlement invalidated ownership evidence")
			}
		})
	}
}
