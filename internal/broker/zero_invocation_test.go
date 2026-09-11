package broker

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
	"time"

	"github.com/orka-agents/agent-runtime-foundry/internal/brokerapi"
	"github.com/orka-agents/agent-runtime-foundry/internal/foundry"
)

func TestBrokerUnsentPromptPreservesRemoteConversation(t *testing.T) {
	for _, mode := range []string{"no-invocations", "expiry-before-inference", "reserved-on-restart", "rejected"} {
		t.Run(mode, func(t *testing.T) {
			f := newBrokerFixture(t, "success")
			f.server.Close()
			var historyLost, rejectNext atomic.Bool
			f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, ":stop") {
					historyLost.Store(true)
				}
				if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/protocols/openai/responses") {
					raw, err := io.ReadAll(r.Body)
					var request foundry.ResponseRequest
					if err != nil || json.Unmarshal(raw, &request) != nil {
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					r.Body = io.NopCloser(bytes.NewReader(raw))
					if rejectNext.Swap(false) {
						w.WriteHeader(http.StatusTooManyRequests)
						return
					}
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
			status, data, err := brokerTestHTTP(context.Background(), server.URL, brokerapi.ResponsesPath, c, brokerTestBody(""))
			var first foundry.Response
			if err != nil || status != http.StatusOK || json.Unmarshal(data, &first) != nil {
				t.Fatal("initial fixture response failed")
			}
			_ = brokerTestControl(t, server.URL, brokerapi.SettlePath, c)

			c.TaskUID, c.PromptID, c.OperationID = "unsent-task", "unsent-prompt", "unsent-invocation"
			if mode == "expiry-before-inference" {
				c.LeaseExpiresAt = time.Now().Add(500 * time.Millisecond).UTC().Format(time.RFC3339Nano)
			}
			_ = brokerTestControl(t, server.URL, brokerapi.RenewPath, c)
			switch mode {
			case "expiry-before-inference":
				brokerAwait(t, func() bool {
					b.mu.Lock()
					defer b.mu.Unlock()
					return b.ledger.Sessions[foundry.JSONDigest(c.Owner)].Prompts[c.promptKey()].Settled
				})
			case "reserved-on-restart":
				// A crash after durable reservation leaves no live request and
				// never resumes the reserved invocation on broker restart.
				c.BodySHA256 = foundry.Digest(brokerTestBody(first.ID))
				b.mu.Lock()
				err := b.commitLocked(func(next *brokerLedger) error {
					session := next.Sessions[foundry.JSONDigest(c.Owner)]
					if _, err := brokerRecordOperation(session, brokerapi.ResponsesPath, c); err != nil {
						return err
					}
					prompt := session.Prompts[c.promptKey()]
					prompt.LastSequence = c.InvocationSequence
					prompt.Invocations[c.InvocationSequence] = &brokerInvocation{
						Sequence: c.InvocationSequence, OperationID: c.OperationID, BodyDigest: c.BodySHA256, State: "reserved",
					}
					return nil
				})
				valid := brokerLedgerValid(b.ledger, cfg.configDigest)
				b.mu.Unlock()
				if err != nil || !valid {
					t.Fatal("reserved crash fixture did not preserve valid durable ownership")
				}
				b.close()
				server.Close()
				b, server = startBrokerTest(t, cfg)
			case "rejected":
				rejectNext.Store(true)
				status, _, err := brokerTestHTTP(context.Background(), server.URL, brokerapi.ResponsesPath, c, brokerTestBody(first.ID))
				if err != nil || status == http.StatusOK {
					t.Fatal("definite admission rejection was exposed as a response")
				}
			}
			proof := brokerTestControl(t, server.URL, brokerapi.SettlePath, c)
			if !proof.SettlementProven || proof.ActiveInvocations != 0 || proof.AmbiguousInvocations != 0 || !foundry.DigestValid(proof.ProofDigest) {
				t.Fatal("unsent prompt did not obtain durable settlement")
			}
			_, _, stops, _ := f.counts()
			if stops != 0 {
				t.Error("settling an unsent prompt stopped the retained remote conversation")
			}
			c.TaskUID, c.PromptID, c.OperationID = "continued-task", "continued-prompt", "continued-invocation"
			c.LeaseExpiresAt = time.Now().Add(10 * time.Second).UTC().Format(time.RFC3339Nano)
			status, _, err = brokerTestHTTP(context.Background(), server.URL, brokerapi.ResponsesPath, c, brokerTestBody(first.ID))
			if err != nil || status != http.StatusOK {
				t.Fatalf("unsent prompt destroyed previous-response continuation: status=%d", status)
			}
			_ = brokerTestControl(t, server.URL, brokerapi.SettlePath, c)
			proof = brokerTestControl(t, server.URL, brokerapi.RetirePath, c)
			creates, inferences, stops, deletes := f.counts()
			if !proof.RetirementProven || creates != 1 || inferences != 2 || stops != 0 || deletes != 1 {
				t.Fatal("unsent prompt cleanup replayed work or lost retirement ownership")
			}
			b.mu.Lock()
			valid := brokerLedgerValid(b.ledger, cfg.configDigest)
			b.mu.Unlock()
			if !valid {
				t.Fatal("unsent prompt settlement invalidated durable ownership")
			}
		})
	}
}
