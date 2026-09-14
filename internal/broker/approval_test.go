package broker

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/orka-agents/agent-runtime-foundry/internal/brokerapi"
	"github.com/orka-agents/agent-runtime-foundry/internal/foundry"
)

// The broker is idle while Orka holds tools/call. Its completed proposal and
// exact pending-call map must survive a live lease renewal, but never authorize
// a continuation after expiry, cancellation, or broker recovery.
func TestBrokerPendingApprovalContinuationRequiresLiveOwnership(t *testing.T) {
	for _, mode := range []string{"renewed", "stale_lease", "expired", "cancelled", "restarted"} {
		t.Run(mode, func(t *testing.T) {
			f := newBrokerFixture(t, "functions")
			cfg := brokerTestConfig(t, f)
			cfg.agentKitProof = brokerAgentKitFixtureProof
			b, server := startBrokerTest(t, cfg)
			c := brokerTestContext(cfg)
			if mode == "renewed" || mode == "expired" {
				c.LeaseExpiresAt = time.Now().Add(time.Second).UTC().Format(time.RFC3339Nano)
			}
			status, raw, err := brokerTestHTTP(context.Background(), server.URL, brokerapi.ResponsesPath, c, brokerTestBody(""))
			var proposal foundry.Response
			if err != nil || status != http.StatusOK || json.Unmarshal(raw, &proposal) != nil || len(proposal.Output) != 1 {
				t.Fatal("initial pending tool call was not durably recorded")
			}
			original := c
			switch mode {
			case "renewed", "stale_lease":
				renewal := c
				renewal.LeaseGeneration++
				renewal.LeaseExpiresAt = time.Now().Add(2 * time.Minute).UTC().Format(time.RFC3339Nano)
				proof := brokerTestControl(t, server.URL, brokerapi.RenewPath, renewal)
				if proof.State != "open" || proof.LeaseGeneration != 2 {
					t.Fatal("idle approval wait did not retain its renewed ownership")
				}
				if mode == "renewed" {
					c = renewal
				}
			case "cancelled":
				_ = brokerTestControl(t, server.URL, brokerapi.SettlePath, c)
			case "restarted":
				b.close()
				server.Close()
				b, server = startBrokerTest(t, cfg)
			}
			if mode == "renewed" || mode == "expired" {
				expiry, _ := time.Parse(time.RFC3339Nano, original.LeaseExpiresAt)
				time.Sleep(time.Until(expiry.Add(50 * time.Millisecond)))
			}
			_, inferences, _, _ := f.counts()
			if inferences != 1 {
				t.Fatal("a pending review submitted model work")
			}
			c.InvocationSequence++
			c.OperationID = "held-approval-result"
			body := brokerAgentKitFunctionBody(proposal.ID, proposal.Output[0].CallID, brokerAgentKitOrkaError("approval_declined"))
			status, _, err = brokerTestHTTP(context.Background(), server.URL, brokerapi.ResponsesPath, c, body)
			wantStatus := http.StatusGone
			if mode == "renewed" {
				wantStatus = http.StatusOK
			} else if mode == "stale_lease" {
				wantStatus = http.StatusConflict
			}
			if err != nil || status != wantStatus {
				t.Fatalf("held continuation status=%d, want %d", status, wantStatus)
			}
			c.InvocationSequence++
			c.OperationID = "repeated-held-approval-result"
			status, _, err = brokerTestHTTP(context.Background(), server.URL, brokerapi.ResponsesPath, c, body)
			if err != nil || status == http.StatusOK {
				t.Fatal("a repeated or stale decision resubmitted the original tool result")
			}
			_, inferences, _, _ = f.counts()
			wantInferences := 1
			if mode == "renewed" {
				wantInferences = 2
			}
			if inferences != wantInferences {
				t.Fatal("lost ownership or duplicate continuation repeated model work")
			}
			if mode == "restarted" {
				b.mu.Lock()
				session := b.ledger.Sessions[foundry.JSONDigest(c.Owner)]
				stored := session.Responses[proposal.ID]
				preserved := session.Prompts[c.promptKey()].Closing && stored.CallIDs[proposal.Output[0].CallID] != ""
				b.mu.Unlock()
				if !preserved {
					t.Fatal("broker restart erased the closed original approval ownership")
				}
			}
			_ = brokerTestControl(t, server.URL, brokerapi.SettlePath, c)
			_ = brokerTestControl(t, server.URL, brokerapi.RetirePath, c)
		})
	}
}
