package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/orka-agents/agent-runtime-foundry/internal/brokerapi"
	"github.com/orka-agents/agent-runtime-foundry/internal/foundry"
)

func TestBrokerLegacyIntentAtByteCapRemainsContainable(t *testing.T) {
	f := newBrokerFixture(t, "hold-unknown")
	cfg := brokerTestConfig(t, f)
	b, server := startBrokerTest(t, cfg)
	c := brokerTestContext(cfg)
	c.LeaseExpiresAt = time.Now().Add(4 * time.Minute).UTC().Format(time.RFC3339Nano)
	done := brokerAsyncInference(context.Background(), server.URL, c, brokerTestBody(""))
	select {
	case <-f.started:
	case <-time.After(3 * time.Second):
		t.Fatal("original request did not reach provider transport")
	}
	// The legacy schema admitted this valid retained history before ordinary
	// admission reserved cleanup bytes. Preserve the actual in-flight owner.
	brokerCapacityFillHistory(t, b, c, brokerMaxLedgerBytes, false)
	path := filepath.Join(cfg.stateDir, "state.json")
	before, err := os.ReadFile(path)
	var original brokerLedger
	if err != nil || json.Unmarshal(before, &original) != nil || !brokerLedgerValid(&original, cfg.configDigest) || len(before) != brokerMaxLedgerBytes {
		t.Fatal("legacy fixture does not reach the valid byte boundary")
	}
	b.close()
	server.Close()
	result := brokerWaitInference(t, done)
	if result.err == nil && result.status == http.StatusOK {
		t.Fatal("interrupted inference returned usable output")
	}
	retained, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, retained) {
		t.Fatal("unpersistable uncertainty overwrote the legacy owner")
	}
	recovered, restarted := startBrokerTest(t, cfg)
	proof := brokerTestControl(t, restarted.URL, brokerapi.StatusPath, c)
	if proof.State != "blocked" || proof.AmbiguousInvocations != 1 || proof.ActiveInvocations != 0 ||
		proof.CreatePending || !proof.RemoteSessionCreated || proof.SettlementProven || proof.RetirementProven || proof.ProofDigest != "" {
		t.Fatal("abandoned intent was reported as active or proven cleanup")
	}
	brokerAwait(t, func() bool { _, _, stops, _ := f.counts(); return stops > 0 })
	recovered.mu.Lock()
	session := recovered.ledger.Sessions[foundry.JSONDigest(c.Owner)]
	prompt := session.Prompts[c.promptKey()]
	valid := brokerLedgerValid(recovered.ledger, cfg.configDigest) && prompt.Closing && !prompt.Settled &&
		!session.Retired && prompt.Invocations[c.InvocationSequence].State == "intent"
	recovered.mu.Unlock()
	after, err := os.ReadFile(path)
	creates, inferences, _, deletes := f.counts()
	if !valid || err != nil || len(after) > len(before) || creates != 1 || inferences != 1 || deletes != 0 {
		t.Fatal("legacy containment grew the ledger, lost ownership, replayed work, or claimed deletion")
	}
}
