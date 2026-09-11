package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"testing"
	"time"
)

const brokerHistoricalOperationCapacity = 16384

func brokerFillHistoricalOperationCapacity(t *testing.T, b *lifecycleBroker, c brokerContext, count int) (brokerContext, map[string]string) {
	t.Helper()
	renewal, body := brokerTestControlContext(brokerRenewPath, c)
	renewal.BodySHA256 = brokerSHA(body)
	b.mu.Lock()
	err := b.commitLocked(func(next *brokerLedger) error {
		session := next.Sessions[brokerJSONDigest(c.Owner)]
		prompt := session.Prompts[c.promptKey()]
		for len(session.Operations) < count {
			renewal.OperationID = fmt.Sprintf("historical-renewal-%d", len(session.Operations))
			renewal.LeaseGeneration = prompt.LeaseGeneration + 1
			session.Operations[renewal.OperationID] = brokerOperationDigest(brokerRenewPath, renewal)
			prompt.LeaseGeneration = renewal.LeaseGeneration
		}
		return nil
	})
	valid := brokerLedgerValid(b.ledger, b.cfg.configDigest)
	operations := make(map[string]string, len(b.ledger.Sessions[brokerJSONDigest(c.Owner)].Operations))
	for key, value := range b.ledger.Sessions[brokerJSONDigest(c.Owner)].Operations {
		operations[key] = value
	}
	b.mu.Unlock()
	if err != nil || !valid {
		t.Fatal("saturated historical fixture did not preserve valid durable ownership")
	}
	return renewal, operations
}

func TestBrokerOperationCapacityPreservesSettlementAndRetirement(t *testing.T) {
	for _, settleFirst := range []bool{false, true} {
		name := "direct-retirement"
		if settleFirst {
			name = "settlement-then-retirement"
		}
		t.Run(name, func(t *testing.T) {
			f := newBrokerFixture(t, "success")
			cfg := brokerTestConfig(t, f)
			b, server := startBrokerTest(t, cfg)
			c := brokerTestContext(cfg)
			c.LeaseExpiresAt = time.Now().Add(2 * time.Minute).UTC().Format(time.RFC3339Nano)
			status, _, err := brokerTestHTTP(context.Background(), server.URL, brokerResponsesPath, c, brokerTestBody(""))
			if err != nil || status != http.StatusOK {
				t.Fatal("initial fixture inference failed")
			}
			renewal, operations := brokerFillHistoricalOperationCapacity(t, b, c, brokerHistoricalOperationCapacity)
			fresh := renewal
			fresh.OperationID = "renewal-after-capacity"
			fresh.LeaseGeneration++
			status, _, err = brokerTestHTTP(context.Background(), server.URL, brokerRenewPath, fresh, []byte("{}"))
			if err != nil || status != http.StatusServiceUnavailable {
				t.Fatal("saturated owner admitted more ordinary operation records")
			}
			status, _, err = brokerTestHTTP(context.Background(), server.URL, brokerRenewPath, renewal, []byte("{}"))
			if err != nil || status != http.StatusOK {
				t.Fatal("saturation rejected an exact recorded renewal duplicate")
			}
			status, _, err = brokerTestHTTP(context.Background(), server.URL, brokerResponsesPath, c, brokerTestBody(""))
			if err != nil || status != http.StatusConflict {
				t.Fatal("saturation replayed a recorded inference")
			}
			conflict, body := brokerTestControlContext(brokerRetirePath, c)
			conflict.OperationID = renewal.OperationID
			status, _, err = brokerTestHTTP(context.Background(), server.URL, brokerRetirePath, conflict, body)
			if err != nil || status != http.StatusConflict {
				t.Fatal("cleanup capacity bypassed a recorded operation conflict")
			}
			if settleFirst {
				proof := brokerTestControl(t, server.URL, brokerSettlePath, c)
				if !proof.SettlementProven {
					t.Fatal("saturated owner could not settle its current prompt")
				}
				extra, body := brokerTestControlContext(brokerSettlePath, c)
				extra.OperationID = "extra-settlement-after-capacity"
				status, _, err := brokerTestHTTP(context.Background(), server.URL, brokerSettlePath, extra, body)
				if err != nil || status != http.StatusServiceUnavailable {
					t.Fatal("extra settlement consumed retirement capacity")
				}
			}
			proof := brokerTestControl(t, server.URL, brokerRetirePath, c)
			if !proof.RetirementProven || proof.OwnerDigest != brokerJSONDigest(c.Owner) || !brokerDigestValid(proof.ProofDigest) {
				t.Fatal("saturated owner lacked exact durable retirement proof")
			}
			// Distinct cleanup IDs remain bounded; the original request stays
			// replayable even after the two reserved slots are occupied.
			for i := range 3 {
				extra, body := brokerTestControlContext(brokerRetirePath, c)
				extra.OperationID = fmt.Sprintf("extra-retirement-%d", i)
				status, _, err := brokerTestHTTP(context.Background(), server.URL, brokerRetirePath, extra, body)
				expected := http.StatusServiceUnavailable
				if !settleFirst && i == 0 {
					expected = http.StatusOK
				}
				if err != nil || status != expected {
					t.Fatal("retirement operation capacity was not bounded")
				}
			}
			b.mu.Lock()
			session := b.ledger.Sessions[brokerJSONDigest(c.Owner)]
			bounded := len(session.Operations) == brokerHistoricalOperationCapacity+2
			retained := true
			for key, value := range operations {
				retained = retained && session.Operations[key] == value
			}
			valid := brokerLedgerValid(b.ledger, cfg.configDigest)
			b.mu.Unlock()
			if !bounded || !retained || !valid {
				t.Fatal("cleanup capacity lost idempotency records or invalidated the bounded ledger")
			}
			b.close()
			server.Close()
			_, server = startBrokerTest(t, cfg)
			reopened := brokerTestControl(t, server.URL, brokerRetirePath, c)
			if !reopened.RetirementProven || reopened.ProofDigest != proof.ProofDigest {
				t.Fatal("saturated retired owner did not reopen with the same proof")
			}
			creates, inferences, stops, deletes := f.counts()
			if creates != 1 || inferences != 1 || stops != 0 || deletes != 1 {
				t.Fatal("capacity recovery replayed work or repeated remote deletion")
			}
		})
	}
}

func TestBrokerOperationCapacityRetainsUnknownCreation(t *testing.T) {
	f := newBrokerFixture(t, "late-create")
	cfg := brokerTestConfig(t, f)
	b, server := startBrokerTest(t, cfg)
	c := brokerTestContext(cfg)
	c.LeaseExpiresAt = time.Now().Add(2 * time.Minute).UTC().Format(time.RFC3339Nano)
	_ = brokerTestControl(t, server.URL, brokerRenewPath, c)
	renewed, _ := brokerFillHistoricalOperationCapacity(t, b, c, brokerHistoricalOperationCapacity-1)
	c.LeaseGeneration = renewed.LeaseGeneration
	c.LeaseExpiresAt = renewed.LeaseExpiresAt
	done := brokerAsyncInference(context.Background(), server.URL, c, brokerTestBody(""))
	select {
	case <-f.started:
	case <-time.After(3 * time.Second):
		t.Fatal("last ordinary operation did not attempt the original creation")
	}
	_ = brokerWaitInference(t, done)
	retirement, body := brokerTestControlContext(brokerRetirePath, c)
	status, data, err := brokerTestHTTP(context.Background(), server.URL, brokerRetirePath, retirement, body)
	var proof brokerControlResponse
	if err != nil || status != http.StatusConflict || json.Unmarshal(data, &proof) != nil ||
		!proof.CreatePending || proof.RetirementProven || proof.SettlementProven || proof.ProofDigest != "" {
		t.Fatalf("saturated unknown creation did not retain pending retirement: status=%d", status)
	}
	creates, inferences, _, deletes := f.counts()
	if creates != 1 || inferences != 0 || deletes != 0 {
		t.Fatal("capacity handling replayed or deleted an unknown creation")
	}
	f.unblock()
	brokerAwait(t, func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		return len(f.sessions) == 1
	})
	// Reserved cleanup capacity cannot manufacture the original CREATE ack.
	brokerPendingControl(t, server.URL, brokerRetirePath, c, true, 0)
	creates, inferences, _, deletes = f.counts()
	if creates != 1 || inferences != 0 || deletes != 0 {
		t.Fatal("saturated unacknowledged creation was replayed or retired")
	}
}

func TestBrokerOperationCapacityCancelsAcknowledgedInvocation(t *testing.T) {
	f := newBrokerFixture(t, "hold-known")
	cfg := brokerTestConfig(t, f)
	b, server := startBrokerTest(t, cfg)
	c := brokerTestContext(cfg)
	c.LeaseExpiresAt = time.Now().Add(2 * time.Minute).UTC().Format(time.RFC3339Nano)
	done := brokerAsyncInference(context.Background(), server.URL, c, brokerTestBody(""))
	brokerAwait(t, func() bool { return brokerInvocationState(b, c) == "accepted" })
	_, _ = brokerFillHistoricalOperationCapacity(t, b, c, brokerHistoricalOperationCapacity)
	settlement, body := brokerTestControlContext(brokerSettlePath, c)
	status, _, err := brokerTestHTTP(context.Background(), server.URL, brokerSettlePath, settlement, body)
	if err != nil || (status != http.StatusOK && status != http.StatusConflict) {
		t.Fatal("saturated owner rejected cancellation of an active invocation")
	}
	result := brokerWaitInference(t, done)
	if result.err == nil && result.status == http.StatusOK {
		t.Fatal("cancelled active invocation exposed a terminal result")
	}
	var firstProof string
	for i := range 3 {
		proof := brokerTestControl(t, server.URL, brokerSettlePath, c)
		if !proof.SettlementProven || proof.ActiveInvocations != 0 || proof.AmbiguousInvocations != 0 {
			t.Fatal("repeated cancellation lost its settlement proof")
		}
		if i == 0 {
			firstProof = proof.ProofDigest
		} else if proof.ProofDigest != firstProof {
			t.Fatal("repeated cancellation replaced its durable settlement proof")
		}
		extra := settlement
		extra.OperationID = fmt.Sprintf("extra-cancellation-%d", i)
		status, _, err := brokerTestHTTP(context.Background(), server.URL, brokerSettlePath, extra, body)
		if err != nil || status != http.StatusServiceUnavailable {
			t.Fatal("distinct repeated cancellation consumed retirement capacity")
		}
	}
	proof := brokerTestControl(t, server.URL, brokerRetirePath, c)
	creates, inferences, stops, deletes := f.counts()
	if !proof.RetirementProven || creates != 1 || inferences != 1 || stops != 1 || deletes != 1 {
		t.Fatal("saturated acknowledged invocation lost containment or retirement ownership")
	}
	b.mu.Lock()
	session := b.ledger.Sessions[brokerJSONDigest(c.Owner)]
	valid := len(session.Operations) == brokerHistoricalOperationCapacity+2 && brokerLedgerValid(b.ledger, cfg.configDigest)
	b.mu.Unlock()
	if !valid {
		t.Fatal("repeated cancellation grew or invalidated the durable ledger")
	}
}

func TestBrokerOperationCapacityRejectsOversizedLedger(t *testing.T) {
	cfg := brokerConfiguration{configDigest: brokerSHA([]byte("bounded-capacity-fixture")), stateDir: filepath.Join(t.TempDir(), "broker")}
	c := brokerTestContext(cfg)
	store, ledger, err := openBrokerStore(cfg.stateDir, cfg.configDigest)
	if err != nil {
		t.Fatal("could not create bounded ledger fixture")
	}
	session := &brokerSession{Owner: c.Owner, CreateState: "none", Prompts: map[string]*brokerPrompt{},
		Responses: map[string]brokerResponseID{}, Operations: map[string]string{}}
	ledger.Sessions[brokerJSONDigest(c.Owner)] = session
	for i := range brokerHistoricalOperationCapacity + 2 {
		session.Operations[fmt.Sprintf("historical-operation-%d", i)] = brokerSHA([]byte(fmt.Sprintf("operation-%d", i)))
	}
	err = store.save(ledger)
	store.close()
	if err != nil {
		t.Fatal("could not save maximum ledger fixture")
	}
	store, ledger, err = openBrokerStore(cfg.stateDir, cfg.configDigest)
	if err != nil {
		t.Fatal("recovery rejected the maximum operation count")
	}
	ledger.Sessions[brokerJSONDigest(c.Owner)].Operations["over-capacity"] = brokerSHA([]byte("extra-operation"))
	err = store.save(ledger)
	store.close()
	if err != nil {
		t.Fatal("could not save oversized ledger fixture")
	}
	store, _, err = openBrokerStore(cfg.stateDir, cfg.configDigest)
	if err == nil {
		store.close()
		t.Fatal("recovery accepted an oversized operation ledger")
	}
}
