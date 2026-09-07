package main

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"
)

func TestBrokerLostCreateAckCannotRetireFromLaterGET(t *testing.T) {
	f := newBrokerFixture(t, "hold-create")
	defer f.unblock()
	cfg := brokerTestConfig(t, f)
	b, server := startBrokerTest(t, cfg)
	c := brokerTestContext(cfg)
	done := brokerAsyncInference(context.Background(), server.URL, c, brokerTestBody(""))
	select {
	case <-f.started:
	case <-time.After(3 * time.Second):
		t.Fatal("original CREATE did not reach its held acknowledgement")
	}
	result := brokerWaitInference(t, done)
	if result.err != nil || result.status != http.StatusConflict {
		t.Fatalf("original CREATE timeout HTTP = %d, want ambiguous 409", result.status)
	}
	brokerAwait(t, func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		return len(b.workers) == 0
	})
	retire, body := brokerTestControlContext(brokerRetirePath, c)
	status, _, err := brokerTestHTTP(context.Background(), server.URL, brokerRetirePath, retire, body)
	if err != nil || status != http.StatusOK && status != http.StatusConflict {
		t.Fatal("original retirement request failed unexpectedly")
	}
	brokerAwait(t, func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		return len(b.workers) == 0
	})
	statusContext, body := brokerTestControlContext(brokerStatusPath, c)
	status, data, err := brokerTestHTTP(context.Background(), server.URL, brokerStatusPath, statusContext, body)
	var proof brokerControlResponse
	if err != nil || status != http.StatusOK || json.Unmarshal(data, &proof) != nil {
		t.Fatal("original owner status is unreadable")
	}
	select {
	case <-f.release:
		t.Fatal("original CREATE acknowledgement was unexpectedly released")
	default:
	}
	creates, inference, stops, deletes := f.counts()
	if creates != 1 || inference != 0 {
		t.Fatal("reproducer replayed CREATE or sent inference")
	}
	t.Logf("originalCREATEHeld=true creates=%d inference=%d stops=%d deletes=%d createPending=%v retired=%v", creates, inference, stops, deletes, proof.CreatePending, proof.RetirementProven)
	if !proof.CreatePending || proof.SettlementProven || proof.RetirementProven || proof.ProofDigest != "" || deletes != 0 {
		t.Error("later GET authorized retirement before the original CREATE acknowledgement")
	}
}
