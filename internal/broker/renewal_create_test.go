package broker

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/orka-agents/agent-runtime-foundry/internal/brokerapi"
	"github.com/orka-agents/agent-runtime-foundry/internal/foundry"
)

func requirePendingCreateRenewal(t *testing.T, proof brokerControlResponse, renewal brokerContext) {
	t.Helper()
	expires, err := time.Parse(time.RFC3339Nano, proof.LeaseExpiresAt)
	wantExpires, _ := time.Parse(time.RFC3339Nano, renewal.LeaseExpiresAt)
	if err != nil || proof.State != "open" || proof.LeaseGeneration != renewal.LeaseGeneration || !expires.Equal(wantExpires) ||
		!proof.CreatePending || proof.ActiveInvocations != 1 || proof.AmbiguousInvocations != 0 ||
		proof.SettlementProven || proof.RetirementProven || proof.RemoteSessionCreated || proof.ProofDigest != "" {
		t.Fatalf("live creation renewal lacks exact acknowledgement: state=%s generation=%d pending=%v active=%d ambiguous=%d",
			proof.State, proof.LeaseGeneration, proof.CreatePending, proof.ActiveInvocations, proof.AmbiguousInvocations)
	}
}

func requireBlockedRenewalReplay(t *testing.T, base string, renewal brokerContext, pending bool, ambiguous uint32) {
	t.Helper()
	proof := brokerTestControl(t, base, brokerapi.RenewPath, renewal)
	if proof.State != "blocked" || proof.CreatePending != pending || proof.AmbiguousInvocations != ambiguous ||
		proof.SettlementProven || proof.RetirementProven || proof.ProofDigest != "" {
		t.Fatal("renewal replay reopened unresolved ownership")
	}
}

func TestBrokerRenewalDuringPendingCreateSurvivesOriginalExpiry(t *testing.T) {
	f := newBrokerFixture(t, "hold-create")
	defer f.unblock()
	cfg := brokerTestConfig(t, f)
	cfg.operationTimeout = 5 * time.Second
	b, server := startBrokerTest(t, cfg)
	c := brokerTestContext(cfg)
	originalExpiry := time.Now().Add(time.Second)
	c.LeaseExpiresAt = originalExpiry.UTC().Format(time.RFC3339Nano)
	done := brokerAsyncInference(context.Background(), server.URL, c, brokerTestBody(""))
	select {
	case <-f.started:
	case <-time.After(3 * time.Second):
		t.Fatal("original session creation did not reach the held acknowledgement")
	}
	renewal := c
	renewal.LeaseGeneration = 2
	renewal.LeaseExpiresAt = originalExpiry.Add(3 * time.Second).UTC().Format(time.RFC3339Nano)
	proof := brokerTestControl(t, server.URL, brokerapi.RenewPath, renewal)
	requirePendingCreateRenewal(t, proof, renewal)
	// Only this exact idempotent control may be repeated. The original create
	// and inference each retain their one attempt.
	requirePendingCreateRenewal(t, brokerTestControl(t, server.URL, brokerapi.RenewPath, renewal), renewal)
	status := brokerTestControl(t, server.URL, brokerapi.StatusPath, c)
	if status.State != "blocked" || !status.CreatePending || status.SettlementProven || status.RetirementProven {
		t.Fatal("renewal changed ordinary pending-creation status")
	}
	time.Sleep(time.Until(originalExpiry.Add(150 * time.Millisecond)))
	b.mu.Lock()
	prompt := b.ledger.Sessions[foundry.JSONDigest(c.Owner)].Prompts[c.promptKey()]
	active := !prompt.Closing && !prompt.Settled && prompt.LeaseGeneration == 2 && prompt.LeaseExpiresAt.After(time.Now())
	b.mu.Unlock()
	if !active {
		t.Fatal("the original expiry closed the renewed creation")
	}
	creates, inferences, stops, deletes := f.counts()
	if creates != 1 || inferences != 0 || stops != 0 || deletes != 0 {
		t.Fatal("renewal replayed creation or started inference before creation acknowledgement")
	}
	f.unblock()
	result := brokerWaitInference(t, done)
	if result.err != nil || result.status != http.StatusOK {
		t.Fatal("the original invocation failed after its renewed creation completed")
	}
	proof = brokerTestControl(t, server.URL, brokerapi.SettlePath, c)
	if !proof.SettlementProven || proof.CreatePending || proof.LeaseGeneration != 2 {
		t.Fatal("the original prompt did not settle under its renewed lease")
	}
	proof = brokerTestControl(t, server.URL, brokerapi.RetirePath, c)
	creates, inferences, stops, deletes = f.counts()
	if !proof.RetirementProven || creates != 1 || inferences != 1 || stops != 0 || deletes != 1 {
		t.Fatal("renewal changed exact-attempt execution or retirement")
	}
}

func TestBrokerRenewalDuringPendingCreateCannotReopenClosing(t *testing.T) {
	for _, path := range []string{brokerapi.SettlePath, brokerapi.RetirePath} {
		t.Run(path, func(t *testing.T) {
			f := newBrokerFixture(t, "hold-create")
			defer f.unblock()
			cfg := brokerTestConfig(t, f)
			_, server := startBrokerTest(t, cfg)
			c := brokerTestContext(cfg)
			done := brokerAsyncInference(context.Background(), server.URL, c, brokerTestBody(""))
			select {
			case <-f.started:
			case <-time.After(3 * time.Second):
				t.Fatal("creation acknowledgement was not held")
			}
			renewal := c
			renewal.LeaseGeneration = 2
			renewal.LeaseExpiresAt = time.Now().Add(15 * time.Second).UTC().Format(time.RFC3339Nano)
			requirePendingCreateRenewal(t, brokerTestControl(t, server.URL, brokerapi.RenewPath, renewal), renewal)
			brokerPendingControl(t, server.URL, path, c, true, 0)
			if path == brokerapi.SettlePath {
				requireBlockedRenewalReplay(t, server.URL, renewal, true, 0)
			} else {
				cc, body := brokerTestControlContext(brokerapi.RenewPath, renewal)
				status, _, err := brokerTestHTTP(context.Background(), server.URL, brokerapi.RenewPath, cc, body)
				if err != nil || status != http.StatusGone {
					t.Fatal("retiring owner accepted a renewal replay")
				}
			}
			f.unblock()
			result := brokerWaitInference(t, done)
			if result.err == nil && result.status == http.StatusOK {
				t.Fatal("closed creation proceeded to inference")
			}
			proof := brokerTestControl(t, server.URL, brokerapi.RetirePath, c)
			creates, inferences, stops, deletes := f.counts()
			if !proof.RetirementProven || creates != 1 || inferences != 0 || stops != 0 || deletes != 1 {
				t.Fatal("pending-creation cleanup lost ownership or replayed work")
			}
		})
	}
}

func TestBrokerRenewalDuringPendingCreateCannotReopenLostAcknowledgement(t *testing.T) {
	f := newBrokerFixture(t, "late-create")
	defer f.unblock()
	createEntered, releaseCreate := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(releaseCreate) })
	defer unblock()
	f.createCheck = func(string) {
		close(createEntered)
		<-releaseCreate
	}
	cfg := brokerTestConfig(t, f)
	b, server := startBrokerTest(t, cfg)
	c := brokerTestContext(cfg)
	done := brokerAsyncInference(context.Background(), server.URL, c, brokerTestBody(""))
	select {
	case <-createEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("creation did not enter the original attempt")
	}
	renewal := c
	renewal.LeaseGeneration = 2
	renewal.LeaseExpiresAt = time.Now().Add(15 * time.Second).UTC().Format(time.RFC3339Nano)
	requirePendingCreateRenewal(t, brokerTestControl(t, server.URL, brokerapi.RenewPath, renewal), renewal)
	unblock()
	result := brokerWaitInference(t, done)
	if result.err == nil && result.status == http.StatusOK {
		t.Fatal("lost creation acknowledgement exposed inference output")
	}
	brokerAwait(t, func() bool { return brokerInvocationState(b, c) == "rejected" })
	requireBlockedRenewalReplay(t, server.URL, renewal, true, 0)
	b.close()
	server.Close()
	_, server = startBrokerTest(t, cfg)
	requireBlockedRenewalReplay(t, server.URL, renewal, true, 0)
	brokerPendingControl(t, server.URL, brokerapi.RetirePath, c, true, 0)
	creates, inferences, stops, deletes := f.counts()
	if creates != 1 || inferences != 0 || stops != 0 || deletes != 0 {
		t.Fatal("abandoned creation was replayed or retired from missing-session evidence")
	}
	f.unblock()
	brokerAwait(t, func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		return len(f.sessions) == 1
	})
	// Renewal and a later object cannot replace the lost original CREATE ack.
	brokerPendingControl(t, server.URL, brokerapi.RetirePath, c, true, 0)
	creates, inferences, stops, deletes = f.counts()
	if creates != 1 || inferences != 0 || stops != 0 || deletes != 0 {
		t.Fatal("late unacknowledged creation was replayed or retired")
	}
}

func TestBrokerRenewalReplayCannotClearAmbiguousInference(t *testing.T) {
	f := newBrokerFixture(t, "hold-unknown")
	defer f.unblock()
	cfg := brokerTestConfig(t, f)
	b, server := startBrokerTest(t, cfg)
	c := brokerTestContext(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := brokerAsyncInference(ctx, server.URL, c, brokerTestBody(""))
	select {
	case <-f.started:
	case <-time.After(3 * time.Second):
		t.Fatal("inference did not enter the original request")
	}
	renewal := c
	renewal.LeaseGeneration = 2
	renewal.LeaseExpiresAt = time.Now().Add(15 * time.Second).UTC().Format(time.RFC3339Nano)
	proof := brokerTestControl(t, server.URL, brokerapi.RenewPath, renewal)
	if proof.State != "open" || proof.LeaseGeneration != 2 {
		t.Fatal("live inference did not acknowledge its lease renewal")
	}
	cancel()
	_ = brokerWaitInference(t, done)
	brokerAwait(t, func() bool { return brokerInvocationState(b, c) == "uncertain" })
	requireBlockedRenewalReplay(t, server.URL, renewal, false, 1)
	brokerPendingControl(t, server.URL, brokerapi.RetirePath, c, false, 1)
	creates, inferences, _, deletes := f.counts()
	if creates != 1 || inferences != 1 || deletes != 0 {
		t.Fatal("ambiguous inference was replayed or deleted")
	}
	statusContext, body := brokerTestControlContext(brokerapi.StatusPath, c)
	status, data, err := brokerTestHTTP(context.Background(), server.URL, brokerapi.StatusPath, statusContext, body)
	if err != nil || status != http.StatusOK || json.Unmarshal(data, &proof) != nil || proof.State != "blocked" ||
		proof.SettlementProven || proof.RetirementProven {
		t.Fatal("ambiguous ownership no longer blocks status and cleanup")
	}
}
