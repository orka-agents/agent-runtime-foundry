package broker

import (
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

type brokerHTTPResult struct {
	status int
	data   []byte
	err    error
}

func brokerAsyncInference(ctx context.Context, base string, c brokerContext, body []byte) <-chan brokerHTTPResult {
	done := make(chan brokerHTTPResult, 1)
	go func() {
		status, data, err := brokerTestHTTP(ctx, base, brokerapi.ResponsesPath, c, body)
		done <- brokerHTTPResult{status, data, err}
	}()
	return done
}

func brokerWaitInference(t *testing.T, done <-chan brokerHTTPResult) brokerHTTPResult {
	t.Helper()
	select {
	case result := <-done:
		return result
	case <-time.After(4 * time.Second):
		t.Fatal("inference did not settle within bound")
		return brokerHTTPResult{}
	}
}

func brokerInvocationState(b *lifecycleBroker, c brokerContext) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	if session := b.ledger.Sessions[foundry.JSONDigest(c.Owner)]; session != nil {
		if prompt := session.Prompts[c.promptKey()]; prompt != nil {
			if invocation := prompt.Invocations[c.InvocationSequence]; invocation != nil {
				return invocation.State
			}
		}
	}
	return ""
}

func brokerPendingControl(t *testing.T, base, path string, c brokerContext, pendingCreate bool, ambiguous uint32) {
	t.Helper()
	cc, body := brokerTestControlContext(path, c)
	status, data, err := brokerTestHTTP(context.Background(), base, path, cc, body)
	var proof brokerControlResponse
	if err != nil || status != http.StatusConflict || json.Unmarshal(data, &proof) != nil ||
		proof.CreatePending != pendingCreate || proof.AmbiguousInvocations != ambiguous ||
		proof.SettlementProven || proof.RetirementProven || proof.ProofDigest != "" {
		t.Fatalf("pending operation claimed proof or lost uncertainty: status=%d", status)
	}
}

func TestBrokerAcknowledgedDisconnectExpiryAndTruncation(t *testing.T) {
	for _, mode := range []string{"disconnect", "expiry", "truncated"} {
		t.Run(mode, func(t *testing.T) {
			fixtureMode := "hold-known"
			if mode == "truncated" {
				fixtureMode = mode
			}
			f := newBrokerFixture(t, fixtureMode)
			cfg := brokerTestConfig(t, f)
			b, server := startBrokerTest(t, cfg)
			c := brokerTestContext(cfg)
			if mode == "expiry" {
				c.LeaseExpiresAt = time.Now().Add(700 * time.Millisecond).UTC().Format(time.RFC3339Nano)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := brokerAsyncInference(ctx, server.URL, c, brokerTestBody(""))
			brokerAwait(t, func() bool {
				state := brokerInvocationState(b, c)
				return state == "accepted" || state == "settled"
			})
			if mode == "disconnect" {
				cancel()
			}
			result := brokerWaitInference(t, done)
			if result.err == nil && result.status == http.StatusOK {
				t.Fatal("interrupted inference exposed a terminal result")
			}
			proof := brokerTestControl(t, server.URL, brokerapi.SettlePath, c)
			if !proof.SettlementProven || proof.ActiveInvocations != 0 || proof.AmbiguousInvocations != 0 {
				t.Fatal("acknowledged interrupted response did not settle")
			}
			creates, inferences, stops, deletes := f.counts()
			if creates != 1 || inferences != 1 || stops < 1 || deletes != 0 {
				t.Fatal("disconnect/expiry cleanup skipped stop or replayed inference")
			}
			_ = brokerTestControl(t, server.URL, brokerapi.RetirePath, c)
		})
	}
}

func TestBrokerDelayedCreateRetainsOwnershipThrough404(t *testing.T) {
	f := newBrokerFixture(t, "late-create")
	cfg := brokerTestConfig(t, f)
	b, server := startBrokerTest(t, cfg)
	c := brokerTestContext(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := brokerAsyncInference(ctx, server.URL, c, brokerTestBody(""))
	select {
	case <-f.started:
	case <-time.After(3 * time.Second):
		t.Fatal("create did not start")
	}
	cancel()
	_ = brokerWaitInference(t, done)
	brokerAwait(t, func() bool { return brokerInvocationState(b, c) == "rejected" })
	for range 3 {
		brokerPendingControl(t, server.URL, brokerapi.RetirePath, c, true, 0)
	}
	creates, inferences, _, deletes := f.counts()
	if creates != 1 || inferences != 0 || deletes != 0 {
		t.Fatal("unknown creation retried or deleted without acceptance proof")
	}
	// Even a broker restart must retain the original ID and its pending intent.
	b.close()
	server.Close()
	_, server = startBrokerTest(t, cfg)
	brokerPendingControl(t, server.URL, brokerapi.RetirePath, c, true, 0)
	f.unblock()
	brokerAwait(t, func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		return len(f.sessions) == 1
	})
	// The object appearing after a lost acknowledgement does not establish
	// completion of the original CREATE, including after a broker restart.
	brokerPendingControl(t, server.URL, brokerapi.RetirePath, c, true, 0)
	creates, inferences, _, deletes = f.counts()
	if creates != 1 || inferences != 0 || deletes != 0 {
		t.Fatal("same-intent recovery replayed work or deleted an unacknowledged owner")
	}
}

func TestBrokerCancellationWaitsForCreateAcknowledgement(t *testing.T) {
	f := newBrokerFixture(t, "hold-create")
	cfg := brokerTestConfig(t, f)
	providerClient := newBrokerHTTPClient()
	transport := providerClient.Transport
	creationContexts := make(chan context.Context, 1)
	providerClient.Transport = brokerFixtureTransport(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodPost && r.URL.Path == "/api/projects/fixture/agents/fixture/endpoint/sessions" {
			select {
			case creationContexts <- r.Context():
			default:
				t.Error("session creation was replayed")
			}
		}
		return transport.RoundTrip(r)
	})
	_, server := startBrokerTestWithClient(t, cfg, providerClient)
	c := brokerTestContext(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := brokerAsyncInference(ctx, server.URL, c, brokerTestBody(""))
	select {
	case <-f.started:
	case <-time.After(3 * time.Second):
		t.Fatal("creation acknowledgement was not held")
	}
	createCtx := <-creationContexts
	cancel()
	if result := brokerWaitInference(t, done); result.err == nil {
		t.Fatal("cancelled caller received a response")
	}
	// This public close request synchronously cancels the prompt context while
	// the fixture still holds the original creation acknowledgement.
	brokerPendingControl(t, server.URL, brokerapi.SettlePath, c, true, 0)
	if createCtx.Err() != nil {
		t.Fatal("prompt cancellation aborted durable session creation")
	}
	if deadline, ok := createCtx.Deadline(); !ok || time.Until(deadline) > cfg.operationTimeout {
		t.Fatal("session creation lacks the broker's operation bound")
	}
	f.unblock()
	proof := brokerTestControl(t, server.URL, brokerapi.SettlePath, c)
	if !proof.SettlementProven || !proof.RemoteSessionCreated || proof.CreatePending ||
		proof.ActiveInvocations != 0 || proof.AmbiguousInvocations != 0 {
		t.Fatal("acknowledged creation did not settle the cancelled prompt")
	}
	data, err := os.ReadFile(filepath.Join(cfg.stateDir, "state.json"))
	var ledger brokerLedger
	if err != nil || json.Unmarshal(data, &ledger) != nil || !brokerLedgerValid(&ledger, cfg.configDigest) {
		t.Fatal("settlement did not preserve a valid durable ledger")
	}
	owned := ledger.Sessions[foundry.JSONDigest(c.Owner)]
	if owned == nil || owned.CreateState != "known" || owned.RemoteID == "" || !owned.Prompts[c.promptKey()].Settled {
		t.Fatal("positive creation acknowledgement was not durably retained")
	}
	proof = brokerTestControl(t, server.URL, brokerapi.RetirePath, c)
	if !proof.RetirementProven || !foundry.DigestValid(proof.ProofDigest) {
		t.Fatal("cancelled prompt's acknowledged session did not retire")
	}
	creates, inferences, stops, deletes := f.counts()
	if creates != 1 || inferences != 0 || stops != 0 || deletes != 1 {
		t.Fatalf("cancelled creation replayed work or skipped cleanup: %d %d %d %d", creates, inferences, stops, deletes)
	}
}

func TestBrokerCreationAdmissionRejectionVersusUnknownFailure(t *testing.T) {
	for _, mode := range []string{"create-rejected", "create-rejected-server", "create-rejected-conflict", "create-rejected-truncated", "create-rejected-oversized"} {
		t.Run(mode, func(t *testing.T) {
			f := newBrokerFixture(t, mode)
			cfg := brokerTestConfig(t, f)
			_, server := startBrokerTest(t, cfg)
			c := brokerTestContext(cfg)
			status, _, err := brokerTestHTTP(context.Background(), server.URL, brokerapi.ResponsesPath, c, brokerTestBody(""))
			if err != nil || status == http.StatusOK {
				t.Fatal("failed creation exposed an inference result")
			}
			if mode == "create-rejected" {
				proof := brokerTestControl(t, server.URL, brokerapi.SettlePath, c)
				if !proof.SettlementProven || proof.RemoteSessionCreated || proof.CreatePending {
					t.Fatal("complete creation rejection did not prove no remote session")
				}
				proof = brokerTestControl(t, server.URL, brokerapi.RetirePath, c)
				if !proof.RetirementProven || proof.RemoteSessionCreated {
					t.Fatal("rejected creation could not retire without a remote session")
				}
			} else {
				brokerPendingControl(t, server.URL, brokerapi.RetirePath, c, true, 0)
			}
			creates, inferences, stops, deletes := f.counts()
			if creates != 1 || inferences != 0 || stops != 0 || deletes != 0 {
				t.Fatal("creation failure was replayed or deleted without ownership proof")
			}
		})
	}
}

func TestBrokerRestartSettlesOnlyAcknowledgedInference(t *testing.T) {
	for _, acknowledged := range []bool{true, false} {
		name, mode := "unknown", "hold-unknown"
		if acknowledged {
			name, mode = "acknowledged", "hold-known"
		}
		t.Run(name, func(t *testing.T) {
			f := newBrokerFixture(t, mode)
			cfg := brokerTestConfig(t, f)
			b, server := startBrokerTest(t, cfg)
			c := brokerTestContext(cfg)
			done := brokerAsyncInference(context.Background(), server.URL, c, brokerTestBody(""))
			if acknowledged {
				brokerAwait(t, func() bool { return brokerInvocationState(b, c) == "accepted" })
			} else {
				select {
				case <-f.started:
				case <-time.After(3 * time.Second):
					t.Fatal("inference did not start")
				}
			}
			b.close()
			_ = brokerWaitInference(t, done)
			server.Close()
			_, server = startBrokerTest(t, cfg)
			if acknowledged {
				proof := brokerTestControl(t, server.URL, brokerapi.SettlePath, c)
				if !proof.SettlementProven {
					t.Fatal("original acknowledged owner did not settle after restart")
				}
				_ = brokerTestControl(t, server.URL, brokerapi.RetirePath, c)
			} else {
				for range 3 {
					brokerPendingControl(t, server.URL, brokerapi.SettlePath, c, false, 1)
					brokerPendingControl(t, server.URL, brokerapi.RetirePath, c, false, 1)
				}
			}
			creates, inferences, _, deletes := f.counts()
			if creates != 1 || inferences != 1 || (!acknowledged && deletes != 0) || (acknowledged && deletes != 1) {
				t.Fatal("restart replayed inference or fabricated deletion proof")
			}
		})
	}
}

func TestBrokerRenewalExtendsActiveRequestAndOldLeaseCanClose(t *testing.T) {
	f := newBrokerFixture(t, "hold-known")
	cfg := brokerTestConfig(t, f)
	b, server := startBrokerTest(t, cfg)
	c := brokerTestContext(cfg)
	originalExpiry := time.Now().Add(800 * time.Millisecond)
	c.LeaseExpiresAt = originalExpiry.UTC().Format(time.RFC3339Nano)
	done := brokerAsyncInference(context.Background(), server.URL, c, brokerTestBody(""))
	brokerAwait(t, func() bool { return brokerInvocationState(b, c) == "accepted" })
	renewal := c
	renewal.LeaseGeneration = 2
	renewal.LeaseExpiresAt = time.Now().Add(3 * time.Second).UTC().Format(time.RFC3339Nano)
	proof := brokerTestControl(t, server.URL, brokerapi.RenewPath, renewal)
	if proof.LeaseGeneration != 2 || proof.State != "open" {
		t.Fatal("renewal did not bind the active request")
	}
	time.Sleep(time.Until(originalExpiry.Add(150 * time.Millisecond)))
	_, inferences, stops, _ := f.counts()
	if inferences != 1 || stops != 0 || brokerInvocationState(b, c) != "accepted" {
		t.Fatal("old expiry canceled a renewed request")
	}
	proof = brokerTestControl(t, server.URL, brokerapi.SettlePath, c)
	if !proof.SettlementProven || proof.LeaseGeneration != 2 {
		t.Fatal("old lease could not close exact prompt authority")
	}
	result := brokerWaitInference(t, done)
	if result.status == http.StatusOK {
		t.Fatal("settled cancellation exposed output")
	}
	_ = brokerTestControl(t, server.URL, brokerapi.RetirePath, c)
}

func TestBrokerKnownAbsentSessionStillRequiresDeleteAcknowledgement(t *testing.T) {
	f := newBrokerFixture(t, "success")
	cfg := brokerTestConfig(t, f)
	_, server := startBrokerTest(t, cfg)
	c := brokerTestContext(cfg)
	status, _, err := brokerTestHTTP(context.Background(), server.URL, brokerapi.ResponsesPath, c, brokerTestBody(""))
	if err != nil || status != http.StatusOK {
		t.Fatal("initial request failed")
	}
	// Model DELETE succeeding immediately before a broker loses its response.
	f.mu.Lock()
	for id := range f.sessions {
		delete(f.sessions, id)
	}
	f.mu.Unlock()
	proof := brokerTestControl(t, server.URL, brokerapi.RetirePath, c)
	_, _, _, deletes := f.counts()
	if !proof.RetirementProven || deletes != 1 {
		t.Fatal("known missing target did not obtain DELETE204 + GET404 proof")
	}
}

func TestBrokerCompleteAdmissionRejectionVersusUnknownFailure(t *testing.T) {
	for _, mode := range []string{"rejected", "rejected-server", "rejected-truncated", "rejected-oversized"} {
		t.Run(mode, func(t *testing.T) {
			f := newBrokerFixture(t, mode)
			cfg := brokerTestConfig(t, f)
			_, server := startBrokerTest(t, cfg)
			c := brokerTestContext(cfg)
			status, _, err := brokerTestHTTP(context.Background(), server.URL, brokerapi.ResponsesPath, c, brokerTestBody(""))
			if err != nil || status == http.StatusOK {
				t.Fatal("provider rejection exposed a result")
			}
			if mode == "rejected" {
				_ = brokerTestControl(t, server.URL, brokerapi.SettlePath, c)
				_ = brokerTestControl(t, server.URL, brokerapi.RetirePath, c)
			} else {
				brokerPendingControl(t, server.URL, brokerapi.RetirePath, c, false, 1)
			}
			creates, inferences, _, deletes := f.counts()
			if creates != 1 || inferences != 1 || (mode != "rejected" && deletes != 0) {
				t.Fatal("uncertain failure was replayed or retired")
			}
		})
	}
}
