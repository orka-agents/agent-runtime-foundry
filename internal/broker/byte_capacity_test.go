package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orka-agents/agent-runtime-foundry/internal/brokerapi"
	"github.com/orka-agents/agent-runtime-foundry/internal/foundry"
)

// Retained, valid synthetic history places the small public requests below at
// the byte boundary. The independent real-admission reproduction uses provider
// call maps; this fixture avoids replaying megabytes of historical responses in
// every capacity regression. It never removes an operation or an owner.
func brokerCapacityFillHistory(t *testing.T, b *lifecycleBroker, c brokerContext, target int, ordinary bool) {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	err := b.commitCapacityLocked(ordinary, func(next *brokerLedger) error {
		owner := c.Owner
		owner.RuntimeSessionUID = "capacity-retained-history"
		key := foundry.JSONDigest(owner)
		if next.Sessions[key] != nil {
			return errBrokerConflict
		}
		history := &brokerSession{Owner: owner, CreateState: "none", Retiring: true, Retired: true,
			ProofDigest: foundry.Digest([]byte("synthetic-retained-proof")), Prompts: map[string]*brokerPrompt{},
			Responses: map[string]brokerResponseID{}, Operations: map[string]string{}}
		next.Sessions[key] = history
		data, err := json.Marshal(next)
		if err != nil {
			return err
		}
		size := len(data)
		digest := foundry.Digest([]byte("synthetic-retained-operation"))
		for i := 0; i < brokerOperationLimit; i++ {
			prefix := fmt.Sprintf("history-%05d-", i)
			id := prefix + strings.Repeat("<", 512-len(prefix))
			encoded, _ := json.Marshal(id)
			growth := len(encoded) + len(digest) + 4 // value quotes, colon, comma
			if len(history.Operations) == 0 {
				growth--
			}
			if size+growth > target {
				break
			}
			history.Operations[id] = digest
			size += growth
		}
		// Fit the last entry with a mix of six-byte HTML escapes and ASCII.
		// A few spare bytes are harmless; no identifier bound is weakened.
		prefix := "capacity-tail-"
		encodedLength := target - size - len(digest) - 6
		if len(history.Operations) == 0 {
			encodedLength++
		}
		for encodedLength >= len(prefix) {
			rest := encodedLength - len(prefix)
			id := prefix + strings.Repeat("<", rest/6) + strings.Repeat("x", rest%6)
			if len(id) <= 512 {
				history.Operations[id] = digest
				break
			}
			encodedLength--
		}
		data, err = json.Marshal(next)
		if err != nil || !brokerLedgerValid(next, next.ConfigDigest) || len(data) > target || target-len(data) > 128 {
			return errBrokerInvalid
		}
		return nil
	})
	if err != nil {
		t.Fatalf("could not establish the valid byte-boundary fixture: %v", err)
	}
}

func TestBrokerByteReserveCoversEscapedAcceptanceAndCleanup(t *testing.T) {
	cfg := brokerConfiguration{configDigest: foundry.Digest([]byte("capacity-bounds"))}
	c := brokerTestContext(cfg)
	c.BodySHA256 = foundry.Digest([]byte("synthetic-body"))
	c.OperationID = strings.Repeat("&", 512)
	ledger := &brokerLedger{Version: 1, ConfigDigest: cfg.configDigest, Sessions: map[string]*brokerSession{}}
	session, err := brokerEnsureSession(ledger, c)
	if err != nil {
		t.Fatal("could not establish bound owner")
	}
	if _, err := brokerRecordOperation(session, brokerapi.ResponsesPath, c); err != nil {
		t.Fatal("could not establish bound operation")
	}
	prompt, err := brokerEnsurePrompt(session, c)
	if err != nil {
		t.Fatal("could not establish bound prompt")
	}
	prompt.LastSequence = c.InvocationSequence
	invocation := &brokerInvocation{Sequence: c.InvocationSequence, OperationID: c.OperationID,
		BodyDigest: c.BodySHA256, State: "intent"}
	prompt.Invocations[c.InvocationSequence] = invocation
	before, err := json.Marshal(ledger)
	if err != nil || !brokerLedgerValid(ledger, cfg.configDigest) {
		t.Fatal("initial bound ledger is invalid")
	}
	if got := brokerLedgerReserveBytes(ledger); got != brokerOwnerReserveBytes+brokerPrincipalReserveBytes {
		t.Fatal("owner or first principal reserve is missing")
	}
	ledger.PrincipalDigest = foundry.Digest([]byte("bound-principal"))
	principal, _ := json.Marshal(ledger)
	if len(principal)-len(before) > brokerPrincipalReserveBytes {
		t.Fatal("first principal exceeds its reserve")
	}
	session.RemoteID, session.CreateState = brokerIdentityRemoteSession, "deleted"
	session.Retiring, session.Retired = true, true
	session.ProofDigest = foundry.Digest([]byte("retirement-proof"))
	prompt.Closing, prompt.Settled = true, true
	prompt.ProofDigest = foundry.Digest([]byte("settlement-proof"))
	invocation.ResponseID = strings.Repeat("<", foundry.MaxIdentifierBytes)
	invocation.ResponseAlias, invocation.State = "fr_11111111-1111-4111-8111-111111111111", "settled"
	session.Responses[invocation.ResponseAlias] = brokerResponseID{RemoteID: invocation.ResponseID, PromptKey: c.promptKey()}
	for _, path := range []string{brokerapi.SettlePath, brokerapi.RetirePath} {
		control, body := brokerTestControlContext(path, c)
		control.OperationID = strings.Repeat("<", 512)
		if path == brokerapi.RetirePath {
			control.OperationID = strings.Repeat(">", 512)
		}
		control.BodySHA256 = foundry.Digest(body)
		if _, err := brokerRecordOperation(session, path, control); err != nil {
			t.Fatal("maximum escaped cleanup operation was rejected")
		}
	}
	after, err := json.Marshal(ledger)
	if err != nil || !brokerLedgerValid(ledger, cfg.configDigest) {
		t.Fatal("maximal acceptance and cleanup ledger is invalid")
	}
	// Final proof fields dominate the few bytes by which an intermediate state
	// can be longer. Add an explicit 128-byte margin for those intermediate
	// strings and the optional LastAlias that ordinary completion already sizes.
	growth := len(after) - len(principal) + 128
	if growth > brokerOwnerReserveBytes {
		t.Fatalf("bounded owner growth=%d exceeds reserve=%d", growth, brokerOwnerReserveBytes)
	}
	if brokerLedgerReserveBytes(ledger) != 0 {
		t.Fatal("retired ownership retained an unnecessary future-growth reserve")
	}
	t.Logf("principalGrowthBytes=%d ownerGrowthWithMarginBytes=%d ownerReserveBytes=%d", len(principal)-len(before), growth, brokerOwnerReserveBytes)
}

func TestBrokerByteCapacityRejectsBeforeIOAndKeepsFailureClosed(t *testing.T) {
	b, c := newBrokerResponseIdentityFixture(t)
	brokerCapacityFillHistory(t, b, c, brokerMaxLedgerBytes-brokerOwnerReserveBytes-brokerBootReserveBytes-512, true)
	before := brokerIdentityLedgerBytes(t, b)
	path := filepath.Join(b.store.dir, "state.json")
	record, err := os.Open(path)
	if err != nil {
		t.Fatal("could not pin original ledger")
	}
	defer func() { _ = record.Close() }()
	oldInfo, _ := record.Stat()
	active, cancel := context.WithCancel(context.Background())
	defer cancel()
	b.active[foundry.JSONDigest(c.Owner)] = brokerActive{prompt: c.promptKey(), cancel: cancel}
	// A write would fail here. Capacity rejection must happen first and must
	// leave this original active request and the durable writer healthy.
	if os.Rename(b.store.dir, b.store.dir+"-hidden") != nil {
		t.Fatal("could not hide the fixture directory")
	}
	defer func() { _ = os.Rename(b.store.dir+"-hidden", b.store.dir) }()
	renewal, body := brokerTestControlContext(brokerapi.RenewPath, c)
	renewal.OperationID = strings.Repeat("<", 512)
	renewal.BodySHA256 = foundry.Digest(body)
	renewal.LeaseGeneration++
	if err := b.control(brokerapi.RenewPath, renewal); !errors.Is(err, errBrokerCapacity) {
		t.Fatalf("ordinary growth did not stop before I/O: %v", err)
	}
	err = b.commitLocked(func(next *brokerLedger) error {
		session := next.Sessions[foundry.JSONDigest(c.Owner)]
		for i := range 32 {
			id := fmt.Sprintf("extra-%02d-", i) + strings.Repeat("<", 500)
			session.Operations[id] = foundry.Digest([]byte("extra-operation"))
		}
		return nil
	})
	if !errors.Is(err, errBrokerCapacity) || b.storageError != nil || active.Err() != nil ||
		foundry.Digest(before) != foundry.JSONDigest(b.ledger) {
		t.Fatal("hard-cap refusal poisoned the writer, cancelled work, or mutated ownership")
	}
	if os.Rename(b.store.dir+"-hidden", b.store.dir) != nil {
		t.Fatal("could not restore the fixture directory")
	}
	newInfo, err := os.Stat(path)
	if err != nil || !os.SameFile(oldInfo, newInfo) || !bytes.Equal(before, brokerIdentityLedgerBytes(t, b)) {
		t.Fatal("capacity rejection rewrote the ledger")
	}
	if os.Rename(b.store.dir, b.store.dir+"-hidden") != nil {
		t.Fatal("could not inject a real storage failure")
	}
	err = b.commitLocked(func(next *brokerLedger) error {
		next.Sessions[foundry.JSONDigest(c.Owner)].Prompts[c.promptKey()].Closing = true
		return nil
	})
	if !errors.Is(err, errBrokerStorage) || !errors.Is(b.storageError, errBrokerStorage) || active.Err() != context.Canceled {
		t.Fatal("actual I/O failure did not remain fail-closed")
	}
	if foundry.Digest(before) != foundry.JSONDigest(b.ledger) {
		t.Fatal("failed I/O replaced original durable ownership")
	}
}

func brokerCapacityControlProof(t *testing.T, base, path string, c brokerContext) brokerControlResponse {
	t.Helper()
	// Race instrumentation must repeatedly encode and decode the full 32 MiB
	// history. Give that work a bounded window on slower native builders.
	deadline := time.Now().Add(90 * time.Second)
	for {
		status, data, err := brokerTestHTTP(context.Background(), base, path, c, []byte("{}"))
		var proof brokerControlResponse
		if err != nil || json.Unmarshal(data, &proof) != nil || (status != http.StatusOK && status != http.StatusConflict) {
			t.Fatalf("bounded cleanup failed at byte capacity: status=%d", status)
		}
		if status == http.StatusOK {
			return proof
		}
		if time.Now().After(deadline) {
			t.Fatal("bounded cleanup did not reach proof")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestBrokerByteCapacityConcurrentOwnersPreserveAcceptanceAndCleanup(t *testing.T) {
	responseID := strings.Repeat("<", foundry.MaxIdentifierBytes)
	f := newBrokerEvidenceFixture(t, func(request foundry.ResponseRequest) (string, []byte) {
		response := foundry.Response{ID: responseID, AgentSessionID: request.AgentSessionID, Status: "completed"}
		for i := range foundry.DefaultMaxBrokeredCalls {
			prefix := fmt.Sprintf("call-%03d-", i)
			response.Output = append(response.Output, foundry.OutputItem{ID: fmt.Sprintf("item-%03d", i), Type: "function_call",
				Name: "hosted-probe-read", CallID: prefix + strings.Repeat("x", foundry.MaxIdentifierBytes-len(prefix)), Arguments: json.RawMessage(`"{}"`)})
		}
		data, err := json.Marshal(response)
		if err != nil {
			t.Error("could not encode bounded fixture response")
		}
		return "application/json", data
	})
	started, release := make(chan struct{}, 2), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	f.createCheck = func(string) { started <- struct{}{}; <-release }
	cfg := brokerTestConfig(t, f)
	cfg.operationTimeout = 2 * time.Minute
	b, server := startBrokerTest(t, cfg)
	owners := make([]brokerContext, 2)
	results := make([]<-chan brokerHTTPResult, len(owners))
	for i := range owners {
		c := brokerTestContext(cfg)
		c.Owner.RuntimeSessionUID = fmt.Sprintf("capacity-owner-%d", i)
		c.OperationID = fmt.Sprintf("foundry-%064x", i+1)
		c.LeaseExpiresAt = time.Now().Add(4 * time.Minute).UTC().Format(time.RFC3339Nano)
		owners[i] = c
		results[i] = brokerAsyncInference(context.Background(), server.URL, c, brokerTestBody(""))
	}
	for range owners {
		select {
		case <-started:
		case <-time.After(10 * time.Second):
			t.Fatal("original creates did not reach the acknowledgement gate")
		}
	}
	b.mu.Lock()
	reserve := brokerLedgerReserveBytes(b.ledger)
	for _, c := range owners {
		if b.ledger.Sessions[foundry.JSONDigest(c.Owner)].CreateState != "intent" {
			b.mu.Unlock()
			t.Fatal("original creation intent was not durable before submission")
		}
	}
	b.mu.Unlock()
	if reserve != len(owners)*brokerOwnerReserveBytes+brokerBootReserveBytes {
		t.Fatal("concurrent ownership was not fully reserved")
	}
	brokerCapacityFillHistory(t, b, owners[0], brokerMaxLedgerBytes-reserve-128, true)
	unblock()
	for _, result := range results {
		select {
		case response := <-result:
			if response.err != nil || response.status != http.StatusServiceUnavailable {
				t.Fatal("oversized completion mapping was exposed or original acceptance failed")
			}
		case <-time.After(2 * time.Minute):
			t.Fatal("bounded inference did not return after capacity refusal")
		}
	}
	b.mu.Lock()
	valid, healthy := brokerLedgerValid(b.ledger, cfg.configDigest), b.storageError == nil
	for _, c := range owners {
		session := b.ledger.Sessions[foundry.JSONDigest(c.Owner)]
		invocation := session.Prompts[c.promptKey()].Invocations[c.InvocationSequence]
		link := session.Responses[invocation.ResponseAlias]
		valid = valid && session.CreateState == "known" && invocation.ResponseID == responseID &&
			link.RemoteID == responseID && !link.Completed && len(link.CallIDs) == 0
	}
	b.mu.Unlock()
	if !valid || !healthy {
		t.Fatal("capacity withholding lost original acknowledgement or poisoned the writer")
	}
	proofs := make([]string, len(owners))
	for i, c := range owners {
		settlement, _ := brokerTestControlContext(brokerapi.SettlePath, c)
		settlement.OperationID = strings.Repeat("<", 512)
		for range 3 {
			proof := brokerCapacityControlProof(t, server.URL, brokerapi.SettlePath, settlement)
			if !proof.SettlementProven || proof.ActiveInvocations != 0 || proof.AmbiguousInvocations != 0 {
				t.Fatal("exact settlement retries consumed another owner's cleanup space")
			}
		}
		retirement, _ := brokerTestControlContext(brokerapi.RetirePath, c)
		retirement.OperationID = strings.Repeat(">", 512)
		proof := brokerCapacityControlProof(t, server.URL, brokerapi.RetirePath, retirement)
		if !proof.RetirementProven {
			t.Fatal("owner could not persist exact retirement proof")
		}
		proofs[i] = proof.ProofDigest
	}
	before := brokerIdentityLedgerBytes(t, b)
	b.close()
	server.Close()
	reopened, restarted := startBrokerTest(t, cfg)
	for i, c := range owners {
		retirement, _ := brokerTestControlContext(brokerapi.RetirePath, c)
		retirement.OperationID = strings.Repeat(">", 512)
		proof := brokerCapacityControlProof(t, restarted.URL, brokerapi.RetirePath, retirement)
		if !proof.RetirementProven || proof.ProofDigest != proofs[i] {
			t.Fatal("restart lost the original retirement receipt")
		}
		status, _, err := brokerTestHTTP(context.Background(), restarted.URL, brokerapi.ResponsesPath, c, brokerTestBody(""))
		if err != nil || status != http.StatusGone {
			t.Fatal("restart admitted inference on retired ownership")
		}
	}
	if !bytes.Equal(before, brokerIdentityLedgerBytes(t, reopened)) {
		t.Fatal("exact retry or rejected replay changed retained capacity history")
	}
	creates, inferences, stops, deletes := f.counts()
	if creates != len(owners) || inferences != len(owners) || stops != len(owners) || deletes != len(owners) {
		t.Fatal("capacity recovery replayed a create, inference or cleanup")
	}
	t.Logf("owners=%d reservedBytes=%d ledgerBytes=%d accepted=%d withheld=%d settled=%d retired=%d replayed=0", len(owners), reserve, len(before), inferences, inferences, stops, deletes)
}

func TestBrokerByteCapacityLegacyLedgerStillRecovers(t *testing.T) {
	f := newBrokerFixture(t, "success")
	cfg := brokerTestConfig(t, f)
	b, server := startBrokerTest(t, cfg)
	c := brokerTestContext(cfg)
	c.LeaseExpiresAt = time.Now().Add(4 * time.Minute).UTC().Format(time.RFC3339Nano)
	_ = brokerTestControl(t, server.URL, brokerapi.RenewPath, c)
	_ = brokerTestControl(t, server.URL, brokerapi.SettlePath, c)
	// An old valid ledger need not contain the newly required admission reserve.
	// Preserve it without erasure or newly fabricated ownership on reopen.
	brokerCapacityFillHistory(t, b, c, brokerMaxLedgerBytes-8192, false)
	before := brokerIdentityLedgerBytes(t, b)
	b.close()
	server.Close()
	reopened, restarted := startBrokerTest(t, cfg)
	if !bytes.Equal(before, brokerIdentityLedgerBytes(t, reopened)) {
		t.Fatal("legacy recovery rewrote existing ownership")
	}
	status, _, err := brokerTestHTTP(context.Background(), restarted.URL, brokerapi.ResponsesPath, c, brokerTestBody(""))
	if err != nil || status != http.StatusGone {
		t.Fatal("legacy recovery replayed a closed prompt")
	}
	fresh := c
	fresh.Owner.RuntimeSessionUID = "new-owner-after-legacy-capacity"
	fresh.OperationID = "new-ordinary-operation"
	status, _, err = brokerTestHTTP(context.Background(), restarted.URL, brokerapi.ResponsesPath, fresh, brokerTestBody(""))
	if err != nil || status != http.StatusServiceUnavailable {
		t.Fatal("legacy byte-capacity state admitted unreserved ownership")
	}
	retirement, _ := brokerTestControlContext(brokerapi.RetirePath, c)
	proof := brokerCapacityControlProof(t, restarted.URL, brokerapi.RetirePath, retirement)
	if !proof.RetirementProven {
		t.Fatal("existing legacy cleanup was rejected by the new reserve rule")
	}
	reopened.mu.Lock()
	healthy := reopened.storageError == nil && reopened.ledger.Sessions[foundry.JSONDigest(fresh.Owner)] == nil
	reopened.mu.Unlock()
	creates, inferences, stops, deletes := f.counts()
	if !healthy || creates+inferences+stops+deletes != 0 {
		t.Fatal("legacy capacity refusal poisoned the writer or submitted remote work")
	}
}
