package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orka-agents/agent-runtime-foundry/internal/brokerapi"
	"github.com/orka-agents/agent-runtime-foundry/internal/foundry"
)

func brokerRecoveryHTTP(t *testing.T, base, path, method string, body []byte, authenticated bool) (int, []byte) {
	t.Helper()
	r, err := http.NewRequest(method, base+path, bytes.NewReader(body))
	if err != nil {
		t.Fatal("could not construct broker recovery request")
	}
	if authenticated {
		r.Header.Set("Authorization", "Bearer "+brokerFixtureBearer)
	}
	response, err := http.DefaultClient.Do(r)
	if err != nil {
		t.Fatal("broker recovery request failed")
	}
	defer response.Body.Close() //nolint:errcheck
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal("could not read broker recovery response")
	}
	return response.StatusCode, data
}

func brokerReadIdentity(t *testing.T, base string) brokerIdentityResponse {
	t.Helper()
	status, data := brokerRecoveryHTTP(t, base, brokerapi.IdentityPath, http.MethodGet, nil, true)
	var identity brokerIdentityResponse
	if status != http.StatusOK || json.Unmarshal(data, &identity) != nil || identity.Protocol != brokerProtocol ||
		!foundry.DigestValid(identity.LedgerIdentityDigest) || !foundry.DigestValid(identity.AgentConfigurationDigest) {
		t.Fatalf("broker identity was not available: status=%d", status)
	}
	return identity
}

func brokerBootRequest(t *testing.T, base string, c brokerContext) brokerRetireBootRequest {
	t.Helper()
	identity := brokerReadIdentity(t, base)
	return brokerRetireBootRequest{brokerProtocol, identity.LedgerIdentityDigest, identity.AgentConfigurationDigest,
		c.Owner.bootFence(), "boot-retirement-fixture"}
}

func brokerRetireBoot(t *testing.T, base string, request brokerRetireBootRequest) (int, brokerRetireBootResponse) {
	t.Helper()
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal("could not encode boot retirement request")
	}
	status, data := brokerRecoveryHTTP(t, base, brokerapi.RetireBootPath, http.MethodPost, body, true)
	var proof brokerRetireBootResponse
	if (status != http.StatusOK && status != http.StatusConflict) || json.Unmarshal(data, &proof) != nil ||
		proof.Protocol != brokerProtocol || proof.LedgerIdentityDigest != request.LedgerIdentityDigest ||
		proof.AgentConfigurationDigest != request.AgentConfigurationDigest || proof.RetiredFenceDigest != foundry.JSONDigest(request.RetiredFence) ||
		proof.OperationID != request.OperationID || proof.ContextSHA256 != foundry.Digest(body) || !proof.Sealed {
		t.Fatalf("boot recovery response was not bound to its request: status=%d", status)
	}
	return status, proof
}

func brokerAwaitBootRetirement(t *testing.T, base string, request brokerRetireBootRequest) brokerRetireBootResponse {
	t.Helper()
	var proof brokerRetireBootResponse
	brokerAwait(t, func() bool {
		status, current := brokerRetireBoot(t, base, request)
		proof = current
		return status == http.StatusOK
	})
	if proof.State != "retired" || !proof.RetirementProven || !proof.SettlementProven || proof.ActiveInvocations != 0 ||
		proof.AmbiguousInvocations != 0 || proof.PendingCreates != 0 || proof.ProofDigest != proof.canonicalProofDigest() {
		t.Fatal("boot retirement omitted complete quiescent proof")
	}
	return proof
}

func TestBrokerIdentityIsAuthenticatedPersistentAndDistinct(t *testing.T) {
	f := newBrokerFixture(t, "success")
	cfg := brokerTestConfig(t, f)
	b, server := startBrokerTest(t, cfg)
	for _, path := range []string{brokerapi.IdentityPath, brokerapi.RetireBootPath} {
		status, _ := brokerRecoveryHTTP(t, server.URL, path, http.MethodGet, nil, false)
		if status != http.StatusUnauthorized {
			t.Fatal("recovery interface accepted an unauthenticated request")
		}
	}
	identity := brokerReadIdentity(t, server.URL)
	if identity.AgentConfigurationDigest != cfg.configDigest {
		t.Fatal("identity was not bound to broker configuration")
	}
	for _, test := range []struct{ path, method, body string }{
		{brokerapi.IdentityPath, http.MethodPost, ""},
		{brokerapi.IdentityPath + "?extra=1", http.MethodGet, ""},
		{brokerapi.IdentityPath, http.MethodGet, "{}"},
	} {
		status, _ := brokerRecoveryHTTP(t, server.URL, test.path, test.method, []byte(test.body), true)
		if status != http.StatusBadRequest {
			t.Fatal("identity accepted a noncanonical request")
		}
	}
	b.close()
	server.Close()
	_, restarted := startBrokerTest(t, cfg)
	if brokerReadIdentity(t, restarted.URL) != identity {
		t.Fatal("broker restart replaced its durable identity")
	}
	_, replacement := startBrokerTest(t, brokerTestConfig(t, f))
	if brokerReadIdentity(t, replacement.URL).LedgerIdentityDigest == identity.LedgerIdentityDigest {
		t.Fatal("fresh ledger reused an existing identity")
	}
}

func TestBrokerBootProofCanonicalDigest(t *testing.T) {
	proof := brokerRetireBootResponse{Protocol: brokerProtocol,
		LedgerIdentityDigest: "sha256:" + strings.Repeat("1", 64), AgentConfigurationDigest: "sha256:" + strings.Repeat("2", 64),
		RetiredFenceDigest: "sha256:" + strings.Repeat("3", 64), State: "retired", Sealed: true,
		SettlementProven: true, RetirementProven: true, OwnerCount: 2, OwnerSetDigest: "sha256:" + strings.Repeat("4", 64)}
	const expected = "sha256:f2dc20e1892c36df3942420dbbdce832b9aedc520139af8feefb13486b9f1dbb"
	if proof.canonicalProofDigest() != expected {
		t.Fatal("public proof digest differs from the cross-runtime canonical JSON contract")
	}
	proof.OperationID, proof.ContextSHA256, proof.ProofDigest = "retry", foundry.Digest([]byte("request")), "ignored"
	if proof.canonicalProofDigest() != expected {
		t.Fatal("retry-specific binding changed the durable proof digest")
	}
	proof.OwnerCount++
	if proof.canonicalProofDigest() == expected {
		t.Fatal("changed retirement evidence preserved the proof digest")
	}
}

func TestBrokerBootRetirementSealsAllOwnersAndSurvivesRestart(t *testing.T) {
	f := newBrokerFixture(t, "success")
	cfg := brokerTestConfig(t, f)
	b, server := startBrokerTest(t, cfg)
	first := brokerTestContext(cfg)
	status, _, err := brokerTestHTTP(context.Background(), server.URL, brokerapi.ResponsesPath, first, brokerTestBody(""))
	if err != nil || status != http.StatusOK {
		t.Fatal("could not create original remote owner")
	}
	second := first
	second.Owner.RuntimeSessionUID = "renew-only-owner"
	_ = brokerTestControl(t, server.URL, brokerapi.RenewPath, second)
	unrelated := first
	unrelated.Owner.SupervisorBootID = "unrelated-boot"
	_ = brokerTestControl(t, server.URL, brokerapi.RenewPath, unrelated)
	request := brokerBootRequest(t, server.URL, first)
	proof := brokerAwaitBootRetirement(t, server.URL, request)
	owners := []string{foundry.JSONDigest(first.Owner), foundry.JSONDigest(second.Owner)}
	sort.Strings(owners)
	if proof.OwnerCount != 2 || proof.OwnerSetDigest != foundry.JSONDigest(owners) {
		t.Fatal("boot proof did not cover the entire exact owner set")
	}
	for i := range 3 {
		request.OperationID = fmt.Sprintf("fresh-recovery-operation-%d", i)
		duplicate := brokerAwaitBootRetirement(t, server.URL, request)
		if duplicate.ProofDigest != proof.ProofDigest || duplicate.OwnerSetDigest != proof.OwnerSetDigest {
			t.Fatal("fresh recovery operation replaced durable boot proof")
		}
	}
	delayed := first
	delayed.Owner.RuntimeSessionUID = "late-owner-after-seal"
	for _, path := range []string{brokerapi.ResponsesPath, brokerapi.RenewPath, brokerapi.RetirePath} {
		c, body := delayed, brokerTestBody("")
		if path != brokerapi.ResponsesPath {
			c, body = brokerTestControlContext(path, delayed)
		}
		status, _, err := brokerTestHTTP(context.Background(), server.URL, path, c, body)
		if err != nil || status != http.StatusGone {
			t.Fatal("sealed boot created a new owner or fabricated unknown-owner retirement")
		}
	}
	b.mu.Lock()
	valid := brokerLedgerValid(b.ledger, cfg.configDigest) && len(b.ledger.Sessions) == 3 &&
		!b.ledger.Sessions[foundry.JSONDigest(unrelated.Owner)].Retiring
	b.mu.Unlock()
	if !valid {
		t.Fatal("boot retirement changed unrelated authority or retained invalid state")
	}
	b.close()
	server.Close()
	_, restarted := startBrokerTest(t, cfg)
	if reopened := brokerAwaitBootRetirement(t, restarted.URL, request); reopened.ProofDigest != proof.ProofDigest {
		t.Fatal("boot proof changed across restart")
	}
	status, _, err = brokerTestHTTP(context.Background(), restarted.URL, brokerapi.ResponsesPath, delayed, brokerTestBody(""))
	if err != nil || status != http.StatusGone {
		t.Fatal("restart lost the permanent admission seal")
	}
	creates, inferences, _, deletes := f.counts()
	if creates != 1 || inferences != 1 || deletes != 1 {
		t.Fatal("boot recovery replayed work or repeated remote deletion")
	}
}

func TestBrokerEmptyBootSealRequiresExactIdentityAndRejectsLateAdmission(t *testing.T) {
	f := newBrokerFixture(t, "success")
	cfg := brokerTestConfig(t, f)
	b, server := startBrokerTest(t, cfg)
	c := brokerTestContext(cfg)
	request := brokerBootRequest(t, server.URL, c)
	for _, test := range []struct {
		name   string
		change func(*brokerRetireBootRequest)
		status int
	}{
		{"wrong-ledger", func(r *brokerRetireBootRequest) { r.LedgerIdentityDigest = foundry.Digest([]byte("other-ledger")) }, http.StatusConflict},
		{"wrong-config", func(r *brokerRetireBootRequest) { r.AgentConfigurationDigest = foundry.Digest([]byte("other-config")) }, http.StatusBadRequest},
		{"session-fence", func(r *brokerRetireBootRequest) { r.RetiredFence.RuntimeSessionUID = "invalid-session" }, http.StatusBadRequest},
		{"zero-epoch", func(r *brokerRetireBootRequest) { r.RetiredFence.ControllerEpoch = 0 }, http.StatusBadRequest},
	} {
		t.Run(test.name, func(t *testing.T) {
			invalid := request
			test.change(&invalid)
			body, _ := json.Marshal(invalid)
			status, _ := brokerRecoveryHTTP(t, server.URL, brokerapi.RetireBootPath, http.MethodPost, body, true)
			b.mu.Lock()
			unchanged := len(b.ledger.Sessions) == 0 && len(b.ledger.SealedBoots) == 0
			b.mu.Unlock()
			if status != test.status || !unchanged {
				t.Fatal("invalid recovery request changed ownership")
			}
		})
	}
	body, _ := json.Marshal(request)
	for _, malformed := range [][]byte{
		append(body[:len(body)-1:len(body)-1], []byte(`,"extra":true}`)...),
		append(body[:len(body)-1:len(body)-1], []byte(`,"operationID":"duplicate"}`)...),
	} {
		status, _ := brokerRecoveryHTTP(t, server.URL, brokerapi.RetireBootPath, http.MethodPost, malformed, true)
		if status != http.StatusBadRequest {
			t.Fatal("boot recovery accepted duplicate or unknown JSON fields")
		}
	}
	proof := brokerAwaitBootRetirement(t, server.URL, request)
	if proof.OwnerCount != 0 || proof.OwnerSetDigest != foundry.JSONDigest([]string{}) {
		t.Fatal("empty boot owner set was not canonical")
	}
	status, _, err := brokerTestHTTP(context.Background(), server.URL, brokerapi.ResponsesPath, c, brokerTestBody(""))
	if err != nil || status != http.StatusGone {
		t.Fatal("an empty boot seal admitted delayed inference")
	}
	creates, inferences, stops, deletes := f.counts()
	if creates != 0 || inferences != 0 || stops != 0 || deletes != 0 {
		t.Fatal("empty boot retirement made a provider request")
	}
}

func TestBrokerBootSealWaitsForOriginalCreateAcknowledgement(t *testing.T) {
	f := newBrokerFixture(t, "hold-create")
	cfg := brokerTestConfig(t, f)
	_, server := startBrokerTest(t, cfg)
	c := brokerTestContext(cfg)
	done := brokerAsyncInference(context.Background(), server.URL, c, brokerTestBody(""))
	select {
	case <-f.started:
	case <-time.After(3 * time.Second):
		t.Fatal("original session creation did not start")
	}
	request := brokerBootRequest(t, server.URL, c)
	status, pending := brokerRetireBoot(t, server.URL, request)
	if status != http.StatusConflict || pending.PendingCreates != 1 || pending.RetirementProven || pending.ProofDigest != "" {
		t.Fatal("boot seal claimed cleanup before original create acknowledgement")
	}
	f.unblock()
	result := brokerWaitInference(t, done)
	if result.err == nil && result.status == http.StatusOK {
		t.Fatal("boot retirement exposed inference output")
	}
	proof := brokerAwaitBootRetirement(t, server.URL, request)
	creates, inferences, _, deletes := f.counts()
	if proof.OwnerCount != 1 || creates != 1 || inferences != 0 || deletes != 1 {
		t.Fatal("boot recovery lost the acknowledged create or replayed inference")
	}
}

func TestBrokerBootSealNeverProvesAmbiguousCreationOrInvocation(t *testing.T) {
	for _, mode := range []string{"late-create", "hold-unknown"} {
		t.Run(mode, func(t *testing.T) {
			f := newBrokerFixture(t, mode)
			cfg := brokerTestConfig(t, f)
			b, server := startBrokerTest(t, cfg)
			c := brokerTestContext(cfg)
			done := brokerAsyncInference(context.Background(), server.URL, c, brokerTestBody(""))
			select {
			case <-f.started:
			case <-time.After(3 * time.Second):
				t.Fatal("ambiguous original operation did not start")
			}
			request := brokerBootRequest(t, server.URL, c)
			_, _ = brokerRetireBoot(t, server.URL, request)
			_ = brokerWaitInference(t, done)
			b.close()
			server.Close()
			_, restarted := startBrokerTest(t, cfg)
			for i := range 3 {
				request.OperationID = fmt.Sprintf("retry-ambiguous-boot-%d", i)
				status, proof := brokerRetireBoot(t, restarted.URL, request)
				if status != http.StatusConflict || proof.State != "blocked" || proof.SettlementProven || proof.RetirementProven || proof.ProofDigest != "" ||
					(mode == "late-create" && proof.PendingCreates != 1) || (mode == "hold-unknown" && proof.AmbiguousInvocations != 1) {
					t.Fatal("boot recovery erased unresolved remote ownership")
				}
			}
			creates, inferences, _, deletes := f.counts()
			if creates != 1 || deletes != 0 || (mode == "late-create" && inferences != 0) || (mode == "hold-unknown" && inferences != 1) {
				t.Fatal("boot recovery replayed work or deleted uncertain ownership")
			}
		})
	}
}

func TestBrokerBootSealStorageFailureCannotReleaseProof(t *testing.T) {
	f := newBrokerFixture(t, "success")
	cfg := brokerTestConfig(t, f)
	b, server := startBrokerTest(t, cfg)
	c := brokerTestContext(cfg)
	_ = brokerTestControl(t, server.URL, brokerapi.RenewPath, c)
	request := brokerBootRequest(t, server.URL, c)
	if os.Rename(cfg.stateDir, cfg.stateDir+"-hidden") != nil {
		t.Fatal("could not inject unavailable broker storage")
	}
	defer func() { _ = os.Rename(cfg.stateDir+"-hidden", cfg.stateDir) }()
	body, _ := json.Marshal(request)
	status, _ := brokerRecoveryHTTP(t, server.URL, brokerapi.RetireBootPath, http.MethodPost, body, true)
	identityStatus, _ := brokerRecoveryHTTP(t, server.URL, brokerapi.IdentityPath, http.MethodGet, nil, true)
	stored, err := os.ReadFile(filepath.Join(cfg.stateDir+"-hidden", "state.json"))
	var ledger brokerLedger
	if status != http.StatusServiceUnavailable || identityStatus != http.StatusServiceUnavailable || err != nil ||
		json.Unmarshal(stored, &ledger) != nil || len(ledger.SealedBoots) != 0 || ledger.Sessions[foundry.JSONDigest(c.Owner)].Retiring {
		t.Fatal("failed seal write exposed proof or replaced durable authority")
	}
	b.mu.Lock()
	poisoned := b.storageError != nil && b.ctx.Err() != nil
	b.mu.Unlock()
	if !poisoned {
		t.Fatal("uncertain seal durability left broker admissions enabled")
	}
}

func TestBrokerBootSealCancelsAcknowledgedInvocation(t *testing.T) {
	f := newBrokerFixture(t, "hold-known")
	cfg := brokerTestConfig(t, f)
	b, server := startBrokerTest(t, cfg)
	c := brokerTestContext(cfg)
	done := brokerAsyncInference(context.Background(), server.URL, c, brokerTestBody(""))
	brokerAwait(t, func() bool { return brokerInvocationState(b, c) == "accepted" })
	request := brokerBootRequest(t, server.URL, c)
	proof := brokerAwaitBootRetirement(t, server.URL, request)
	result := brokerWaitInference(t, done)
	creates, inferences, _, deletes := f.counts()
	if (result.err == nil && result.status == http.StatusOK) || proof.OwnerCount != 1 || creates != 1 || inferences != 1 || deletes != 1 {
		t.Fatal("sealed boot replayed or exposed cancelled inference, or omitted retirement")
	}
}

func TestBrokerBootRetirementRecoversLostDeletionPersistence(t *testing.T) {
	f := newBrokerFixture(t, "success")
	cfg := brokerTestConfig(t, f)
	client := newBrokerHTTPClient()
	transport := client.Transport
	var failOnce sync.Once
	client.Transport = brokerFixtureTransport(func(r *http.Request) (*http.Response, error) {
		response, err := transport.RoundTrip(r)
		if err == nil && r.Method == http.MethodDelete && strings.Contains(r.URL.Path, "/endpoint/sessions/") {
			failOnce.Do(func() {
				if os.Rename(cfg.stateDir, cfg.stateDir+"-hidden") != nil {
					t.Error("could not inject failed proof persistence after deletion")
				}
			})
		}
		return response, err
	})
	b, server := startBrokerTestWithClient(t, cfg, client)
	c := brokerTestContext(cfg)
	status, _, err := brokerTestHTTP(context.Background(), server.URL, brokerapi.ResponsesPath, c, brokerTestBody(""))
	if err != nil || status != http.StatusOK {
		t.Fatal("could not create original known remote owner")
	}
	request := brokerBootRequest(t, server.URL, c)
	body, _ := json.Marshal(request)
	status, _ = brokerRecoveryHTTP(t, server.URL, brokerapi.RetireBootPath, http.MethodPost, body, true)
	if status != http.StatusConflict && status != http.StatusServiceUnavailable {
		t.Fatal("unpersisted remote deletion produced boot proof")
	}
	brokerAwait(t, func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		return b.storageError != nil
	})
	b.close()
	server.Close()
	if os.Rename(cfg.stateDir+"-hidden", cfg.stateDir) != nil {
		t.Fatal("could not restore retained broker state")
	}
	_, restarted := startBrokerTestWithClient(t, cfg, client)
	proof := brokerAwaitBootRetirement(t, restarted.URL, request)
	creates, inferences, _, deletes := f.counts()
	if proof.OwnerCount != 1 || creates != 1 || inferences != 1 || deletes != 2 {
		t.Fatal("lost deletion proof was fabricated or recovery replayed original work")
	}
}

func TestBrokerBootSealSerializesConcurrentRenewalAdmission(t *testing.T) {
	f := newBrokerFixture(t, "success")
	cfg := brokerTestConfig(t, f)
	b, server := startBrokerTest(t, cfg)
	for i := range 12 {
		c := brokerTestContext(cfg)
		c.Owner.SupervisorBootID = fmt.Sprintf("racing-boot-%d", i)
		request := brokerBootRequest(t, server.URL, c)
		renewal, body := brokerTestControlContext(brokerapi.RenewPath, c)
		ready := make(chan struct{})
		admission := make(chan brokerHTTPResult, 1)
		go func() {
			<-ready
			status, data, err := brokerTestHTTP(context.Background(), server.URL, brokerapi.RenewPath, renewal, body)
			admission <- brokerHTTPResult{status, data, err}
		}()
		close(ready)
		proof := brokerAwaitBootRetirement(t, server.URL, request)
		result := brokerWaitInference(t, admission)
		if result.err != nil || (result.status != http.StatusOK && result.status != http.StatusGone && result.status != http.StatusConflict) {
			t.Fatal("racing renewal returned an unexpected result")
		}
		b.mu.Lock()
		owner := b.ledger.Sessions[foundry.JSONDigest(c.Owner)]
		valid := brokerLedgerValid(b.ledger, cfg.configDigest)
		if owner == nil {
			valid = valid && proof.OwnerCount == 0
		} else {
			valid = valid && owner.Retired && proof.OwnerCount == 1
		}
		b.mu.Unlock()
		if !valid {
			t.Fatal("concurrent renewal escaped the frozen boot owner set")
		}
	}
	creates, inferences, _, deletes := f.counts()
	if creates != 0 || inferences != 0 || deletes != 0 {
		t.Fatal("renewal admission race submitted provider work")
	}
}
