package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func TestBrokerStoreRejectsCorruptOwnership(t *testing.T) {
	f := newBrokerFixture(t, "functions")
	cfg := brokerTestConfig(t, f)
	b, server := startBrokerTest(t, cfg)
	c := brokerTestContext(cfg)
	status, _, err := brokerTestHTTP(context.Background(), server.URL, brokerResponsesPath, c, brokerTestBody(""))
	if err != nil || status != http.StatusOK {
		t.Fatal("fixture ownership was not created")
	}
	b.close()
	server.Close()
	baseline, err := os.ReadFile(filepath.Join(cfg.stateDir, "state.json"))
	if err != nil {
		t.Fatal("fixture ledger unavailable")
	}
	for _, kind := range []string{"unknown_state", "missing_response", "orphan_response", "response_completion", "owner_fence",
		"missing_operation", "operation_digest", "last_sequence", "last_alias", "premature_proof", "premature_retirement",
		"missing_principal", "missing_current_prompt", "prompt_identity", "duplicate_member"} {
		t.Run(kind, func(t *testing.T) {
			var ledger brokerLedger
			if json.Unmarshal(baseline, &ledger) != nil {
				t.Fatal("invalid baseline fixture")
			}
			session := ledger.Sessions[brokerJSONDigest(c.Owner)]
			prompt := session.Prompts[c.promptKey()]
			invocation := prompt.Invocations[1]
			switch kind {
			case "unknown_state":
				invocation.State = "looks-finished"
			case "missing_response":
				delete(session.Responses, invocation.ResponseAlias)
			case "orphan_response":
				session.Responses["orphan"] = session.Responses[invocation.ResponseAlias]
			case "response_completion":
				link := session.Responses[invocation.ResponseAlias]
				link.Completed = false
				session.Responses[invocation.ResponseAlias] = link
			case "owner_fence":
				session.Owner.ControllerEpoch++
			case "missing_operation":
				delete(session.Operations, invocation.OperationID)
			case "operation_digest":
				session.Operations[invocation.OperationID] = "broken"
			case "last_sequence":
				prompt.LastSequence++
			case "last_alias":
				prompt.LastAlias = ""
			case "premature_proof":
				prompt.ProofDigest = brokerSHA([]byte("invented"))
			case "premature_retirement":
				session.Retired = true
			case "missing_principal":
				ledger.PrincipalDigest = ""
			case "missing_current_prompt":
				session.CurrentPrompt = ""
			case "prompt_identity":
				prompt.Identity.PromptRequestDigest = "broken"
			}
			data, _ := json.Marshal(ledger)
			if kind == "duplicate_member" {
				data = append([]byte(`{"version":1,`), data[1:]...)
			}
			dir := filepath.Join(t.TempDir(), "broker")
			if os.Mkdir(dir, 0o700) != nil || os.WriteFile(filepath.Join(dir, "state.json"), data, 0o600) != nil {
				t.Fatal("could not prepare corrupt ledger fixture")
			}
			store, _, err := openBrokerStore(dir, cfg.configDigest)
			if err == nil {
				store.close()
				t.Fatal("corrupt state was accepted for recovery")
			}
		})
	}
	store, _, err := openBrokerStore(cfg.stateDir, cfg.configDigest)
	if err != nil {
		t.Fatal("unchanged valid ownership was rejected")
	}
	store.close()
}

func TestBrokerStorePrivatePermissionsAndSingleWriter(t *testing.T) {
	digest := brokerSHA([]byte("private-store-test"))
	dir := filepath.Join(t.TempDir(), "broker")
	store, _, err := openBrokerStore(dir, digest)
	if err != nil {
		t.Fatal("private ledger initialization failed")
	}
	other, _, err := openBrokerStore(dir, digest)
	if err == nil {
		other.close()
		t.Fatal("two durable writers acquired ownership")
	}
	store.close()
	if os.Chmod(filepath.Join(dir, "state.json"), 0o644) != nil {
		t.Fatal("permission fixture failed")
	}
	store, _, err = openBrokerStore(dir, digest)
	if err == nil {
		store.close()
		t.Fatal("publicly readable ledger was accepted")
	}
	if os.Chmod(filepath.Join(dir, "state.json"), 0o600) != nil || os.Chmod(dir, 0o755) != nil {
		t.Fatal("directory permission fixture failed")
	}
	store, _, err = openBrokerStore(dir, digest)
	if err == nil {
		store.close()
		t.Fatal("nonprivate state directory was accepted")
	}
}

func TestBrokerPersistenceFailureClosesAdmissionAndHealth(t *testing.T) {
	f := newBrokerFixture(t, "success")
	cfg := brokerTestConfig(t, f)
	b, server := startBrokerTest(t, cfg)
	if os.Rename(cfg.stateDir, cfg.stateDir+"-moved") != nil {
		t.Fatal("could not inject persistence failure")
	}
	c := brokerTestContext(cfg)
	for range 2 {
		status, _, err := brokerTestHTTP(context.Background(), server.URL, brokerResponsesPath, c, brokerTestBody(""))
		if err != nil || status != http.StatusServiceUnavailable {
			t.Fatal("persistence failure did not close inference admission")
		}
	}
	request := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	response := httptest.NewRecorder()
	b.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatal("poisoned ledger was reported healthy")
	}
	creates, inferences, _, _ := f.counts()
	if creates != 0 || inferences != 0 {
		t.Fatal("remote work preceded durable ownership")
	}
}

func TestBrokerHealthCLIRequiresNoConfigurationCredentialOrLock(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Method != http.MethodGet || r.URL.Path != "/healthz" || r.Header.Get("Authorization") != "" {
					t.Error("health checker expanded authority")
				}
				w.WriteHeader(status)
			}))
			defer server.Close()
			t.Setenv("ORKA_FOUNDRY_BROKER_ADDR", strings.TrimPrefix(server.URL, "http://"))
			t.Setenv("ORKA_FOUNDRY_BROKER_STATE_DIR", "/not-used-for-health")
			handled, err := maybeServeBroker([]string{"--protocol", "broker", "--health-check", "--config", "/missing.json"})
			if !handled || calls.Load() != 1 || (err == nil) != (status == http.StatusOK) {
				t.Fatal("health command initialized config/auth or misclassified readiness")
			}
		})
	}
	for _, address := range []string{"example.com:80", "10.0.0.1:8091", "0.0.0.0:8091", "127.0.0.1:0", "127.0.0.1:65536"} {
		if checkBrokerHealth(address) == nil {
			t.Fatal("health checker accepted a nonlocal or invalid target")
		}
	}
}

func TestBrokerEmptyRetirementIsDurableAdmissionTombstone(t *testing.T) {
	f := newBrokerFixture(t, "success")
	cfg := brokerTestConfig(t, f)
	b, server := startBrokerTest(t, cfg)
	c := brokerTestContext(cfg)
	proof := brokerTestControl(t, server.URL, brokerRetirePath, c)
	if !proof.RetirementProven || proof.RemoteSessionCreated {
		t.Fatal("no-inference retirement lacked proof")
	}
	b.close()
	server.Close()
	_, server = startBrokerTest(t, cfg)
	status, _, err := brokerTestHTTP(context.Background(), server.URL, brokerResponsesPath, c, brokerTestBody(""))
	if err != nil || status != http.StatusGone {
		t.Fatal("restarted tombstone admitted delayed inference")
	}
	data, err := os.ReadFile(filepath.Join(cfg.stateDir, "state.json"))
	if err != nil || bytes.Contains(data, []byte("fixture-input-do-not-persist")) {
		t.Fatal("retirement persisted child input")
	}
	creates, inferences, stops, deletes := f.counts()
	if creates+inferences+stops+deletes != 0 {
		t.Fatal("empty tombstone caused a remote operation")
	}
}
