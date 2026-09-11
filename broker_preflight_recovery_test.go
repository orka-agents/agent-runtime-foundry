package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func TestBrokerPreparedRequestsKeepDurableIntentBeforeTransport(t *testing.T) {
	f := newBrokerFixture(t, "success")
	cfg := brokerTestConfig(t, f)
	c := brokerTestContext(cfg)
	var posts atomic.Int64
	client := &http.Client{Transport: brokerFixtureTransport(func(request *http.Request) (*http.Response, error) {
		if request.Method == http.MethodPost && (strings.HasSuffix(request.URL.Path, "/endpoint/sessions") ||
			strings.HasSuffix(request.URL.Path, "/endpoint/protocols/openai/responses")) {
			raw, err := os.ReadFile(filepath.Join(cfg.stateDir, "state.json"))
			var ledger brokerLedger
			if err != nil || json.Unmarshal(raw, &ledger) != nil {
				t.Error("outbound submission has no readable durable ownership")
				return nil, errBrokerStorage
			}
			owner := ledger.Sessions[brokerJSONDigest(c.Owner)]
			if owner == nil || owner.RemoteID == "" {
				t.Error("outbound submission lost its exact owner")
				return nil, errBrokerStorage
			}
			if strings.HasSuffix(request.URL.Path, "/endpoint/sessions") {
				if owner.CreateState != "intent" {
					t.Error("creation reached transport before durable intent")
				}
			} else if owner.Prompts[c.promptKey()].Invocations[c.InvocationSequence].State != "intent" {
				t.Error("inference reached transport before durable intent")
			}
			if request.GetBody != nil {
				t.Error("prepared request permits implicit replay")
			}
			posts.Add(1)
		}
		return http.DefaultTransport.RoundTrip(request)
	})}
	_, server := startBrokerTestWithClient(t, cfg, client)
	status, _, err := brokerTestHTTP(context.Background(), server.URL, brokerResponsesPath, c, brokerTestBody(""))
	if err != nil || status != http.StatusOK || posts.Load() != 2 {
		t.Fatal("prepared request did not complete exactly one creation and inference")
	}
	_ = brokerTestControl(t, server.URL, brokerSettlePath, c)
	_ = brokerTestControl(t, server.URL, brokerRetirePath, c)
}

func TestBrokerPreflightCrashDoesNotStrandUnsentOwnership(t *testing.T) {
	for _, phase := range []string{"create", "inference"} {
		for _, failure := range []string{"token-error", "malformed-token", "principal-drift", "cancelled-token", "cancelled-after-token"} {
			t.Run(phase+"/"+failure, func(t *testing.T) {
				f := newBrokerFixture(t, "success")
				cfg := brokerTestConfig(t, f)
				c := brokerTestContext(cfg)
				failAt := int64(3)
				if phase == "inference" {
					failAt = 5
				}
				var calls atomic.Int64
				var b *lifecycleBroker
				crashLedger := make(chan []byte, 1)
				provider := brokerEvidenceTokenProvider(func(context.Context) (string, error) {
					if calls.Add(1) != failAt {
						return brokerTestToken(), nil
					}
					// Capture exactly what a restart can recover if the process
					// dies during authentication, before any failure rollback.
					raw, err := os.ReadFile(filepath.Join(cfg.stateDir, "state.json"))
					if err != nil {
						t.Error("could not capture preflight crash ledger")
						return "", errBrokerRemote
					}
					crashLedger <- raw
					switch failure {
					case "token-error":
						return "", errors.New("fixture authentication unavailable")
					case "malformed-token":
						return "invalid-fixture-identity", nil
					case "principal-drift":
						claims := []byte(`{"aud":"https://ai.azure.com","tid":"test-tenant","oid":"other-principal","appid":"test-client"}`)
						return "fixture." + base64.RawURLEncoding.EncodeToString(claims) + ".fixture", nil
					default:
						b.mu.Lock()
						cancel := b.active[brokerJSONDigest(c.Owner)].cancel
						b.mu.Unlock()
						cancel()
						if failure == "cancelled-token" {
							return "", context.Canceled
						}
						return brokerTestToken(), nil
					}
				})
				var err error
				b, err = newLifecycleBroker(context.Background(), cfg, provider, nil)
				if err != nil {
					t.Fatal("could not start preflight crash fixture")
				}
				server := httptest.NewServer(b)
				t.Cleanup(func() { b.close(); server.Close() })
				status, _, err := brokerTestHTTP(context.Background(), server.URL, brokerResponsesPath, c, brokerTestBody(""))
				if err != nil || status == http.StatusOK {
					t.Fatal("definitely-unsent preflight unexpectedly succeeded")
				}
				var raw []byte
				select {
				case raw = <-crashLedger:
				default:
					t.Fatal("request did not reach the selected authentication boundary")
				}
				b.close()
				server.Close()
				var ledger brokerLedger
				if json.Unmarshal(raw, &ledger) != nil || !brokerLedgerValid(&ledger, cfg.configDigest) {
					t.Fatal("preflight snapshot is not valid durable ownership")
				}
				owner := ledger.Sessions[brokerJSONDigest(c.Owner)]
				if owner == nil || owner.Prompts[c.promptKey()] == nil {
					t.Fatal("preflight snapshot lost reserved ownership")
				}
				if phase == "create" && (owner.CreateState != "none" || owner.RemoteID != "") {
					t.Fatal("crash during create authentication leaves possibly-sent creation")
				}
				if owner.Prompts[c.promptKey()].Invocations[c.InvocationSequence].State != "reserved" {
					t.Fatal("crash during authentication leaves possibly-sent inference")
				}
				// Discard later graceful-cleanup writes to model this exact crash.
				if os.WriteFile(filepath.Join(cfg.stateDir, "state.json"), raw, 0o600) != nil {
					t.Fatal("could not restore crash-boundary fixture")
				}
				_, restarted := startBrokerTest(t, cfg)
				proof := brokerTestControl(t, restarted.URL, brokerSettlePath, c)
				if !proof.SettlementProven || proof.CreatePending || proof.AmbiguousInvocations != 0 {
					t.Fatal("restart stranded definitely-unsent ownership")
				}
				proof = brokerTestControl(t, restarted.URL, brokerRetirePath, c)
				creates, inferences, stops, deletes := f.counts()
				if !proof.RetirementProven || inferences != 0 || stops != 0 ||
					(phase == "create" && (creates != 0 || deletes != 0)) ||
					(phase == "inference" && (creates != 1 || deletes != 1)) {
					t.Fatal("preflight crash recovery replayed work or lost exact retirement")
				}
			})
		}
	}
}
