package main

import (
	"bytes"
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

func newBrokerEvidenceFixture(t *testing.T, reply func(foundryResponseRequest) (string, []byte)) *brokerFixture {
	t.Helper()
	f := newBrokerFixture(t, "success")
	f.server.Close()
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/protocols/openai/responses") {
			f.serve(w, r)
			return
		}
		if r.Header.Get("Authorization") != "Bearer "+brokerTestToken() || r.URL.Query().Get("api-version") != "v1" {
			t.Error("inference lacked the fixture's exact provider authority")
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		var request foundryResponseRequest
		if json.NewDecoder(r.Body).Decode(&request) != nil {
			t.Error("inference request was not decodable")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		f.mu.Lock()
		_, exists := f.sessions[request.AgentSessionID]
		if exists {
			f.sessions[request.AgentSessionID] = "active"
			f.inferences++
			f.requests = append(f.requests, request)
		}
		f.mu.Unlock()
		if !exists {
			t.Error("inference lacked an owned remote session")
			w.WriteHeader(http.StatusNotFound)
			return
		}
		media, body := reply(request)
		w.Header().Set("Content-Type", media)
		_, _ = w.Write(body)
	}))
	return f
}

func TestBrokerMalformedSSECannotAcknowledgeInference(t *testing.T) {
	for _, name := range []string{
		"missing-type", "unknown-type", "wrong-event", "missing-status", "unknown-status",
		"created-completed", "queued-in-progress", "completed-in-progress", "completed-error", "wrong-session",
	} {
		t.Run(name, func(t *testing.T) {
			f := newBrokerEvidenceFixture(t, func(request foundryResponseRequest) (string, []byte) {
				response := map[string]any{"id": "provider-evidence", "status": "in_progress", "agent_session_id": request.AgentSessionID, "output": []any{}}
				event := map[string]any{"type": "response.created", "response": response}
				switch name {
				case "missing-type":
					delete(event, "type")
				case "unknown-type":
					event["type"] = "not-a-response-event"
				case "wrong-event":
					event["type"] = "response.output_text.delta"
					event["delta"] = "fixture"
				case "missing-status":
					delete(response, "status")
				case "unknown-status":
					response["status"] = "not-a-response-status"
				case "created-completed":
					response["status"] = "completed"
				case "queued-in-progress":
					event["type"] = "response.queued"
				case "completed-in-progress":
					event["type"] = "response.completed"
				case "completed-error":
					event["type"], response["status"] = "response.completed", "completed"
					response["error"] = map[string]string{"code": "server_error", "message": "fixture-error-do-not-persist"}
				case "wrong-session":
					response["agent_session_id"] = "another-session"
				}
				data, _ := json.Marshal(event)
				return "text/event-stream", []byte("data: " + string(data) + "\n\n")
			})
			cfg := brokerTestConfig(t, f)
			b, server := startBrokerTest(t, cfg)
			c := brokerTestContext(cfg)
			status, _, err := brokerTestHTTP(context.Background(), server.URL, brokerResponsesPath, c, brokerTestBody(""))
			if err != nil || status == http.StatusOK {
				t.Fatal("malformed inference exposed a successful response")
			}
			if state := brokerInvocationState(b, c); state != "uncertain" {
				t.Fatalf("malformed SSE fabricated acknowledgement: state=%s", state)
			}
			brokerPendingControl(t, server.URL, brokerSettlePath, c, false, 1)
			brokerPendingControl(t, server.URL, brokerRetirePath, c, false, 1)
			creates, inferences, _, deletes := f.counts()
			if creates != 1 || inferences != 1 || deletes != 0 {
				t.Fatal("malformed SSE replayed work or authorized deletion")
			}
		})
	}
}

func TestBrokerFailureResponseRetainsAcknowledgement(t *testing.T) {
	for _, media := range []string{"application/json", "text/event-stream"} {
		for _, state := range []string{"failed", "incomplete"} {
			t.Run(media+"/"+state, func(t *testing.T) {
				f := newBrokerEvidenceFixture(t, func(request foundryResponseRequest) (string, []byte) {
					response := map[string]any{"id": "provider-failure", "status": state, "agent_session_id": request.AgentSessionID, "output": []any{}}
					if state == "failed" {
						response["error"] = map[string]string{"code": "server_error", "message": "fixture-error-do-not-persist"}
					} else {
						response["incomplete_details"] = map[string]string{"reason": "max_output_tokens"}
					}
					if media == "text/event-stream" {
						data, _ := json.Marshal(map[string]any{"type": "response." + state, "response": response})
						return media, []byte("data: " + string(data) + "\n\n")
					}
					data, _ := json.Marshal(response)
					return media, data
				})
				cfg := brokerTestConfig(t, f)
				b, server := startBrokerTest(t, cfg)
				c := brokerTestContext(cfg)
				status, _, err := brokerTestHTTP(context.Background(), server.URL, brokerResponsesPath, c, brokerTestBody(""))
				if err != nil || status == http.StatusOK {
					t.Fatal("failed provider response was returned as success")
				}
				if actual := brokerInvocationState(b, c); actual != "accepted" && actual != "settled" {
					t.Fatalf("coherent failure lost its acknowledgement: state=%s", actual)
				}
				proof := brokerTestControl(t, server.URL, brokerSettlePath, c)
				if !proof.SettlementProven || proof.ActiveInvocations != 0 || proof.AmbiguousInvocations != 0 {
					t.Fatal("acknowledged failure could not prove stop settlement")
				}
				proof = brokerTestControl(t, server.URL, brokerRetirePath, c)
				if !proof.RetirementProven {
					t.Fatal("acknowledged failure could not retire its owner")
				}
				creates, inferences, stops, deletes := f.counts()
				if creates != 1 || inferences != 1 || stops == 0 || deletes != 1 {
					t.Fatal("failure cleanup skipped containment or replayed work")
				}
				ledger, err := os.ReadFile(filepath.Join(cfg.stateDir, "state.json"))
				if err != nil || bytes.Contains(ledger, []byte("fixture-error-do-not-persist")) {
					t.Fatal("failure evidence persisted provider content")
				}
			})
		}
	}
}

func TestBrokerValidAcknowledgementSurvivesMalformedTail(t *testing.T) {
	f := newBrokerEvidenceFixture(t, func(request foundryResponseRequest) (string, []byte) {
		response := map[string]any{"id": "provider-accepted", "status": "in_progress", "agent_session_id": request.AgentSessionID, "output": []any{}}
		data, _ := json.Marshal(map[string]any{"type": "response.created", "response": response})
		return "text/event-stream", []byte("data: " + string(data) + "\n\ndata: {\"type\":\"invalid-after-ack\"}\n\n")
	})
	cfg := brokerTestConfig(t, f)
	_, server := startBrokerTest(t, cfg)
	c := brokerTestContext(cfg)
	status, _, err := brokerTestHTTP(context.Background(), server.URL, brokerResponsesPath, c, brokerTestBody(""))
	if err != nil || status == http.StatusOK {
		t.Fatal("malformed tail exposed a successful response")
	}
	proof := brokerTestControl(t, server.URL, brokerSettlePath, c)
	if !proof.SettlementProven || proof.AmbiguousInvocations != 0 {
		t.Fatal("later malformed data erased valid acknowledgement")
	}
	_ = brokerTestControl(t, server.URL, brokerRetirePath, c)
	creates, inferences, stops, deletes := f.counts()
	if creates != 1 || inferences != 1 || stops == 0 || deletes != 1 {
		t.Fatal("acknowledged malformed response lacked exact containment")
	}
}

type brokerEvidenceTokenProvider func(context.Context) (string, error)

func (p brokerEvidenceTokenProvider) AccessToken(ctx context.Context) (string, error) { return p(ctx) }

func TestBrokerUnsentRequestsRetainNoAmbiguousIntent(t *testing.T) {
	for _, phase := range []string{"create", "inference"} {
		for _, failure := range []string{"token-error", "malformed-token", "principal-drift"} {
			t.Run(phase+"/"+failure, func(t *testing.T) {
				f := newBrokerFixture(t, "success")
				cfg := brokerTestConfig(t, f)
				failAt := int64(3) // Two target validation GETs precede the create POST.
				if phase == "inference" {
					failAt = 5 // Creation and its exact-session GET precede inference.
				}
				var tokenCalls atomic.Int64
				provider := brokerEvidenceTokenProvider(func(context.Context) (string, error) {
					if tokenCalls.Add(1) == failAt {
						switch failure {
						case "token-error":
							return "", errors.New("fixture identity unavailable")
						case "malformed-token":
							return "invalid-fixture-identity", nil
						case "principal-drift":
							claims := []byte(`{"aud":"https://ai.azure.com","tid":"test-tenant","oid":"other-principal","appid":"test-client"}`)
							return "fixture." + base64.RawURLEncoding.EncodeToString(claims) + ".fixture", nil
						}
					}
					return brokerTestToken(), nil
				})
				b, err := newLifecycleBroker(context.Background(), cfg, provider, nil)
				if err != nil {
					t.Fatal("fixture broker could not start")
				}
				server := httptest.NewServer(b)
				t.Cleanup(func() { b.close(); server.Close() })
				c := brokerTestContext(cfg)
				status, _, err := brokerTestHTTP(context.Background(), server.URL, brokerResponsesPath, c, brokerTestBody(""))
				if err != nil || status == http.StatusOK || tokenCalls.Load() < failAt {
					t.Fatal("pre-send identity failure was not exercised")
				}
				b.mu.Lock()
				owner := b.ledger.Sessions[brokerJSONDigest(c.Owner)]
				createState, remoteID := owner.CreateState, owner.RemoteID
				b.mu.Unlock()
				if phase == "create" && (createState != "none" || remoteID != "") {
					t.Fatalf("unsent creation retained ambiguous ownership: state=%s", createState)
				}
				proof := brokerTestControl(t, server.URL, brokerSettlePath, c)
				if !proof.SettlementProven || proof.CreatePending || proof.AmbiguousInvocations != 0 {
					t.Fatal("definitively unsent request did not settle")
				}
				proof = brokerTestControl(t, server.URL, brokerRetirePath, c)
				if !proof.RetirementProven {
					t.Fatal("unsent request left an unretirable owner")
				}
				creates, inferences, stops, deletes := f.counts()
				if inferences != 0 || (phase == "create" && (creates != 0 || stops != 0 || deletes != 0)) ||
					(phase == "inference" && (creates != 1 || stops != 0 || deletes != 1)) {
					t.Fatal("pre-send failure replayed or dispatched remote work")
				}
			})
		}
	}
}
