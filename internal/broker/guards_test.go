package broker

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/orka-agents/agent-runtime-foundry/internal/brokerapi"
	"github.com/orka-agents/agent-runtime-foundry/internal/foundry"
)

func brokerFunctionBody(previous, call string) []byte {
	body, _ := json.Marshal(foundry.ModelResponseRequest{Model: "fixture-model", ResponseRequest: foundry.ResponseRequest{
		Stream: true, Store: true, PreviousResponseID: previous,
		Input: []foundry.FunctionOutput{{Type: "function_call_output", CallID: call, Output: "fixture-tool-result"}},
	}})
	return body
}

func TestBrokerOpaqueAliasesAndNoFunctionReplay(t *testing.T) {
	f := newBrokerFixture(t, "functions")
	cfg := brokerTestConfig(t, f)
	_, server := startBrokerTest(t, cfg)
	c := brokerTestContext(cfg)
	c.InvocationSequence = 3 // Supervisor allocation order can contain gaps.
	status, data, err := brokerTestHTTP(context.Background(), server.URL, brokerapi.ResponsesPath, c, brokerTestBody(""))
	var first foundry.Response
	if err != nil || status != 200 || json.Unmarshal(data, &first) != nil || len(first.Output) != 1 {
		t.Fatalf("function proposal failed: %d", status)
	}
	call := first.Output[0].CallID
	if !brokerAliasValid(first.ID, "fr_") || !brokerAliasValid(call, "fc_") ||
		!brokerAliasValid(first.Output[0].ID, "fi_") || bytes.Contains(data, []byte("provider-")) {
		t.Fatal("native identities were not replaced by opaque aliases")
	}
	for _, which := range []string{"duplicate", "native_call", "cross_owner", "cross_prompt", "missing_output"} {
		bad := c
		bad.OperationID = "bad-" + which
		bad.InvocationSequence = 4
		body := brokerFunctionBody(first.ID, call)
		switch which {
		case "duplicate":
			bad = c
			body = brokerTestBody("")
		case "native_call":
			body = brokerFunctionBody(first.ID, "provider-call-id")
		case "cross_owner":
			bad.Owner.RuntimeSessionUID = "another-runtime-session"
		case "cross_prompt":
			bad.PromptID = "another-prompt"
			bad.TaskUID = "another-task"
		case "missing_output":
			body = brokerTestBody(first.ID)
		}
		status, _, err = brokerTestHTTP(context.Background(), server.URL, brokerapi.ResponsesPath, bad, body)
		if err != nil || status != http.StatusConflict {
			t.Fatalf("invalid function ownership %s was accepted: %d", which, status)
		}
	}
	next := c
	next.OperationID = "function-output"
	next.InvocationSequence = 8
	status, data, err = brokerTestHTTP(context.Background(), server.URL, brokerapi.ResponsesPath, next, brokerFunctionBody(first.ID, call))
	var second foundry.Response
	if err != nil || status != 200 || json.Unmarshal(data, &second) != nil {
		t.Fatal("valid owned function output failed")
	}
	for _, previous := range []string{first.ID, second.ID} {
		replay := next
		replay.InvocationSequence = 9
		replay.OperationID = "replay-" + previous
		status, _, err = brokerTestHTTP(context.Background(), server.URL, brokerapi.ResponsesPath, replay, brokerFunctionBody(previous, call))
		if err != nil || status != http.StatusConflict {
			t.Fatal("consumed function output was replayed")
		}
	}
	f.mu.Lock()
	if len(f.requests) != 2 || f.requests[1].PreviousResponseID != "provider-response-1" {
		t.Error("provider continuation did not use owned response identity")
	} else {
		items, ok := f.requests[1].Input.([]any)
		if !ok || len(items) != 1 || items[0].(map[string]any)["call_id"] != "provider-call-id" {
			t.Error("provider function result did not use owned call identity")
		}
	}
	f.mu.Unlock()
	_ = brokerTestControl(t, server.URL, brokerapi.SettlePath, next)
	_ = brokerTestControl(t, server.URL, brokerapi.RetirePath, next)
}

func TestBrokerConcurrentDuplicateAdmitsOnce(t *testing.T) {
	f := newBrokerFixture(t, "hold-known")
	cfg := brokerTestConfig(t, f)
	b, server := startBrokerTest(t, cfg)
	c := brokerTestContext(cfg)
	first := brokerAsyncInference(context.Background(), server.URL, c, brokerTestBody(""))
	brokerAwait(t, func() bool { return brokerInvocationState(b, c) == "accepted" })
	others := make([]<-chan brokerHTTPResult, 8)
	for i := range others {
		others[i] = brokerAsyncInference(context.Background(), server.URL, c, brokerTestBody(""))
	}
	for _, done := range others {
		result := brokerWaitInference(t, done)
		if result.err != nil || result.status != http.StatusConflict {
			t.Fatal("concurrent duplicate was admitted")
		}
	}
	f.unblock()
	result := brokerWaitInference(t, first)
	if result.err != nil || result.status != http.StatusOK {
		t.Fatal("original concurrent inference did not complete")
	}
	creates, inferences, _, _ := f.counts()
	if creates != 1 || inferences != 1 {
		t.Fatal("duplicate caused a second provider operation")
	}
	_ = brokerTestControl(t, server.URL, brokerapi.SettlePath, c)
	_ = brokerTestControl(t, server.URL, brokerapi.RetirePath, c)
}

func TestBrokerRejectsUntrustedRoutesContextsAndInput(t *testing.T) {
	f := newBrokerFixture(t, "success")
	cfg := brokerTestConfig(t, f)
	b, _ := startBrokerTest(t, cfg)
	for _, name := range []string{"auth", "query", "duplicate_header", "malformed_context", "duplicate_context_member",
		"config_digest", "empty_epoch", "wrong_digest", "expired", "model", "duplicate_body_member", "session", "background",
		"conversation", "static_tools", "native_item_reference", "object_input", "nonstream", "nonstore"} {
		t.Run(name, func(t *testing.T) {
			c := brokerTestContext(cfg)
			body := brokerTestBody("")
			path := brokerapi.ResponsesPath
			switch name {
			case "query":
				path += "?unexpected=1"
			case "config_digest":
				c.AgentConfigurationDigest = "sha256:" + strings.Repeat("3", 64)
			case "empty_epoch":
				c.Owner.ControllerEpoch = 0
			case "expired":
				c.LeaseExpiresAt = time.Now().Add(-time.Second).UTC().Format(time.RFC3339Nano)
			case "model":
				body = bytes.Replace(body, []byte("fixture-model"), []byte("different-model"), 1)
			case "duplicate_body_member":
				body = append([]byte(`{"model":"fixture-model",`), body[1:]...)
			case "session", "background", "conversation", "static_tools":
				field := map[string]string{"session": `"agent_session_id":"injected",`, "background": `"background":true,`,
					"conversation": `"conversation":"injected",`, "static_tools": `"tools":[],`}[name]
				body = append([]byte("{"+field), body[1:]...)
			case "native_item_reference", "object_input":
				var fields map[string]any
				_ = json.Unmarshal(body, &fields)
				fields["input"] = map[string]any{"type": "item_reference", "id": "injected-provider-id"}
				if name == "native_item_reference" {
					fields["input"] = []any{fields["input"]}
				}
				body, _ = json.Marshal(fields)
			case "nonstream":
				body = bytes.Replace(body, []byte(`"stream":true`), []byte(`"stream":false`), 1)
			case "nonstore":
				body = bytes.Replace(body, []byte(`"store":true`), []byte(`"store":false`), 1)
			}
			c.BodySHA256 = foundry.Digest(body)
			if name == "wrong_digest" {
				c.BodySHA256 = "sha256:" + strings.Repeat("3", 64)
			}
			raw, _ := json.Marshal(c)
			if name == "duplicate_context_member" {
				raw = append([]byte(`{"protocol":"orka.foundry.broker.v1",`), raw[1:]...)
			}
			request := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
			request.Header.Set("Authorization", "Bearer "+brokerFixtureBearer)
			request.Header.Set(brokerContextHeader, base64.RawURLEncoding.EncodeToString(raw))
			if name == "auth" {
				request.Header.Del("Authorization")
			}
			if name == "malformed_context" {
				request.Header.Set(brokerContextHeader, "invalid==")
			}
			if name == "duplicate_header" {
				request.Header.Add(brokerContextHeader, request.Header.Get(brokerContextHeader))
			}
			response := httptest.NewRecorder()
			b.ServeHTTP(response, request)
			if response.Code != http.StatusBadRequest && response.Code != http.StatusGone && response.Code != http.StatusUnauthorized {
				t.Fatalf("invalid request was accepted: %d", response.Code)
			}
		})
	}
	creates, inferences, stops, deletes := f.counts()
	if creates+inferences+stops+deletes != 0 {
		t.Fatal("an invalid request reached provider mutations")
	}
}

func TestBrokerControlProofBindsExactHeaderBytes(t *testing.T) {
	f := newBrokerFixture(t, "success")
	cfg := brokerTestConfig(t, f)
	_, server := startBrokerTest(t, cfg)
	c, body := brokerTestControlContext(brokerapi.SettlePath, brokerTestContext(cfg))
	c.BodySHA256 = foundry.Digest(body)
	status, data, err := brokerTestHTTP(context.Background(), server.URL, brokerapi.SettlePath, c, body)
	var proof brokerControlResponse
	if err != nil || (status != 200 && status != 409) || json.Unmarshal(data, &proof) != nil {
		t.Fatal("control reply unavailable")
	}
	if proof.Protocol != brokerProtocol || proof.OwnerDigest != foundry.JSONDigest(c.Owner) ||
		proof.OperationID != c.OperationID || proof.ContextSHA256 != foundry.JSONDigest(c) {
		t.Fatal("proof did not bind the exact trusted owner and context")
	}
	// An operation ID cannot be reassigned to a different owner-context body.
	c.LeaseGeneration++
	status, _, err = brokerTestHTTP(context.Background(), server.URL, brokerapi.SettlePath, c, body)
	if err != nil || status != http.StatusConflict {
		t.Fatal("control idempotency key accepted another context")
	}
}
