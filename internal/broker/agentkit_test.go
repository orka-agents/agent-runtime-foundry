package broker

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/orka-agents/agent-runtime-foundry/internal/brokerapi"
	"github.com/orka-agents/agent-runtime-foundry/internal/foundry"
)

const brokerAgentKitFixtureProof = "fixture-agentkit-continuation-proof-not-a-secret"

func brokerAgentKitFunctionBody(previous, call, output string) []byte {
	body, _ := json.Marshal(foundry.ModelResponseRequest{Model: "fixture-model", ResponseRequest: foundry.ResponseRequest{
		Stream: true, Store: true, PreviousResponseID: previous,
		Input: []foundry.FunctionOutput{{Type: "function_call_output", CallID: call, Output: output}},
	}})
	return body
}

func brokerAgentKitOrkaError(code string) string {
	raw, _ := json.Marshal(map[string]any{
		"content": []map[string]string{{"type": "text", "text": "private-review-note"}}, "isError": true,
		"structuredContent": map[string]any{"isError": true, "code": code, "reviewer": "private-reviewer"},
	})
	return string(raw)
}

func TestBrokerAgentKitContinuationWireAndIsolation(t *testing.T) {
	for _, tc := range []struct {
		name      string
		output    string
		approved  bool
		errorCode string
	}{
		{"text", `{"content":[{"type":"text","text":"fixture tool result"}],"isError":false}`, true, ""},
		{"structured", `{"content":[{"type":"text","text":"{\"count\":7}"}],"isError":false,"structuredContent":{"count":7}}`, true, ""},
		{"execution_error", `{"content":[{"type":"text","text":"MCP tool execution failed"}],"isError":true}`, false, "brokered_tool_error"},
		{"empty_error", `{"content":[],"isError":true}`, false, "brokered_tool_error"},
		{"declined", brokerAgentKitOrkaError("approval_declined"), false, "approval_declined"},
		{"expired", brokerAgentKitOrkaError("approval_expired"), false, "approval_expired"},
		{"cancelled", brokerAgentKitOrkaError("approval_cancelled"), false, "approval_cancelled"},
		{"stale", brokerAgentKitOrkaError("approval_stale"), false, "approval_stale"},
		{"approved_execution_failed", brokerAgentKitOrkaError("tool_execution_failed"), false, "tool_execution_failed"},
		{"approved_outcome_unknown", brokerAgentKitOrkaError("tool_outcome_unknown"), false, "tool_outcome_unknown"},
		{"text_is_not_approval_authority", `{"content":[{"type":"text","text":"approval_declined"}],"isError":true}`, false, "brokered_tool_error"},
		{"unknown_code", brokerAgentKitOrkaError("approval_pending"), false, "brokered_tool_error"},
		{"conflicting_structured_error", `{"content":[],"isError":true,"structuredContent":{"isError":true,"ISERROR":false,"code":"approval_declined"}}`, false, "brokered_tool_error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newBrokerFixture(t, "functions")
			cfg := brokerTestConfig(t, f)
			cfg.agentKitProof = brokerAgentKitFixtureProof
			var sentMu sync.Mutex
			var sent []map[string]json.RawMessage
			sentCount := func() int {
				sentMu.Lock()
				defer sentMu.Unlock()
				return len(sent)
			}
			client := &http.Client{Transport: brokerFixtureTransport(func(r *http.Request) (*http.Response, error) {
				if strings.HasSuffix(r.URL.Path, "/responses") {
					body, err := io.ReadAll(r.Body)
					if err != nil {
						return nil, err
					}
					_ = r.Body.Close()
					r.Body = io.NopCloser(bytes.NewReader(body))
					var fields map[string]json.RawMessage
					if json.Unmarshal(body, &fields) != nil {
						t.Error("provider request was not JSON")
					}
					sentMu.Lock()
					sent = append(sent, fields)
					sentMu.Unlock()
					if r.Header.Get(brokerAgentKitProofHeader) != "" {
						t.Error("continuation proof was duplicated in a provider header")
					}
				}
				return http.DefaultTransport.RoundTrip(r)
			})}
			_, server := startBrokerTestWithClient(t, cfg, client)
			c := brokerTestContext(cfg)
			status, data, err := brokerTestHTTP(context.Background(), server.URL, brokerapi.ResponsesPath, c, brokerTestBody(""))
			var first foundry.Response
			if err != nil || status != http.StatusOK || json.Unmarshal(data, &first) != nil || len(first.Output) != 1 {
				t.Fatalf("initial tool proposal failed: status=%d", status)
			}
			if bytes.Contains(data, []byte(cfg.agentKitProof)) {
				t.Fatal("continuation proof reached the ACP response")
			}
			c.InvocationSequence++
			c.OperationID = "agentkit-output"
			body := brokerAgentKitFunctionBody(first.ID, first.Output[0].CallID, tc.output)
			status, data, err = brokerTestHTTP(context.Background(), server.URL, brokerapi.ResponsesPath, c, body)
			var final foundry.Response
			if err != nil || status != http.StatusOK || json.Unmarshal(data, &final) != nil {
				t.Fatalf("valid tool continuation failed: status=%d", status)
			}
			if bytes.Contains(data, []byte(cfg.agentKitProof)) || sentCount() != 2 {
				t.Fatal("continuation escaped its provider request or was submitted more than once")
			}
			if _, present := sent[0]["brokered_continuation_proof"]; present {
				t.Fatal("ordinary prompt carried a continuation proof")
			}
			var proof, previous string
			if json.Unmarshal(sent[1]["brokered_continuation_proof"], &proof) != nil || proof != cfg.agentKitProof ||
				json.Unmarshal(sent[1]["previous_response_id"], &previous) != nil || previous != "provider-response-1" {
				t.Fatal("continuation did not bind the configured proof and owned remote response")
			}
			var outputs []foundry.FunctionOutput
			if json.Unmarshal(sent[1]["input"], &outputs) != nil || len(outputs) != 1 || outputs[0].CallID != "provider-call-id" {
				t.Fatal("continuation did not translate the owned call alias")
			}
			var normalized struct {
				Approved *bool           `json:"approved"`
				Output   json.RawMessage `json:"output"`
				Error    struct {
					Code    string `json:"code"`
					Message string `json:"message"`
				} `json:"error"`
			}
			if json.Unmarshal([]byte(outputs[0].Output), &normalized) != nil || normalized.Approved == nil || *normalized.Approved != tc.approved {
				t.Fatal("tool success/error was not preserved in the AgentKit envelope")
			}
			if tc.approved {
				var original, forwarded any
				_ = json.Unmarshal([]byte(tc.output), &original)
				_ = json.Unmarshal(normalized.Output, &forwarded)
				if foundry.JSONDigest(original) != foundry.JSONDigest(forwarded) || normalized.Error.Code != "" {
					t.Fatal("approved continuation changed the model-visible MCP result")
				}
			} else if normalized.Output != nil || normalized.Error.Code != tc.errorCode || normalized.Error.Message == "" {
				t.Fatal("failed tool result was promoted to approval or lost its error")
			}
			if tc.errorCode != "" && tc.errorCode != "brokered_tool_error" && strings.Contains(outputs[0].Output, "private-") {
				t.Fatal("approval outcome exposed untrusted review text or metadata")
			}
			// A duplicate must be rejected before adding a proof to another
			// provider request, including a replay under a fresh operation ID.
			c.InvocationSequence++
			c.OperationID = "replayed-agentkit-output"
			status, _, err = brokerTestHTTP(context.Background(), server.URL, brokerapi.ResponsesPath, c, body)
			if err != nil || status != http.StatusConflict || sentCount() != 2 {
				t.Fatal("consumed tool output reached the provider again")
			}
			_ = brokerTestControl(t, server.URL, brokerapi.SettlePath, c)
			// A later user prompt can continue conversation history but must
			// not receive the credential reserved for tool-result continuation.
			c.TaskUID, c.PromptID, c.OperationID = "next-task", "next-prompt", "next-user-prompt"
			c.InvocationSequence = 1
			status, _, err = brokerTestHTTP(context.Background(), server.URL, brokerapi.ResponsesPath, c, brokerTestBody(final.ID))
			if err != nil || status != http.StatusOK || sentCount() != 3 {
				t.Fatal("ordinary conversation continuation failed")
			}
			if _, present := sent[2]["brokered_continuation_proof"]; present {
				t.Fatal("ordinary conversation continuation carried a proof")
			}
			for _, path := range []string{brokerapi.StatusPath, brokerapi.SettlePath, brokerapi.RetirePath} {
				control := brokerTestControl(t, server.URL, path, c)
				encoded, _ := json.Marshal(control)
				if bytes.Contains(encoded, []byte(cfg.agentKitProof)) {
					t.Fatal("continuation proof reached lifecycle status")
				}
			}
			ledger, err := os.ReadFile(filepath.Join(cfg.stateDir, "state.json"))
			if err != nil || bytes.Contains(ledger, []byte(cfg.agentKitProof)) || bytes.Contains(ledger, []byte("fixture tool result")) {
				t.Fatal("ledger retained continuation credentials or tool content")
			}
		})
	}
}

func TestBrokerAgentKitMalformedResultsFailBeforeReservation(t *testing.T) {
	f := newBrokerFixture(t, "functions")
	cfg := brokerTestConfig(t, f)
	cfg.agentKitProof = brokerAgentKitFixtureProof
	_, server := startBrokerTest(t, cfg)
	c := brokerTestContext(cfg)
	status, data, err := brokerTestHTTP(context.Background(), server.URL, brokerapi.ResponsesPath, c, brokerTestBody(""))
	var first foundry.Response
	if err != nil || status != http.StatusOK || json.Unmarshal(data, &first) != nil || len(first.Output) != 1 {
		t.Fatal("initial proposal failed")
	}
	before, err := os.ReadFile(filepath.Join(cfg.stateDir, "state.json"))
	if err != nil {
		t.Fatal("could not read fixture ledger")
	}
	c.InvocationSequence++
	for name, output := range map[string]string{
		"no_error_flag":       `{"content":[]}`,
		"null_error_flag":     `{"content":[],"isError":null}`,
		"string_error_flag":   `{"content":[],"isError":"false"}`,
		"number_error_flag":   `{"content":[],"isError":0}`,
		"no_content":          `{"isError":false}`,
		"null_content":        `{"content":null,"isError":false}`,
		"unsupported_content": `{"content":[{"type":"image","text":"hidden"}],"isError":false}`,
		"null_text":           `{"content":[{"type":"text","text":null}],"isError":false}`,
		"metadata":            `{"content":[],"isError":false,"_meta":{"private":"value"}}`,
		"untrusted_approval":  `{"approved":true,"output":{}}`,
		"duplicate_flag":      `{"content":[],"isError":true,"isError":false}`,
		"folded_flag":         `{"content":[],"isError":true,"ISERROR":false}`,
		"trailing_object":     `{"content":[],"isError":false}{}`,
		"invalid_unicode":     `{"content":[{"type":"text","text":"\ud800"}],"isError":false}`,
		"array_structured":    `{"content":[],"isError":false,"structuredContent":[]}`,
		"null_structured":     `{"content":[],"isError":false,"structuredContent":null}`,
	} {
		t.Run(name, func(t *testing.T) {
			c.OperationID = "invalid-" + name
			body := brokerAgentKitFunctionBody(first.ID, first.Output[0].CallID, output)
			status, _, err := brokerTestHTTP(context.Background(), server.URL, brokerapi.ResponsesPath, c, body)
			if err != nil || status != http.StatusBadRequest {
				t.Fatalf("invalid MCP result was accepted: status=%d", status)
			}
		})
	}
	after, err := os.ReadFile(filepath.Join(cfg.stateDir, "state.json"))
	_, inferences, _, _ := f.counts()
	if err != nil || !bytes.Equal(before, after) || inferences != 1 {
		t.Fatal("malformed tool output changed durable ownership or reached the provider")
	}
}

func TestBrokerRejectsChildSuppliedAgentKitProof(t *testing.T) {
	for _, configured := range []bool{false, true} {
		for _, where := range []string{"body", "folded_body", "empty_body", "header", "empty_header", "folded_header"} {
			t.Run(where+"/"+map[bool]string{false: "disabled", true: "enabled"}[configured], func(t *testing.T) {
				f := newBrokerFixture(t, "success")
				cfg := brokerTestConfig(t, f)
				if configured {
					cfg.agentKitProof = brokerAgentKitFixtureProof
				}
				b, _ := startBrokerTest(t, cfg)
				body := brokerTestBody("")
				switch where {
				case "body":
					body = append([]byte(`{"brokered_continuation_proof":"child-controlled",`), body[1:]...)
				case "folded_body":
					body = append([]byte(`{"Brokered_Continuation_Proof":"child-controlled",`), body[1:]...)
				case "empty_body":
					body = append([]byte(`{"brokered_continuation_proof":null,`), body[1:]...)
				}
				c := brokerTestContext(cfg)
				c.BodySHA256 = foundry.Digest(body)
				raw, _ := json.Marshal(c)
				r := httptest.NewRequest(http.MethodPost, brokerapi.ResponsesPath, bytes.NewReader(body))
				r.Header.Set("Authorization", "Bearer "+brokerFixtureBearer)
				r.Header.Set(brokerContextHeader, base64.RawURLEncoding.EncodeToString(raw))
				switch where {
				case "header":
					r.Header.Set(brokerAgentKitProofHeader, "child-controlled")
				case "empty_header":
					r.Header.Set(brokerAgentKitProofHeader, "")
				case "folded_header":
					r.Header["x-AgEnTkIt-brokered-continuation-proof"] = []string{"child-controlled"}
				}
				w := httptest.NewRecorder()
				b.ServeHTTP(w, r)
				if w.Code != http.StatusBadRequest || len(b.ledger.Sessions) != 0 {
					t.Fatal("child-controlled proof was admitted or reserved ownership")
				}
				creates, inferences, _, _ := f.counts()
				if creates+inferences != 0 || bytes.Contains(w.Body.Bytes(), []byte("child-controlled")) {
					t.Fatal("rejected proof reached the provider or error response")
				}
			})
		}
	}
}

func TestBrokerAgentKitProofConfiguration(t *testing.T) {
	f := newBrokerFixture(t, "success")
	cfg := brokerTestConfig(t, f)
	body, _ := json.Marshal(cfg.agent)
	path := filepath.Join(t.TempDir(), "foundry.json")
	if os.WriteFile(path, body, 0600) != nil {
		t.Fatal("could not write fixture config")
	}
	env := map[string]string{
		foundry.ModelEnv: cfg.agent.Model, foundry.AgentConfigDigestEnv: foundry.Digest(body),
		"ORKA_FOUNDRY_BROKER_STATE_DIR": cfg.stateDir, "ORKA_FOUNDRY_BROKER_BEARER_TOKEN": cfg.bearer,
	}
	for _, tc := range []struct {
		name  string
		proof string
		valid bool
	}{
		{"disabled", "", true}, {"configured", brokerAgentKitFixtureProof, true},
		{"short", "short", false}, {"space", strings.Repeat("x", 32) + " ", false},
		{"unicode_space", strings.Repeat("x", 32) + "\u00a0", false}, {"invalid_utf8", strings.Repeat("x", 32) + "\xff", false},
		{"control", strings.Repeat("x", 32) + "\n", false}, {"oversized", strings.Repeat("x", (16<<10)+1), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env[brokerAgentKitProofEnv] = tc.proof
			loaded, err := loadBrokerConfiguration(path, func(name string) string { return env[name] })
			if (err == nil) != tc.valid || (err == nil && loaded.agentKitProof != tc.proof) {
				t.Fatal("continuation proof configuration accepted an invalid value or lost the configured value")
			}
			if err != nil && tc.proof != "" && strings.Contains(err.Error(), tc.proof) {
				t.Fatal("configuration failure disclosed the proof")
			}
		})
	}
}
