package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/orka-agents/agent-runtime-foundry/internal/brokerapi"
	"github.com/orka-agents/agent-runtime-foundry/internal/foundry"
)

func TestBrokerResponseFailureDiagnostics(t *testing.T) {
	for _, test := range []struct {
		name       string
		stage      string
		terminal   string
		code       string
		upstream   int
		frames     int
		accepted   bool
		complete   bool
		successful bool
	}{
		{name: "failed", stage: "strict_decode", terminal: "failed", code: "ModelAuthRejected", upstream: 403, frames: 1, accepted: true, complete: true},
		{name: "json-failed", stage: "response_terminal", terminal: "failed", code: "ModelAuthRejected", upstream: 403, frames: 1, accepted: true},
		{name: "unknown-code", stage: "strict_decode", terminal: "failed", code: "unknown", frames: 1, accepted: true, complete: true},
		{name: "incomplete", stage: "strict_decode", terminal: "incomplete", frames: 1, accepted: true, complete: true},
		{name: "cancelled", stage: "strict_decode", terminal: "cancelled", frames: 1, accepted: true, complete: true},
		{name: "malformed-success", stage: "strict_decode", terminal: "completed", frames: 1, accepted: true, complete: true},
		{name: "wrong-identity", stage: "stream_read"},
		{name: "missing-terminal", stage: "strict_decode", accepted: true, complete: true},
		{name: "malformed-tail", stage: "stream_read", accepted: true},
		{name: "multiple-terminals", stage: "strict_decode", terminal: "multiple", frames: 2, accepted: true, complete: true},
		{name: "success", successful: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newBrokerEvidenceFixture(t, func(request foundry.ResponseRequest) (string, []byte) {
				response := map[string]any{
					"id": "provider-response-do-not-log", "status": "in_progress",
					"agent_session_id": request.AgentSessionID, "output": []any{},
				}
				frame := func(kind string) string {
					raw, _ := json.Marshal(map[string]any{"type": kind, "response": response})
					return testSSE(string(raw))
				}
				created := frame("response.created")
				response["status"] = "failed"
				response["error"] = map[string]any{
					"code": "ModelAuthRejected", "message": "provider-message-do-not-log",
					"upstream_status": 403, "request_id": "provider-request-do-not-log",
				}
				switch test.name {
				case "json-failed":
					raw, _ := json.Marshal(response)
					return "application/json", raw
				case "unknown-code":
					response["error"].(map[string]any)["code"] = "Bearer credential-shaped-code-do-not-log"
				case "incomplete":
					delete(response, "error")
					response["status"] = "incomplete"
					response["incomplete_details"] = map[string]any{"reason": "provider-reason-do-not-log"}
				case "cancelled":
					delete(response, "error")
					response["status"] = "cancelled"
				case "malformed-success":
					delete(response, "error")
					response["status"] = "completed"
					response["output"] = []any{map[string]any{"type": "untrusted_native_tool", "name": "provider-native-tool-do-not-log"}}
				case "wrong-identity":
					response["agent_session_id"] = "provider-unowned-session-do-not-log"
					return "text/event-stream", []byte(frame("response.failed"))
				case "missing-terminal":
					return "text/event-stream", []byte(created)
				case "malformed-tail":
					return "text/event-stream", []byte(created + "data: {\n\n")
				case "multiple-terminals":
					return "text/event-stream", []byte(created + frame("response.failed") + frame("response.failed"))
				case "success":
					delete(response, "error")
					response["status"] = "completed"
					response["output"] = []any{map[string]any{"type": "message", "role": "assistant", "content": []any{map[string]any{"type": "output_text", "text": "provider-success-do-not-log"}}}}
				}
				return "text/event-stream", []byte(created + frame("response."+response["status"].(string)))
			})
			cfg := brokerTestConfig(t, f)
			b, server := startBrokerTest(t, cfg)
			var logs bytes.Buffer
			b.diagnosticLog = slog.New(slog.NewJSONHandler(&logs, nil))
			c := brokerTestContext(cfg)
			status, body, err := brokerTestHTTP(context.Background(), server.URL, brokerapi.ResponsesPath, c, brokerTestBody(""))
			if err != nil || (status == http.StatusOK) != test.successful {
				t.Fatal("diagnostics changed inference outcome")
			}
			if test.successful {
				if logs.Len() != 0 {
					t.Fatal("successful inference emitted a failure diagnostic")
				}
				return
			}
			record := decodeBrokerDiagnostic(t, logs.Bytes())
			if record["stage"] != test.stage || record["response_acknowledged"] != test.accepted ||
				record["stream_complete"] != test.complete || record["terminal_frames"] != float64(test.frames) ||
				record["http_status"] != float64(200) || record["invocation_sequence"] != float64(1) ||
				record["owner_digest"] != foundry.JSONDigest(c.Owner) {
				t.Fatal("diagnostic did not identify the bounded failure boundary")
			}
			if terminal, _ := record["observed_terminal_status"].(string); terminal != test.terminal {
				t.Fatal("diagnostic conflated provider failure and malformed completion")
			}
			if code, _ := record["error_code"].(string); code != test.code {
				t.Fatal("diagnostic did not preserve only the allowlisted error code")
			}
			if upstream, _ := record["upstream_status"].(float64); upstream != float64(test.upstream) {
				t.Fatal("diagnostic did not bound the upstream status")
			}
			for _, forbidden := range []string{
				"do-not-log", brokerFixtureBearer, brokerTestToken(), "fixture-input-do-not-persist",
				c.Owner.RuntimeSessionUID, c.TaskUID, c.PromptID, f.server.URL,
			} {
				if bytes.Contains(logs.Bytes(), []byte(forbidden)) {
					t.Fatal("diagnostic exposed provider or owner data")
				}
			}
			if bytes.Contains(body, []byte("ModelAuthRejected")) || bytes.Contains(body, []byte("upstream_status")) {
				t.Fatal("diagnostics changed the child error envelope")
			}
			if !test.accepted {
				if brokerInvocationState(b, c) != "uncertain" {
					t.Fatal("diagnostics fabricated remote acknowledgement")
				}
				brokerPendingControl(t, server.URL, brokerapi.SettlePath, c, false, 1)
				return
			}
			proof := brokerTestControl(t, server.URL, brokerapi.SettlePath, c)
			if !proof.SettlementProven || proof.AmbiguousInvocations != 0 {
				t.Fatal("diagnostics changed acknowledged failure settlement")
			}
			proof = brokerTestControl(t, server.URL, brokerapi.RetirePath, c)
			if !proof.RetirementProven {
				t.Fatal("diagnostics changed owner retirement")
			}
			_ = decodeBrokerDiagnostic(t, logs.Bytes())
		})
	}
}

func decodeBrokerDiagnostic(t *testing.T, data []byte) map[string]any {
	t.Helper()
	var record map[string]any
	decoder := json.NewDecoder(bytes.NewReader(data))
	if decoder.Decode(&record) != nil {
		t.Fatal("failure did not emit a JSON diagnostic")
	}
	var extra any
	if !errors.Is(decoder.Decode(&extra), io.EOF) {
		t.Fatal("one rejected invocation emitted multiple diagnostics")
	}
	allowed := map[string]bool{}
	for _, field := range []string{"time", "level", "msg", "stage", "owner_digest", "invocation_sequence", "response_acknowledged", "stream_complete", "terminal_frames", "http_status", "observed_terminal_status", "error_code", "upstream_status"} {
		allowed[field] = true
	}
	for field := range record {
		if !allowed[field] {
			t.Fatal("diagnostic emitted an unreviewed field")
		}
	}
	return record
}

func TestBrokerResponseDiagnosticRejectsUnsafeStatusMetadata(t *testing.T) {
	for _, raw := range []string{`"403"`, `403.0`, `true`, `null`, `399`, `600`, `{"secret":"do-not-log"}`} {
		t.Run(raw, func(t *testing.T) {
			diagnostic := brokerResponseDiagnostic{}
			data := []byte(`{"error":{"code":"ModelAuthRejected","message":"do-not-log","upstream_status":` + raw + `}}`)
			diagnostic.observe(data, foundry.Response{Status: "failed", Error: &foundry.ResponseError{Code: "ModelAuthRejected"}})
			if diagnostic.upstreamStatus != 0 || diagnostic.errorCode != "ModelAuthRejected" {
				t.Fatal("untrusted status metadata entered the diagnostic")
			}
		})
	}
}

func TestBrokerResponseDiagnosticDoesNotClaimFailedIdentityWrite(t *testing.T) {
	b, c := newBrokerResponseIdentityFixture(t)
	before := brokerIdentityLedgerBytes(t, b)
	if err := os.Rename(b.store.dir, b.store.dir+"-unavailable"); err != nil {
		t.Fatal("could not inject identity persistence failure")
	}
	diagnostic := brokerResponseDiagnostic{}
	frame := testSSE(`{"type":"response.failed","response":{"id":"provider-do-not-log","status":"failed","error":{"code":"ModelAuthRejected","message":"do-not-log","upstream_status":403}}}`)
	_, err := b.readTrackedStream(strings.NewReader(frame), c, brokerIdentityRemoteSession, &diagnostic, nil)
	if !errors.Is(err, errBrokerStorage) || diagnostic.accepted || diagnostic.errorCode != "" || diagnostic.terminalFrames != 0 {
		t.Fatal("diagnostics claimed acceptance after failed persistence")
	}
	if foundry.Digest(before) != foundry.JSONDigest(b.ledger) {
		t.Fatal("diagnostics changed failed-write authority")
	}
}

func TestBrokerResponseDiagnosticDistinguishesHTTPAndTransportFailure(t *testing.T) {
	for _, test := range []struct {
		name       string
		stage      string
		httpStatus int
		state      string
	}{
		{name: "rejected", stage: "http_response", httpStatus: 429, state: "rejected"},
		{name: "rejected-server", stage: "http_response", httpStatus: 503, state: "uncertain"},
		{name: "transport", stage: "transport", state: "uncertain"},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newBrokerFixture(t, test.name)
			if test.name == "transport" {
				f.server.Close()
				f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/protocols/openai/responses") {
						f.serve(w, r)
						return
					}
					_, _ = io.Copy(io.Discard, r.Body)
					connection, _, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Error("could not inject response transport loss")
						return
					}
					_ = connection.Close()
				}))
			}
			cfg := brokerTestConfig(t, f)
			b, server := startBrokerTest(t, cfg)
			var logs bytes.Buffer
			b.diagnosticLog = slog.New(slog.NewJSONHandler(&logs, nil))
			c := brokerTestContext(cfg)
			status, _, err := brokerTestHTTP(context.Background(), server.URL, brokerapi.ResponsesPath, c, brokerTestBody(""))
			state := brokerInvocationState(b, c)
			validState := state == test.state || (test.state == "rejected" && state == "settled")
			if err != nil || status == http.StatusOK || !validState {
				t.Fatal("diagnostics changed rejected or uncertain ownership")
			}
			record := decodeBrokerDiagnostic(t, logs.Bytes())
			httpStatus, _ := record["http_status"].(float64)
			if record["stage"] != test.stage || httpStatus != float64(test.httpStatus) || record["response_acknowledged"] != false {
				t.Fatal("diagnostic conflated HTTP rejection and transport loss")
			}
			if test.state == "uncertain" {
				brokerPendingControl(t, server.URL, brokerapi.SettlePath, c, false, 1)
			} else if !brokerTestControl(t, server.URL, brokerapi.SettlePath, c).SettlementProven {
				t.Fatal("definite HTTP rejection could not settle")
			}
		})
	}
}
