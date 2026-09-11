package broker

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/orka-agents/agent-runtime-foundry/internal/brokerapi"
	"github.com/orka-agents/agent-runtime-foundry/internal/foundry"
)

const brokerCaseToolSchema = `[{"type":"function","name":"hosted-probe-read","parameters":{"type":"object","properties":{},"additionalProperties":false}}]`

func brokerCaseRequest(t *testing.T, b *lifecycleBroker, c brokerContext, members string) *httptest.ResponseRecorder {
	t.Helper()
	body := brokerTestBody("")
	if members != "" {
		body = append([]byte("{"+members+","), body[1:]...)
	}
	c.BodySHA256 = foundry.Digest(body)
	raw, err := json.Marshal(c)
	if err != nil {
		t.Fatal("could not encode fixture context")
	}
	request := httptest.NewRequest(http.MethodPost, brokerapi.ResponsesPath, bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer "+brokerFixtureBearer)
	request.Header.Set(brokerContextHeader, base64.RawURLEncoding.EncodeToString(raw))
	response := httptest.NewRecorder()
	b.ServeHTTP(response, request)
	return response
}

func brokerCaseRejectBeforeOwnership(t *testing.T, mode, members string) {
	t.Helper()
	f := newBrokerFixture(t, "success")
	cfg := brokerTestConfig(t, f)
	cfg.agent.ToolSchemaMode = mode
	cfg.configDigest = foundry.JSONDigest(cfg.agent)
	var tokenCalls, transportCalls atomic.Int64
	provider := brokerEvidenceTokenProvider(func(context.Context) (string, error) {
		tokenCalls.Add(1)
		return brokerTestToken(), nil
	})
	client := &http.Client{Transport: brokerFixtureTransport(func(r *http.Request) (*http.Response, error) {
		transportCalls.Add(1)
		return http.DefaultTransport.RoundTrip(r)
	})}
	b, err := newLifecycleBroker(context.Background(), cfg, provider, client)
	if err != nil {
		t.Fatal("could not start protected-field fixture")
	}
	t.Cleanup(b.close)
	path := filepath.Join(cfg.stateDir, "state.json")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal("could not capture initial fixture ledger")
	}
	response := brokerCaseRequest(t, b, brokerTestContext(cfg), members)
	if response.Code != http.StatusBadRequest {
		t.Errorf("protected field was not rejected: status=%d", response.Code)
	}
	if tokenCalls.Load() != 0 || transportCalls.Load() != 0 {
		t.Errorf("protected field reached authentication or transport: tokenCalls=%d transportCalls=%d", tokenCalls.Load(), transportCalls.Load())
	}
	b.mu.Lock()
	owners, active := len(b.ledger.Sessions), len(b.active)
	b.mu.Unlock()
	if owners != 0 || active != 0 {
		t.Errorf("protected field reserved ownership: owners=%d active=%d", owners, active)
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Error("protected field changed the durable ledger")
	}
	creates, inferences, stops, deletes := f.counts()
	if creates+inferences+stops+deletes != 0 {
		t.Error("protected field reached a provider mutation")
	}
}

func TestBrokerProtectedFieldsRejectDecoderCaseVariantsBeforeOwnership(t *testing.T) {
	fields := []struct {
		name   string
		keys   []string
		values []string
		modes  []string
	}{
		{"tools", []string{`"tools"`, `"Tools"`, `"TOOLS"`, `"tOoLs"`, `"toolſ"`, `"TOOLſ"`, `"\u0054ools"`, `"tool\u017f"`},
			[]string{brokerCaseToolSchema, `[]`, `null`}, []string{foundry.ToolSchemaModeProviderStatic}},
		{"agent_session_id", []string{`"agent_session_id"`, `"Agent_Session_ID"`, `"AGENT_SESSION_ID"`, `"aGeNt_sEsSiOn_iD"`, `"agent_ſeſſion_id"`, `"\u0041gent_session_id"`, `"agent_\u017fe\u017f\u017fion_id"`},
			[]string{`"fixture-injected-session"`, `""`, `null`}, []string{foundry.ToolSchemaModeProviderStatic, foundry.ToolSchemaModeRequest}},
	}
	for _, field := range fields {
		for _, mode := range field.modes {
			for _, key := range field.keys {
				for i, value := range field.values {
					t.Run(field.name+"/"+mode+"/"+key+"/"+[]string{"value", "empty", "null"}[i], func(t *testing.T) {
						brokerCaseRejectBeforeOwnership(t, mode, key+":"+value)
					})
				}
			}
		}
	}
}

func TestBrokerProtectedFieldsRejectDuplicatePresenceBeforeOwnership(t *testing.T) {
	for _, tc := range []struct {
		name    string
		members string
		modes   []string
	}{
		{"tools_folded_null_last", `"Tools":` + brokerCaseToolSchema + `,"TOOLS":null`, []string{foundry.ToolSchemaModeProviderStatic}},
		{"tools_folded_value_last", `"Tools":null,"TOOLS":` + brokerCaseToolSchema, []string{foundry.ToolSchemaModeProviderStatic}},
		{"tools_folded_empty_last", `"Tools":` + brokerCaseToolSchema + `,"TOOLſ":[]`, []string{foundry.ToolSchemaModeProviderStatic}},
		{"tools_canonical_null", `"tools":null,"Tools":` + brokerCaseToolSchema, []string{foundry.ToolSchemaModeProviderStatic}},
		{"tools_exact_duplicate", `"Tools":null,"Tools":[]`, []string{foundry.ToolSchemaModeProviderStatic}},
		{"tools_escaped_duplicate", `"Tools":null,"\u0054ools":[]`, []string{foundry.ToolSchemaModeProviderStatic}},
		{"session_folded_null_last", `"Agent_Session_ID":"fixture-injected-session","AGENT_SESSION_ID":null`, []string{foundry.ToolSchemaModeProviderStatic, foundry.ToolSchemaModeRequest}},
		{"session_folded_value_last", `"Agent_Session_ID":null,"AGENT_SESSION_ID":"fixture-injected-session"`, []string{foundry.ToolSchemaModeProviderStatic, foundry.ToolSchemaModeRequest}},
		{"session_folded_empty_last", `"Agent_Session_ID":"fixture-injected-session","agent_ſeſſion_id":""`, []string{foundry.ToolSchemaModeProviderStatic, foundry.ToolSchemaModeRequest}},
		{"session_canonical_null", `"agent_session_id":null,"Agent_Session_ID":""`, []string{foundry.ToolSchemaModeProviderStatic, foundry.ToolSchemaModeRequest}},
		{"session_exact_duplicate", `"Agent_Session_ID":null,"Agent_Session_ID":""`, []string{foundry.ToolSchemaModeProviderStatic, foundry.ToolSchemaModeRequest}},
		{"session_escaped_duplicate", `"Agent_Session_ID":null,"\u0041gent_Session_ID":""`, []string{foundry.ToolSchemaModeProviderStatic, foundry.ToolSchemaModeRequest}},
	} {
		for _, mode := range tc.modes {
			t.Run(tc.name+"/"+mode, func(t *testing.T) {
				brokerCaseRejectBeforeOwnership(t, mode, tc.members)
			})
		}
	}
}

func TestBrokerRequestToolCaseVariantsRemainValid(t *testing.T) {
	for _, tc := range []struct {
		name    string
		mode    string
		members string
		tools   int
	}{
		{"static_omitted", foundry.ToolSchemaModeProviderStatic, "", 0},
		{"request_omitted", foundry.ToolSchemaModeRequest, "", 0},
		{"canonical", foundry.ToolSchemaModeRequest, `"tools":` + brokerCaseToolSchema, 1},
		{"title", foundry.ToolSchemaModeRequest, `"Tools":` + brokerCaseToolSchema, 1},
		{"upper", foundry.ToolSchemaModeRequest, `"TOOLS":` + brokerCaseToolSchema, 1},
		{"mixed", foundry.ToolSchemaModeRequest, `"tOoLs":` + brokerCaseToolSchema, 1},
		{"unicode_fold", foundry.ToolSchemaModeRequest, `"toolſ":` + brokerCaseToolSchema, 1},
		{"escaped", foundry.ToolSchemaModeRequest, `"\u0054ools":` + brokerCaseToolSchema, 1},
		{"escaped_unicode_fold", foundry.ToolSchemaModeRequest, `"tool\u017f":` + brokerCaseToolSchema, 1},
		{"empty", foundry.ToolSchemaModeRequest, `"Tools":[]`, 0},
		{"null", foundry.ToolSchemaModeRequest, `"TOOLS":null`, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newBrokerFixture(t, "success")
			cfg := brokerTestConfig(t, f)
			cfg.agent.ToolSchemaMode = tc.mode
			cfg.configDigest = foundry.JSONDigest(cfg.agent)
			b, server := startBrokerTest(t, cfg)
			c := brokerTestContext(cfg)
			if response := brokerCaseRequest(t, b, c, tc.members); response.Code != http.StatusOK {
				t.Fatalf("valid tool mode request failed: status=%d", response.Code)
			}
			f.mu.Lock()
			if len(f.requests) != 1 || len(f.requests[0].Tools) != tc.tools {
				t.Error("valid request did not preserve its tool schemas")
			} else if tc.tools != 0 {
				encoded, err := json.Marshal(f.requests[0].Tools)
				if err != nil || string(encoded) != brokerCaseToolSchema {
					t.Error("valid request changed its tool schema")
				}
			}
			f.mu.Unlock()
			creates, inferences, _, _ := f.counts()
			if creates != 1 || inferences != 1 {
				t.Error("valid request did not submit exactly one owned inference")
			}
			_ = brokerTestControl(t, server.URL, brokerapi.SettlePath, c)
			_ = brokerTestControl(t, server.URL, brokerapi.RetirePath, c)
		})
	}
}
