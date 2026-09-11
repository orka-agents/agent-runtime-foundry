package adapter

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/orka-agents/agent-runtime-foundry/internal/foundry"
	"github.com/orka-agents/agent-runtime-foundry/internal/harness"
)

type staticFoundryTokenProvider string

func (p staticFoundryTokenProvider) AccessToken(context.Context) (string, error) {
	return string(p), nil
}

func TestFoundryResponsesClientRequestShapeAndHeaders(t *testing.T) {
	var requestBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/projects/demo/agents/hosted-agent/endpoint/protocols/openai/responses" {
			t.Fatalf("path = %q", r.URL.Path)
		}
		if r.URL.Query().Get("api-version") != "v1" {
			t.Fatalf("api-version = %q", r.URL.Query().Get("api-version"))
		}
		if got := r.Header.Get("Authorization"); got != "Bearer mock-token" {
			t.Fatalf("authorization = %q", got)
		}
		if got := r.Header.Get("Foundry-Features"); got != "HostedAgents=V1Preview" {
			t.Fatalf("feature header = %q", got)
		}
		if got := r.Header.Get("x-ms-user-isolation-key"); got != "isolation-1" {
			t.Fatalf("isolation header = %q", got)
		}
		if err := json.NewDecoder(r.Body).Decode(&requestBody); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp-2\",\"status\":\"in_progress\",\"agent_session_id\":\"session-2\"}}\n\n")
		_, _ = io.WriteString(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp-2\",\"status\":\"completed\",\"agent_session_id\":\"session-2\"}}\n\n")
	}))
	defer server.Close()

	cfg := testConfig(server.URL)
	cfg.isolationMode = "header"
	client := testResponsesClient(cfg, server.URL)
	ctx := withFoundryIsolationKey(context.Background(), "isolation-1")
	_, err := client.createResponse(ctx, foundry.ResponseRequest{
		Input:              []foundry.FunctionOutput{{Type: "function_call_output", CallID: "call-1", Output: `{"ok":true}`}},
		PreviousResponseID: "resp-1",
		AgentSessionID:     "session-1",
		Tools:              []foundry.ToolSchema{{Type: "function", Name: "lookup", Parameters: json.RawMessage(`{"type":"object"}`)}},
	}, foundry.ResponseCallbacks{})
	if err != nil {
		t.Fatalf("createResponse: %v", err)
	}
	if requestBody["stream"] != true || requestBody["store"] != true || requestBody["previous_response_id"] != "resp-1" || requestBody["agent_session_id"] != "session-1" {
		t.Fatalf("request body = %#v", requestBody)
	}
	input, ok := requestBody["input"].([]any)
	if !ok || len(input) != 1 || input[0].(map[string]any)["type"] != "function_call_output" {
		t.Fatalf("input = %#v", requestBody["input"])
	}
	tools, ok := requestBody["tools"].([]any)
	if !ok || len(tools) != 1 || tools[0].(map[string]any)["name"] != "lookup" {
		t.Fatalf("tools = %#v", requestBody["tools"])
	}
}

func TestFoundryResponsesClientVersionValidation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/projects/demo/agents/hosted-agent":
			_, _ = io.WriteString(w, `{"name":"hosted-agent"}`)
		case "/api/projects/demo/agents/hosted-agent/versions/2":
			_, _ = io.WriteString(w, `{"status":"active"}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	cfg := testConfig(server.URL)
	cfg.agentVersion = "2"
	client := testResponsesClient(cfg, server.URL)
	if err := client.validateAgent(context.Background()); err != nil {
		t.Fatalf("validateAgent: %v", err)
	}
}

func TestProviderSafeMessageRedactsOpaqueFailures(t *testing.T) {
	if got := providerSafeMessage(errors.New("token is super-secret")); got != "Foundry request failed" {
		t.Fatalf("message = %q", got)
	}
}

func testConfig(baseURL string) config {
	return config{
		addr:                 ":0",
		runtimeName:          "foundry-test",
		adapterBearer:        "adapter-token",
		projectEndpoint:      baseURL + "/api/projects/demo",
		agentName:            "hosted-agent",
		apiVersion:           "v1",
		turnTimeout:          5 * time.Second,
		isolationMode:        "entra",
		foundryFeatures:      "HostedAgents=V1Preview",
		maxOutputBytes:       1 << 20,
		maxStreamBytes:       1 << 20,
		maxEventBytes:        1 << 16,
		maxBrokeredBytes:     1 << 16,
		maxBrokeredTurnBytes: 1 << 20,
		maxBrokeredCalls:     128,
		maxEvents:            128,
		maxConcurrent:        1,
		brokeredToolClasses: []harness.BrokeredToolClass{
			harness.BrokeredToolClassRead,
			harness.BrokeredToolClassWrite,
		},
	}
}

func testResponsesClient(cfg config, serverURL string) *foundryResponsesClient {
	return &foundryResponsesClient{cfg, newFoundryHTTPClient(serverURL), staticFoundryTokenProvider("mock-token")}
}
