package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/orka-agents/agent-runtime-foundry/internal/harness"
)

type staticFoundryTokenProvider string

func (p staticFoundryTokenProvider) AccessToken(context.Context) (string, error) {
	return string(p), nil
}

func TestParseFoundrySSETextAndCompletion(t *testing.T) {
	stream := strings.Join([]string{
		`data: {"type":"response.created","response":{"id":"resp-1","status":"in_progress","agent_session_id":"session-1"}}`,
		"",
		`data: {"type":"response.output_text.delta","delta":"hello "}`,
		"",
		`data: {"type":"response.output_text.delta","delta":"world"}`,
		"",
		`data: {"type":"response.completed","response":{"id":"resp-1","status":"completed","agent_session_id":"session-1"}}`,
		"",
	}, "\n")
	var deltas []string
	summary, err := parseFoundrySSE(strings.NewReader(stream), 1<<20, 1<<16, 32, responseCallbacks{
		OnTextDelta: func(delta string) error {
			deltas = append(deltas, delta)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("parseFoundrySSE: %v", err)
	}
	if summary.ResponseID != "resp-1" || summary.AgentSessionID != "session-1" || summary.Status != "completed" {
		t.Fatalf("summary = %#v", summary)
	}
	if got := strings.Join(deltas, ""); got != "hello world" {
		t.Fatalf("deltas = %q", got)
	}
}

func TestParseFoundrySSEFunctionCall(t *testing.T) {
	stream := "data: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"function_call\",\"call_id\":\"call-1\",\"name\":\"lookup\",\"arguments\":\"{\\\"id\\\":\\\"1\\\"}\"}}\n\n" +
		"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp-1\",\"status\":\"completed\"}}\n\n"
	var calls []foundryOutputItem
	summary, err := parseFoundrySSE(strings.NewReader(stream), 1<<20, 1<<16, 32, responseCallbacks{
		OnFunctionCall: func(call foundryOutputItem) error {
			calls = append(calls, call)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("parseFoundrySSE: %v", err)
	}
	if len(calls) != 1 || calls[0].CallID != "call-1" || calls[0].Name != "lookup" {
		t.Fatalf("calls = %#v", calls)
	}
	if len(summary.FunctionCalls) != 1 {
		t.Fatalf("summary calls = %#v", summary.FunctionCalls)
	}
}

func TestParseFoundryJSONFailedAndIncomplete(t *testing.T) {
	failed, err := parseFoundryJSON(strings.NewReader(`{"id":"resp-f","status":"failed","error":{"code":"server_error"}}`), 1<<20, responseCallbacks{})
	if err != nil {
		t.Fatalf("failed parse: %v", err)
	}
	if failed.Status != "failed" || failed.Error == nil || failed.Error.Code != "server_error" {
		t.Fatalf("failed = %#v", failed)
	}
	incomplete, err := parseFoundryJSON(strings.NewReader(`{"id":"resp-i","status":"incomplete","incomplete_details":{"reason":"max_output_tokens"}}`), 1<<20, responseCallbacks{})
	if err != nil {
		t.Fatalf("incomplete parse: %v", err)
	}
	if incomplete.Status != "incomplete" || incomplete.Incomplete == nil || incomplete.Incomplete.Reason != "max_output_tokens" {
		t.Fatalf("incomplete = %#v", incomplete)
	}
}

func TestParseFoundrySSERejectsMalformedAndOversizedStreams(t *testing.T) {
	if _, err := parseFoundrySSE(strings.NewReader("data: {not-json}\n\n"), 1024, 512, 8, responseCallbacks{}); err == nil {
		t.Fatal("expected malformed stream error")
	}
	large := "data: {\"type\":\"response.output_text.delta\",\"delta\":\"" + strings.Repeat("x", 1024) + "\"}\n\n"
	if _, err := parseFoundrySSE(strings.NewReader(large), 256, 2048, 8, responseCallbacks{}); err == nil {
		t.Fatal("expected oversized stream error")
	}
	if _, err := parseFoundrySSE(strings.NewReader("data: {\"type\":\"response.output_text.delta\",\"delta\":\"x\"}\n\n"), 1024, 512, 8, responseCallbacks{}); err == nil {
		t.Fatal("expected missing terminal event error")
	}
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
	_, err := client.createResponse(ctx, foundryResponseRequest{
		Input:              []foundryFunctionOutput{{Type: "function_call_output", CallID: "call-1", Output: `{"ok":true}`}},
		PreviousResponseID: "resp-1",
		AgentSessionID:     "session-1",
		Tools:              []foundryToolSchema{{Type: "function", Name: "lookup", Parameters: json.RawMessage(`{"type":"object"}`)}},
	}, responseCallbacks{})
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

func TestParseFoundrySSETerminalOutputFallback(t *testing.T) {
	stream := strings.Join([]string{
		`data: {"type":"response.created","response":{"id":"resp-fallback","status":"in_progress"}}`,
		"",
		`data: {"type":"response.completed","response":{"id":"resp-fallback","status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"fallback text"}]},{"type":"function_call","call_id":"call-fallback","name":"lookup","arguments":"{}"}]}}`,
		"",
	}, "\n")
	var text strings.Builder
	var calls []foundryOutputItem
	summary, err := parseFoundrySSE(strings.NewReader(stream), 1<<20, 1<<16, 32, responseCallbacks{
		OnTextDelta: func(delta string) error {
			text.WriteString(delta)
			return nil
		},
		OnFunctionCall: func(call foundryOutputItem) error {
			calls = append(calls, call)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("parseFoundrySSE: %v", err)
	}
	if text.String() != "fallback text" || summary.Text != "fallback text" {
		t.Fatalf("text callback=%q summary=%q", text.String(), summary.Text)
	}
	if len(calls) != 1 || calls[0].CallID != "call-fallback" {
		t.Fatalf("calls = %#v", calls)
	}
}
