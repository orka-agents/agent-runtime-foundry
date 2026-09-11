package foundry

import (
	"strings"
	"testing"
)

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
	summary, err := ParseSSE(strings.NewReader(stream), 1<<20, 1<<16, 32, ResponseCallbacks{
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
	var calls []OutputItem
	summary, err := ParseSSE(strings.NewReader(stream), 1<<20, 1<<16, 32, ResponseCallbacks{
		OnFunctionCall: func(call OutputItem) error {
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
	failed, err := ParseJSON(strings.NewReader(`{"id":"resp-f","status":"failed","error":{"code":"server_error"}}`), 1<<20, ResponseCallbacks{})
	if err != nil {
		t.Fatalf("failed parse: %v", err)
	}
	if failed.Status != "failed" || failed.Error == nil || failed.Error.Code != "server_error" {
		t.Fatalf("failed = %#v", failed)
	}
	incomplete, err := ParseJSON(strings.NewReader(`{"id":"resp-i","status":"incomplete","incomplete_details":{"reason":"max_output_tokens"}}`), 1<<20, ResponseCallbacks{})
	if err != nil {
		t.Fatalf("incomplete parse: %v", err)
	}
	if incomplete.Status != "incomplete" || incomplete.Incomplete == nil || incomplete.Incomplete.Reason != "max_output_tokens" {
		t.Fatalf("incomplete = %#v", incomplete)
	}
}

func TestParseFoundrySSERejectsMalformedAndOversizedStreams(t *testing.T) {
	if _, err := ParseSSE(strings.NewReader("data: {not-json}\n\n"), 1024, 512, 8, ResponseCallbacks{}); err == nil {
		t.Fatal("expected malformed stream error")
	}
	large := "data: {\"type\":\"response.output_text.delta\",\"delta\":\"" + strings.Repeat("x", 1024) + "\"}\n\n"
	if _, err := ParseSSE(strings.NewReader(large), 256, 2048, 8, ResponseCallbacks{}); err == nil {
		t.Fatal("expected oversized stream error")
	}
	if _, err := ParseSSE(strings.NewReader("data: {\"type\":\"response.output_text.delta\",\"delta\":\"x\"}\n\n"), 1024, 512, 8, ResponseCallbacks{}); err == nil {
		t.Fatal("expected missing terminal event error")
	}
}

func TestParseFoundrySSETerminalOutputFallback(t *testing.T) {
	stream := strings.Join([]string{
		`data: {"type":"response.created","response":{"id":"resp-fallback","status":"in_progress"}}`,
		"",
		`data: {"type":"response.completed","response":{"id":"resp-fallback","status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"fallback text"}]},{"type":"function_call","call_id":"call-fallback","name":"lookup","arguments":"{}"}]}}`,
		"",
	}, "\n")
	var text strings.Builder
	var calls []OutputItem
	summary, err := ParseSSE(strings.NewReader(stream), 1<<20, 1<<16, 32, ResponseCallbacks{
		OnTextDelta: func(delta string) error {
			text.WriteString(delta)
			return nil
		},
		OnFunctionCall: func(call OutputItem) error {
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
