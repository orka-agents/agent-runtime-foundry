package acp

import (
	"encoding/json"
	"net/http"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orka-agents/agent-runtime-foundry/internal/foundry"
)

func acpTestCall(name, id, arguments string) foundry.OutputItem {
	encoded, _ := json.Marshal(arguments)
	return foundry.OutputItem{Type: "function_call", CallID: id, Name: name, Arguments: encoded}
}

func TestACPStdioSuccessfulContinuationAndLargeUnicodeOutput(t *testing.T) {
	large := strings.Repeat("héllo 世界 🔒\n", 18000)
	var requests atomic.Int32
	mcp := &acpTestMCP{}
	peer := newACPTestPeer(t, foundry.ToolSchemaModeRequest, func(w http.ResponseWriter, r *http.Request) {
		body := acpTestReadProvider(t, r)
		switch requests.Add(1) {
		case 1:
			if _, exists := body["previous_response_id"]; exists {
				t.Error("fresh child replayed provider history")
			}
			acpTestCompleted(w, "opaque-first", large)
		case 2:
			if string(body["previous_response_id"]) != `"opaque-first"` {
				t.Error("successful continuation lost previous response")
			}
			if string(body["input"]) != `"continue"` {
				t.Error("continuation replayed earlier prompt content")
			}
			acpTestCompleted(w, "opaque-second", "done")
		default:
			t.Error("unexpected provider request")
			w.WriteHeader(http.StatusInternalServerError)
		}
	}, mcp)
	acpAssertStop(t, peer.reply(peer.prompt("first")), "end_turn")
	if acpOutput(peer.events) != large {
		t.Fatal("Unicode output was split incorrectly")
	}
	chunks := len(peer.events)
	if chunks < 2 {
		t.Fatal("large output was not bounded into multiple ACP frames")
	}
	for _, event := range peer.events {
		if len(event["content"].(map[string]any)["text"].(string)) > acpTextChunkBytes {
			t.Fatal("ACP chunk exceeded byte bound")
		}
	}
	peer.events = nil
	acpAssertStop(t, peer.reply(peer.prompt("continue")), "end_turn")
	if acpOutput(peer.events) != "done" || mcp.lists.Load() != 2 || requests.Load() != 2 || mcp.calls.Load() != 0 {
		t.Fatal("continuation did not use one fresh discovery and one provider request")
	}
}

func TestACPStaticToolsExactArgumentsConcurrentOutputAndFreshAllowlist(t *testing.T) {
	const nested = `{"text":"héllo 世界 🌍","nested":{"items":["é","日本語",{"deeper":["🔒",null,false]}],"count":42,"enabled":true}}`
	var requests atomic.Int32
	var revoked atomic.Bool
	secondStarted := make(chan struct{})
	firstCompleted := make(chan struct{})
	mcp := &acpTestMCP{
		tools: func() []map[string]any {
			if revoked.Load() {
				return acpTestTools()
			}
			return acpTestTools("echo", "quick")
		},
		execute: func(w http.ResponseWriter, r *http.Request, id json.RawMessage, name string, args json.RawMessage) {
			switch name {
			case "echo":
				var want, got any
				_ = json.Unmarshal([]byte(nested), &want)
				_ = json.Unmarshal(args, &got)
				if !reflect.DeepEqual(want, got) {
					t.Error("nested Unicode tool arguments changed")
				}
				select {
				case <-secondStarted:
				case <-r.Context().Done():
					return
				}
				acpTestToolResult(w, id, string(args), false)
				close(firstCompleted)
			case "quick":
				close(secondStarted)
				acpTestToolResult(w, id, `{"quick":true}`, false)
			default:
				t.Error("unlisted tool executed")
			}
		},
	}
	peer := newACPTestPeer(t, foundry.ToolSchemaModeProviderStatic, func(w http.ResponseWriter, r *http.Request) {
		body := acpTestReadProvider(t, r)
		if _, exists := body["tools"]; exists {
			t.Error("static mode sent request-level tool schemas")
		}
		switch requests.Add(1) {
		case 1:
			acpTestCompleted(w, "tools-response", "", acpTestCall("echo", "provider-call-private-1", nested), acpTestCall("quick", "provider-call-private-2", `{}`))
		case 2:
			select {
			case <-firstCompleted:
			default:
				t.Error("provider resumed before all tool calls joined")
			}
			var outputs []foundry.FunctionOutput
			if json.Unmarshal(body["input"], &outputs) != nil || len(outputs) != 2 {
				t.Error("invalid function output continuation")
			}
			for i, output := range outputs {
				if output.Type != "function_call_output" || output.CallID != []string{"provider-call-private-1", "provider-call-private-2"}[i] {
					t.Error("function output correlation changed")
				}
			}
			if string(body["previous_response_id"]) != `"tools-response"` {
				t.Error("tool continuation lost provider checkpoint")
			}
			acpTestCompleted(w, "tools-final", "finished")
		case 3:
			if string(body["previous_response_id"]) != `"tools-final"` {
				t.Error("prompt continuation lost final checkpoint")
			}
			acpTestCompleted(w, "forbidden-call", "", acpTestCall("echo", "forbidden", `{}`))
		default:
			t.Error("provider call replayed")
			w.WriteHeader(http.StatusInternalServerError)
		}
	}, mcp)
	acpAssertStop(t, peer.reply(peer.prompt("nested batch")), "end_turn")
	acpAssertToolEvents(t, peer.events, 2, 0)
	encoded, _ := json.Marshal(peer.events)
	if strings.Contains(string(encoded), "provider-call-private") || strings.Contains(string(encoded), "héllo") || strings.Contains(string(encoded), "test-only") {
		t.Fatal("tool lifecycle exposed provider IDs, arguments, or credentials")
	}
	revoked.Store(true)
	peer.events = nil
	acpAssertFailure(t, peer.reply(peer.prompt("fresh allowlist")))
	if mcp.lists.Load() != 2 || mcp.calls.Load() != 2 || requests.Load() != 3 || len(peer.events) != 0 {
		t.Fatal("stale allowed tool was executed or replayed")
	}
}

func TestACPMCPAdmittedErrorRemainsRecoverable(t *testing.T) {
	var requests atomic.Int32
	mcp := &acpTestMCP{tools: func() []map[string]any { return acpTestTools("probe") }}
	mcp.execute = func(w http.ResponseWriter, _ *http.Request, id json.RawMessage, _ string, _ json.RawMessage) {
		failed := mcp.calls.Load() == 1
		acpTestToolResult(w, id, `{"status":"fixture"}`, failed)
	}
	peer := newACPTestPeer(t, foundry.ToolSchemaModeRequest, func(w http.ResponseWriter, r *http.Request) {
		body := acpTestReadProvider(t, r)
		if _, present := body["tools"]; !present {
			t.Error("request schema mode omitted discovered tool")
		}
		switch requests.Add(1) {
		case 1:
			acpTestCompleted(w, "response-a", "", acpTestCall("probe", "call-a", `{}`))
		case 2:
			var outputs []foundry.FunctionOutput
			if json.Unmarshal(body["input"], &outputs) != nil || len(outputs) != 1 || !strings.Contains(outputs[0].Output, `"isError":true`) {
				t.Error("admitted error did not reach model as function output")
			}
			acpTestCompleted(w, "response-b", "", acpTestCall("probe", "call-b", `{}`))
		case 3:
			acpTestCompleted(w, "response-c", "recovered")
		default:
			t.Error("unexpected model retry")
			w.WriteHeader(http.StatusInternalServerError)
		}
	}, mcp)
	acpAssertStop(t, peer.reply(peer.prompt("recover")), "end_turn")
	acpAssertToolEvents(t, peer.events, 1, 1)
	if requests.Load() != 3 || mcp.calls.Load() != 2 || acpOutput(peer.events) != "recovered" {
		t.Fatal("admitted tool recovery failed")
	}
}

func TestACPFatalMCPFailureCancelsAndJoinsSiblingWithoutContinuation(t *testing.T) {
	for _, failure := range []string{"rpc", "http", "malformed", "wrong-id", "wrong-version", "both-result-error"} {
		t.Run(failure, func(t *testing.T) {
			var requests atomic.Int32
			slowStarted := make(chan struct{})
			slowCancelled := make(chan struct{})
			mcp := &acpTestMCP{tools: func() []map[string]any { return acpTestTools("probe") }}
			mcp.execute = func(w http.ResponseWriter, r *http.Request, id json.RawMessage, _ string, args json.RawMessage) {
				var params struct {
					Slot int `json:"slot"`
				}
				_ = json.Unmarshal(args, &params)
				if params.Slot == 1 {
					close(slowStarted)
					<-r.Context().Done()
					close(slowCancelled)
					return
				}
				select {
				case <-slowStarted:
				case <-r.Context().Done():
					return
				}
				w.Header().Set("Content-Type", "application/json")
				switch failure {
				case "rpc":
					_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": id, "error": map[string]any{"code": -32002, "message": "test-only-sensitive-provider-detail"}})
				case "http":
					w.WriteHeader(http.StatusServiceUnavailable)
				case "malformed":
					_, _ = w.Write([]byte(`{"jsonrpc":`))
				case "wrong-id":
					acpTestToolResult(w, json.RawMessage("9999"), "not correlated", false)
				case "wrong-version":
					_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "1.0", "id": id, "result": map[string]any{}})
				case "both-result-error":
					_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": id, "result": map[string]any{}, "error": map[string]any{"code": -1}})
				}
			}
			peer := newACPTestPeer(t, foundry.ToolSchemaModeProviderStatic, func(w http.ResponseWriter, r *http.Request) {
				acpTestReadProvider(t, r)
				requests.Add(1)
				acpTestCompleted(w, "fatal-batch", "", acpTestCall("probe", "private-fatal", `{"slot":0}`), acpTestCall("probe", "private-slow", `{"slot":1}`))
			}, mcp)
			acpAssertFailure(t, peer.reply(peer.prompt("fatal batch")))
			select {
			case <-slowCancelled:
			case <-time.After(2 * time.Second):
				t.Fatal("fatal MCP failure left sibling request active")
			}
			acpAssertToolEvents(t, peer.events, 0, 2)
			if requests.Load() != 1 || mcp.calls.Load() != 2 || acpOutput(peer.events) != "" {
				t.Fatal("fatal MCP failure continued or replayed")
			}
			encoded, _ := json.Marshal(peer.events)
			if strings.Contains(string(encoded), "private-") || strings.Contains(string(encoded), "test-only-") {
				t.Fatal("fatal tool metadata leaked sensitive detail")
			}
			if peer.reply(peer.prompt("must not reuse poisoned child"))["error"] == nil || requests.Load() != 1 {
				t.Fatal("failed child resumed provider state")
			}
		})
	}
}

func TestACPCancelJoinsToolsUnderStdoutBackpressure(t *testing.T) {
	for _, method := range []string{"session/cancel", "$/cancel_request"} {
		t.Run(method, func(t *testing.T) {
			var requests atomic.Int32
			started := make(chan struct{}, 2)
			cancelled := make(chan struct{}, 2)
			mcp := &acpTestMCP{
				tools: func() []map[string]any { return acpTestTools("hold") },
				execute: func(_ http.ResponseWriter, r *http.Request, _ json.RawMessage, _ string, _ json.RawMessage) {
					started <- struct{}{}
					<-r.Context().Done()
					cancelled <- struct{}{}
				},
			}
			peer := newACPTestPeer(t, foundry.ToolSchemaModeRequest, func(w http.ResponseWriter, r *http.Request) {
				acpTestReadProvider(t, r)
				requests.Add(1)
				acpTestCompleted(w, "held", "", acpTestCall("hold", "hold-a", `{}`), acpTestCall("hold", "hold-b", `{}`))
			}, mcp)
			id := peer.prompt("hold two calls")
			for range 2 {
				message := peer.read()
				if message["method"] != "session/update" {
					t.Fatal("missing held tool start")
				}
			}
			for range 2 {
				select {
				case <-started:
				case <-time.After(2 * time.Second):
					t.Fatal("tools did not overlap")
				}
			}
			params := map[string]any{"sessionId": peer.session}
			if method == "$/cancel_request" {
				params = map[string]any{"requestId": id}
			}
			peer.send(map[string]any{"jsonrpc": "2.0", "method": method, "params": params})
			// Deliberately do not read stdout until both HTTP contexts are gone.
			for range 2 {
				select {
				case <-cancelled:
				case <-time.After(2 * time.Second):
					t.Fatal("stdout pressure blocked tool cancellation")
				}
			}
			acpAssertStop(t, peer.reply(id), "cancelled")
			acpAssertToolEvents(t, peer.events, 0, 2)
			if requests.Load() != 1 || mcp.calls.Load() != 2 {
				t.Fatal("cancelled batch was replayed")
			}
		})
	}
}

func TestACPFatalMCPFailureCancelsSiblingBeforeBlockedEventWrite(t *testing.T) {
	var requests atomic.Int32
	started, cancelled := make(chan struct{}), make(chan struct{})
	releaseFailure := make(chan struct{})
	mcp := &acpTestMCP{tools: func() []map[string]any { return acpTestTools("probe") }}
	mcp.execute = func(w http.ResponseWriter, r *http.Request, id json.RawMessage, _ string, args json.RawMessage) {
		var params struct {
			Slot int `json:"slot"`
		}
		_ = json.Unmarshal(args, &params)
		if params.Slot == 1 {
			close(started)
			<-r.Context().Done()
			close(cancelled)
			return
		}
		select {
		case <-releaseFailure:
		case <-r.Context().Done():
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": id,
			"error": map[string]any{"code": -32002, "message": "test-only-protocol-failure"}})
	}
	peer := newACPTestPeer(t, foundry.ToolSchemaModeRequest, func(w http.ResponseWriter, r *http.Request) {
		acpTestReadProvider(t, r)
		requests.Add(1)
		acpTestCompleted(w, "fatal-paused", "", acpTestCall("probe", "fatal", `{"slot":0}`), acpTestCall("probe", "held", `{"slot":1}`))
	}, mcp)
	id := peer.prompt("fatal failure with stdout paused")
	for range 2 {
		if peer.read()["method"] != "session/update" {
			t.Fatal("missing tool start")
		}
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("held sibling did not start")
	}
	close(releaseFailure)
	// A terminal event cannot be delivered until read resumes. Its delivery
	// must not be a prerequisite for revoking the still-running HTTP call.
	select {
	case <-cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("fatal MCP error left sibling running behind blocked stdout")
	}
	acpAssertFailure(t, peer.reply(id))
	acpAssertToolEvents(t, peer.events, 0, 2)
	if requests.Load() != 1 || mcp.calls.Load() != 2 || acpOutput(peer.events) != "" {
		t.Fatal("fatal failure continued the model or replayed tool calls")
	}
}

func TestACPProviderFailureAfterToolDoesNotReplayOrCommitOutput(t *testing.T) {
	var requests atomic.Int32
	mcp := &acpTestMCP{
		tools: func() []map[string]any { return acpTestTools("probe") },
		execute: func(w http.ResponseWriter, _ *http.Request, id json.RawMessage, _ string, _ json.RawMessage) {
			acpTestToolResult(w, id, "side effect completed", false)
		},
	}
	peer := newACPTestPeer(t, foundry.ToolSchemaModeRequest, func(w http.ResponseWriter, r *http.Request) {
		acpTestReadProvider(t, r)
		if requests.Add(1) == 1 {
			acpTestCompleted(w, "before-failure", "uncommitted draft", acpTestCall("probe", "once", `{}`))
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("test-only-sensitive-provider-detail"))
		}
	}, mcp)
	acpAssertFailure(t, peer.reply(peer.prompt("provider failure")))
	acpAssertToolEvents(t, peer.events, 1, 0)
	if requests.Load() != 2 || mcp.calls.Load() != 1 || acpOutput(peer.events) != "" {
		t.Fatal("provider fault replayed tool or committed draft output")
	}
}

func TestACPRejectsUnsupportedPromptAndSessionCapabilities(t *testing.T) {
	var requests atomic.Int32
	peer := newACPTestPeer(t, foundry.ToolSchemaModeRequest, func(w http.ResponseWriter, r *http.Request) {
		acpTestReadProvider(t, r)
		requests.Add(1)
		acpTestCompleted(w, "valid", "ok")
	}, &acpTestMCP{})
	for _, method := range []string{"session/load", "session/request_permission", "fs/read_text_file", "terminal/create", "authenticate"} {
		if peer.call(method, map[string]any{})["error"] == nil {
			t.Fatal("unsupported capability accepted")
		}
	}
	if peer.call("session/new", map[string]any{})["error"] == nil {
		t.Fatal("second child session accepted")
	}
	if peer.call("session/prompt", map[string]any{"sessionId": peer.session, "prompt": []map[string]string{{"type": "image", "data": "unsupported"}}})["error"] == nil {
		t.Fatal("unsupported prompt block accepted")
	}
	if requests.Load() != 0 {
		t.Fatal("unsupported request reached provider")
	}
	acpAssertStop(t, peer.reply(peer.prompt("valid")), "end_turn")
}

func TestACPCancellationClosesProviderStream(t *testing.T) {
	started, cancelled := make(chan struct{}), make(chan struct{})
	var requests atomic.Int32
	peer := newACPTestPeer(t, foundry.ToolSchemaModeRequest, func(w http.ResponseWriter, r *http.Request) {
		acpTestReadProvider(t, r)
		requests.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(acpTestSSE(`{"type":"response.created","response":{"id":"held","status":"in_progress"}}`, `{"type":"response.output_text.delta","delta":"uncommitted"}`)))
		w.(http.Flusher).Flush()
		close(started)
		<-r.Context().Done()
		close(cancelled)
	}, &acpTestMCP{})
	id := peer.prompt("hold provider")
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("provider did not start")
	}
	peer.send(map[string]any{"jsonrpc": "2.0", "method": "session/cancel", "params": map[string]string{"sessionId": peer.session}})
	acpAssertStop(t, peer.reply(id), "cancelled")
	select {
	case <-cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("provider connection remained active after cancellation")
	}
	if requests.Load() != 1 || len(peer.events) != 0 {
		t.Fatal("cancelled stream replayed or committed text")
	}
}

func TestACPResourceLinkProjectsTextWithoutFilesystemOrHTTPAccess(t *testing.T) {
	peer := newACPTestPeer(t, foundry.ToolSchemaModeRequest, func(w http.ResponseWriter, r *http.Request) {
		body := acpTestReadProvider(t, r)
		var input string
		if json.Unmarshal(body["input"], &input) != nil || input != "inspect\nResource link: file\nURI: file:///workspace/file\nMIME type: text/plain" {
			t.Error("resource link projection changed")
		}
		acpTestCompleted(w, "resource", "ok")
	}, &acpTestMCP{})
	response := peer.call("session/prompt", map[string]any{"sessionId": peer.session, "prompt": []map[string]string{
		{"type": "text", "text": "inspect"},
		{"type": "resource_link", "name": "file", "uri": "file:///workspace/file", "mimeType": "text/plain"},
	}})
	acpAssertStop(t, response, "end_turn")
}
