package acp

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/orka-agents/agent-runtime-foundry/internal/foundry"
)

func acpTestSSE(events ...string) string {
	return "data: " + strings.Join(events, "\n\ndata: ") + "\n\n"
}

func TestACPInvalidProviderCallsNeverReachMCP(t *testing.T) {
	valid := acpTestCall("probe", "call-1", `{}`)
	for name, calls := range map[string][]foundry.OutputItem{
		"unknown tool":        {acpTestCall("forbidden", "call-1", `{}`)},
		"duplicate call":      {valid, valid},
		"missing call ID":     {acpTestCall("probe", "", `{}`)},
		"array arguments":     {acpTestCall("probe", "call-1", `[]`)},
		"null arguments":      {acpTestCall("probe", "call-1", `null`)},
		"malformed arguments": {acpTestCall("probe", "call-1", `{`)},
		"duplicate arguments": {acpTestCall("probe", "call-1", `{"key":1,"key":2}`)},
		"unpaired surrogate":  {acpTestCall("probe", "call-1", `{"key":"\ud800"}`)},
		"missing arguments":   {{Type: "function_call", Name: "probe", CallID: "call-1"}},
		"bad sibling":         {valid, acpTestCall("forbidden", "call-2", `{}`)},
		"native tool":         {{Type: "web_search_call", ID: "native"}},
	} {
		t.Run(name, func(t *testing.T) {
			var requests atomic.Int32
			mcp := &acpTestMCP{
				tools: func() []map[string]any { return acpTestTools("probe") },
				execute: func(w http.ResponseWriter, _ *http.Request, id json.RawMessage, _ string, _ json.RawMessage) {
					acpTestToolResult(w, id, "unexpected", false)
				},
			}
			peer := newACPTestPeer(t, foundry.ToolSchemaModeProviderStatic, func(w http.ResponseWriter, r *http.Request) {
				acpTestReadProvider(t, r)
				requests.Add(1)
				acpTestCompleted(w, "invalid-calls", "", calls...)
			}, mcp)
			acpAssertFailure(t, peer.reply(peer.prompt("invalid call")))
			if mcp.calls.Load() != 0 || requests.Load() != 1 || len(peer.events) != 0 {
				t.Fatal("malformed batch admitted a tool or model replay")
			}
		})
	}
}

func TestACPTruncatedStreamNeverExecutesCompleteToolItem(t *testing.T) {
	var requests atomic.Int32
	mcp := &acpTestMCP{
		tools: func() []map[string]any { return acpTestTools("probe") },
		execute: func(w http.ResponseWriter, _ *http.Request, id json.RawMessage, _ string, _ json.RawMessage) {
			acpTestToolResult(w, id, "unexpected", false)
		},
	}
	peer := newACPTestPeer(t, foundry.ToolSchemaModeProviderStatic, func(w http.ResponseWriter, r *http.Request) {
		acpTestReadProvider(t, r)
		requests.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, acpTestSSE(`{"type":"response.output_item.done","item":{"type":"function_call","name":"probe","call_id":"call-1","arguments":"{}"}}`))
	}, mcp)
	acpAssertFailure(t, peer.reply(peer.prompt("truncated")))
	if requests.Load() != 1 || mcp.calls.Load() != 0 || len(peer.events) != 0 {
		t.Fatal("truncated response admitted a tool call")
	}
}

func TestACPResponsesFoldedEventCannotExecuteTool(t *testing.T) {
	var requests atomic.Int32
	mcp := &acpTestMCP{
		tools: func() []map[string]any { return acpTestTools("probe") },
		execute: func(w http.ResponseWriter, _ *http.Request, id json.RawMessage, _ string, _ json.RawMessage) {
			acpTestToolResult(w, id, "unexpected", false)
		},
	}
	peer := newACPTestPeer(t, foundry.ToolSchemaModeProviderStatic, func(w http.ResponseWriter, r *http.Request) {
		acpTestReadProvider(t, r)
		if requests.Add(1) > 1 {
			acpTestCompleted(w, "response-2", "unexpected")
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, acpTestSSE(
			`{"type":"response.output_item.done","item":{"type":"message"},"Item":{"id":"item-1","type":"function_call","name":"probe","call_id":"call-1","arguments":"{}","status":"in_progress"}}`,
			`{"type":"response.completed","response":{"id":"response-1","status":"completed"}}`))
	}, mcp)
	reply := peer.reply(peer.prompt("folded event validation"))
	if requests.Load() != 1 || mcp.calls.Load() != 0 || len(peer.events) != 0 {
		t.Fatal("ambiguous provider item admitted a tool or another model request")
	}
	acpAssertFailure(t, reply)
}

func TestACPResponsesFoldedOutputCannotExecuteTool(t *testing.T) {
	const response = `{"id":"response-1","status":"completed","output":[{"id":"item-1","type":"function_call","status":"in_progress","name":"probe","call_id":"call-1","arguments":"{}"}],"Output":[{"type":"function_call"}]}`
	for _, mediaType := range []string{"application/json", "text/event-stream"} {
		t.Run(mediaType, func(t *testing.T) {
			var requests atomic.Int32
			mcp := &acpTestMCP{
				tools: func() []map[string]any { return acpTestTools("probe") },
				execute: func(w http.ResponseWriter, _ *http.Request, id json.RawMessage, _ string, _ json.RawMessage) {
					acpTestToolResult(w, id, "unexpected", false)
				},
			}
			peer := newACPTestPeer(t, foundry.ToolSchemaModeProviderStatic, func(w http.ResponseWriter, r *http.Request) {
				acpTestReadProvider(t, r)
				if requests.Add(1) > 1 {
					acpTestCompleted(w, "response-2", "unexpected")
					return
				}
				w.Header().Set("Content-Type", mediaType)
				if mediaType == "application/json" {
					_, _ = fmt.Fprint(w, response)
				} else {
					_, _ = fmt.Fprint(w, acpTestSSE(`{"type":"response.completed","response":`+response+`}`))
				}
			}, mcp)
			reply := peer.reply(peer.prompt("folded output validation"))
			if requests.Load() != 1 || mcp.calls.Load() != 0 || len(peer.events) != 0 {
				t.Fatalf("ambiguous response output admitted effects: provider requests=%d, tool calls=%d, events=%d", requests.Load(), mcp.calls.Load(), len(peer.events))
			}
			acpAssertFailure(t, reply)
		})
	}
}
