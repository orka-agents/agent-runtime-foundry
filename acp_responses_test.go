package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
)

func acpTestSSE(events ...string) string {
	return "data: " + strings.Join(events, "\n\ndata: ") + "\n\n"
}

func TestACPResponsesRequireExplicitCoherentCompletion(t *testing.T) {
	const created = `{"type":"response.created","response":{"id":"response-1","status":"in_progress"}}`
	const delta = `{"type":"response.output_text.delta","delta":"héllo"}`
	const completed = `{"type":"response.completed","response":{"id":"response-1","status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"héllo"}]}]}}`
	for name, stream := range map[string]string{
		"streamed text":     acpTestSSE(created, delta, completed, "[DONE]"),
		"terminal fallback": acpTestSSE(completed),
	} {
		t.Run(name, func(t *testing.T) {
			summary, err := acpParseFoundrySSE(strings.NewReader(stream))
			if err != nil || summary.Text != "héllo" || summary.ResponseID != "response-1" || summary.Status != "completed" {
				t.Fatal("valid terminal response rejected")
			}
		})
	}
	for name, stream := range map[string]string{
		"created only":              acpTestSSE(created),
		"partial text":              acpTestSSE(created, delta),
		"done without terminal":     acpTestSSE(created, delta, "[DONE]"),
		"created lies completed":    acpTestSSE(`{"type":"response.created","response":{"id":"response-1","status":"completed"}}`),
		"error after completed":     acpTestSSE(created, delta, completed, `{"type":"error","error":{"message":"test-only-private-detail"}}`),
		"duplicate terminal":        acpTestSSE(completed, completed),
		"missing terminal response": acpTestSSE(created, `{"type":"response.completed"}`),
		"wrong terminal status":     acpTestSSE(created, `{"type":"response.completed","response":{"id":"response-1","status":"in_progress"}}`),
		"terminal error":            acpTestSSE(`{"type":"response.completed","response":{"id":"response-1","status":"completed","error":{"message":"private"}}}`),
		"terminal incomplete":       acpTestSSE(`{"type":"response.completed","response":{"id":"response-1","status":"completed","incomplete_details":{"reason":"max_output_tokens"}}}`),
		"wrong response identity":   acpTestSSE(created, strings.Replace(completed, "response-1", "response-2", 1)),
		"changed streamed text":     acpTestSSE(created, strings.Replace(delta, "héllo", "wrong", 1), completed),
		"native tool event":         acpTestSSE(created, `{"type":"response.web_search_call.completed"}`, completed),
		"native tool item":          acpTestSSE(created, `{"type":"response.output_item.done","item":{"type":"web_search_call","id":"native"}}`, completed),
		"duplicate JSON field":      acpTestSSE(`{"type":"error","type":"response.completed","response":{"id":"response-1","status":"completed"}}`),
		"truncated JSON":            acpTestSSE(created, `{"type":"response.completed","response":`),
		"truncated event":           strings.TrimSuffix(acpTestSSE(completed), "\n"),
		"missing delta":             acpTestSSE(created, `{"type":"response.output_text.delta"}`, completed),
		"null delta":                acpTestSSE(created, `{"type":"response.output_text.delta","delta":null}`, completed),
		"failed terminal":           acpTestSSE(created, `{"type":"response.failed","response":{"id":"response-1","status":"failed"}}`),
		"cancelled terminal":        acpTestSSE(created, `{"type":"response.cancelled","response":{"id":"response-1","status":"cancelled"}}`),
		"oversized output":          acpTestSSE(created, `{"type":"response.output_text.delta","delta":"`+strings.Repeat("x", defaultMaxOutputBytes+1)+`"}`, completed),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := acpParseFoundrySSE(strings.NewReader(stream)); err == nil {
				t.Fatal("incomplete or malformed provider stream settled successfully")
			}
		})
	}
}

func TestACPResponsesWaitForCompleteFunctionCallItems(t *testing.T) {
	const added = `{"type":"response.output_item.added","item":{"id":"item-1","type":"function_call","name":"probe","call_id":"call-1","arguments":"","status":"in_progress"}}`
	const delta = `{"type":"response.function_call_arguments.delta","item_id":"item-1","delta":"{"}`
	const argsDone = `{"type":"response.function_call_arguments.done","item_id":"item-1","arguments":"{}"}`
	const itemDone = `{"type":"response.output_item.done","item":{"id":"item-1","type":"function_call","name":"probe","call_id":"call-1","arguments":"{}","status":"completed"}}`
	const completed = `{"type":"response.completed","response":{"id":"response-1","status":"completed"}}`
	summary, err := acpParseFoundrySSE(strings.NewReader(acpTestSSE(added, delta, argsDone, itemDone, completed)))
	if err != nil || len(summary.FunctionCalls) != 1 || summary.FunctionCalls[0].CallID != "call-1" {
		t.Fatal("complete function call stream rejected")
	}
	for name, stream := range map[string]string{
		"only added":                          acpTestSSE(added, completed),
		"partial arguments":                   acpTestSSE(added, delta, completed),
		"arguments done without item":         acpTestSSE(added, delta, argsDone, completed),
		"item done without response terminal": acpTestSSE(itemDone),
		"item still incomplete":               acpTestSSE(strings.Replace(itemDone, `"status":"completed"`, `"status":"in_progress"`, 1), completed),
		"terminal omits pending item":         acpTestSSE(added, strings.Replace(itemDone, "item-1", "item-2", 1), completed),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := acpParseFoundrySSE(strings.NewReader(stream)); err == nil {
				t.Fatal("partial tool-call stream accepted")
			}
		})
	}
}

func TestACPResponsesEventAndStreamBounds(t *testing.T) {
	for name, stream := range map[string]string{
		"event count":  strings.Repeat(acpTestSSE(`{"type":"response.reasoning_text.delta","delta":"x"}`), defaultMaxEvents+1),
		"single event": acpTestSSE(`{"type":"response.reasoning_text.delta","delta":"` + strings.Repeat("x", defaultMaxEventBytes) + `"}`),
		"stream bytes": strings.Repeat(":"+strings.Repeat("x", 1<<20)+"\n", 17),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := acpParseFoundrySSE(strings.NewReader(stream)); err == nil {
				t.Fatal("unbounded provider stream accepted")
			}
		})
	}
}

func TestACPInvalidProviderCallsNeverReachMCP(t *testing.T) {
	valid := acpTestCall("probe", "call-1", `{}`)
	for name, calls := range map[string][]foundryOutputItem{
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
			peer := newACPTestPeer(t, toolSchemaModeProviderStatic, func(w http.ResponseWriter, r *http.Request) {
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
	peer := newACPTestPeer(t, toolSchemaModeProviderStatic, func(w http.ResponseWriter, r *http.Request) {
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

func TestACPResponsesRejectAmbiguousFoldedEventFields(t *testing.T) {
	const response = `{"id":"response-1","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"checked"}]}]}`
	const changed = `{"id":"response-2","status":"completed","output":[{"type":"message","role":"user","content":[{"type":"output_text","text":"unchecked"}]}]}`
	const completed = `{"type":"response.completed","response":{"id":"response-1","status":"completed"}}`
	const call = `{"id":"item-1","type":"function_call","name":"probe","call_id":"call-1","arguments":"{}","status":"in_progress"}`
	for name, stream := range map[string]string{
		"response identity and role":   acpTestSSE(`{"type":"response.completed","response":` + response + `,"Response":` + changed + `}`),
		"response reverse order":       acpTestSSE(`{"type":"response.completed","Response":` + changed + `,"response":` + response + `}`),
		"Unicode folded response":      acpTestSSE(`{"type":"response.completed","response":` + response + `,"reſponſe":` + changed + `}`),
		"escaped folded response":      acpTestSSE(`{"type":"response.completed","response":` + response + `,"re\u017fpon\u017fe":` + changed + `}`),
		"item skips status validation": acpTestSSE(`{"type":"response.output_item.done","item":{"type":"message"},"Item":`+call+`}`, completed),
		"item reverse order":           acpTestSSE(`{"type":"response.output_item.done","Item":`+call+`,"item":{"type":"message"}}`, completed),
		"delta changes value":          acpTestSSE(`{"type":"response.output_text.delta","delta":"checked","Delta":"unchecked"}`, completed),
		"type changes terminal":        acpTestSSE(`{"type":"error","Type":"response.completed","response":` + response + `}`),
		"item identity aliases":        acpTestSSE(`{"type":"response.function_call_arguments.done","item_id":"item-1","Item_ID":"other","arguments":"{}"}`, `{"type":"response.output_item.done","item":`+strings.Replace(call, "in_progress", "completed", 1)+`}`, completed),
	} {
		t.Run(name, func(t *testing.T) {
			summary, err := acpParseFoundrySSE(strings.NewReader(stream))
			if err == nil || summary.ResponseID != "" || summary.Text != "" || len(summary.FunctionCalls) != 0 {
				t.Fatal("ambiguous provider event exposed a response or tool call")
			}
		})
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
	peer := newACPTestPeer(t, toolSchemaModeProviderStatic, func(w http.ResponseWriter, r *http.Request) {
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

func TestACPResponsesMatchSingleFoldedEnvelopeFields(t *testing.T) {
	const response = `{"id":"response-1","status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"checked"}]}]}`
	for name, stream := range map[string]string{
		"response": acpTestSSE(`{"Type":"response.completed","Reſponſe":` + response + `}`),
		"delta":    acpTestSSE(`{"type":"response.output_text.delta","Delta":"checked"}`, `{"type":"response.completed","response":`+response+`}`),
	} {
		t.Run(name, func(t *testing.T) {
			summary, err := acpParseFoundrySSE(strings.NewReader(stream))
			if err != nil || summary.ResponseID != "response-1" || summary.Text != "checked" || len(summary.FunctionCalls) != 0 {
				t.Fatal("unambiguous folded event fields changed the response")
			}
		})
	}
	stream := acpTestSSE(
		`{"type":"response.function_call_arguments.done","Item_ID":"item-1","arguments":"{\"Key\":1,\"key\":2}"}`,
		`{"type":"response.output_item.done","Item":{"id":"item-1","type":"function_call","name":"probe","call_id":"call-1","arguments":"{\"Key\":1,\"key\":2}","status":"completed"}}`,
		`{"Type":"response.completed","Response":{"id":"response-1","status":"completed"}}`)
	summary, err := acpParseFoundrySSE(strings.NewReader(stream))
	if err != nil || len(summary.FunctionCalls) != 1 {
		t.Fatal("unambiguous folded item fields rejected a complete tool call")
	}
	var arguments string
	if json.Unmarshal(summary.FunctionCalls[0].Arguments, &arguments) != nil || arguments != `{"Key":1,"key":2}` {
		t.Fatal("case-sensitive tool argument keys changed")
	}
}

func TestACPResponsesRejectAmbiguousFoldedOutput(t *testing.T) {
	const userMessage = `[{"type":"message","role":"user","content":[{"type":"output_text","text":"unchecked"}]}]`
	const emptyAssistant = `[{"type":"message","role":"assistant"}]`
	const incompleteCall = `[{"id":"item-1","type":"function_call","status":"in_progress","name":"probe","call_id":"call-1","arguments":"{}"}]`
	const emptyCall = `[{"type":"function_call"}]`
	for name, fields := range map[string]string{
		"message content preserved": `"output":` + userMessage + `,"Output":` + emptyAssistant,
		"message reverse casing":    `"Output":` + userMessage + `,"output":` + emptyAssistant,
		"escaped output alias":      `"output":` + userMessage + `,"\u004futput":` + emptyAssistant,
		"incomplete call preserved": `"output":` + incompleteCall + `,"Output":` + emptyCall,
		"call reverse casing":       `"Output":` + incompleteCall + `,"output":` + emptyCall,
	} {
		t.Run(name, func(t *testing.T) {
			response := `{"id":"response-1","status":"completed",` + fields + `}`
			document, err := acpDecodeFoundryResponse([]byte(response))
			if err == nil || document.ID != "" || len(document.Output) != 0 {
				t.Error("ambiguous response output exposed a decoded document")
			}
			summary, err := acpParseFoundrySSE(strings.NewReader(acpTestSSE(`{"type":"response.completed","response":` + response + `}`)))
			if err == nil || summary.ResponseID != "" || summary.Text != "" || len(summary.FunctionCalls) != 0 {
				t.Error("ambiguous response output exposed a response or tool call")
			}
		})
	}
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
			peer := newACPTestPeer(t, toolSchemaModeProviderStatic, func(w http.ResponseWriter, r *http.Request) {
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

func TestACPResponsesSingleFoldedOutputPreservesValidatedItems(t *testing.T) {
	const response = `{"id":"response-1","status":"completed","OuTpUt":[{"type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"checked"}]},{"id":"item-1","type":"function_call","status":"completed","name":"probe","call_id":"call-1","arguments":"{\"Key\":1,\"key\":2}"}]}`
	document, err := acpDecodeFoundryResponse([]byte(response))
	if err != nil || len(document.Output) != 2 || document.Output[0].Type != "message" || document.Output[1].Type != "function_call" {
		t.Fatal("single folded output lost validated items")
	}
	summary, err := acpParseFoundrySSE(strings.NewReader(acpTestSSE(`{"type":"response.completed","response":` + response + `}`)))
	if err != nil || summary.ResponseID != "response-1" || summary.Text != "checked" || len(summary.FunctionCalls) != 1 || summary.FunctionCalls[0].Name != "probe" || summary.FunctionCalls[0].CallID != "call-1" {
		t.Fatal("single folded output changed text or function-call identity")
	}
	var arguments string
	if json.Unmarshal(summary.FunctionCalls[0].Arguments, &arguments) != nil || arguments != `{"Key":1,"key":2}` {
		t.Fatal("single folded output changed case-sensitive tool arguments")
	}
}
