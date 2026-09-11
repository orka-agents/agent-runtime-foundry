package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
)

func TestACPFoldedResponseFieldsCannotExecuteTool(t *testing.T) {
	const call = `{"id":"item-1","type":"function_call","name":"probe","call_id":"call-1","arguments":"{}","status":"completed"}`
	for name, document := range map[string]string{
		"response status":        `{"id":"response-1","status":"in_progress","Status":"completed","output":[` + call + `]}`,
		"failed response":        `{"id":"response-1","status":"failed","Status":"completed","error":{"code":"fixture-failure"},"Error":null,"output":[` + call + `]}`,
		"incomplete response":    `{"id":"response-1","status":"completed","incomplete_details":{"reason":"max_output_tokens"},"Incomplete_Details":null,"output":[` + call + `]}`,
		"native item type":       `{"id":"response-1","status":"completed","output":[` + strings.Replace(call, `"type":"function_call"`, `"type":"web_search_call","Type":"function_call"`, 1) + `]}`,
		"incomplete item status": `{"id":"response-1","status":"completed","output":[` + strings.Replace(call, `"status":"completed"`, `"status":"in_progress","Status":"completed"`, 1) + `]}`,
	} {
		for _, mediaType := range []string{"application/json", "text/event-stream"} {
			t.Run(name+"/"+mediaType, func(t *testing.T) {
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
						_, _ = fmt.Fprint(w, document)
					} else {
						_, _ = fmt.Fprint(w, acpTestSSE(`{"type":"response.completed","response":`+document+`}`))
					}
				}, mcp)
				reply := peer.reply(peer.prompt("folded semantic fields"))
				if requests.Load() != 1 || mcp.calls.Load() != 0 || len(peer.events) != 0 {
					t.Fatalf("malformed response admitted effects: providerRequests=%d toolCalls=%d events=%d", requests.Load(), mcp.calls.Load(), len(peer.events))
				}
				acpAssertFailure(t, reply)
			})
		}
	}
}

func TestACPFoldedResponseNestedFieldsRejected(t *testing.T) {
	for name, document := range map[string]string{
		"response identity": `{"id":"wrong","ID":"response-1","status":"completed"}`,
		"Session identity":  `{"id":"response-1","status":"completed","agent_session_id":"wrong","Agent_Session_ID":"owned"}`,
		"item identity":     `{"id":"response-1","status":"completed","output":[{"id":"wrong","ID":"item-1","type":"message"}]}`,
		"item role":         `{"id":"response-1","status":"completed","output":[{"type":"message","role":"user","Role":"assistant","content":[{"type":"output_text","text":"fixture"}]}]}`,
		"content type":      `{"id":"response-1","status":"completed","output":[{"type":"message","content":[{"type":"refusal","Type":"output_text","text":"fixture"}]}]}`,
		"Unicode status":    `{"id":"response-1","status":"in_progress","ſtatus":"completed"}`,
		"escaped status":    `{"id":"response-1","status":"in_progress","\u0053tatus":"completed"}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := acpDecodeFoundryResponse([]byte(document)); err == nil {
				t.Error("ambiguous response or nested field accepted")
			}
			if _, err := acpParseFoundrySSE(strings.NewReader(acpTestSSE(`{"type":"response.completed","response":` + document + `}`))); err == nil {
				t.Error("ambiguous SSE response or nested field accepted")
			}
		})
	}
}
