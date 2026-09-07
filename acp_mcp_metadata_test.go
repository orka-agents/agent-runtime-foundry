package main

import (
	"encoding/json"
	"net/http"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
)

func TestACPToolOutputForwardsOnlyValidatedModelContent(t *testing.T) {
	for _, mode := range []string{"text", "structured", "error"} {
		t.Run(mode, func(t *testing.T) {
			content := []map[string]any{{"type": "text", "text": "héllo 世界"}}
			want := map[string]any{"content": content}
			if mode == "structured" {
				// Structured content is model-visible data, including its own
				// application fields. Only protocol metadata is omitted.
				want["structuredContent"] = map[string]any{"value": "visible", "_meta": "application data"}
				want["isError"] = false
			}
			if mode == "error" {
				want["isError"] = true
			}
			mcp := &acpTestMCP{tools: func() []map[string]any { return acpTestTools("probe") }}
			mcp.execute = func(w http.ResponseWriter, _ *http.Request, id json.RawMessage, _ string, _ json.RawMessage) {
				result := map[string]any{}
				for key, value := range want {
					result[key] = value
				}
				result["content"] = []map[string]any{{
					"type": "text", "text": "héllo 世界",
					"_meta":     map[string]any{"private": "client-only-content-marker"},
					"extension": "unvalidated-content-marker",
				}}
				result["_meta"] = map[string]any{"private": "client-only-result-marker"}
				result["extension"] = "unvalidated-result-marker"
				acpTestMCPResult(w, id, result)
			}
			var requests atomic.Int32
			peer := newACPTestPeer(t, toolSchemaModeRequest, func(w http.ResponseWriter, r *http.Request) {
				body := acpTestReadProvider(t, r)
				switch requests.Add(1) {
				case 1:
					acpTestCompleted(w, "tool-response", "", acpTestCall("probe", "call-probe", `{}`))
				case 2:
					var outputs []foundryFunctionOutput
					if json.Unmarshal(body["input"], &outputs) != nil || len(outputs) != 1 {
						t.Error("invalid function output continuation")
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					var got, expected any
					encoded, _ := json.Marshal(want)
					if json.Unmarshal([]byte(outputs[0].Output), &got) != nil || json.Unmarshal(encoded, &expected) != nil ||
						!reflect.DeepEqual(got, expected) {
						t.Error("provider function output was not limited to validated model content")
					}
					for _, marker := range []string{"client-only-", "unvalidated-"} {
						if strings.Contains(outputs[0].Output, marker) {
							t.Error("unvalidated tool metadata reached the provider")
						}
					}
					acpTestCompleted(w, "final-response", "done")
				default:
					t.Error("unexpected provider request")
					w.WriteHeader(http.StatusInternalServerError)
				}
			}, mcp)
			acpAssertStop(t, peer.reply(peer.prompt("use probe")), "end_turn")
			if requests.Load() != 2 || mcp.calls.Load() != 1 || acpOutput(peer.events) != "done" {
				t.Fatal("tool continuation did not complete exactly once")
			}
		})
	}
}
