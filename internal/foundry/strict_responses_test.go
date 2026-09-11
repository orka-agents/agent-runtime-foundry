package foundry

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

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
			if _, err := DecodeResponse([]byte(document)); err == nil {
				t.Error("ambiguous response or nested field accepted")
			}
			if _, err := ParseStrictSSE(strings.NewReader(testSSE(`{"type":"response.completed","response":` + document + `}`))); err == nil {
				t.Error("ambiguous SSE response or nested field accepted")
			}
		})
	}
}

func TestACPStructFieldsSingleAliasesPreserveResponseAndArguments(t *testing.T) {
	data := []byte(`{"ID":"response-1","ſtatus":"completed","OuTpUt":[{"ID":"item-1","TyPe":"function_call","ſtatus":"completed","Name":"probe","CALL_ID":"call-1","Arguments":{"Key":1,"key":2}}]}`)
	response, err := DecodeResponse(data)
	if err != nil || response.Status != "completed" || len(response.Output) != 1 ||
		response.Output[0].Type != "function_call" || !bytes.Equal(response.Output[0].Arguments, []byte(`{"Key":1,"key":2}`)) {
		t.Fatal("single response/item aliases changed case-sensitive arguments")
	}
}

func TestACPResponsesRequireExplicitCoherentCompletion(t *testing.T) {
	const created = `{"type":"response.created","response":{"id":"response-1","status":"in_progress"}}`
	const delta = `{"type":"response.output_text.delta","delta":"héllo"}`
	const completed = `{"type":"response.completed","response":{"id":"response-1","status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"héllo"}]}]}}`
	for name, stream := range map[string]string{
		"streamed text":     testSSE(created, delta, completed, "[DONE]"),
		"terminal fallback": testSSE(completed),
	} {
		t.Run(name, func(t *testing.T) {
			summary, err := ParseStrictSSE(strings.NewReader(stream))
			if err != nil || summary.Text != "héllo" || summary.ResponseID != "response-1" || summary.Status != "completed" {
				t.Fatal("valid terminal response rejected")
			}
		})
	}
	for name, stream := range map[string]string{
		"created only":              testSSE(created),
		"partial text":              testSSE(created, delta),
		"done without terminal":     testSSE(created, delta, "[DONE]"),
		"created lies completed":    testSSE(`{"type":"response.created","response":{"id":"response-1","status":"completed"}}`),
		"error after completed":     testSSE(created, delta, completed, `{"type":"error","error":{"message":"test-only-private-detail"}}`),
		"duplicate terminal":        testSSE(completed, completed),
		"missing terminal response": testSSE(created, `{"type":"response.completed"}`),
		"wrong terminal status":     testSSE(created, `{"type":"response.completed","response":{"id":"response-1","status":"in_progress"}}`),
		"terminal error":            testSSE(`{"type":"response.completed","response":{"id":"response-1","status":"completed","error":{"message":"private"}}}`),
		"terminal incomplete":       testSSE(`{"type":"response.completed","response":{"id":"response-1","status":"completed","incomplete_details":{"reason":"max_output_tokens"}}}`),
		"wrong response identity":   testSSE(created, strings.Replace(completed, "response-1", "response-2", 1)),
		"changed streamed text":     testSSE(created, strings.Replace(delta, "héllo", "wrong", 1), completed),
		"native tool event":         testSSE(created, `{"type":"response.web_search_call.completed"}`, completed),
		"native tool item":          testSSE(created, `{"type":"response.output_item.done","item":{"type":"web_search_call","id":"native"}}`, completed),
		"duplicate JSON field":      testSSE(`{"type":"error","type":"response.completed","response":{"id":"response-1","status":"completed"}}`),
		"truncated JSON":            testSSE(created, `{"type":"response.completed","response":`),
		"truncated event":           strings.TrimSuffix(testSSE(completed), "\n"),
		"missing delta":             testSSE(created, `{"type":"response.output_text.delta"}`, completed),
		"null delta":                testSSE(created, `{"type":"response.output_text.delta","delta":null}`, completed),
		"failed terminal":           testSSE(created, `{"type":"response.failed","response":{"id":"response-1","status":"failed"}}`),
		"cancelled terminal":        testSSE(created, `{"type":"response.cancelled","response":{"id":"response-1","status":"cancelled"}}`),
		"oversized output":          testSSE(created, `{"type":"response.output_text.delta","delta":"`+strings.Repeat("x", DefaultMaxOutputBytes+1)+`"}`, completed),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseStrictSSE(strings.NewReader(stream)); err == nil {
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
	summary, err := ParseStrictSSE(strings.NewReader(testSSE(added, delta, argsDone, itemDone, completed)))
	if err != nil || len(summary.FunctionCalls) != 1 || summary.FunctionCalls[0].CallID != "call-1" {
		t.Fatal("complete function call stream rejected")
	}
	for name, stream := range map[string]string{
		"only added":                          testSSE(added, completed),
		"partial arguments":                   testSSE(added, delta, completed),
		"arguments done without item":         testSSE(added, delta, argsDone, completed),
		"item done without response terminal": testSSE(itemDone),
		"item still incomplete":               testSSE(strings.Replace(itemDone, `"status":"completed"`, `"status":"in_progress"`, 1), completed),
		"terminal omits pending item":         testSSE(added, strings.Replace(itemDone, "item-1", "item-2", 1), completed),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseStrictSSE(strings.NewReader(stream)); err == nil {
				t.Fatal("partial tool-call stream accepted")
			}
		})
	}
}

func TestACPResponsesEventAndStreamBounds(t *testing.T) {
	for name, stream := range map[string]string{
		"event count":  strings.Repeat(testSSE(`{"type":"response.reasoning_text.delta","delta":"x"}`), DefaultMaxEvents+1),
		"single event": testSSE(`{"type":"response.reasoning_text.delta","delta":"` + strings.Repeat("x", DefaultMaxEventBytes) + `"}`),
		"stream bytes": strings.Repeat(":"+strings.Repeat("x", 1<<20)+"\n", 17),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseStrictSSE(strings.NewReader(stream)); err == nil {
				t.Fatal("unbounded provider stream accepted")
			}
		})
	}
}

func TestACPResponsesRejectAmbiguousFoldedEventFields(t *testing.T) {
	const response = `{"id":"response-1","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"checked"}]}]}`
	const changed = `{"id":"response-2","status":"completed","output":[{"type":"message","role":"user","content":[{"type":"output_text","text":"unchecked"}]}]}`
	const completed = `{"type":"response.completed","response":{"id":"response-1","status":"completed"}}`
	const call = `{"id":"item-1","type":"function_call","name":"probe","call_id":"call-1","arguments":"{}","status":"in_progress"}`
	for name, stream := range map[string]string{
		"response identity and role":   testSSE(`{"type":"response.completed","response":` + response + `,"Response":` + changed + `}`),
		"response reverse order":       testSSE(`{"type":"response.completed","Response":` + changed + `,"response":` + response + `}`),
		"Unicode folded response":      testSSE(`{"type":"response.completed","response":` + response + `,"reſponſe":` + changed + `}`),
		"escaped folded response":      testSSE(`{"type":"response.completed","response":` + response + `,"re\u017fpon\u017fe":` + changed + `}`),
		"item skips status validation": testSSE(`{"type":"response.output_item.done","item":{"type":"message"},"Item":`+call+`}`, completed),
		"item reverse order":           testSSE(`{"type":"response.output_item.done","Item":`+call+`,"item":{"type":"message"}}`, completed),
		"delta changes value":          testSSE(`{"type":"response.output_text.delta","delta":"checked","Delta":"unchecked"}`, completed),
		"type changes terminal":        testSSE(`{"type":"error","Type":"response.completed","response":` + response + `}`),
		"item identity aliases":        testSSE(`{"type":"response.function_call_arguments.done","item_id":"item-1","Item_ID":"other","arguments":"{}"}`, `{"type":"response.output_item.done","item":`+strings.Replace(call, "in_progress", "completed", 1)+`}`, completed),
	} {
		t.Run(name, func(t *testing.T) {
			summary, err := ParseStrictSSE(strings.NewReader(stream))
			if err == nil || summary.ResponseID != "" || summary.Text != "" || len(summary.FunctionCalls) != 0 {
				t.Fatal("ambiguous provider event exposed a response or tool call")
			}
		})
	}
}

func TestACPResponsesMatchSingleFoldedEnvelopeFields(t *testing.T) {
	const response = `{"id":"response-1","status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"checked"}]}]}`
	for name, stream := range map[string]string{
		"response": testSSE(`{"Type":"response.completed","Reſponſe":` + response + `}`),
		"delta":    testSSE(`{"type":"response.output_text.delta","Delta":"checked"}`, `{"type":"response.completed","response":`+response+`}`),
	} {
		t.Run(name, func(t *testing.T) {
			summary, err := ParseStrictSSE(strings.NewReader(stream))
			if err != nil || summary.ResponseID != "response-1" || summary.Text != "checked" || len(summary.FunctionCalls) != 0 {
				t.Fatal("unambiguous folded event fields changed the response")
			}
		})
	}
	stream := testSSE(
		`{"type":"response.function_call_arguments.done","Item_ID":"item-1","arguments":"{\"Key\":1,\"key\":2}"}`,
		`{"type":"response.output_item.done","Item":{"id":"item-1","type":"function_call","name":"probe","call_id":"call-1","arguments":"{\"Key\":1,\"key\":2}","status":"completed"}}`,
		`{"Type":"response.completed","Response":{"id":"response-1","status":"completed"}}`)
	summary, err := ParseStrictSSE(strings.NewReader(stream))
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
			document, err := DecodeResponse([]byte(response))
			if err == nil || document.ID != "" || len(document.Output) != 0 {
				t.Error("ambiguous response output exposed a decoded document")
			}
			summary, err := ParseStrictSSE(strings.NewReader(testSSE(`{"type":"response.completed","response":` + response + `}`)))
			if err == nil || summary.ResponseID != "" || summary.Text != "" || len(summary.FunctionCalls) != 0 {
				t.Error("ambiguous response output exposed a response or tool call")
			}
		})
	}
}

func TestACPResponsesSingleFoldedOutputPreservesValidatedItems(t *testing.T) {
	const response = `{"id":"response-1","status":"completed","OuTpUt":[{"type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"checked"}]},{"id":"item-1","type":"function_call","status":"completed","name":"probe","call_id":"call-1","arguments":"{\"Key\":1,\"key\":2}"}]}`
	document, err := DecodeResponse([]byte(response))
	if err != nil || len(document.Output) != 2 || document.Output[0].Type != "message" || document.Output[1].Type != "function_call" {
		t.Fatal("single folded output lost validated items")
	}
	summary, err := ParseStrictSSE(strings.NewReader(testSSE(`{"type":"response.completed","response":` + response + `}`)))
	if err != nil || summary.ResponseID != "response-1" || summary.Text != "checked" || len(summary.FunctionCalls) != 1 || summary.FunctionCalls[0].Name != "probe" || summary.FunctionCalls[0].CallID != "call-1" {
		t.Fatal("single folded output changed text or function-call identity")
	}
	var arguments string
	if json.Unmarshal(summary.FunctionCalls[0].Arguments, &arguments) != nil || arguments != `{"Key":1,"key":2}` {
		t.Fatal("single folded output changed case-sensitive tool arguments")
	}
}

func testSSE(events ...string) string {
	return "data: " + strings.Join(events, "\n\ndata: ") + "\n\n"
}
