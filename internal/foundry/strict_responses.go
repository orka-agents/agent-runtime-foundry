package foundry

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"

	"github.com/orka-agents/agent-runtime-foundry/internal/strictjson"
)

var ErrResponse = errors.New("Foundry ACP provider request failed")

type ModelResponseRequest struct {
	ResponseRequest
	Model string `json:"model"`
}

func DecodeResponse(data []byte) (Response, error) {
	var response Response
	if strictjson.DecodeStruct(data, &response, false) != nil || response.ID == "" || ValidateIdentifier("response", response.ID) != nil ||
		response.Error != nil || response.Incomplete != nil {
		return Response{}, ErrResponse
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(data, &fields) != nil {
		return Response{}, ErrResponse
	}
	var rawOutput json.RawMessage
	for name, value := range fields {
		if strings.EqualFold(name, "output") {
			if rawOutput != nil {
				return Response{}, ErrResponse
			}
			rawOutput = value
		}
	}
	var output []json.RawMessage
	if rawOutput != nil && json.Unmarshal(rawOutput, &output) != nil {
		return Response{}, ErrResponse
	}
	// Return the validated objects, never a separately decoded typed slice
	// whose elements could retain fields across folded output aliases.
	response.Output = nil
	for _, rawItem := range output {
		item, err := decodeOutputItem(rawItem, true)
		if err != nil {
			return Response{}, err
		}
		response.Output = append(response.Output, item)
	}
	return response, nil
}

func decodeOutputItem(data []byte, done bool) (OutputItem, error) {
	var item struct {
		OutputItem
		Status string `json:"status"`
		Role   string `json:"role"`
	}
	if strictjson.DecodeStruct(data, &item, false) != nil || (done && item.Status != "" && item.Status != "completed") {
		return OutputItem{}, ErrResponse
	}
	switch item.Type {
	case "message":
		if item.Role != "" && item.Role != "assistant" {
			return OutputItem{}, ErrResponse
		}
		for _, content := range item.Content {
			if content.Type != "output_text" {
				return OutputItem{}, ErrResponse
			}
		}
	case "function_call", "reasoning":
	default:
		// Hosted/native tools have no authority in the child. Only ordinary
		// function calls, later checked against tools/list, may execute.
		return OutputItem{}, ErrResponse
	}
	return item.OutputItem, nil
}

func ValidateSummary(summary StreamSummary) error {
	if summary.Status != "completed" || summary.ResponseID == "" ||
		ValidateIdentifier("response", summary.ResponseID) != nil ||
		summary.Error != nil || summary.Incomplete != nil || len(summary.Text) > DefaultMaxOutputBytes ||
		len(summary.FunctionCalls) > DefaultMaxBrokeredCalls {
		return ErrResponse
	}
	return nil
}

// Keep the existing Responses types and terminal-output reconciliation, but
// require one explicit, coherent terminal event. A created/in-progress status,
// [DONE] alone, or a complete function-call item does not settle a response.
func ParseStrictSSE(reader io.Reader) (StreamSummary, error) {
	limited := &io.LimitedReader{R: reader, N: DefaultMaxStreamBytes + 1}
	scanner := bufio.NewScanner(limited)
	scanner.Buffer(make([]byte, 32<<10), DefaultMaxEventBytes)
	var summary StreamSummary
	var data []byte
	terminal, done := false, false
	events := 0
	pending := map[string]bool{}
	apply := func() error {
		if len(data) == 0 {
			return nil
		}
		events++
		if events > DefaultMaxEvents || done {
			return ErrResponse
		}
		if bytes.Equal(bytes.TrimSpace(data), []byte("[DONE]")) {
			if !terminal {
				return ErrResponse
			}
			done = true
			return nil
		}
		if terminal {
			return ErrResponse
		}
		var event ResponseEvent
		var rawFields map[string]json.RawMessage
		if strictjson.DecodeStruct(data, &event, false) != nil || json.Unmarshal(data, &rawFields) != nil {
			return ErrResponse
		}
		// Match the struct decoder's Unicode field folding without allowing two
		// envelope members to validate one value and apply another. Tool argument
		// objects keep their case-sensitive keys.
		fields := make(map[string]json.RawMessage, 7)
		for key, value := range rawFields {
			for _, name := range []string{"type", "delta", "sequence_number", "response", "item", "error", "item_id"} {
				if strings.EqualFold(key, name) {
					if _, exists := fields[name]; exists {
						return ErrResponse
					}
					fields[name] = value
					break
				}
			}
		}
		if event.Response != nil {
			response, err := DecodeResponse(fields["response"])
			if err != nil || (summary.ResponseID != "" && summary.ResponseID != response.ID) {
				return ErrResponse
			}
			event.Response = &response
		}
		switch event.Type {
		case "response.created", "response.in_progress", "response.queued":
			if event.Response == nil || (event.Response.Status != "in_progress" && event.Response.Status != "queued") {
				return ErrResponse
			}
		case "response.completed":
			if event.Response == nil || event.Response.Status != "completed" {
				return ErrResponse
			}
			for _, item := range event.Response.Output {
				if item.Type == "function_call" {
					delete(pending, item.ID)
				}
			}
			if len(pending) != 0 {
				return ErrResponse
			}
			terminal = true
		case "response.output_item.added", "response.output_item.done":
			item, err := decodeOutputItem(fields["item"], event.Type == "response.output_item.done")
			if err != nil {
				return err
			}
			event.Item = &item
			if item.Type == "function_call" {
				if event.Type == "response.output_item.added" {
					if !SafeString(item.ID, MaxIdentifierBytes) {
						return ErrResponse
					}
					pending[item.ID] = true
				} else {
					delete(pending, item.ID)
				}
			}
		case "response.function_call_arguments.delta", "response.function_call_arguments.done":
			var itemID string
			if json.Unmarshal(fields["item_id"], &itemID) != nil || !SafeString(itemID, MaxIdentifierBytes) {
				return ErrResponse
			}
			pending[itemID] = true
		case "response.output_text.delta":
			if len(fields["delta"]) == 0 || fields["delta"][0] != '"' || len(summary.Text)+len(event.Delta) > DefaultMaxOutputBytes {
				return ErrResponse
			}
		case "response.output_text.done", "response.content_part.added", "response.content_part.done",
			"response.reasoning_summary_text.delta", "response.reasoning_summary_text.done",
			"response.reasoning_summary_part.added", "response.reasoning_summary_part.done",
			"response.reasoning_text.delta", "response.reasoning_text.done":
		default:
			return ErrResponse
		}
		if err := applyEvent(&summary, event, ResponseCallbacks{}); err != nil || len(summary.Text) > DefaultMaxOutputBytes {
			return ErrResponse
		}
		return nil
	}
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			if err := apply(); err != nil {
				return StreamSummary{}, err
			}
			data = nil
			continue
		}
		if part, ok := bytes.CutPrefix(line, []byte("data:")); ok {
			part = bytes.TrimPrefix(part, []byte(" "))
			if len(data)+len(part)+1 > DefaultMaxEventBytes {
				return StreamSummary{}, ErrResponse
			}
			if len(data) != 0 {
				data = append(data, '\n')
			}
			data = append(data, part...)
		} else if !bytes.HasPrefix(line, []byte(":")) && !strings.HasPrefix(string(line), "event:") &&
			!strings.HasPrefix(string(line), "id:") && !strings.HasPrefix(string(line), "retry:") {
			return StreamSummary{}, ErrResponse
		}
	}
	if scanner.Err() != nil || limited.N <= 0 || len(data) != 0 || !terminal || ValidateSummary(summary) != nil {
		return StreamSummary{}, ErrResponse
	}
	return summary, nil
}
