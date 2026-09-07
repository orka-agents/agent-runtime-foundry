package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime"
	"net/http"
	"strings"
)

type acpResponseRequest struct {
	foundryResponseRequest
	Model string `json:"model"`
}

// ACP never constructs a Foundry SDK client: the privileged supervisor owns the
// remote agent session and rewrites that binding outside this child process.
func acpCreateResponse(ctx context.Context, cfg acpConfiguration, client *http.Client, request foundryResponseRequest) (foundryStreamSummary, error) {
	request.Stream, request.Store = true, true
	request.AgentSessionID = ""
	body, err := json.Marshal(acpResponseRequest{foundryResponseRequest: request, Model: cfg.agent.Model})
	if err != nil {
		return foundryStreamSummary{}, errACPProvider
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.providerURL, bytes.NewReader(body))
	if err != nil {
		return foundryStreamSummary{}, errACPProvider
	}
	httpRequest.Header.Set("Authorization", "Bearer "+cfg.token)
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Accept", "text/event-stream, application/json")
	response, err := client.Do(httpRequest)
	if err != nil {
		return foundryStreamSummary{}, errACPProvider
	}
	defer response.Body.Close() //nolint:errcheck
	mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || response.StatusCode != http.StatusOK {
		return foundryStreamSummary{}, errACPProvider
	}
	switch mediaType {
	case "text/event-stream":
		return acpParseFoundrySSE(response.Body)
	case "application/json":
		data, err := io.ReadAll(io.LimitReader(response.Body, defaultMaxStreamBytes+1))
		if err != nil || len(data) > defaultMaxStreamBytes {
			return foundryStreamSummary{}, errACPProvider
		}
		document, err := acpDecodeFoundryResponse(data)
		if err != nil || document.Status != "completed" {
			return foundryStreamSummary{}, errACPProvider
		}
		summary, err := processCompletedResponse(document, responseCallbacks{})
		if err != nil || acpValidateSummary(summary) != nil {
			return foundryStreamSummary{}, errACPProvider
		}
		return summary, nil
	default:
		return foundryStreamSummary{}, errACPProvider
	}
}

func acpDecodeFoundryResponse(data []byte) (foundryResponse, error) {
	var response foundryResponse
	if acpDecodeStruct(data, &response, false) != nil || response.ID == "" || validateProviderIdentifier("response", response.ID) != nil ||
		response.Error != nil || response.Incomplete != nil {
		return foundryResponse{}, errACPProvider
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(data, &fields) != nil {
		return foundryResponse{}, errACPProvider
	}
	var rawOutput json.RawMessage
	for name, value := range fields {
		if strings.EqualFold(name, "output") {
			if rawOutput != nil {
				return foundryResponse{}, errACPProvider
			}
			rawOutput = value
		}
	}
	var output []json.RawMessage
	if rawOutput != nil && json.Unmarshal(rawOutput, &output) != nil {
		return foundryResponse{}, errACPProvider
	}
	// Return the validated objects, never a separately decoded typed slice
	// whose elements could retain fields across folded output aliases.
	response.Output = nil
	for _, rawItem := range output {
		item, err := acpDecodeFoundryItem(rawItem, true)
		if err != nil {
			return foundryResponse{}, err
		}
		response.Output = append(response.Output, item)
	}
	return response, nil
}

func acpDecodeFoundryItem(data []byte, done bool) (foundryOutputItem, error) {
	var item struct {
		foundryOutputItem
		Status string `json:"status"`
		Role   string `json:"role"`
	}
	if acpDecodeStruct(data, &item, false) != nil || (done && item.Status != "" && item.Status != "completed") {
		return foundryOutputItem{}, errACPProvider
	}
	switch item.Type {
	case "message":
		if item.Role != "" && item.Role != "assistant" {
			return foundryOutputItem{}, errACPProvider
		}
		for _, content := range item.Content {
			if content.Type != "output_text" {
				return foundryOutputItem{}, errACPProvider
			}
		}
	case "function_call", "reasoning":
	default:
		// Hosted/native tools have no authority in the child. Only ordinary
		// function calls, later checked against tools/list, may execute.
		return foundryOutputItem{}, errACPProvider
	}
	return item.foundryOutputItem, nil
}

func acpValidateSummary(summary foundryStreamSummary) error {
	if summary.Status != "completed" || summary.ResponseID == "" ||
		validateProviderIdentifier("response", summary.ResponseID) != nil ||
		summary.Error != nil || summary.Incomplete != nil || len(summary.Text) > defaultMaxOutputBytes ||
		len(summary.FunctionCalls) > defaultMaxBrokeredCalls {
		return errACPProvider
	}
	return nil
}

// Keep the existing Responses types and terminal-output reconciliation, but
// require one explicit, coherent terminal event. A created/in-progress status,
// [DONE] alone, or a complete function-call item does not settle a response.
func acpParseFoundrySSE(reader io.Reader) (foundryStreamSummary, error) {
	limited := &io.LimitedReader{R: reader, N: defaultMaxStreamBytes + 1}
	scanner := bufio.NewScanner(limited)
	scanner.Buffer(make([]byte, 32<<10), defaultMaxEventBytes)
	var summary foundryStreamSummary
	var data []byte
	terminal, done := false, false
	events := 0
	pending := map[string]bool{}
	apply := func() error {
		if len(data) == 0 {
			return nil
		}
		events++
		if events > defaultMaxEvents || done {
			return errACPProvider
		}
		if bytes.Equal(bytes.TrimSpace(data), []byte("[DONE]")) {
			if !terminal {
				return errACPProvider
			}
			done = true
			return nil
		}
		if terminal {
			return errACPProvider
		}
		var event foundryResponseEvent
		var rawFields map[string]json.RawMessage
		if acpDecodeStruct(data, &event, false) != nil || json.Unmarshal(data, &rawFields) != nil {
			return errACPProvider
		}
		// Match the struct decoder's Unicode field folding without allowing two
		// envelope members to validate one value and apply another. Tool argument
		// objects keep their case-sensitive keys.
		fields := make(map[string]json.RawMessage, 7)
		for key, value := range rawFields {
			for _, name := range []string{"type", "delta", "sequence_number", "response", "item", "error", "item_id"} {
				if strings.EqualFold(key, name) {
					if _, exists := fields[name]; exists {
						return errACPProvider
					}
					fields[name] = value
					break
				}
			}
		}
		if event.Response != nil {
			response, err := acpDecodeFoundryResponse(fields["response"])
			if err != nil || (summary.ResponseID != "" && summary.ResponseID != response.ID) {
				return errACPProvider
			}
			event.Response = &response
		}
		switch event.Type {
		case "response.created", "response.in_progress", "response.queued":
			if event.Response == nil || (event.Response.Status != "in_progress" && event.Response.Status != "queued") {
				return errACPProvider
			}
		case "response.completed":
			if event.Response == nil || event.Response.Status != "completed" {
				return errACPProvider
			}
			for _, item := range event.Response.Output {
				if item.Type == "function_call" {
					delete(pending, item.ID)
				}
			}
			if len(pending) != 0 {
				return errACPProvider
			}
			terminal = true
		case "response.output_item.added", "response.output_item.done":
			item, err := acpDecodeFoundryItem(fields["item"], event.Type == "response.output_item.done")
			if err != nil {
				return err
			}
			event.Item = &item
			if item.Type == "function_call" {
				if event.Type == "response.output_item.added" {
					if !acpSafeString(item.ID, maxProviderIdentifierBytes) {
						return errACPProvider
					}
					pending[item.ID] = true
				} else {
					delete(pending, item.ID)
				}
			}
		case "response.function_call_arguments.delta", "response.function_call_arguments.done":
			var itemID string
			if json.Unmarshal(fields["item_id"], &itemID) != nil || !acpSafeString(itemID, maxProviderIdentifierBytes) {
				return errACPProvider
			}
			pending[itemID] = true
		case "response.output_text.delta":
			if len(fields["delta"]) == 0 || fields["delta"][0] != '"' || len(summary.Text)+len(event.Delta) > defaultMaxOutputBytes {
				return errACPProvider
			}
		case "response.output_text.done", "response.content_part.added", "response.content_part.done",
			"response.reasoning_summary_text.delta", "response.reasoning_summary_text.done",
			"response.reasoning_summary_part.added", "response.reasoning_summary_part.done",
			"response.reasoning_text.delta", "response.reasoning_text.done":
		default:
			return errACPProvider
		}
		if err := applyFoundryEvent(&summary, event, responseCallbacks{}); err != nil || len(summary.Text) > defaultMaxOutputBytes {
			return errACPProvider
		}
		return nil
	}
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			if err := apply(); err != nil {
				return foundryStreamSummary{}, err
			}
			data = nil
			continue
		}
		if part, ok := bytes.CutPrefix(line, []byte("data:")); ok {
			part = bytes.TrimPrefix(part, []byte(" "))
			if len(data)+len(part)+1 > defaultMaxEventBytes {
				return foundryStreamSummary{}, errACPProvider
			}
			if len(data) != 0 {
				data = append(data, '\n')
			}
			data = append(data, part...)
		} else if !bytes.HasPrefix(line, []byte(":")) && !strings.HasPrefix(string(line), "event:") &&
			!strings.HasPrefix(string(line), "id:") && !strings.HasPrefix(string(line), "retry:") {
			return foundryStreamSummary{}, errACPProvider
		}
	}
	if scanner.Err() != nil || limited.N <= 0 || len(data) != 0 || !terminal || acpValidateSummary(summary) != nil {
		return foundryStreamSummary{}, errACPProvider
	}
	return summary, nil
}
