package foundry

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

type ResponseRequest struct {
	Input              any          `json:"input"`
	Stream             bool         `json:"stream"`
	Store              bool         `json:"store"`
	PreviousResponseID string       `json:"previous_response_id,omitempty"`
	AgentSessionID     string       `json:"agent_session_id,omitempty"`
	Tools              []ToolSchema `json:"tools,omitempty"`
}

type ToolSchema struct {
	Type        string          `json:"type"`
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters"`
}

type FunctionOutput struct {
	Type   string `json:"type"`
	CallID string `json:"call_id"`
	Output string `json:"output"`
}

type ResponseEvent struct {
	Type           string         `json:"type"`
	Delta          string         `json:"delta,omitempty"`
	SequenceNumber int64          `json:"sequence_number,omitempty"`
	Response       *Response      `json:"response,omitempty"`
	Item           *OutputItem    `json:"item,omitempty"`
	Error          *ResponseError `json:"error,omitempty"`
}

type Response struct {
	ID             string         `json:"id"`
	Status         string         `json:"status"`
	AgentSessionID string         `json:"agent_session_id,omitempty"`
	Output         []OutputItem   `json:"output,omitempty"`
	Error          *ResponseError `json:"error,omitempty"`
	Incomplete     *Incomplete    `json:"incomplete_details,omitempty"`
}

type OutputItem struct {
	ID        string          `json:"id,omitempty"`
	Type      string          `json:"type"`
	CallID    string          `json:"call_id,omitempty"`
	Name      string          `json:"name,omitempty"`
	Arguments json.RawMessage `json:"arguments,omitempty"`
	Content   []OutputContent `json:"content,omitempty"`
}

type OutputContent struct {
	Type string `json:"type,omitempty"`
	Text string `json:"text,omitempty"`
}

type ResponseError struct {
	Type    string `json:"type,omitempty"`
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
	Param   string `json:"param,omitempty"`
}

type Incomplete struct {
	Reason string `json:"reason,omitempty"`
}

type StreamSummary struct {
	ResponseID     string
	AgentSessionID string
	Status         string
	Text           string
	FunctionCalls  []OutputItem
	Error          *ResponseError
	Incomplete     *Incomplete
}

type ResponseCallbacks struct {
	OnCreated      func(Response) error
	OnTextDelta    func(string) error
	OnFunctionCall func(OutputItem) error
}

func ParseJSON(r io.Reader, maxBytes int64, callbacks ResponseCallbacks) (StreamSummary, error) {
	data, err := io.ReadAll(io.LimitReader(r, maxBytes+1))
	if err != nil {
		return StreamSummary{}, err
	}
	if int64(len(data)) > maxBytes {
		return StreamSummary{}, errors.New("foundry response exceeded adapter stream limit")
	}
	var response Response
	if err := json.Unmarshal(data, &response); err != nil {
		return StreamSummary{}, errors.New("foundry response was invalid JSON")
	}
	return CompleteResponse(response, callbacks)
}

func ParseSSE(
	r io.Reader,
	maxBytes int64,
	maxEventBytes int64,
	maxEvents int,
	callbacks ResponseCallbacks,
) (StreamSummary, error) {
	reader := bufio.NewReader(io.LimitReader(r, maxBytes+1))
	var summary StreamSummary
	var total int64
	var eventData []byte
	var eventCount int
	flushEvent := func() error {
		if len(eventData) == 0 {
			return nil
		}
		eventCount++
		if eventCount > maxEvents {
			return errors.New("foundry response exceeded adapter event limit")
		}
		if int64(len(eventData)) > maxEventBytes {
			return errors.New("foundry response event exceeded adapter limit")
		}
		if bytes.Equal(bytes.TrimSpace(eventData), []byte("[DONE]")) {
			eventData = nil
			return nil
		}
		var event ResponseEvent
		if err := json.Unmarshal(eventData, &event); err != nil {
			return errors.New("foundry response stream contained invalid JSON")
		}
		eventData = nil
		return applyEvent(&summary, event, callbacks)
	}
	for {
		line, err := reader.ReadBytes('\n')
		total += int64(len(line))
		if total > maxBytes {
			return StreamSummary{}, errors.New("foundry response exceeded adapter stream limit")
		}
		trimmed := bytes.TrimRight(line, "\r\n")
		if len(trimmed) == 0 {
			if err := flushEvent(); err != nil {
				return StreamSummary{}, err
			}
		} else if rawData, ok := bytes.CutPrefix(trimmed, []byte("data:")); ok {
			part := bytes.TrimSpace(rawData)
			if len(eventData)+len(part)+1 > int(maxEventBytes) {
				return StreamSummary{}, errors.New("foundry response event exceeded adapter limit")
			}
			if len(eventData) > 0 {
				eventData = append(eventData, '\n')
			}
			eventData = append(eventData, part...)
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return StreamSummary{}, err
			}
			if flushErr := flushEvent(); flushErr != nil {
				return StreamSummary{}, flushErr
			}
			break
		}
	}
	if summary.Status == "" {
		return StreamSummary{}, errors.New("foundry response stream ended without a terminal event")
	}
	return summary, nil
}

func applyEvent(summary *StreamSummary, event ResponseEvent, callbacks ResponseCallbacks) error {
	switch event.Type {
	case "response.created", "response.in_progress", "response.queued":
		if event.Response != nil {
			mergeResponse(summary, *event.Response)
			if event.Type == "response.created" && callbacks.OnCreated != nil {
				return callbacks.OnCreated(*event.Response)
			}
		}
	case "response.output_text.delta":
		summary.Text += event.Delta
		if callbacks.OnTextDelta != nil && event.Delta != "" {
			return callbacks.OnTextDelta(event.Delta)
		}
	case "response.output_item.done":
		if event.Item != nil && event.Item.Type == "function_call" {
			summary.FunctionCalls = append(summary.FunctionCalls, *event.Item)
			if callbacks.OnFunctionCall != nil {
				return callbacks.OnFunctionCall(*event.Item)
			}
		}
	case "response.completed", "response.failed", "response.incomplete", "response.cancelled", "response.canceled":
		if event.Response != nil {
			mergeResponse(summary, *event.Response)
			if event.Type == "response.completed" {
				if err := applyTerminalOutputFallback(summary, *event.Response, callbacks); err != nil {
					return err
				}
			}
		}
		if summary.Status == "" {
			summary.Status = strings.TrimPrefix(event.Type, "response.")
		}
	case "error":
		summary.Status = "failed"
		summary.Error = event.Error
	}
	return nil
}

func applyTerminalOutputFallback(
	summary *StreamSummary,
	response Response,
	callbacks ResponseCallbacks,
) error {
	var terminalText strings.Builder
	for _, item := range response.Output {
		if item.Type != "message" {
			continue
		}
		for _, content := range item.Content {
			terminalText.WriteString(content.Text)
		}
	}
	fullText := terminalText.String()
	if fullText != "" {
		if !strings.HasPrefix(fullText, summary.Text) {
			return errors.New("foundry terminal output does not match streamed text")
		}
		remainder := strings.TrimPrefix(fullText, summary.Text)
		if remainder != "" {
			summary.Text += remainder
			if callbacks.OnTextDelta != nil {
				if err := callbacks.OnTextDelta(remainder); err != nil {
					return err
				}
			}
		}
	}
	for _, item := range response.Output {
		if item.Type != "function_call" {
			continue
		}
		seen, conflict := functionCallState(summary.FunctionCalls, item)
		if conflict {
			return fmt.Errorf("foundry function call %q changed within one response", item.CallID)
		}
		if seen {
			continue
		}
		summary.FunctionCalls = append(summary.FunctionCalls, item)
		if callbacks.OnFunctionCall != nil {
			if err := callbacks.OnFunctionCall(item); err != nil {
				return err
			}
		}
	}
	return nil
}

func functionCallState(calls []OutputItem, item OutputItem) (bool, bool) {
	for _, call := range calls {
		if call.CallID != item.CallID {
			continue
		}
		return true, call.Name != item.Name || !bytes.Equal(call.Arguments, item.Arguments)
	}
	return false, false
}

func CompleteResponse(response Response, callbacks ResponseCallbacks) (StreamSummary, error) {
	summary := StreamSummary{}
	mergeResponse(&summary, response)
	if callbacks.OnCreated != nil {
		if err := callbacks.OnCreated(response); err != nil {
			return StreamSummary{}, err
		}
	}
	if !strings.EqualFold(response.Status, "completed") {
		return summary, nil
	}
	for _, item := range response.Output {
		switch item.Type {
		case "message":
			for _, content := range item.Content {
				if content.Text == "" {
					continue
				}
				summary.Text += content.Text
				if callbacks.OnTextDelta != nil {
					if err := callbacks.OnTextDelta(content.Text); err != nil {
						return StreamSummary{}, err
					}
				}
			}
		case "function_call":
			summary.FunctionCalls = append(summary.FunctionCalls, item)
			if callbacks.OnFunctionCall != nil {
				if err := callbacks.OnFunctionCall(item); err != nil {
					return StreamSummary{}, err
				}
			}
		}
	}
	return summary, nil
}

func mergeResponse(summary *StreamSummary, response Response) {
	if response.ID != "" {
		summary.ResponseID = response.ID
	}
	if response.AgentSessionID != "" {
		summary.AgentSessionID = response.AgentSessionID
	}
	if response.Status != "" {
		summary.Status = response.Status
	}
	if response.Error != nil {
		summary.Error = response.Error
	}
	if response.Incomplete != nil {
		summary.Incomplete = response.Incomplete
	}
}
