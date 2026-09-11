package adapter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orka-agents/agent-runtime-foundry/conformance"
	"github.com/orka-agents/agent-runtime-foundry/internal/foundry"
	"github.com/orka-agents/agent-runtime-foundry/internal/harness"
)

type scriptedResponse struct {
	validate func(foundry.ResponseRequest) error
	run      func(context.Context, foundry.ResponseCallbacks) (foundry.StreamSummary, error)
}

type fakeResponsesBackend struct {
	mu              sync.Mutex
	responses       []scriptedResponse
	requests        []foundry.ResponseRequest
	createSessionID string
	createSessions  int
	validateErr     error
}

func (f *fakeResponsesBackend) CreateResponse(ctx context.Context, request foundry.ResponseRequest, callbacks foundry.ResponseCallbacks) (foundry.StreamSummary, error) {
	f.mu.Lock()
	f.requests = append(f.requests, request)
	if len(f.responses) == 0 {
		f.mu.Unlock()
		return foundry.StreamSummary{}, errors.New("no scripted response")
	}
	response := f.responses[0]
	f.responses = f.responses[1:]
	f.mu.Unlock()
	if response.validate != nil {
		if err := response.validate(request); err != nil {
			return foundry.StreamSummary{}, err
		}
	}
	return response.run(ctx, callbacks)
}

func (f *fakeResponsesBackend) CreateSession(context.Context) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createSessions++
	if f.createSessionID == "" {
		return "", errors.New("session creation failed")
	}
	return f.createSessionID, nil
}

func (f *fakeResponsesBackend) ValidateAgent(context.Context) error {
	return f.validateErr
}

func TestAdapterObservedTurnStreamsAndReusesSession(t *testing.T) {
	backend := &fakeResponsesBackend{responses: []scriptedResponse{
		textResponseScript("resp-1", "session-1", "first answer"),
		{
			validate: func(request foundry.ResponseRequest) error {
				if request.PreviousResponseID != "resp-1" || request.AgentSessionID != "session-1" {
					return errors.New("continuation identifiers were not reused")
				}
				return nil
			},
			run: textResponseScript("resp-2", "session-1", "second answer").run,
		},
	}}
	cfg := testConfig("http://127.0.0.1")
	adapter := newAdapter(cfg, backend)

	first := startRequest("observed-1", "shared-session")
	turn1, _, err := adapter.startTurn(first)
	if err != nil {
		t.Fatalf("start first: %v", err)
	}
	waitTurnDone(t, turn1)
	if got := completedResult(turn1.frames); got != "first answer" {
		t.Fatalf("first result = %q", got)
	}
	second := startRequest("observed-2", "shared-session")
	turn2, _, err := adapter.startTurn(second)
	if err != nil {
		t.Fatalf("start second: %v", err)
	}
	waitTurnDone(t, turn2)
	if got := completedResult(turn2.frames); got != "second answer" {
		t.Fatalf("second result = %q", got)
	}
	if backend.createSessions != 0 {
		t.Fatalf("created sessions = %d, want automatic session from response", backend.createSessions)
	}
}

func TestAdapterBrokeredFunctionCallContinuation(t *testing.T) {
	backend := &fakeResponsesBackend{responses: []scriptedResponse{
		{
			validate: func(request foundry.ResponseRequest) error {
				if len(request.Tools) != 1 || request.Tools[0].Name != "lookup_ticket" {
					return errors.New("safe Orka tool schema missing")
				}
				return nil
			},
			run: func(_ context.Context, callbacks foundry.ResponseCallbacks) (foundry.StreamSummary, error) {
				if err := callbacks.OnCreated(foundry.Response{ID: "resp-tool", Status: "in_progress", AgentSessionID: "session-tool"}); err != nil {
					return foundry.StreamSummary{}, err
				}
				call := foundry.OutputItem{Type: "function_call", CallID: "call-1", Name: "lookup_ticket", Arguments: json.RawMessage(`"{\"ticket\":\"INC-1\"}"`)}
				if err := callbacks.OnFunctionCall(call); err != nil {
					return foundry.StreamSummary{}, err
				}
				return foundry.StreamSummary{ResponseID: "resp-tool", AgentSessionID: "session-tool", Status: "completed", FunctionCalls: []foundry.OutputItem{call}}, nil
			},
		},
		{
			validate: func(request foundry.ResponseRequest) error {
				if request.PreviousResponseID != "resp-tool" || request.AgentSessionID != "session-tool" {
					return errors.New("tool continuation identifiers missing")
				}
				outputs, ok := request.Input.([]foundry.FunctionOutput)
				if !ok || len(outputs) != 1 || outputs[0].CallID != "call-1" || outputs[0].Type != "function_call_output" {
					return errors.New("function_call_output missing")
				}
				return nil
			},
			run: textResponseScript("resp-final", "session-tool", "ticket is open").run,
		},
	}}
	adapter := newAdapter(testConfig("http://127.0.0.1"), backend)
	request := startRequest("brokered", "brokered-session")
	request.ToolExecutionMode = harness.ToolExecutionModeBrokered
	request.Input.Tools = []harness.ToolDefinition{{
		Name:          "lookup_ticket",
		Description:   "Look up a ticket",
		BrokeredClass: harness.BrokeredToolClassRead,
		Parameters:    json.RawMessage(`{"type":"object","properties":{"ticket":{"type":"string"}},"required":["ticket"]}`),
	}}
	turn, _, err := adapter.startTurn(request)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	waitForFrame(t, adapter, turn, harness.FrameToolCallRequested)
	if turn.completed {
		t.Fatal("turn completed before brokered result")
	}
	continueRequest := harness.ContinueTurnRequest{
		Version:          harness.ProtocolVersion,
		Namespace:        request.Namespace,
		TaskName:         request.TaskName,
		SessionName:      request.SessionName,
		RuntimeSessionID: request.RuntimeSessionID,
		TurnID:           request.TurnID,
		CorrelationID:    request.CorrelationID,
		ToolResults: []harness.ToolCallResult{{
			Version:          harness.ProtocolVersion,
			RuntimeSessionID: request.RuntimeSessionID,
			TurnID:           request.TurnID,
			ToolCallID:       orkaToolCallID(request.RuntimeSessionID, request.TurnID, "call-1"),
			IdempotencyKey:   harness.ToolRequestIdempotencyKey(request.RuntimeSessionID, request.TurnID, orkaToolCallID(request.RuntimeSessionID, request.TurnID, "call-1")),
			Approved:         true,
			Output:           json.RawMessage(`{"status":"open"}`),
		}},
	}
	if err := adapter.continueTurn(continueRequest); err != nil {
		t.Fatalf("continue: %v", err)
	}
	waitTurnDone(t, turn)
	if got := completedResult(turn.frames); got != "ticket is open" {
		t.Fatalf("result = %q", got)
	}
	if findFrame(turn.frames, harness.FrameToolResultReceived) == nil {
		t.Fatal("ToolResultReceived frame missing")
	}
}

func TestAdapterProviderStaticBrokeredFunctionCallContinuation(t *testing.T) {
	backend := &fakeResponsesBackend{responses: []scriptedResponse{
		{
			validate: func(request foundry.ResponseRequest) error {
				if len(request.Tools) != 0 {
					return errors.New("provider-static mode forwarded request tool schemas")
				}
				return nil
			},
			run: func(_ context.Context, callbacks foundry.ResponseCallbacks) (foundry.StreamSummary, error) {
				if err := callbacks.OnCreated(foundry.Response{ID: "resp-static", Status: "in_progress", AgentSessionID: "session-static"}); err != nil {
					return foundry.StreamSummary{}, err
				}
				call := foundry.OutputItem{Type: "function_call", CallID: "call-static", Name: "lookup_ticket", Arguments: json.RawMessage(`{"ticket":"INC-1"}`)}
				if err := callbacks.OnFunctionCall(call); err != nil {
					return foundry.StreamSummary{}, err
				}
				return foundry.StreamSummary{ResponseID: "resp-static", AgentSessionID: "session-static", Status: "completed", FunctionCalls: []foundry.OutputItem{call}}, nil
			},
		},
		{
			validate: func(request foundry.ResponseRequest) error {
				if request.PreviousResponseID != "resp-static" || request.AgentSessionID != "session-static" {
					return errors.New("provider-static continuation identifiers missing")
				}
				if len(request.Tools) != 0 {
					return errors.New("provider-static continuation forwarded request tool schemas")
				}
				outputs, ok := request.Input.([]foundry.FunctionOutput)
				if !ok || len(outputs) != 1 || outputs[0].CallID != "call-static" || outputs[0].Type != "function_call_output" {
					return errors.New("provider-static function_call_output missing")
				}
				return nil
			},
			run: textResponseScript("resp-static-final", "session-static", "ticket is open").run,
		},
	}}
	cfg := testConfig("http://127.0.0.1")
	cfg.toolSchemaMode = foundry.ToolSchemaModeProviderStatic
	adapter := newAdapter(cfg, backend)
	request := startRequest("provider-static", "provider-static-session")
	request.ToolExecutionMode = harness.ToolExecutionModeBrokered
	request.Input.Tools = []harness.ToolDefinition{{
		Name:          "lookup_ticket",
		Description:   "Look up a ticket",
		BrokeredClass: harness.BrokeredToolClassRead,
		Parameters:    json.RawMessage(`{"type":"object","properties":{"ticket":{"type":"string"}},"required":["ticket"]}`),
	}}
	turn, _, err := adapter.startTurn(request)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	waitForFrame(t, adapter, turn, harness.FrameToolCallRequested)
	continueRequest := harness.ContinueTurnRequest{
		Version:          harness.ProtocolVersion,
		Namespace:        request.Namespace,
		TaskName:         request.TaskName,
		SessionName:      request.SessionName,
		RuntimeSessionID: request.RuntimeSessionID,
		TurnID:           request.TurnID,
		CorrelationID:    request.CorrelationID,
		ToolResults: []harness.ToolCallResult{{
			Version:          harness.ProtocolVersion,
			RuntimeSessionID: request.RuntimeSessionID,
			TurnID:           request.TurnID,
			ToolCallID:       orkaToolCallID(request.RuntimeSessionID, request.TurnID, "call-static"),
			IdempotencyKey:   harness.ToolRequestIdempotencyKey(request.RuntimeSessionID, request.TurnID, orkaToolCallID(request.RuntimeSessionID, request.TurnID, "call-static")),
			Approved:         true,
			Output:           json.RawMessage(`{"status":"open"}`),
		}},
	}
	if err := adapter.continueTurn(continueRequest); err != nil {
		t.Fatalf("continue: %v", err)
	}
	waitTurnDone(t, turn)
	if got := completedResult(turn.frames); got != "ticket is open" {
		t.Fatalf("result = %q", got)
	}
}

func TestAdapterVersionPinCreatesSessionOnce(t *testing.T) {
	backend := &fakeResponsesBackend{createSessionID: "pinned-session", responses: []scriptedResponse{
		{
			validate: func(request foundry.ResponseRequest) error {
				if request.AgentSessionID != "pinned-session" {
					return errors.New("pinned session id missing")
				}
				return nil
			},
			run: textResponseScript("resp-pinned", "pinned-session", "pinned").run,
		},
	}}
	cfg := testConfig("http://127.0.0.1")
	cfg.agentVersion = "2"
	adapter := newAdapter(cfg, backend)
	turn, _, err := adapter.startTurn(startRequest("pinned", "pinned-runtime"))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	waitTurnDone(t, turn)
	if backend.createSessions != 1 {
		t.Fatalf("create sessions = %d, want 1", backend.createSessions)
	}
}

func TestAdapterCancellationCancelsStreamAndResponse(t *testing.T) {
	started := make(chan struct{})
	backend := &fakeResponsesBackend{responses: []scriptedResponse{{
		run: func(ctx context.Context, callbacks foundry.ResponseCallbacks) (foundry.StreamSummary, error) {
			if err := callbacks.OnCreated(foundry.Response{ID: "resp-cancel", Status: "in_progress"}); err != nil {
				return foundry.StreamSummary{}, err
			}
			close(started)
			<-ctx.Done()
			return foundry.StreamSummary{}, ctx.Err()
		},
	}}}
	adapter := newAdapter(testConfig("http://127.0.0.1"), backend)
	request := startRequest("cancel", "cancel-session")
	turn, _, err := adapter.startTurn(request)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	<-started
	if err := adapter.cancelTurn(cancelRequest(request)); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	waitTurnDone(t, turn)
	if findFrame(turn.frames, harness.FrameTurnCancelled) == nil {
		t.Fatalf("frames = %#v, want TurnCancelled", turn.frames)
	}
}

func TestAdapterMapsFailedAndIncompleteResponses(t *testing.T) {
	for _, test := range []struct {
		name   string
		script scriptedResponse
		reason string
	}{
		{
			name: "failed",
			script: scriptedResponse{run: func(_ context.Context, callbacks foundry.ResponseCallbacks) (foundry.StreamSummary, error) {
				_ = callbacks.OnCreated(foundry.Response{ID: "resp-failed", Status: "in_progress"})
				return foundry.StreamSummary{ResponseID: "resp-failed", Status: "failed", Error: &foundry.ResponseError{Code: "server_error"}}, nil
			}},
			reason: "foundry_response_failed",
		},
		{
			name: "incomplete",
			script: scriptedResponse{run: func(_ context.Context, callbacks foundry.ResponseCallbacks) (foundry.StreamSummary, error) {
				_ = callbacks.OnCreated(foundry.Response{ID: "resp-incomplete", Status: "in_progress"})
				return foundry.StreamSummary{ResponseID: "resp-incomplete", Status: "incomplete", Incomplete: &foundry.Incomplete{Reason: "max_output_tokens"}}, nil
			}},
			reason: "foundry_response_incomplete",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			adapter := newAdapter(testConfig("http://127.0.0.1"), &fakeResponsesBackend{responses: []scriptedResponse{test.script}})
			turn, _, err := adapter.startTurn(startRequest(test.name, test.name+"-session"))
			if err != nil {
				t.Fatalf("start: %v", err)
			}
			waitTurnDone(t, turn)
			failed := findFrame(turn.frames, harness.FrameTurnFailed)
			if failed == nil || failed.Failed == nil || failed.Failed.Reason != test.reason {
				t.Fatalf("failed frame = %#v", failed)
			}
		})
	}
}

func TestAdapterRejectsUnsafeToolCallAndOversizedResult(t *testing.T) {
	backend := &fakeResponsesBackend{responses: []scriptedResponse{{
		run: func(_ context.Context, callbacks foundry.ResponseCallbacks) (foundry.StreamSummary, error) {
			_ = callbacks.OnCreated(foundry.Response{ID: "resp-unsafe", Status: "in_progress"})
			if err := callbacks.OnFunctionCall(foundry.OutputItem{Type: "function_call", CallID: "call-1", Name: "not_allowed", Arguments: json.RawMessage(`{}`)}); err != nil {
				return foundry.StreamSummary{}, err
			}
			return foundry.StreamSummary{ResponseID: "resp-unsafe", Status: "completed"}, nil
		},
	}}}
	cfg := testConfig("http://127.0.0.1")
	cfg.maxBrokeredBytes = 64
	adapter := newAdapter(cfg, backend)
	request := startRequest("unsafe", "unsafe-session")
	request.ToolExecutionMode = harness.ToolExecutionModeBrokered
	request.Input.Tools = []harness.ToolDefinition{{Name: "allowed", BrokeredClass: harness.BrokeredToolClassRead, Parameters: json.RawMessage(`{"type":"object"}`)}}
	turn, _, err := adapter.startTurn(request)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	waitTurnDone(t, turn)
	failed := findFrame(turn.frames, harness.FrameTurnFailed)
	if failed == nil || failed.Failed == nil || failed.Failed.Message != `Foundry requested tool "not_allowed" that was not supplied by Orka` {
		if failed == nil || failed.Failed == nil {
			t.Fatalf("failed = %#v", failed)
		}
		t.Fatalf("failed message = %q", failed.Failed.Message)
	}
	if err := validateBrokeredResultFrames([]harness.ToolCallResult{{ToolCallID: "call-1", Output: json.RawMessage(`"` + strings.Repeat("x", 128) + `"`)}}, 64); err == nil {
		t.Fatal("expected oversized result error")
	}
}

func TestHarnessConformanceObservedAndBrokered(t *testing.T) {
	for _, probe := range []struct {
		name      string
		toolClass harness.BrokeredToolClass
		configure func(*conformance.Target)
	}{
		{name: "observed", configure: func(target *conformance.Target) { target.ProbeTurn = true }},
		{name: "brokered-read", toolClass: harness.BrokeredToolClassRead, configure: func(target *conformance.Target) { target.ProbeBrokeredRead = true }},
		{name: "brokered-write", toolClass: harness.BrokeredToolClassWrite, configure: func(target *conformance.Target) { target.ProbeBrokeredWrite = true }},
	} {
		t.Run(probe.name, func(t *testing.T) {
			backend := conformanceBackend(probe.toolClass)
			cfg := testConfig("http://127.0.0.1")
			adapter := newAdapter(cfg, backend)
			server := httptest.NewServer((&server{cfg: cfg, adapter: adapter}).handler())
			defer server.Close()
			target := conformance.Target{BaseURL: server.URL, BearerToken: "adapter-token", RequireAuth: true, ControlTimeout: 2 * time.Second}
			probe.configure(&target)
			result := conformance.Check(context.Background(), target)
			if !result.Passed {
				t.Fatalf("conformance failed: %s (%#v)", result.Message, result.Failures)
			}
		})
	}
}

func conformanceBackend(class harness.BrokeredToolClass) *fakeResponsesBackend {
	if class == "" {
		return &fakeResponsesBackend{responses: []scriptedResponse{textResponseScript("resp-observed", "session-observed", "ok")}}
	}
	return &fakeResponsesBackend{responses: []scriptedResponse{
		{
			run: func(_ context.Context, callbacks foundry.ResponseCallbacks) (foundry.StreamSummary, error) {
				_ = callbacks.OnCreated(foundry.Response{ID: "resp-tool", Status: "in_progress", AgentSessionID: "session-tool"})
				call := foundry.OutputItem{Type: "function_call", CallID: "call-1", Name: "conformance_" + string(class), Arguments: json.RawMessage(`{"value":"probe"}`)}
				if err := callbacks.OnFunctionCall(call); err != nil {
					return foundry.StreamSummary{}, err
				}
				return foundry.StreamSummary{ResponseID: "resp-tool", AgentSessionID: "session-tool", Status: "completed", FunctionCalls: []foundry.OutputItem{call}}, nil
			},
		},
		textResponseScript("resp-final", "session-tool", "ok"),
	}}
}

func textResponseScript(responseID, sessionID, text string) scriptedResponse {
	return scriptedResponse{run: func(_ context.Context, callbacks foundry.ResponseCallbacks) (foundry.StreamSummary, error) {
		if err := callbacks.OnCreated(foundry.Response{ID: responseID, Status: "in_progress", AgentSessionID: sessionID}); err != nil {
			return foundry.StreamSummary{}, err
		}
		if err := callbacks.OnTextDelta(text); err != nil {
			return foundry.StreamSummary{}, err
		}
		return foundry.StreamSummary{ResponseID: responseID, AgentSessionID: sessionID, Status: "completed", Text: text}, nil
	}}
}

func startRequest(name, runtimeSession string) harness.StartTurnRequest {
	return harness.StartTurnRequest{
		Version:           harness.ProtocolVersion,
		Namespace:         "default",
		TaskName:          name,
		SessionName:       runtimeSession,
		RuntimeSessionID:  harness.RuntimeSessionID(runtimeSession),
		TurnID:            harness.HarnessTurnID(name + "-turn"),
		CorrelationID:     name + "-correlation",
		Deadline:          time.Now().UTC().Add(10 * time.Second),
		AuthIdentity:      harness.AuthIdentity{Subject: "task:default/" + name},
		ToolExecutionMode: harness.ToolExecutionModeObserved,
		Input:             harness.TurnInput{Prompt: "Run the task"},
	}
}

func cancelRequest(start harness.StartTurnRequest) harness.CancelTurnRequest {
	return harness.CancelTurnRequest{
		Version:          harness.ProtocolVersion,
		Namespace:        start.Namespace,
		TaskName:         start.TaskName,
		SessionName:      start.SessionName,
		RuntimeSessionID: start.RuntimeSessionID,
		TurnID:           start.TurnID,
		CorrelationID:    start.CorrelationID,
		Reason:           "test cancellation",
	}
}

func waitTurnDone(t *testing.T, turn *turnState) {
	t.Helper()
	select {
	case <-turn.done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for turn")
	}
}

func waitForFrame(t *testing.T, adapter *adapter, turn *turnState, typ harness.FrameType) *harness.HarnessEventFrame {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		adapter.mu.Lock()
		frame := findFrame(turn.frames, typ)
		adapter.mu.Unlock()
		if frame != nil {
			return frame
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", typ)
	return nil
}

func completedResult(frames []harness.HarnessEventFrame) string {
	frame := findFrame(frames, harness.FrameTurnCompleted)
	if frame == nil || frame.Completed == nil {
		return ""
	}
	return frame.Completed.Result
}

func findFrame(frames []harness.HarnessEventFrame, typ harness.FrameType) *harness.HarnessEventFrame {
	for i := range frames {
		if frames[i].Type == typ {
			return &frames[i]
		}
	}
	return nil
}

func TestAdapterDeadlineWhileWaitingForTool(t *testing.T) {
	backend := &fakeResponsesBackend{responses: []scriptedResponse{{
		run: func(_ context.Context, callbacks foundry.ResponseCallbacks) (foundry.StreamSummary, error) {
			_ = callbacks.OnCreated(foundry.Response{ID: "resp-wait", Status: "in_progress"})
			call := foundry.OutputItem{Type: "function_call", CallID: "call-wait", Name: "lookup", Arguments: json.RawMessage(`{}`)}
			if err := callbacks.OnFunctionCall(call); err != nil {
				return foundry.StreamSummary{}, err
			}
			return foundry.StreamSummary{ResponseID: "resp-wait", AgentSessionID: "session-wait", Status: "completed", FunctionCalls: []foundry.OutputItem{call}}, nil
		},
	}}}
	cfg := testConfig("http://127.0.0.1")
	cfg.turnTimeout = 30 * time.Millisecond
	adapter := newAdapter(cfg, backend)
	request := startRequest("wait-deadline", "wait-session")
	request.ToolExecutionMode = harness.ToolExecutionModeBrokered
	request.Input.Tools = []harness.ToolDefinition{{Name: "lookup", BrokeredClass: harness.BrokeredToolClassRead, Parameters: json.RawMessage(`{"type":"object"}`)}}
	turn, _, err := adapter.startTurn(request)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	waitTurnDone(t, turn)
	failed := findFrame(turn.frames, harness.FrameTurnFailed)
	if failed == nil || failed.Failed == nil || failed.Failed.Reason != "foundry_timeout" {
		t.Fatalf("failed = %#v", failed)
	}
}

func TestAdapterContinuationRetryIsIdempotent(t *testing.T) {
	block := make(chan struct{})
	backend := &fakeResponsesBackend{responses: []scriptedResponse{
		{
			run: func(_ context.Context, callbacks foundry.ResponseCallbacks) (foundry.StreamSummary, error) {
				_ = callbacks.OnCreated(foundry.Response{ID: "resp-idem", Status: "in_progress"})
				call := foundry.OutputItem{Type: "function_call", CallID: "call-idem", Name: "lookup", Arguments: json.RawMessage(`{}`)}
				if err := callbacks.OnFunctionCall(call); err != nil {
					return foundry.StreamSummary{}, err
				}
				return foundry.StreamSummary{ResponseID: "resp-idem", AgentSessionID: "session-idem", Status: "completed", FunctionCalls: []foundry.OutputItem{call}}, nil
			},
		},
		{
			run: func(ctx context.Context, callbacks foundry.ResponseCallbacks) (foundry.StreamSummary, error) {
				_ = callbacks.OnCreated(foundry.Response{ID: "resp-idem-final", Status: "in_progress"})
				select {
				case <-block:
				case <-ctx.Done():
					return foundry.StreamSummary{}, ctx.Err()
				}
				_ = callbacks.OnTextDelta("ok")
				return foundry.StreamSummary{ResponseID: "resp-idem-final", Status: "completed", Text: "ok"}, nil
			},
		},
	}}
	adapter := newAdapter(testConfig("http://127.0.0.1"), backend)
	request := startRequest("idem", "idem-session")
	request.ToolExecutionMode = harness.ToolExecutionModeBrokered
	request.Input.Tools = []harness.ToolDefinition{{Name: "lookup", BrokeredClass: harness.BrokeredToolClassRead, Parameters: json.RawMessage(`{"type":"object"}`)}}
	turn, _, err := adapter.startTurn(request)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	waitForFrame(t, adapter, turn, harness.FrameToolCallRequested)
	result := harness.ToolCallResult{
		Version:          harness.ProtocolVersion,
		RuntimeSessionID: request.RuntimeSessionID,
		TurnID:           request.TurnID,
		ToolCallID:       orkaToolCallID(request.RuntimeSessionID, request.TurnID, "call-idem"),
		IdempotencyKey: harness.ToolRequestIdempotencyKey(
			request.RuntimeSessionID,
			request.TurnID,
			orkaToolCallID(request.RuntimeSessionID, request.TurnID, "call-idem"),
		),
		Approved: true,
		Output:   json.RawMessage(`{"ok":true}`),
	}
	continueRequest := harness.ContinueTurnRequest{Version: harness.ProtocolVersion, Namespace: request.Namespace, TaskName: request.TaskName, SessionName: request.SessionName, RuntimeSessionID: request.RuntimeSessionID, TurnID: request.TurnID, CorrelationID: request.CorrelationID, ToolResults: []harness.ToolCallResult{result}}
	if err := adapter.continueTurn(continueRequest); err != nil {
		t.Fatalf("first continue: %v", err)
	}
	if err := adapter.continueTurn(continueRequest); err != nil {
		t.Fatalf("retry continue: %v", err)
	}
	changed := continueRequest
	changed.ToolResults = []harness.ToolCallResult{result}
	changed.ToolResults[0].Output = json.RawMessage(`{"ok":false}`)
	if err := adapter.continueTurn(changed); err == nil {
		t.Fatal("changed retry was accepted")
	}
	close(block)
	waitTurnDone(t, turn)
}

func TestAdapterRejectsConcurrentTurnsAndPrunesCompletedState(t *testing.T) {
	block := make(chan struct{})
	backend := &fakeResponsesBackend{responses: []scriptedResponse{{
		run: func(ctx context.Context, callbacks foundry.ResponseCallbacks) (foundry.StreamSummary, error) {
			_ = callbacks.OnCreated(foundry.Response{ID: "resp-active", Status: "in_progress"})
			select {
			case <-block:
			case <-ctx.Done():
				return foundry.StreamSummary{}, ctx.Err()
			}
			return foundry.StreamSummary{ResponseID: "resp-active", Status: "completed"}, nil
		},
	}}}
	cfg := testConfig("http://127.0.0.1")
	adapter := newAdapter(cfg, backend)
	first := startRequest("active-1", "same-session")
	turn, _, err := adapter.startTurn(first)
	if err != nil {
		t.Fatalf("start first: %v", err)
	}
	waitForFrame(t, adapter, turn, harness.FrameTurnStarted)
	if _, _, err := adapter.startTurn(startRequest("active-2", "same-session")); !errors.Is(err, errRuntimeSessionBusy) {
		t.Fatalf("same-session start error = %v", err)
	}
	if _, _, err := adapter.startTurn(startRequest("active-3", "other-session")); !errors.Is(err, errAdapterAtCapacity) {
		t.Fatalf("capacity start error = %v", err)
	}
	close(block)
	waitTurnDone(t, turn)
	adapter.mu.Lock()
	turn.cleanupAt = time.Now().Add(-time.Second)
	adapter.pruneLocked(time.Now())
	_, retained := adapter.turns[first.TurnID]
	_, tombstone := adapter.turnTombstones[first.TurnID]
	adapter.mu.Unlock()
	if retained || !tombstone {
		t.Fatalf("retained=%t tombstone=%t", retained, tombstone)
	}
}

func TestServerRejectsPathBodyTurnMismatch(t *testing.T) {
	backend := &fakeResponsesBackend{responses: []scriptedResponse{
		textResponseScript("resp-a", "session-a", "a"),
		textResponseScript("resp-b", "session-b", "b"),
	}}
	cfg := testConfig("http://127.0.0.1")
	cfg.maxConcurrent = 2
	adapter := newAdapter(cfg, backend)
	first := startRequest("path-a", "path-session-a")
	second := startRequest("path-b", "path-session-b")
	turnA, _, err := adapter.startTurn(first)
	if err != nil {
		t.Fatalf("start A: %v", err)
	}
	turnB, _, err := adapter.startTurn(second)
	if err != nil {
		t.Fatalf("start B: %v", err)
	}
	waitTurnDone(t, turnA)
	waitTurnDone(t, turnB)
	server := httptest.NewServer((&server{cfg: cfg, adapter: adapter}).handler())
	defer server.Close()
	body, _ := json.Marshal(cancelRequest(second))
	req, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/turns/"+string(first.TurnID)+"/cancel", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer adapter-token")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("cancel request: %v", err)
	}
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}

func TestAdapterQueuesEarlyToolResultUntilResponseCompletes(t *testing.T) {
	allowComplete := make(chan struct{})
	backend := &fakeResponsesBackend{responses: []scriptedResponse{
		{
			run: func(_ context.Context, callbacks foundry.ResponseCallbacks) (foundry.StreamSummary, error) {
				_ = callbacks.OnCreated(foundry.Response{ID: "resp-early", Status: "in_progress"})
				call := foundry.OutputItem{Type: "function_call", CallID: "call-early", Name: "lookup", Arguments: json.RawMessage(`{}`)}
				if err := callbacks.OnFunctionCall(call); err != nil {
					return foundry.StreamSummary{}, err
				}
				<-allowComplete
				return foundry.StreamSummary{ResponseID: "resp-early", AgentSessionID: "session-early", Status: "completed", FunctionCalls: []foundry.OutputItem{call}}, nil
			},
		},
		textResponseScript("resp-after-early", "session-early", "continued"),
	}}
	adapter := newAdapter(testConfig("http://127.0.0.1"), backend)
	request := startRequest("early", "early-session")
	request.ToolExecutionMode = harness.ToolExecutionModeBrokered
	request.Input.Tools = []harness.ToolDefinition{{Name: "lookup", BrokeredClass: harness.BrokeredToolClassRead, Parameters: json.RawMessage(`{"type":"object"}`)}}
	turn, _, err := adapter.startTurn(request)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	waitForFrame(t, adapter, turn, harness.FrameToolCallRequested)
	result := harness.ToolCallResult{Version: harness.ProtocolVersion, RuntimeSessionID: request.RuntimeSessionID, TurnID: request.TurnID, ToolCallID: orkaToolCallID(request.RuntimeSessionID, request.TurnID, "call-early"), IdempotencyKey: harness.ToolRequestIdempotencyKey(request.RuntimeSessionID, request.TurnID, orkaToolCallID(request.RuntimeSessionID, request.TurnID, "call-early")), Approved: true, Output: json.RawMessage(`{"ok":true}`)}
	continueRequest := harness.ContinueTurnRequest{Version: harness.ProtocolVersion, Namespace: request.Namespace, TaskName: request.TaskName, SessionName: request.SessionName, RuntimeSessionID: request.RuntimeSessionID, TurnID: request.TurnID, CorrelationID: request.CorrelationID, ToolResults: []harness.ToolCallResult{result}}
	if err := adapter.continueTurn(continueRequest); err != nil {
		t.Fatalf("continue: %v", err)
	}
	backend.mu.Lock()
	requestCount := len(backend.requests)
	backend.mu.Unlock()
	if requestCount != 1 {
		t.Fatalf("requests before completion = %d, want 1", requestCount)
	}
	close(allowComplete)
	waitTurnDone(t, turn)
	if got := completedResult(turn.frames); got != "continued" {
		t.Fatalf("result = %q", got)
	}
}

func TestRuntimeSessionContinuityIsNotPrunedWithTurnState(t *testing.T) {
	adapter := newAdapter(testConfig("http://127.0.0.1"), &fakeResponsesBackend{})
	sessionID := harness.RuntimeSessionID("retained-session")
	adapter.runtimeSessions[sessionID] = &runtimeSessionState{AgentSessionID: "provider-session", PreviousResponse: "resp-1", LastSeen: time.Now().Add(-24 * time.Hour)}
	adapter.mu.Lock()
	adapter.pruneLocked(time.Now())
	_, retained := adapter.runtimeSessions[sessionID]
	adapter.mu.Unlock()
	if !retained {
		t.Fatal("runtime session continuity was pruned")
	}
}

func TestFoundryFrameMetadataDoesNotExposeProviderHandles(t *testing.T) {
	turn := &turnState{responseID: "resp-private", agentSessionID: "session-private"}
	metadata := foundryFrameMetadata(turn)
	if len(metadata) != 1 || metadata["backend"] != backendMetadata {
		t.Fatalf("metadata = %#v", metadata)
	}
}

func TestRetryableResponseFailureIsConservative(t *testing.T) {
	if !retryableResponseFailure(&foundry.ResponseError{Code: "server_error"}) {
		t.Fatal("server_error should be retryable")
	}
	if retryableResponseFailure(&foundry.ResponseError{Code: "content_filter"}) {
		t.Fatal("content_filter should not be retryable")
	}
	if retryableResponseFailure(nil) {
		t.Fatal("unknown failure should not be retryable")
	}
}

func TestUnresolvedToolResponseDoesNotAdvanceSessionCheckpoint(t *testing.T) {
	backend := &fakeResponsesBackend{responses: []scriptedResponse{{
		run: func(_ context.Context, callbacks foundry.ResponseCallbacks) (foundry.StreamSummary, error) {
			_ = callbacks.OnCreated(foundry.Response{ID: "resp-unresolved", Status: "in_progress", AgentSessionID: "session-1"})
			call := foundry.OutputItem{Type: "function_call", CallID: "call-unresolved", Name: "lookup", Arguments: json.RawMessage(`{}`)}
			if err := callbacks.OnFunctionCall(call); err != nil {
				return foundry.StreamSummary{}, err
			}
			return foundry.StreamSummary{ResponseID: "resp-unresolved", AgentSessionID: "session-1", Status: "completed", FunctionCalls: []foundry.OutputItem{call}}, nil
		},
	}}}
	adapter := newAdapter(testConfig("http://127.0.0.1"), backend)
	runtimeID := harness.RuntimeSessionID("checkpoint-session")
	request := startRequest("checkpoint", string(runtimeID))
	adapter.runtimeSessions[runtimeID] = &runtimeSessionState{
		OwnerFingerprint: runtimeSessionOwnerFingerprint(request),
		AgentSessionID:   "session-1",
		PreviousResponse: "resp-committed",
		LastSeen:         time.Now(),
	}
	request.ToolExecutionMode = harness.ToolExecutionModeBrokered
	request.Input.Tools = []harness.ToolDefinition{{Name: "lookup", BrokeredClass: harness.BrokeredToolClassRead, Parameters: json.RawMessage(`{"type":"object"}`)}}
	turn, _, err := adapter.startTurn(request)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	waitForFrame(t, adapter, turn, harness.FrameToolCallRequested)
	if err := adapter.cancelTurn(cancelRequest(request)); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	adapter.mu.Lock()
	checkpoint := adapter.runtimeSessions[runtimeID].PreviousResponse
	adapter.mu.Unlock()
	if checkpoint != "resp-committed" {
		t.Fatalf("checkpoint = %q", checkpoint)
	}
}

func TestBrokeredTurnStateIsCumulativelyBounded(t *testing.T) {
	backend := &fakeResponsesBackend{responses: []scriptedResponse{{
		run: func(_ context.Context, callbacks foundry.ResponseCallbacks) (foundry.StreamSummary, error) {
			_ = callbacks.OnCreated(foundry.Response{ID: "resp-bounded", Status: "in_progress"})
			call := foundry.OutputItem{Type: "function_call", CallID: "call-bounded", Name: "lookup", Arguments: json.RawMessage(`{"key":"value"}`)}
			if err := callbacks.OnFunctionCall(call); err != nil {
				return foundry.StreamSummary{}, err
			}
			return foundry.StreamSummary{ResponseID: "resp-bounded", AgentSessionID: "session-bounded", Status: "completed", FunctionCalls: []foundry.OutputItem{call}}, nil
		},
	}}}
	cfg := testConfig("http://127.0.0.1")
	cfg.maxBrokeredTurnBytes = 96
	adapter := newAdapter(cfg, backend)
	request := startRequest("bounded", "bounded-session")
	request.ToolExecutionMode = harness.ToolExecutionModeBrokered
	request.Input.Tools = []harness.ToolDefinition{{Name: "lookup", BrokeredClass: harness.BrokeredToolClassRead, Parameters: json.RawMessage(`{"type":"object"}`)}}
	turn, _, err := adapter.startTurn(request)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	waitForFrame(t, adapter, turn, harness.FrameToolCallRequested)
	result := harness.ToolCallResult{Version: harness.ProtocolVersion, RuntimeSessionID: request.RuntimeSessionID, TurnID: request.TurnID, ToolCallID: orkaToolCallID(request.RuntimeSessionID, request.TurnID, "call-bounded"), IdempotencyKey: harness.ToolRequestIdempotencyKey(request.RuntimeSessionID, request.TurnID, orkaToolCallID(request.RuntimeSessionID, request.TurnID, "call-bounded")), Approved: true, Output: json.RawMessage(`{"payload":"` + strings.Repeat("x", 80) + `"}`)}
	continueRequest := harness.ContinueTurnRequest{Version: harness.ProtocolVersion, Namespace: request.Namespace, TaskName: request.TaskName, SessionName: request.SessionName, RuntimeSessionID: request.RuntimeSessionID, TurnID: request.TurnID, CorrelationID: request.CorrelationID, ToolResults: []harness.ToolCallResult{result}}
	if err := adapter.continueTurn(continueRequest); err == nil || !strings.Contains(err.Error(), "turn state") {
		t.Fatalf("continue error = %v", err)
	}
}

func TestIsolationKeyUsesExactRuntimeSessionIdentifier(t *testing.T) {
	plain := isolationKeyForRuntimeSession("session")
	padded := isolationKeyForRuntimeSession(" session ")
	if plain == padded {
		t.Fatalf("isolation keys collided: %q", plain)
	}
}

func TestGetTurnPrunesExpiredStateWithoutNewStart(t *testing.T) {
	adapter := newAdapter(testConfig("http://127.0.0.1"), &fakeResponsesBackend{})
	request := startRequest("expired-lookup", "expired-lookup-session")
	turn := &turnState{request: compactStartTurnRequest(request), requestFingerprint: startTurnFingerprint(request), completed: true, cleanupAt: time.Now().Add(-time.Second)}
	adapter.turns[request.TurnID] = turn
	if got := adapter.getTurn(request.TurnID); got != nil {
		t.Fatalf("expired turn was returned: %#v", got)
	}
}

func TestConflictingContinuationRetryIsRejectedAfterCompletion(t *testing.T) {
	adapter := newAdapter(testConfig("http://127.0.0.1"), &fakeResponsesBackend{})
	request := startRequest("terminal-retry", "terminal-retry-session")
	original := harness.ToolCallResult{Version: harness.ProtocolVersion, RuntimeSessionID: request.RuntimeSessionID, TurnID: request.TurnID, ToolCallID: orkaToolCallID(request.RuntimeSessionID, request.TurnID, "call-terminal"), IdempotencyKey: harness.ToolRequestIdempotencyKey(request.RuntimeSessionID, request.TurnID, orkaToolCallID(request.RuntimeSessionID, request.TurnID, "call-terminal")), Approved: true, Output: json.RawMessage(`{"ok":true}`)}
	digest, _, err := toolResultFingerprint(original)
	if err != nil {
		t.Fatalf("fingerprint: %v", err)
	}
	turn := &turnState{request: compactStartTurnRequest(request), requestFingerprint: startTurnFingerprint(request), submittedResultDigests: map[string]string{orkaToolCallID(request.RuntimeSessionID, request.TurnID, "call-terminal"): digest}, completed: true, initDone: closedChannel(), done: closedChannel()}
	adapter.turns[request.TurnID] = turn
	changed := original
	changed.Output = json.RawMessage(`{"ok":false}`)
	continueRequest := harness.ContinueTurnRequest{Version: harness.ProtocolVersion, Namespace: request.Namespace, TaskName: request.TaskName, SessionName: request.SessionName, RuntimeSessionID: request.RuntimeSessionID, TurnID: request.TurnID, CorrelationID: request.CorrelationID, ToolResults: []harness.ToolCallResult{changed}}
	if err := adapter.continueTurn(continueRequest); err == nil || !strings.Contains(err.Error(), "conflicts") {
		t.Fatalf("continue error = %v", err)
	}
}

func closedChannel() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

func TestValidateFoundryToolDefinitionsRejectsDuplicateOrNonObjectSchema(t *testing.T) {
	duplicate := []harness.ToolDefinition{
		{Name: "lookup", BrokeredClass: harness.BrokeredToolClassRead, Parameters: json.RawMessage(`{"type":"object"}`)},
		{Name: "lookup", BrokeredClass: harness.BrokeredToolClassWrite, Parameters: json.RawMessage(`{"type":"object"}`)},
	}
	if err := validateFoundryToolDefinitions(duplicate); err == nil {
		t.Fatal("duplicate tool names were accepted")
	}
	nonObject := []harness.ToolDefinition{{Name: "lookup", BrokeredClass: harness.BrokeredToolClassRead, Parameters: json.RawMessage(`[]`)}}
	if err := validateFoundryToolDefinitions(nonObject); err == nil {
		t.Fatal("array parameter schema was accepted")
	}
}

func TestBrokeredWriteFailureIsNotRetryable(t *testing.T) {
	backend := &fakeResponsesBackend{responses: []scriptedResponse{{
		run: func(_ context.Context, callbacks foundry.ResponseCallbacks) (foundry.StreamSummary, error) {
			_ = callbacks.OnCreated(foundry.Response{ID: "resp-write-failure", Status: "in_progress"})
			if err := callbacks.OnFunctionCall(foundry.OutputItem{Type: "function_call", CallID: "call-write", Name: "write_ticket", Arguments: json.RawMessage(`{}`)}); err != nil {
				return foundry.StreamSummary{}, err
			}
			return foundry.StreamSummary{}, errors.New("temporary provider failure")
		},
	}}}
	adapter := newAdapter(testConfig("http://127.0.0.1"), backend)
	request := startRequest("write-failure", "write-failure-session")
	request.ToolExecutionMode = harness.ToolExecutionModeBrokered
	request.Input.Tools = []harness.ToolDefinition{{Name: "write_ticket", BrokeredClass: harness.BrokeredToolClassWrite, Parameters: json.RawMessage(`{"type":"object"}`)}}
	turn, _, err := adapter.startTurn(request)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	waitTurnDone(t, turn)
	failed := findFrame(turn.frames, harness.FrameTurnFailed)
	if failed == nil || failed.Failed == nil || failed.Failed.Retryable {
		t.Fatalf("failed frame = %#v", failed)
	}
}

func TestCompletedContinuationMustAdvanceResponseID(t *testing.T) {
	backend := &fakeResponsesBackend{responses: []scriptedResponse{
		{
			run: func(_ context.Context, callbacks foundry.ResponseCallbacks) (foundry.StreamSummary, error) {
				_ = callbacks.OnCreated(foundry.Response{ID: "resp-tool-id", Status: "in_progress", AgentSessionID: "session-id"})
				call := foundry.OutputItem{Type: "function_call", CallID: "call-id", Name: "lookup", Arguments: json.RawMessage(`{}`)}
				if err := callbacks.OnFunctionCall(call); err != nil {
					return foundry.StreamSummary{}, err
				}
				return foundry.StreamSummary{ResponseID: "resp-tool-id", AgentSessionID: "session-id", Status: "completed", FunctionCalls: []foundry.OutputItem{call}}, nil
			},
		},
		{
			run: func(_ context.Context, _ foundry.ResponseCallbacks) (foundry.StreamSummary, error) {
				return foundry.StreamSummary{Status: "completed", AgentSessionID: "session-id"}, nil
			},
		},
	}}
	adapter := newAdapter(testConfig("http://127.0.0.1"), backend)
	request := startRequest("missing-id", "missing-id-session")
	request.ToolExecutionMode = harness.ToolExecutionModeBrokered
	request.Input.Tools = []harness.ToolDefinition{{Name: "lookup", BrokeredClass: harness.BrokeredToolClassRead, Parameters: json.RawMessage(`{"type":"object"}`)}}
	turn, _, err := adapter.startTurn(request)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	waitForFrame(t, adapter, turn, harness.FrameToolCallRequested)
	result := harness.ToolCallResult{Version: harness.ProtocolVersion, RuntimeSessionID: request.RuntimeSessionID, TurnID: request.TurnID, ToolCallID: orkaToolCallID(request.RuntimeSessionID, request.TurnID, "call-id"), IdempotencyKey: harness.ToolRequestIdempotencyKey(request.RuntimeSessionID, request.TurnID, orkaToolCallID(request.RuntimeSessionID, request.TurnID, "call-id")), Approved: true, Output: json.RawMessage(`{"ok":true}`)}
	continueRequest := harness.ContinueTurnRequest{Version: harness.ProtocolVersion, Namespace: request.Namespace, TaskName: request.TaskName, SessionName: request.SessionName, RuntimeSessionID: request.RuntimeSessionID, TurnID: request.TurnID, CorrelationID: request.CorrelationID, ToolResults: []harness.ToolCallResult{result}}
	if err := adapter.continueTurn(continueRequest); err != nil {
		t.Fatalf("continue: %v", err)
	}
	waitTurnDone(t, turn)
	failed := findFrame(turn.frames, harness.FrameTurnFailed)
	if failed == nil || failed.Failed == nil || failed.Failed.Reason != "foundry_response_failed" {
		t.Fatalf("failed = %#v", failed)
	}
}

func TestProviderFailureBeforeFirstEventIsNotRetryable(t *testing.T) {
	backend := &fakeResponsesBackend{responses: []scriptedResponse{{
		run: func(_ context.Context, _ foundry.ResponseCallbacks) (foundry.StreamSummary, error) {
			return foundry.StreamSummary{}, providerHTTPError{
				StatusCode: http.StatusServiceUnavailable,
				Operation:  "POST responses",
			}
		},
	}}}
	adapter := newAdapter(testConfig("http://127.0.0.1"), backend)
	turn, _, err := adapter.startTurn(startRequest("pre-event-failure", "pre-event-failure-session"))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	waitTurnDone(t, turn)
	failed := findFrame(turn.frames, harness.FrameTurnFailed)
	if failed == nil || failed.Failed == nil || failed.Failed.Retryable {
		t.Fatalf("failed = %#v", failed)
	}
}

func TestObservedFailureAfterStartIsNotRetryable(t *testing.T) {
	backend := &fakeResponsesBackend{responses: []scriptedResponse{{
		run: func(_ context.Context, callbacks foundry.ResponseCallbacks) (foundry.StreamSummary, error) {
			_ = callbacks.OnCreated(foundry.Response{ID: "resp-observed-failure", Status: "in_progress"})
			return foundry.StreamSummary{}, providerHTTPError{StatusCode: http.StatusServiceUnavailable, Operation: "POST responses"}
		},
	}}}
	adapter := newAdapter(testConfig("http://127.0.0.1"), backend)
	turn, _, err := adapter.startTurn(startRequest("observed-failure", "observed-failure-session"))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	waitTurnDone(t, turn)
	failed := findFrame(turn.frames, harness.FrameTurnFailed)
	if failed == nil || failed.Failed == nil || failed.Failed.Retryable {
		t.Fatalf("failed = %#v", failed)
	}
}

func TestCompletedContinuationMustUseNewResponseID(t *testing.T) {
	backend := &fakeResponsesBackend{responses: []scriptedResponse{
		textResponseScript("resp-same", "session-same", "first"),
		{
			run: func(_ context.Context, callbacks foundry.ResponseCallbacks) (foundry.StreamSummary, error) {
				_ = callbacks.OnCreated(foundry.Response{ID: "resp-same", Status: "in_progress", AgentSessionID: "session-same"})
				return foundry.StreamSummary{ResponseID: "resp-same", AgentSessionID: "session-same", Status: "completed"}, nil
			},
		},
	}}
	adapter := newAdapter(testConfig("http://127.0.0.1"), backend)
	first := startRequest("same-id-first", "same-id-session")
	turn1, _, err := adapter.startTurn(first)
	if err != nil {
		t.Fatalf("start first: %v", err)
	}
	waitTurnDone(t, turn1)
	second := startRequest("same-id-second", "same-id-session")
	turn2, _, err := adapter.startTurn(second)
	if err != nil {
		t.Fatalf("start second: %v", err)
	}
	waitTurnDone(t, turn2)
	failed := findFrame(turn2.frames, harness.FrameTurnFailed)
	if failed == nil || failed.Failed == nil || failed.Failed.Reason != "foundry_response_failed" {
		t.Fatalf("failed = %#v", failed)
	}
}

func TestRecordFunctionCallRejectsSubmittedCallIDReuse(t *testing.T) {
	adapter := newAdapter(testConfig("http://127.0.0.1"), &fakeResponsesBackend{})
	request := startRequest("call-reuse", "call-reuse-session")
	request.ToolExecutionMode = harness.ToolExecutionModeBrokered
	request.Input.Tools = []harness.ToolDefinition{{Name: "lookup", BrokeredClass: harness.BrokeredToolClassRead, Parameters: json.RawMessage(`{"type":"object"}`)}}
	turn := &turnState{
		request:                compactStartTurnRequest(request),
		submittedResultDigests: map[string]string{orkaToolCallID(request.RuntimeSessionID, request.TurnID, "call-reused"): "digest"},
		pendingTools:           map[string]string{},
		providerCallIDs:        map[string]string{},
	}
	if err := adapter.recordFunctionCall(turn, foundry.OutputItem{Type: "function_call", CallID: "call-reused", Name: "lookup", Arguments: json.RawMessage(`{}`)}); err == nil {
		t.Fatal("submitted call id reuse was accepted")
	}
}

func TestBrokeredModeIsRejectedWhenNoClassesAreEnabled(t *testing.T) {
	cfg := testConfig("http://127.0.0.1")
	cfg.brokeredToolClasses = nil
	adapter := newAdapter(cfg, &fakeResponsesBackend{})
	request := startRequest("brokered-disabled", "brokered-disabled-session")
	request.ToolExecutionMode = harness.ToolExecutionModeBrokered
	if _, _, err := adapter.startTurn(request); err == nil {
		t.Fatal("brokered turn was accepted without enabled classes")
	}
}

func TestStartTurnFingerprintIgnoresRetryDeadline(t *testing.T) {
	first := startRequest("fingerprint", "fingerprint-session")
	second := first
	second.Deadline = first.Deadline.Add(time.Minute)
	second.EventCursor = 42
	if startTurnFingerprint(first) != startTurnFingerprint(second) {
		t.Fatal("retry-varying fields changed start fingerprint")
	}
}

func TestAdapterPreservesOpaqueProviderCallIDs(t *testing.T) {
	request := startRequest("opaque-call", "namespace:session:runtime")
	if err := validateAdapterStartRequest(request); err != nil {
		t.Fatalf("canonical colon-delimited runtime session id was rejected: %v", err)
	}
	request.ToolExecutionMode = harness.ToolExecutionModeBrokered
	request.Input.Tools = []harness.ToolDefinition{{
		Name:          "lookup",
		BrokeredClass: harness.BrokeredToolClassRead,
		Parameters:    json.RawMessage(`{"type":"object"}`),
	}}
	adapter := newAdapter(testConfig("http://127.0.0.1"), &fakeResponsesBackend{})
	turn := &turnState{
		request:                compactStartTurnRequest(request),
		pendingTools:           map[string]string{},
		pendingCallDigests:     map[string]string{},
		providerCallIDs:        map[string]string{},
		submittedResultDigests: map[string]string{},
		emittedResults:         map[string]struct{}{},
	}
	providerCallID := " call:1 "
	if err := adapter.recordFunctionCall(turn, foundry.OutputItem{
		Type:      "function_call",
		CallID:    providerCallID,
		Name:      "lookup",
		Arguments: json.RawMessage(`{}`),
	}); err != nil {
		t.Fatalf("recordFunctionCall: %v", err)
	}
	adapterCallID := orkaToolCallID(request.RuntimeSessionID, request.TurnID, providerCallID)
	if turn.providerCallIDs[adapterCallID] != providerCallID {
		t.Fatalf("provider call ID = %q, want %q", turn.providerCallIDs[adapterCallID], providerCallID)
	}
	frame := findFrame(turn.frames, harness.FrameToolCallRequested)
	if frame == nil || frame.ToolCallID != adapterCallID {
		t.Fatalf("tool frame = %#v, want adapter call ID %q", frame, adapterCallID)
	}
}

func TestEmptyExecutionModeIsNormalizedToObserved(t *testing.T) {
	backend := &fakeResponsesBackend{responses: []scriptedResponse{{
		run: func(_ context.Context, callbacks foundry.ResponseCallbacks) (foundry.StreamSummary, error) {
			_ = callbacks.OnCreated(foundry.Response{ID: "resp-empty-mode", Status: "in_progress"})
			return foundry.StreamSummary{}, providerHTTPError{StatusCode: http.StatusServiceUnavailable, Operation: "POST responses"}
		},
	}}}
	adapter := newAdapter(testConfig("http://127.0.0.1"), backend)
	request := startRequest("empty-mode", "empty-mode-session")
	request.ToolExecutionMode = ""
	turn, _, err := adapter.startTurn(request)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	waitTurnDone(t, turn)
	failed := findFrame(turn.frames, harness.FrameTurnFailed)
	if failed == nil || failed.Failed == nil || failed.Failed.Retryable {
		t.Fatalf("failed = %#v", failed)
	}
}

func TestTextDeltaSynthesizesTurnStartedFirst(t *testing.T) {
	backend := &fakeResponsesBackend{responses: []scriptedResponse{{
		run: func(_ context.Context, callbacks foundry.ResponseCallbacks) (foundry.StreamSummary, error) {
			if err := callbacks.OnTextDelta("hello"); err != nil {
				return foundry.StreamSummary{}, err
			}
			return foundry.StreamSummary{ResponseID: "resp-no-created", AgentSessionID: "session-no-created", Status: "completed", Text: "hello"}, nil
		},
	}}}
	adapter := newAdapter(testConfig("http://127.0.0.1"), backend)
	turn, _, err := adapter.startTurn(startRequest("no-created", "no-created-session"))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	waitTurnDone(t, turn)
	if len(turn.frames) < 2 || turn.frames[0].Type != harness.FrameTurnStarted || turn.frames[1].Type != harness.FrameRuntimeOutput {
		t.Fatalf("frames = %#v", turn.frames)
	}
}

func TestManySmallTextDeltasDoNotExhaustEventLimit(t *testing.T) {
	backend := &fakeResponsesBackend{responses: []scriptedResponse{{
		run: func(_ context.Context, callbacks foundry.ResponseCallbacks) (foundry.StreamSummary, error) {
			for range 5000 {
				if err := callbacks.OnTextDelta("x"); err != nil {
					return foundry.StreamSummary{}, err
				}
			}
			return foundry.StreamSummary{
				ResponseID:     "resp-small-deltas",
				AgentSessionID: "session-small-deltas",
				Status:         "completed",
				Text:           strings.Repeat("x", 5000),
			}, nil
		},
	}}}
	cfg := testConfig("http://127.0.0.1")
	cfg.maxEvents = 8
	adapter := newAdapter(cfg, backend)
	turn, _, err := adapter.startTurn(startRequest("small-deltas", "small-deltas-session"))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	waitTurnDone(t, turn)
	if got := completedResult(turn.frames); len(got) != 5000 {
		t.Fatalf("result length = %d, want 5000; frames=%#v", len(got), turn.frames)
	}
	if len(turn.frames) >= cfg.maxEvents {
		t.Fatalf("frame count = %d, want less than %d", len(turn.frames), cfg.maxEvents)
	}
}

func TestFoundryToolNameLengthBoundary(t *testing.T) {
	validName := strings.Repeat("a", 128)
	definitions := []harness.ToolDefinition{{
		Name:          validName,
		BrokeredClass: harness.BrokeredToolClassRead,
		Parameters:    json.RawMessage(`{"type":"object"}`),
	}}
	if err := validateFoundryToolDefinitions(definitions); err != nil {
		t.Fatalf("128-character tool name rejected: %v", err)
	}
	definitions[0].Name += "a"
	if err := validateFoundryToolDefinitions(definitions); err == nil {
		t.Fatal("129-character tool name was accepted")
	}
}

func TestRuntimeSessionOwnerPrefersStableSubjectOverUsername(t *testing.T) {
	first := startRequest("principal-a", "principal-session")
	first.AuthIdentity = harness.AuthIdentity{Issuer: "issuer", Subject: "subject-a", Username: "shared"}
	second := first
	second.AuthIdentity.Subject = "subject-b"
	if runtimeSessionOwnerFingerprint(first) == runtimeSessionOwnerFingerprint(second) {
		t.Fatal("distinct stable subjects shared a runtime-session owner fingerprint")
	}
}

func TestCancelRejectsMismatchedCorrelationBeforeTerminalizing(t *testing.T) {
	adapter := newAdapter(testConfig("http://127.0.0.1"), &fakeResponsesBackend{})
	request := startRequest("cancel-correlation", "cancel-correlation-session")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	turn := &turnState{
		request:            compactStartTurnRequest(request),
		requestFingerprint: startTurnFingerprint(request),
		ctx:                ctx,
		cancel:             cancel,
		initDone:           closedChannel(),
		done:               make(chan struct{}),
	}
	adapter.turns[request.TurnID] = turn
	cancelRequest := cancelRequest(request)
	cancelRequest.CorrelationID = "different-correlation"
	if err := adapter.cancelTurn(cancelRequest); err == nil {
		t.Fatal("mismatched cancellation was accepted")
	}
	if turn.completed {
		t.Fatal("mismatched cancellation terminalized the turn")
	}
}
