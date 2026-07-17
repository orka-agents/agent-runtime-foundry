package conformance

import (
	"testing"
	"time"

	"github.com/orka-agents/agent-runtime-foundry/internal/harness"
)

func TestValidateEventStreamPath(t *testing.T) {
	turnID := harness.HarnessTurnID("turn-1")
	expected, err := harness.EventStreamPath(turnID)
	if err != nil {
		t.Fatalf("EventStreamPath: %v", err)
	}
	if err := validateEventStreamPath(turnID, expected); err != nil {
		t.Fatalf("valid path: %v", err)
	}
	if err := validateEventStreamPath(turnID, "/v1/turns/other/events"); err == nil {
		t.Fatal("wrong path was accepted")
	}
}

func TestStartTurnMayHaveBeenAcceptedTreatsStatusZeroPostAsAmbiguous(t *testing.T) {
	if !startTurnMayHaveBeenAccepted(harness.ClientError{Op: "post", StatusCode: 0}) {
		t.Fatal("status-zero post error should be treated as ambiguous")
	}
	if startTurnMayHaveBeenAccepted(harness.ClientError{Op: "get", StatusCode: 0}) {
		t.Fatal("status-zero get error should not be treated as accepted")
	}
}

func TestValidateProbeFramesRequiresTurnStarted(t *testing.T) {
	request := defaultStartTurnRequest("completion-only")
	frames := []harness.HarnessEventFrame{{
		Version:          harness.ProtocolVersion,
		Type:             harness.FrameTurnCompleted,
		RuntimeSessionID: request.RuntimeSessionID,
		TurnID:           request.TurnID,
		CorrelationID:    request.CorrelationID,
		Seq:              1,
		Completed:        &harness.TurnCompleted{Result: "done"},
	}}
	result := Result{Passed: true}
	if validateProbeFrames(&result, request, frames) {
		t.Fatal("completion-only stream passed validation")
	}
}

func TestReadinessTargetRefreshesCustomObservedIdentity(t *testing.T) {
	request := defaultStartTurnRequest("custom-observed")
	request.Deadline = time.Now().Add(-time.Minute)
	target := Target{StartTurnRequest: &request}
	first := readinessTarget(target, "observed")
	second := readinessTarget(target, "observed")
	if first.StartTurnRequest == nil || second.StartTurnRequest == nil {
		t.Fatal("readiness target omitted start request")
	}
	if first.StartTurnRequest.TurnID == request.TurnID || first.StartTurnRequest.TurnID == second.StartTurnRequest.TurnID {
		t.Fatalf("turn IDs were not refreshed: original=%q first=%q second=%q", request.TurnID, first.StartTurnRequest.TurnID, second.StartTurnRequest.TurnID)
	}
	if !first.StartTurnRequest.Deadline.After(time.Now()) {
		t.Fatalf("deadline was not refreshed: %v", first.StartTurnRequest.Deadline)
	}
}
