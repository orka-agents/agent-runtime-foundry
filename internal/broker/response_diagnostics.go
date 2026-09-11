package broker

import (
	"encoding/json"

	"github.com/orka-agents/agent-runtime-foundry/internal/foundry"
	"github.com/orka-agents/agent-runtime-foundry/internal/strictjson"
)

// Diagnostics never participate in ownership, output validation, or settlement.
// One bounded record is emitted per rejected invocation. Provider text and IDs
// are deliberately absent, including from errors and unknown metadata values.
type brokerResponseDiagnostic struct {
	stage          string
	httpStatus     int
	accepted       bool
	streamComplete bool
	terminalFrames int
	terminalStatus string
	errorCode      string
	upstreamStatus int
}

func (d *brokerResponseDiagnostic) observe(raw []byte, response foundry.Response) {
	if d == nil {
		return
	}
	d.accepted = true
	switch response.Status {
	case "completed", "failed", "incomplete", "cancelled":
	default:
		return
	}
	d.terminalFrames++
	if d.terminalFrames != 1 {
		d.terminalStatus, d.errorCode, d.upstreamStatus = "multiple", "", 0
		return
	}
	d.terminalStatus = response.Status
	if response.Error == nil {
		return
	}
	d.errorCode = brokerSafeResponseCode(response.Error.Code)
	if d.errorCode == "unknown" {
		return
	}
	// This optional metadata is decoded separately so it cannot change the
	// acceptance of an otherwise valid response or the existing wire errors.
	var envelope struct {
		Error json.RawMessage `json:"error"`
	}
	var metadata struct {
		UpstreamStatus json.RawMessage `json:"upstream_status"`
	}
	var status int
	if strictjson.DecodeStruct(raw, &envelope, false) == nil &&
		strictjson.DecodeStruct(envelope.Error, &metadata, false) == nil &&
		json.Unmarshal(metadata.UpstreamStatus, &status) == nil && status >= 400 && status <= 599 {
		d.upstreamStatus = status
	}
}

func brokerSafeResponseCode(code string) string {
	switch code {
	case "ModelAuthMissing", "ModelAuthRejected", "ModelUnavailable", "ModelUpstreamError",
		"InvalidModelResponse", "ModelResponseTooLarge", "ModelResumeError",
		"tool_loop_limit_exceeded", "brokered_response_state_storage_error",
		"brokered_response_state_too_large", "brokered_response_state_full":
		return code
	default:
		return "unknown"
	}
}

func (b *lifecycleBroker) logResponseFailure(c brokerContext, d brokerResponseDiagnostic) {
	if b.diagnosticLog == nil {
		return
	}
	fields := []any{
		"stage", d.stage,
		"owner_digest", foundry.JSONDigest(c.Owner),
		"invocation_sequence", c.InvocationSequence,
		"response_acknowledged", d.accepted,
		"stream_complete", d.streamComplete,
		"terminal_frames", d.terminalFrames,
	}
	if d.httpStatus >= 100 && d.httpStatus <= 599 {
		fields = append(fields, "http_status", d.httpStatus)
	}
	if d.terminalStatus != "" {
		fields = append(fields, "observed_terminal_status", d.terminalStatus)
	}
	if d.errorCode != "" {
		fields = append(fields, "error_code", d.errorCode)
	}
	if d.upstreamStatus != 0 {
		fields = append(fields, "upstream_status", d.upstreamStatus)
	}
	b.diagnosticLog.Warn("Foundry response rejected", fields...)
}
