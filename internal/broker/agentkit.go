package broker

import (
	"encoding/json"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/orka-agents/agent-runtime-foundry/internal/foundry"
	"github.com/orka-agents/agent-runtime-foundry/internal/strictjson"
)

const (
	brokerAgentKitProofEnv    = "ORKA_FOUNDRY_BROKER_AGENTKIT_CONTINUATION_PROOF"
	brokerAgentKitProofHeader = "X-AgentKit-Brokered-Continuation-Proof"
)

func brokerAgentKitProofValid(value string) bool {
	return value == "" || (len(value) >= 32 && utf8.ValidString(value) && foundry.SafeString(value, 16<<10) && strings.IndexFunc(value, unicode.IsSpace) < 0)
}

// The proof never enters the shared request type used by the ACP child. This
// wire-only extension is added after the broker's owner, lease and call checks.
func (b *lifecycleBroker) marshalResponseRequest(request foundry.ResponseRequest) ([]byte, error) {
	proof := ""
	if inputs, ok := request.Input.([]any); ok && len(inputs) != 0 {
		proof = b.cfg.agentKitProof
	}
	return json.Marshal(struct {
		foundry.ResponseRequest
		ContinuationProof string `json:"brokered_continuation_proof,omitempty"`
	}{ResponseRequest: request, ContinuationProof: proof})
}

// Orka's MCP proxy explicitly reports isError, with text content and optional
// structured output. Reject other envelopes rather than infer authorization
// from arbitrary tool data. JSON-RPC authorization failures never reach here.
func brokerAgentKitOutputs(request *foundry.ResponseRequest) error {
	if _, first := request.Input.(string); first {
		return nil
	}
	inputs, ok := request.Input.([]any)
	if !ok || len(inputs) == 0 {
		return errBrokerInvalid
	}
	for _, input := range inputs {
		item, ok := input.(map[string]any)
		if !ok {
			return errBrokerInvalid
		}
		output, ok := item["output"].(string)
		if !ok {
			return errBrokerInvalid
		}
		var result struct {
			Content []struct {
				Type string  `json:"type"`
				Text *string `json:"text"`
			} `json:"content"`
			IsError           *bool           `json:"isError"`
			StructuredContent json.RawMessage `json:"structuredContent,omitempty"`
		}
		if strictjson.DecodeStruct([]byte(output), &result, true) != nil || result.Content == nil || result.IsError == nil {
			return errBrokerInvalid
		}
		var message strings.Builder
		for i, content := range result.Content {
			if content.Type != "text" || content.Text == nil {
				return errBrokerInvalid
			}
			if i != 0 {
				message.WriteByte('\n')
			}
			message.WriteString(*content.Text)
		}
		if len(result.StructuredContent) != 0 && result.StructuredContent[0] != '{' {
			return errBrokerInvalid
		}
		var normalized any
		if *result.IsError {
			if message.Len() == 0 {
				message.WriteString("The governed tool returned an error.")
			}
			code, text := "brokered_tool_error", message.String()
			if safeCode, safeText := brokerOrkaToolError(result.StructuredContent); safeCode != "" {
				code, text = safeCode, safeText
			}
			normalized = map[string]any{"approved": false, "error": map[string]string{
				"code": code, "message": text,
			}}
		} else {
			normalized = map[string]any{"approved": true, "output": result}
		}
		encoded, err := json.Marshal(normalized)
		if err != nil || len(encoded) > foundry.DefaultMaxBrokeredBytes {
			return errBrokerInvalid
		}
		item["output"] = string(encoded)
	}
	return nil
}

// Only Orka's structured final outcome selects an approval code. Tool text is
// not authority and may contain private decision details, so known outcomes
// always use a fixed model-visible message. None of these outcomes means wait.
func brokerOrkaToolError(raw json.RawMessage) (string, string) {
	var outcome struct {
		IsError bool   `json:"isError"`
		Code    string `json:"code"`
	}
	if strictjson.DecodeStruct(raw, &outcome, false) != nil || !outcome.IsError {
		return "", ""
	}
	var message string
	switch outcome.Code {
	case "approval_declined":
		message = "The tool call was declined."
	case "approval_expired":
		message = "The tool approval expired."
	case "approval_cancelled":
		message = "The tool call was cancelled."
	case "approval_stale":
		message = "The tool approval is no longer valid."
	case "tool_execution_failed":
		message = "MCP tool execution failed."
	case "tool_outcome_unknown":
		message = "The tool execution outcome is unknown; do not retry."
	default:
		return "", ""
	}
	return outcome.Code, message
}
