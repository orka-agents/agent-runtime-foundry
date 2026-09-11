package broker

import (
	"testing"

	"github.com/orka-agents/agent-runtime-foundry/internal/foundry"
)

func TestBrokerResponseEvidenceKeepsUnusableOutputOwnership(t *testing.T) {
	const data = `{"id":"response-1","status":"completed","agent_session_id":"owned","output":[{"type":"web_search_call","Type":"function_call"}]}`
	response, err := brokerDecodeResponseEvidence([]byte(data))
	if err != nil || response.ID != "response-1" || response.AgentSessionID != "owned" {
		t.Fatal("coherent response ownership was discarded because output is unusable")
	}
	if _, err := foundry.DecodeResponse([]byte(data)); err == nil {
		t.Fatal("ownership evidence admitted unusable output")
	}
	for _, data := range []string{
		`{"id":"wrong","ID":"response-1","status":"completed"}`,
		`{"id":"response-1","status":"active","Status":"completed"}`,
		`{"id":"response-1","status":"completed","agent_session_id":"wrong","Agent_Session_ID":"owned"}`,
		`{"id":"response-1","status":"completed","error":{"code":"failure"},"Error":null}`,
	} {
		if _, err := brokerDecodeResponseEvidence([]byte(data)); err == nil {
			t.Error("contradictory fields fabricated response ownership")
		}
	}
}
