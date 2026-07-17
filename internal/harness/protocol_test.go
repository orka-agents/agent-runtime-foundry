package harness

import "testing"

func TestCapabilitiesRejectUnsupportedTransport(t *testing.T) {
	response := CapabilitiesResponse{
		Version:                 ProtocolVersion,
		ProtocolVersion:         ProtocolVersion,
		Transport:               "grpc",
		RuntimeName:             "test",
		ProviderKind:            ProviderKindRemote,
		ToolExecutionModes:      []ToolExecutionMode{ToolExecutionModeObserved},
		SupportsCancel:          true,
		SupportsRuntimeSessions: true,
	}
	if err := response.Validate(); err == nil {
		t.Fatal("unsupported transport was accepted")
	}
}
