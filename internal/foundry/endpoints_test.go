package foundry

import "testing"

func TestValidateAgentNameAndVersion(t *testing.T) {
	for _, name := range []string{"agent", "agent-1", "Agent-1"} {
		if err := ValidateAgentName(name); err != nil {
			t.Fatalf("validateAgentName(%q): %v", name, err)
		}
	}
	for _, name := range []string{"", "-agent", "agent-", "agent--one", "agent_one", string(make([]byte, 64))} {
		if err := ValidateAgentName(name); err == nil {
			t.Fatalf("validateAgentName(%q) succeeded", name)
		}
	}
	for _, version := range []string{"", "1", "2026.07.15", "v2-build_1"} {
		if err := ValidateAgentVersion(version); err != nil {
			t.Fatalf("validateAgentVersion(%q): %v", version, err)
		}
	}
	for _, version := range []string{"@latest", " bad", "bad/version"} {
		if err := ValidateAgentVersion(version); err == nil {
			t.Fatalf("validateAgentVersion(%q) succeeded", version)
		}
	}
}

func TestFoundryEndpointIsSafe(t *testing.T) {
	tests := []struct {
		endpoint string
		want     bool
	}{
		{endpoint: "https://example.services.ai.azure.com/api/projects/demo", want: true},
		{endpoint: "http://localhost:8080", want: true},
		{endpoint: "http://127.0.0.1:8080", want: true},
		{endpoint: "http://[::1]:8080", want: true},
		{endpoint: "http://example.services.ai.azure.com", want: false},
		{endpoint: "https://user@example.services.ai.azure.com", want: false},
		{endpoint: "https://example.services.ai.azure.com?api-version=v1", want: false},
		{endpoint: "https://example.services.ai.azure.com#fragment", want: false},
	}
	for _, test := range tests {
		if got := EndpointIsSafe(test.endpoint); got != test.want {
			t.Fatalf("foundryEndpointIsSafe(%q) = %v, want %v", test.endpoint, got, test.want)
		}
	}
}
