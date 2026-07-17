package main

import (
	"testing"

	"github.com/orka-agents/agent-runtime-foundry/internal/harness"
)

func TestValidateAgentNameAndVersion(t *testing.T) {
	for _, name := range []string{"agent", "agent-1", "Agent-1"} {
		if err := validateAgentName(name); err != nil {
			t.Fatalf("validateAgentName(%q): %v", name, err)
		}
	}
	for _, name := range []string{"", "-agent", "agent-", "agent--one", "agent_one", string(make([]byte, 64))} {
		if err := validateAgentName(name); err == nil {
			t.Fatalf("validateAgentName(%q) succeeded", name)
		}
	}
	for _, version := range []string{"", "1", "2026.07.15", "v2-build_1"} {
		if err := validateAgentVersion(version); err != nil {
			t.Fatalf("validateAgentVersion(%q): %v", version, err)
		}
	}
	for _, version := range []string{"@latest", " bad", "bad/version"} {
		if err := validateAgentVersion(version); err == nil {
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
		if got := foundryEndpointIsSafe(test.endpoint); got != test.want {
			t.Fatalf("foundryEndpointIsSafe(%q) = %v, want %v", test.endpoint, got, test.want)
		}
	}
}

func TestDefaultFoundryFeaturesHonorsExplicitEmptyValue(t *testing.T) {
	t.Setenv(envFoundryFeatures, "")
	if got := defaultFoundryFeatures(); got != "" {
		t.Fatalf("defaultFoundryFeatures() = %q", got)
	}
}

func TestConfigRejectsCrossOriginOrWrongAgentResponsesEndpoint(t *testing.T) {
	base := testConfig("https://account.services.ai.azure.com")
	base.adapterBearer = "adapter-token"
	base.projectEndpoint = "https://account.services.ai.azure.com/api/projects/demo"
	base.responsesEndpoint = "https://other.services.ai.azure.com/api/projects/demo/agents/hosted-agent/endpoint/protocols/openai/responses"
	if err := base.validate(); err == nil {
		t.Fatal("cross-origin responses endpoint was accepted")
	}
	base.responsesEndpoint = "https://account.services.ai.azure.com/api/projects/demo/agents/other-agent/endpoint/protocols/openai/responses"
	if err := base.validate(); err == nil {
		t.Fatal("wrong-agent responses endpoint was accepted")
	}
	base.responsesEndpoint = "https://account.services.ai.azure.com/api/projects/demo/agents/hosted-agent/endpoint/protocols/openai/responses"
	if err := base.validate(); err != nil {
		t.Fatalf("valid responses endpoint: %v", err)
	}
}

func TestConfigRejectsUnsupportedProgrammaticBrokeredClass(t *testing.T) {
	cfg := testConfig("https://account.services.ai.azure.com")
	cfg.projectEndpoint = "https://account.services.ai.azure.com/api/projects/demo"
	cfg.brokeredToolClasses = []harness.BrokeredToolClass{harness.BrokeredToolClassCoordination}
	if err := cfg.validate(); err == nil {
		t.Fatal("unsupported brokered class was accepted")
	}
}

func TestInvalidConfiguredTurnTimeoutFailsValidation(t *testing.T) {
	t.Setenv(envTurnTimeout, "not-a-duration")
	t.Setenv(envAdapterBearer, "adapter-token")
	t.Setenv(envProjectEndpoint, "https://account.services.ai.azure.com/api/projects/demo")
	t.Setenv(envAgentName, "hosted-agent")
	cfg := loadConfig()
	if cfg.turnTimeout != 0 {
		t.Fatalf("turn timeout = %v, want zero invalid sentinel", cfg.turnTimeout)
	}
	if err := cfg.validate(); err == nil {
		t.Fatal("invalid timeout configuration was accepted")
	}
}

func TestConfigAcceptsEquivalentExplicitDefaultPort(t *testing.T) {
	cfg := testConfig("https://account.services.ai.azure.com")
	cfg.projectEndpoint = "https://account.services.ai.azure.com/api/projects/demo"
	cfg.responsesEndpoint = "https://account.services.ai.azure.com:443/api/projects/demo/agents/hosted-agent/endpoint/protocols/openai/responses"
	if err := cfg.validate(); err != nil {
		t.Fatalf("equivalent default-port origin rejected: %v", err)
	}
}
