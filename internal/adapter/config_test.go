package adapter

import (
	"testing"

	"github.com/orka-agents/agent-runtime-foundry/internal/foundry"
	"github.com/orka-agents/agent-runtime-foundry/internal/harness"
)

func TestDefaultFoundryFeaturesHonorsExplicitEmptyValue(t *testing.T) {
	t.Setenv(envFoundryFeatures, "")
	if got := defaultFoundryFeatures(); got != "" {
		t.Fatalf("defaultFoundryFeatures() = %q", got)
	}
}

func TestToolSchemaModeConfiguration(t *testing.T) {
	t.Setenv(envToolSchemaMode, foundry.ToolSchemaModeProviderStatic)
	if got := loadConfig().toolSchemaMode; got != foundry.ToolSchemaModeProviderStatic {
		t.Fatalf("tool schema mode = %q, want %q", got, foundry.ToolSchemaModeProviderStatic)
	}

	cfg := testConfig("https://account.services.ai.azure.com")
	cfg.projectEndpoint = "https://account.services.ai.azure.com/api/projects/demo"
	cfg.toolSchemaMode = "unsupported"
	if err := cfg.validate(); err == nil {
		t.Fatal("unsupported tool schema mode was accepted")
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
