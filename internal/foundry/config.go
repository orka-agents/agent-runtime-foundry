package foundry

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"strings"
	"unicode"

	"github.com/orka-agents/agent-runtime-foundry/internal/strictjson"
)

const (
	AgentConfigPath      = "/agent/foundry.json"
	ModelEnv             = "ORKA_FOUNDRY_ACP_MODEL"
	AgentConfigDigestEnv = "ORKA_FOUNDRY_ACP_AGENT_CONFIGURATION_DIGEST"
	MaxAgentConfigBytes  = 64 << 10
	IsolationModeEnv     = "ORKA_FOUNDRY_ISOLATION_MODE"

	ToolSchemaModeRequest        = "request"
	ToolSchemaModeProviderStatic = "provider-static"
)

var ErrAgentConfig = errors.New("invalid Foundry ACP configuration")

type AgentConfig struct {
	Model          string       `json:"model"`
	ToolSchemaMode string       `json:"toolSchemaMode"`
	HostedTarget   HostedTarget `json:"hostedTarget"`
}

type HostedTarget struct {
	ProjectEndpoint string `json:"projectEndpoint"`
	AgentName       string `json:"agentName"`
	AgentVersion    string `json:"agentVersion"`
}

// Both entry points verify one immutable buffer, while only the privileged
// broker uses HostedTarget. No child proxy credential is needed to parse it.
func DecodeAgentConfig(data []byte, expectedDigest, expectedModel string) (AgentConfig, error) {
	actual := sha256.Sum256(data)
	encoded := "sha256:" + hex.EncodeToString(actual[:])
	if len(data) > MaxAgentConfigBytes || subtle.ConstantTimeCompare([]byte(expectedDigest), []byte(encoded)) != 1 {
		return AgentConfig{}, ErrAgentConfig
	}
	var agent AgentConfig
	if strictjson.Decode(data, &agent, true) != nil || !SafeString(agent.Model, 512) || agent.Model != expectedModel {
		return AgentConfig{}, ErrAgentConfig
	}
	if agent.ToolSchemaMode != ToolSchemaModeRequest && agent.ToolSchemaMode != ToolSchemaModeProviderStatic {
		return AgentConfig{}, ErrAgentConfig
	}
	if strings.TrimSpace(agent.HostedTarget.ProjectEndpoint) != agent.HostedTarget.ProjectEndpoint ||
		!EndpointIsSafe(agent.HostedTarget.ProjectEndpoint) ||
		ValidateAgentName(agent.HostedTarget.AgentName) != nil || agent.HostedTarget.AgentVersion == "" ||
		strings.EqualFold(agent.HostedTarget.AgentVersion, "latest") || ValidateAgentVersion(agent.HostedTarget.AgentVersion) != nil {
		return AgentConfig{}, ErrAgentConfig
	}
	return agent, nil
}

func SafeString(value string, limit int) bool {
	if value == "" || len(value) > limit {
		return false
	}
	for _, ch := range value {
		if unicode.IsControl(ch) {
			return false
		}
	}
	return true
}

func FirstNonBlank(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
