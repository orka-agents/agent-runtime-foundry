package adapter

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/orka-agents/agent-runtime-foundry/internal/foundry"
	"github.com/orka-agents/agent-runtime-foundry/internal/harness"
)

const (
	defaultAddr                    = ":8090"
	defaultAPIVersion              = "v1"
	defaultTurnTimeout             = 20 * time.Second
	defaultStateRetention          = 10 * time.Minute
	defaultTombstoneRetention      = 24 * time.Hour
	defaultRuntimeSessionRetention = 30 * 24 * time.Hour
	maxRuntimeSessions             = 10_000
	maxRetainedTurns               = 256
	maxRetainedTurnStateBytes      = 64 << 20
	maxHarnessIdentityBytes        = 512
	maxHarnessFrameBytes           = (8 << 20) - (64 << 10)
	defaultMaxTurns                = 1
	envAddr                        = "ORKA_FOUNDRY_ADAPTER_ADDR"
	envRuntimeName                 = "ORKA_FOUNDRY_RUNTIME_NAME"
	envAdapterBearer               = "ORKA_FOUNDRY_ADAPTER_BEARER_" + "TOKEN"
	envProjectEndpoint             = "ORKA_FOUNDRY_PROJECT_ENDPOINT"
	envResponsesURL                = "ORKA_FOUNDRY_RESPONSES_ENDPOINT"
	envAgentName                   = "ORKA_FOUNDRY_AGENT_NAME"
	envAgentVersion                = "ORKA_FOUNDRY_AGENT_VERSION"
	envAPIVersion                  = "ORKA_FOUNDRY_API_VERSION"
	envTurnTimeout                 = "ORKA_FOUNDRY_TURN_TIMEOUT"
	envFoundryFeatures             = "ORKA_FOUNDRY_FEATURES"
	envBrokeredToolClasses         = "ORKA_FOUNDRY_BROKERED_TOOL_CLASSES"
	envToolSchemaMode              = "ORKA_FOUNDRY_TOOL_SCHEMA_MODE"
)

const foundryEndpointRequirement = "Foundry endpoint must use https " +
	"(http allowed only for loopback) and must not include credentials, query, or fragment"

type config struct {
	addr                     string
	runtimeName              string
	adapterBearer            string
	projectEndpoint          string
	responsesEndpoint        string
	agentName                string
	agentVersion             string
	apiVersion               string
	turnTimeout              time.Duration
	isolationMode            string
	foundryFeatures          string
	maxOutputBytes           int64
	maxStreamBytes           int64
	maxEventBytes            int64
	maxBrokeredBytes         int64
	maxBrokeredTurnBytes     int64
	maxBrokeredCalls         int
	maxEvents                int
	maxConcurrent            int
	brokeredToolClasses      []harness.BrokeredToolClass
	brokeredToolClassSetting string
	toolSchemaMode           string
}

func loadConfig() config {
	projectEndpoint := strings.TrimRight(strings.TrimSpace(os.Getenv(envProjectEndpoint)), "/")
	responsesEndpoint := strings.TrimRight(strings.TrimSpace(os.Getenv(envResponsesURL)), "/")
	foundryFeatures := defaultFoundryFeatures()
	brokeredToolClassSetting := os.Getenv(envBrokeredToolClasses)
	brokeredToolClasses := parseBrokeredToolClasses(brokeredToolClassSetting)
	return config{
		addr:                     foundry.FirstNonBlank(os.Getenv(envAddr), defaultAddr),
		runtimeName:              foundry.FirstNonBlank(os.Getenv(envRuntimeName), "foundry-hosted-runtime"),
		adapterBearer:            strings.TrimSpace(os.Getenv(envAdapterBearer)),
		projectEndpoint:          projectEndpoint,
		responsesEndpoint:        responsesEndpoint,
		agentName:                strings.TrimSpace(os.Getenv(envAgentName)),
		agentVersion:             strings.TrimSpace(os.Getenv(envAgentVersion)),
		apiVersion:               foundry.FirstNonBlank(os.Getenv(envAPIVersion), defaultAPIVersion),
		turnTimeout:              parseDurationEnv(envTurnTimeout, defaultTurnTimeout),
		isolationMode:            foundry.FirstNonBlank(os.Getenv(foundry.IsolationModeEnv), "entra"),
		foundryFeatures:          foundryFeatures,
		maxOutputBytes:           foundry.DefaultMaxOutputBytes,
		maxStreamBytes:           foundry.DefaultMaxStreamBytes,
		maxEventBytes:            foundry.DefaultMaxEventBytes,
		maxBrokeredBytes:         foundry.DefaultMaxBrokeredBytes,
		maxBrokeredTurnBytes:     foundry.DefaultMaxBrokeredTurnBytes,
		maxBrokeredCalls:         foundry.DefaultMaxBrokeredCalls,
		maxEvents:                foundry.DefaultMaxEvents,
		maxConcurrent:            defaultMaxTurns,
		brokeredToolClasses:      brokeredToolClasses,
		brokeredToolClassSetting: brokeredToolClassSetting,
		toolSchemaMode:           strings.ToLower(foundry.FirstNonBlank(os.Getenv(envToolSchemaMode), foundry.ToolSchemaModeRequest)),
	}
}

func parseBrokeredToolClasses(value string) []harness.BrokeredToolClass {
	seen := map[harness.BrokeredToolClass]struct{}{}
	classes := make([]harness.BrokeredToolClass, 0, 2)
	for raw := range strings.SplitSeq(value, ",") {
		class := harness.BrokeredToolClass(strings.ToLower(strings.TrimSpace(raw)))
		if class == "" {
			continue
		}
		if _, duplicate := seen[class]; duplicate {
			continue
		}
		seen[class] = struct{}{}
		classes = append(classes, class)
	}
	return classes
}

func defaultFoundryFeatures() string {
	value, exists := os.LookupEnv(envFoundryFeatures)
	if !exists {
		return "HostedAgents=V1Preview"
	}
	return strings.TrimSpace(value)
}

func (c config) validate() error {
	if strings.TrimSpace(c.adapterBearer) == "" {
		return errors.New("adapter bearer token is required")
	}
	if err := foundry.ValidateAgentName(c.agentName); err != nil {
		return err
	}
	if err := foundry.ValidateAgentVersion(c.agentVersion); err != nil {
		return err
	}
	if c.turnTimeout <= 0 {
		return errors.New("turn timeout must be positive")
	}
	if c.apiVersion == "" {
		return errors.New("foundry API version is required")
	}
	if c.projectEndpoint == "" {
		return errors.New("foundry project endpoint is required")
	}
	if !foundry.EndpointIsSafe(c.projectEndpoint) {
		return errors.New(strings.ToLower(foundryEndpointRequirement[:1]) + foundryEndpointRequirement[1:])
	}
	if c.responsesEndpoint != "" {
		if !foundry.EndpointIsSafe(c.responsesEndpoint) {
			return errors.New(strings.ToLower(foundryEndpointRequirement[:1]) + foundryEndpointRequirement[1:])
		}
		projectURL, projectErr := url.Parse(c.projectEndpoint)
		responsesURL, responsesErr := url.Parse(c.responsesEndpoint)
		if projectErr != nil || responsesErr != nil || projectURL == nil || responsesURL == nil {
			return errors.New("foundry endpoints must be valid URLs")
		}
		if !sameEndpointOrigin(projectURL, responsesURL) {
			return errors.New("foundry responses endpoint must use the project endpoint origin")
		}
		expectedPath := strings.TrimRight(projectURL.Path, "/") + "/agents/" + c.agentName +
			"/endpoint/protocols/openai/responses"
		if strings.TrimRight(responsesURL.Path, "/") != expectedPath {
			return errors.New("foundry responses endpoint must target the configured project and agent")
		}
	}
	for raw := range strings.SplitSeq(c.brokeredToolClassSetting, ",") {
		value := harness.BrokeredToolClass(strings.ToLower(strings.TrimSpace(raw)))
		if value != "" && value != harness.BrokeredToolClassRead && value != harness.BrokeredToolClassWrite {
			return fmt.Errorf("unsupported Foundry brokered tool class %q", value)
		}
	}
	for _, value := range c.brokeredToolClasses {
		if value != harness.BrokeredToolClassRead && value != harness.BrokeredToolClassWrite {
			return fmt.Errorf("unsupported Foundry brokered tool class %q", value)
		}
	}
	switch strings.ToLower(strings.TrimSpace(c.toolSchemaMode)) {
	case "", foundry.ToolSchemaModeRequest, foundry.ToolSchemaModeProviderStatic:
	default:
		return fmt.Errorf("foundry tool schema mode must be %s or %s", foundry.ToolSchemaModeRequest, foundry.ToolSchemaModeProviderStatic)
	}
	switch strings.ToLower(strings.TrimSpace(c.isolationMode)) {
	case "entra", "header":
	default:
		return errors.New("foundry isolation mode must be entra or header")
	}
	return nil
}

func sameEndpointOrigin(left, right *url.URL) bool {
	if left == nil || right == nil || !strings.EqualFold(left.Scheme, right.Scheme) ||
		!strings.EqualFold(left.Hostname(), right.Hostname()) {
		return false
	}
	return effectivePort(left) == effectivePort(right)
}

func effectivePort(endpoint *url.URL) string {
	if endpoint == nil {
		return ""
	}
	if port := endpoint.Port(); port != "" {
		return port
	}
	switch strings.ToLower(endpoint.Scheme) {
	case "https":
		return "443"
	case "http":
		return "80"
	default:
		return ""
	}
}

func parseDurationEnv(name string, fallback time.Duration) time.Duration {
	value, exists := os.LookupEnv(name)
	if !exists || strings.TrimSpace(value) == "" {
		return fallback
	}
	duration, err := time.ParseDuration(strings.TrimSpace(value))
	if err != nil || duration <= 0 {
		return 0
	}
	return duration
}

func parseAfterSeq(value string) int64 {
	parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil || parsed < 0 {
		return 0
	}
	return parsed
}
