package main

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

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
	maxFoundryPromptBytes          = 4 << 20
	maxFoundryToolSchemaBytes      = 2 << 20
	maxProviderIdentifierBytes     = 4 << 10
	maxHarnessFrameBytes           = (8 << 20) - (64 << 10)
	defaultMaxOutputBytes          = 1 << 20
	defaultMaxStreamBytes          = 16 << 20
	defaultMaxEventBytes           = 8 << 20
	defaultMaxBrokeredBytes        = 4 << 20
	defaultMaxBrokeredTurnBytes    = 16 << 20
	defaultMaxBrokeredCalls        = 256
	defaultMaxEvents               = 4096
	defaultMaxTurns                = 1

	envAddr                = "ORKA_FOUNDRY_ADAPTER_ADDR"
	envRuntimeName         = "ORKA_FOUNDRY_RUNTIME_NAME"
	envAdapterBearer       = "ORKA_FOUNDRY_ADAPTER_BEARER_" + "TOKEN"
	envProjectEndpoint     = "ORKA_FOUNDRY_PROJECT_ENDPOINT"
	envResponsesURL        = "ORKA_FOUNDRY_RESPONSES_ENDPOINT"
	envAgentName           = "ORKA_FOUNDRY_AGENT_NAME"
	envAgentVersion        = "ORKA_FOUNDRY_AGENT_VERSION"
	envAPIVersion          = "ORKA_FOUNDRY_API_VERSION"
	envTurnTimeout         = "ORKA_FOUNDRY_TURN_TIMEOUT"
	envIsolationMode       = "ORKA_FOUNDRY_ISOLATION_MODE"
	envFoundryFeatures     = "ORKA_FOUNDRY_FEATURES"
	envBrokeredToolClasses = "ORKA_FOUNDRY_BROKERED_TOOL_CLASSES"
	envToolSchemaMode      = "ORKA_FOUNDRY_TOOL_SCHEMA_MODE"
)

const (
	toolSchemaModeRequest        = "request"
	toolSchemaModeProviderStatic = "provider-static"
)

const foundryEndpointRequirement = "Foundry endpoint must use https " +
	"(http allowed only for loopback) and must not include credentials, query, or fragment"

var (
	agentNameRE    = regexp.MustCompile(`^[A-Za-z0-9]+(?:-[A-Za-z0-9]+)*$`)
	agentVersionRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
)

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
		addr:                     firstNonBlank(os.Getenv(envAddr), defaultAddr),
		runtimeName:              firstNonBlank(os.Getenv(envRuntimeName), "foundry-hosted-runtime"),
		adapterBearer:            strings.TrimSpace(os.Getenv(envAdapterBearer)),
		projectEndpoint:          projectEndpoint,
		responsesEndpoint:        responsesEndpoint,
		agentName:                strings.TrimSpace(os.Getenv(envAgentName)),
		agentVersion:             strings.TrimSpace(os.Getenv(envAgentVersion)),
		apiVersion:               firstNonBlank(os.Getenv(envAPIVersion), defaultAPIVersion),
		turnTimeout:              parseDurationEnv(envTurnTimeout, defaultTurnTimeout),
		isolationMode:            firstNonBlank(os.Getenv(envIsolationMode), "entra"),
		foundryFeatures:          foundryFeatures,
		maxOutputBytes:           defaultMaxOutputBytes,
		maxStreamBytes:           defaultMaxStreamBytes,
		maxEventBytes:            defaultMaxEventBytes,
		maxBrokeredBytes:         defaultMaxBrokeredBytes,
		maxBrokeredTurnBytes:     defaultMaxBrokeredTurnBytes,
		maxBrokeredCalls:         defaultMaxBrokeredCalls,
		maxEvents:                defaultMaxEvents,
		maxConcurrent:            defaultMaxTurns,
		brokeredToolClasses:      brokeredToolClasses,
		brokeredToolClassSetting: brokeredToolClassSetting,
		toolSchemaMode:           strings.ToLower(firstNonBlank(os.Getenv(envToolSchemaMode), toolSchemaModeRequest)),
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
	if err := validateAgentName(c.agentName); err != nil {
		return err
	}
	if err := validateAgentVersion(c.agentVersion); err != nil {
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
	if !foundryEndpointIsSafe(c.projectEndpoint) {
		return errors.New(strings.ToLower(foundryEndpointRequirement[:1]) + foundryEndpointRequirement[1:])
	}
	if c.responsesEndpoint != "" {
		if !foundryEndpointIsSafe(c.responsesEndpoint) {
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
	case "", toolSchemaModeRequest, toolSchemaModeProviderStatic:
	default:
		return fmt.Errorf("foundry tool schema mode must be %s or %s", toolSchemaModeRequest, toolSchemaModeProviderStatic)
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

func validateAgentName(name string) error {
	if name == "" {
		return errors.New("foundry agent name is required")
	}
	if len(name) > 63 || !agentNameRE.MatchString(name) {
		return errors.New("foundry agent name must be 1-63 alphanumeric or hyphen characters without leading, trailing, or repeated hyphens")
	}
	return nil
}

func validateAgentVersion(version string) error {
	if version == "" {
		return nil
	}
	if strings.HasPrefix(version, "@") || !agentVersionRE.MatchString(version) {
		return errors.New("foundry agent version must be a concrete version identifier")
	}
	return nil
}

func foundryEndpointIsSafe(raw string) bool {
	trimmed := strings.TrimSpace(raw)
	parsed, err := url.Parse(trimmed)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return false
	}
	if parsed.User != nil || parsed.ForceQuery || parsed.RawQuery != "" || strings.Contains(trimmed, "#") {
		return false
	}
	if strings.EqualFold(parsed.Scheme, "https") {
		return true
	}
	if !strings.EqualFold(parsed.Scheme, "http") {
		return false
	}
	host := strings.Trim(strings.ToLower(parsed.Hostname()), "[]")
	ip := net.ParseIP(host)
	return host == "localhost" || (ip != nil && ip.IsLoopback())
}

func firstNonBlank(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
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
