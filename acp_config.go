package main

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"flag"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode"
)

const (
	acpConfigPath       = "/agent/foundry.json"
	acpProviderBaseEnv  = "ORKA_FOUNDRY_ACP_PROVIDER_BASE_URL"
	acpProviderTokenEnv = "ORKA_FOUNDRY_ACP_PROVIDER_TOKEN"
	acpModelEnv         = "ORKA_FOUNDRY_ACP_MODEL"
	acpConfigDigestEnv  = "ORKA_FOUNDRY_ACP_AGENT_CONFIGURATION_DIGEST"
	acpMaxConfigBytes   = 64 << 10
	acpMaxMessageBytes  = 8 << 20
	acpTextChunkBytes   = 32 << 10
	acpHTTPTimeout      = 120 * time.Second
)

var (
	errACPConfiguration = errors.New("invalid Foundry ACP configuration")
	errACPProvider      = errors.New("Foundry ACP provider request failed")
	errACPMCP           = errors.New("Foundry ACP MCP request failed")
	errACPTransport     = errors.New("Foundry ACP transport failed")
)

type acpAgentConfiguration struct {
	Model          string          `json:"model"`
	ToolSchemaMode string          `json:"toolSchemaMode"`
	HostedTarget   acpHostedTarget `json:"hostedTarget"`
}

type acpHostedTarget struct {
	ProjectEndpoint string `json:"projectEndpoint"`
	AgentName       string `json:"agentName"`
	AgentVersion    string `json:"agentVersion"`
}

type acpConfiguration struct {
	agent       acpAgentConfiguration
	providerURL string
	token       string
}

// The ACP entry point deliberately precedes Azure credential initialization.
// Its only network authority is the two supervisor-owned loopback proxies.
func maybeServeACP(args []string, input io.ReadCloser, output io.WriteCloser) (bool, error) {
	selected := false
	for _, arg := range args {
		if arg == "--protocol" || strings.HasPrefix(arg, "--protocol=") {
			selected = true
			break
		}
	}
	if !selected {
		return false, nil
	}
	flags := flag.NewFlagSet("foundry-acp", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	protocol := flags.String("protocol", "", "")
	path := flags.String("config", acpConfigPath, "")
	if flags.Parse(args) != nil || flags.NArg() != 0 || *protocol != "acp" {
		return true, errACPConfiguration
	}
	cfg, err := loadACPConfiguration(*path, os.Getenv)
	if err != nil {
		return true, err
	}
	return true, serveACP(context.Background(), cfg, input, output)
}

func loadACPConfiguration(path string, getenv func(string) string) (acpConfiguration, error) {
	file, err := os.Open(path)
	if err != nil {
		return acpConfiguration{}, errACPConfiguration
	}
	defer file.Close() //nolint:errcheck
	data, err := io.ReadAll(io.LimitReader(file, acpMaxConfigBytes+1))
	if err != nil || len(data) > acpMaxConfigBytes {
		return acpConfiguration{}, errACPConfiguration
	}
	return verifyACPConfiguration(data, getenv)
}

func verifyACPConfiguration(data []byte, getenv func(string) string) (acpConfiguration, error) {
	agent, err := decodeACPAgentConfiguration(data, getenv(acpConfigDigestEnv), getenv(acpModelEnv))
	if err != nil {
		return acpConfiguration{}, err
	}
	base, err := acpLoopbackURL(getenv(acpProviderBaseEnv))
	token := getenv(acpProviderTokenEnv)
	if err != nil || !acpSafeString(token, 16<<10) || strings.ContainsAny(token, " \t") {
		return acpConfiguration{}, errACPConfiguration
	}
	base.Path = strings.TrimRight(base.Path, "/") + "/responses"
	return acpConfiguration{agent: agent, providerURL: base.String(), token: token}, nil
}

// Both entry points verify one immutable buffer, while only the privileged
// broker uses HostedTarget. No child proxy credential is needed to parse it.
func decodeACPAgentConfiguration(data []byte, expectedDigest, expectedModel string) (acpAgentConfiguration, error) {
	actual := sha256.Sum256(data)
	encoded := "sha256:" + hex.EncodeToString(actual[:])
	if len(data) > acpMaxConfigBytes || subtle.ConstantTimeCompare([]byte(expectedDigest), []byte(encoded)) != 1 {
		return acpAgentConfiguration{}, errACPConfiguration
	}
	var agent acpAgentConfiguration
	if acpDecode(data, &agent, true) != nil || !acpSafeString(agent.Model, 512) || agent.Model != expectedModel {
		return acpAgentConfiguration{}, errACPConfiguration
	}
	if agent.ToolSchemaMode != toolSchemaModeRequest && agent.ToolSchemaMode != toolSchemaModeProviderStatic {
		return acpAgentConfiguration{}, errACPConfiguration
	}
	if strings.TrimSpace(agent.HostedTarget.ProjectEndpoint) != agent.HostedTarget.ProjectEndpoint ||
		!foundryEndpointIsSafe(agent.HostedTarget.ProjectEndpoint) ||
		validateAgentName(agent.HostedTarget.AgentName) != nil || agent.HostedTarget.AgentVersion == "" ||
		strings.EqualFold(agent.HostedTarget.AgentVersion, "latest") || validateAgentVersion(agent.HostedTarget.AgentVersion) != nil {
		return acpAgentConfiguration{}, errACPConfiguration
	}
	return agent, nil
}

func acpSafeString(value string, limit int) bool {
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

func acpLoopbackURL(value string) (*url.URL, error) {
	u, err := url.Parse(value)
	if err != nil || !acpSafeString(value, 8<<10) || strings.TrimSpace(value) != value || u == nil ||
		(u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil ||
		u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.Contains(value, "#") || u.RawPath != "" {
		return nil, errACPConfiguration
	}
	ip := net.ParseIP(u.Hostname())
	if u.Hostname() != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return nil, errACPConfiguration
	}
	if port := u.Port(); port != "" {
		value, err := strconv.Atoi(port)
		if err != nil || value < 1 || value > 65535 {
			return nil, errACPConfiguration
		}
	}
	return u, nil
}

func newACPHTTPClient() *http.Client {
	dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	return &http.Client{
		Timeout: acpHTTPTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return errACPTransport
		},
		Transport: &http.Transport{
			Proxy: nil,
			DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
				host, port, err := net.SplitHostPort(address)
				if err != nil {
					return nil, errACPTransport
				}
				if host == "localhost" {
					host = "127.0.0.1"
				}
				if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
					return nil, errACPTransport
				}
				return dialer.DialContext(ctx, network, net.JoinHostPort(host, port))
			},
			MaxIdleConns:        4,
			MaxIdleConnsPerHost: 2,
			MaxConnsPerHost:     2,
			IdleConnTimeout:     30 * time.Second,
		},
	}
}
