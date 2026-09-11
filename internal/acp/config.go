package acp

import (
	"context"
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

	"github.com/orka-agents/agent-runtime-foundry/internal/foundry"
)

const (
	acpProviderBaseEnv  = "ORKA_FOUNDRY_ACP_PROVIDER_BASE_URL"
	acpProviderTokenEnv = "ORKA_FOUNDRY_ACP_PROVIDER_TOKEN"
	acpMaxMessageBytes  = 8 << 20
	acpTextChunkBytes   = 32 << 10
	acpHTTPTimeout      = 120 * time.Second
)

var (
	errACPMCP       = errors.New("Foundry ACP MCP request failed")
	errACPTransport = errors.New("Foundry ACP transport failed")
)

type acpConfiguration struct {
	agent       foundry.AgentConfig
	providerURL string
	token       string
}

// The ACP entry point deliberately precedes Azure credential initialization.
// Its only network authority is the two supervisor-owned loopback proxies.
func MaybeServe(args []string, input io.ReadCloser, output io.WriteCloser) (bool, error) {
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
	path := flags.String("config", foundry.AgentConfigPath, "")
	if flags.Parse(args) != nil || flags.NArg() != 0 || *protocol != "acp" {
		return true, foundry.ErrAgentConfig
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
		return acpConfiguration{}, foundry.ErrAgentConfig
	}
	defer file.Close() //nolint:errcheck
	data, err := io.ReadAll(io.LimitReader(file, foundry.MaxAgentConfigBytes+1))
	if err != nil || len(data) > foundry.MaxAgentConfigBytes {
		return acpConfiguration{}, foundry.ErrAgentConfig
	}
	return verifyACPConfiguration(data, getenv)
}

func verifyACPConfiguration(data []byte, getenv func(string) string) (acpConfiguration, error) {
	agent, err := foundry.DecodeAgentConfig(data, getenv(foundry.AgentConfigDigestEnv), getenv(foundry.ModelEnv))
	if err != nil {
		return acpConfiguration{}, err
	}
	base, err := acpLoopbackURL(getenv(acpProviderBaseEnv))
	token := getenv(acpProviderTokenEnv)
	if err != nil || !foundry.SafeString(token, 16<<10) || strings.ContainsAny(token, " \t") {
		return acpConfiguration{}, foundry.ErrAgentConfig
	}
	base.Path = strings.TrimRight(base.Path, "/") + "/responses"
	return acpConfiguration{agent: agent, providerURL: base.String(), token: token}, nil
}

func acpLoopbackURL(value string) (*url.URL, error) {
	u, err := url.Parse(value)
	if err != nil || !foundry.SafeString(value, 8<<10) || strings.TrimSpace(value) != value || u == nil ||
		(u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil ||
		u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.Contains(value, "#") || u.RawPath != "" {
		return nil, foundry.ErrAgentConfig
	}
	ip := net.ParseIP(u.Hostname())
	if u.Hostname() != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return nil, foundry.ErrAgentConfig
	}
	if port := u.Port(); port != "" {
		value, err := strconv.Atoi(port)
		if err != nil || value < 1 || value > 65535 {
			return nil, foundry.ErrAgentConfig
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
