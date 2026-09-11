package broker

import (
	"context"
	"errors"
	"flag"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/orka-agents/agent-runtime-foundry/internal/foundry"
)

type brokerConfiguration struct {
	agent            foundry.AgentConfig
	configDigest     string
	addr             string
	stateDir         string
	bearer           string
	agentKitProof    string
	operationTimeout time.Duration
}

func MaybeServe(args []string) (bool, error) {
	selected := false
	for index, arg := range args {
		if arg == "--protocol=broker" || (arg == "--protocol" && index+1 < len(args) && args[index+1] == "broker") {
			selected = true
		}
	}
	if !selected {
		return false, nil
	}
	flags := flag.NewFlagSet("foundry-broker", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	protocol := flags.String("protocol", "", "")
	path := flags.String("config", foundry.AgentConfigPath, "")
	healthCheck := flags.Bool("health-check", false, "")
	if flags.Parse(args) != nil || flags.NArg() != 0 || *protocol != "broker" {
		return true, errBrokerInvalid
	}
	if *healthCheck {
		return true, checkBrokerHealth(foundry.FirstNonBlank(os.Getenv("ORKA_FOUNDRY_BROKER_ADDR"), "127.0.0.1:8091"))
	}
	cfg, err := loadBrokerConfiguration(*path, os.Getenv)
	if err != nil {
		return true, err
	}
	provider, err := foundry.NewTokenProvider()
	if err != nil {
		return true, errBrokerRemote
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	broker, err := newLifecycleBroker(ctx, cfg, provider, nil)
	if err != nil {
		return true, err
	}
	defer broker.close()
	server := &http.Server{Addr: cfg.addr, Handler: broker, ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 30 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 32 << 10}
	finished := make(chan error, 1)
	go func() { finished <- server.ListenAndServe() }()
	select {
	case err := <-finished:
		if errors.Is(err, http.ErrServerClosed) {
			return true, nil
		}
		return true, errBrokerRemote
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
		return true, nil
	}
}

func loadBrokerConfiguration(path string, getenv func(string) string) (brokerConfiguration, error) {
	file, err := os.Open(path)
	if err != nil {
		return brokerConfiguration{}, errBrokerInvalid
	}
	defer file.Close() //nolint:errcheck
	data, err := io.ReadAll(io.LimitReader(file, foundry.MaxAgentConfigBytes+1))
	if err != nil || len(data) > foundry.MaxAgentConfigBytes {
		return brokerConfiguration{}, errBrokerInvalid
	}
	digest := getenv(foundry.AgentConfigDigestEnv)
	agent, err := foundry.DecodeAgentConfig(data, digest, getenv(foundry.ModelEnv))
	if err != nil {
		return brokerConfiguration{}, errBrokerInvalid
	}
	cfg := brokerConfiguration{agent: agent, configDigest: digest,
		addr:     foundry.FirstNonBlank(getenv("ORKA_FOUNDRY_BROKER_ADDR"), "127.0.0.1:8091"),
		stateDir: getenv("ORKA_FOUNDRY_BROKER_STATE_DIR"), bearer: getenv("ORKA_FOUNDRY_BROKER_BEARER_TOKEN"),
		agentKitProof:    getenv(brokerAgentKitProofEnv),
		operationTimeout: 45 * time.Second}
	if !brokerAddressValid(cfg.addr) || cfg.stateDir == "" ||
		!foundry.SafeString(cfg.bearer, 16<<10) || len(cfg.bearer) < 32 || strings.ContainsAny(cfg.bearer, " \t") ||
		!brokerAgentKitProofValid(cfg.agentKitProof) ||
		(getenv(foundry.IsolationModeEnv) != "" && getenv(foundry.IsolationModeEnv) != "entra") {
		return brokerConfiguration{}, errBrokerInvalid
	}
	return cfg, nil
}

func brokerAddressValid(address string) bool {
	host, port, err := net.SplitHostPort(address)
	ip := net.ParseIP(host)
	value, portErr := strconv.Atoi(port)
	return err == nil && ip != nil && ip.IsLoopback() && portErr == nil && value > 0 && value <= 65535
}

// The distroless image can run its own exec readiness probe without credentials,
// a configuration file, or a second writer of the durable ledger.
func checkBrokerHealth(address string) error {
	if !brokerAddressValid(address) {
		return errBrokerInvalid
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+address+"/healthz", nil)
	if err != nil {
		return errBrokerInvalid
	}
	client := newBrokerHTTPClient()
	defer client.CloseIdleConnections()
	response, err := client.Do(request)
	if err != nil {
		return errBrokerRemote
	}
	defer response.Body.Close() //nolint:errcheck
	if response.StatusCode != http.StatusOK {
		return errBrokerRemote
	}
	return nil
}
