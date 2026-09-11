package main

import (
	"crypto/ed25519"
	"encoding/base64"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	hostedImageConfigPath   = "/agent/hosted.json"
	hostedGatewayConfigPath = "/etc/orka-foundry/gateway.json"
	hostedMaxHandshakeBytes = 64 << 10
	hostedSupervisorAddr    = "127.0.0.1:8080"
	hostedBrokerRelayAddr   = "127.0.0.1:8091"
	hostedOrkaRelayAddr     = "127.0.0.1:8092"
)

// Both configuration files are public. Credentials and the signing key are
// read only from gateway-only Secret mounts, never the hosted image or HOME.
type hostedGatewayConfig struct {
	Protocol             string            `json:"protocol"`
	Image                hostedImageConfig `json:"image"`
	ContainerImage       string            `json:"containerImage"`
	SessionID            string            `json:"sessionID"`
	RuntimeProfileDigest string            `json:"runtimeProfileDigest"`
	RuntimeEnvironment   map[string]string `json:"runtimeEnvironment"`
	OrkaBaseURL          string            `json:"orkaBaseURL"`
	BrokerBaseURL        string            `json:"brokerBaseURL"`
}

type hostedGatewaySettings struct {
	config     hostedGatewayConfig
	bootstrap  hostedBootstrap
	signingKey ed25519.PrivateKey
	stateDir   string
	address    string
}

func readHostedConfig(path string, target any) error {
	data, err := readHostedFile(path, hostedMaxHandshakeBytes)
	if err != nil || acpDecode(data, target, true) != nil {
		return errHostedInvalid
	}
	return nil
}

func readHostedFile(path string, maximum int64) ([]byte, error) {
	if !filepath.IsAbs(path) {
		return nil, errHostedInvalid
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, errHostedInvalid
	}
	defer file.Close() //nolint:errcheck
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > maximum {
		return nil, errHostedInvalid
	}
	data, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil || int64(len(data)) > maximum {
		return nil, errHostedInvalid
	}
	return data, nil
}

func loadHostedImageConfig(path string) (hostedImageConfig, error) {
	var cfg hostedImageConfig
	if readHostedConfig(path, &cfg) != nil || validateHostedImageConfig(cfg) != nil {
		return hostedImageConfig{}, errHostedInvalid
	}
	return cfg, nil
}

func loadHostedGatewaySettings(path string, getenv func(string) string) (hostedGatewaySettings, error) {
	var settings hostedGatewaySettings
	if readHostedConfig(path, &settings.config) != nil || validateHostedGatewayConfig(settings.config) != nil {
		return settings, errHostedInvalid
	}
	settings.stateDir = getenv("ORKA_FOUNDRY_GATEWAY_STATE_DIR")
	settings.address = firstNonBlank(getenv("ORKA_FOUNDRY_GATEWAY_ADDR"), ":8080")
	if !filepath.IsAbs(settings.stateDir) || filepath.Clean(settings.stateDir) == "/" || !hostedListenAddressValid(settings.address) {
		return settings, errHostedInvalid
	}
	key, err := readHostedFile(getenv("ORKA_FOUNDRY_GATEWAY_SIGNING_KEY_FILE"), 4096)
	if err != nil {
		return settings, errHostedInvalid
	}
	defer clear(key)
	decoded, err := base64.RawURLEncoding.Strict().DecodeString(string(key))
	if err != nil || len(decoded) != ed25519.PrivateKeySize || base64.RawURLEncoding.EncodeToString(decoded) != string(key) {
		clear(decoded)
		return settings, errHostedInvalid
	}
	settings.signingKey = ed25519.PrivateKey(decoded)
	// Reject unusable local keys before Azure authentication or session creation.
	if !hostedSigningKeyValid(settings.signingKey) ||
		base64.RawURLEncoding.EncodeToString(settings.signingKey.Public().(ed25519.PublicKey)) != settings.config.Image.SigningPublicKey {
		clear(decoded)
		return settings, errHostedInvalid
	}
	settings.bootstrap.Environment = settings.config.RuntimeEnvironment
	for _, field := range []struct {
		env   string
		value *string
	}{
		{"ORKA_FOUNDRY_GATEWAY_CONTROLLER_TOKEN_FILE", &settings.bootstrap.ControllerToken},
		{"ORKA_FOUNDRY_GATEWAY_CAPABILITY_SECRET_FILE", &settings.bootstrap.CapabilitySecret},
		{"ORKA_FOUNDRY_GATEWAY_PROVIDER_TOKEN_FILE", &settings.bootstrap.ProviderToken},
	} {
		data, err := readHostedFile(getenv(field.env), 16<<10)
		if err != nil {
			clear(settings.signingKey)
			return settings, errHostedInvalid
		}
		*field.value = string(data)
		clear(data)
	}
	if validateHostedBootstrap(settings.config.Image, settings.bootstrap) != nil {
		clear(settings.signingKey)
		return settings, errHostedInvalid
	}
	return settings, nil
}

func validateHostedGatewayConfig(cfg hostedGatewayConfig) error {
	imageName, digest, pinned := strings.Cut(cfg.ContainerImage, "@")
	if cfg.Protocol != hostedProtocol || validateHostedImageConfig(cfg.Image) != nil ||
		!hostedUUIDValid(cfg.SessionID) || !brokerDigestValid(cfg.RuntimeProfileDigest) ||
		!pinned || !brokerDigestValid(digest) || !acpSafeString(imageName, 512) ||
		strings.ContainsAny(imageName, " @\\?#") {
		return errHostedInvalid
	}
	// Validate the environment without reading any credential. These synthetic
	// placeholders never leave this validation function.
	placeholder := strings.Repeat("x", 32)
	if validateHostedBootstrap(cfg.Image, hostedBootstrap{Environment: cfg.RuntimeEnvironment,
		ControllerToken: placeholder, CapabilitySecret: placeholder, ProviderToken: placeholder}) != nil {
		return errHostedInvalid
	}
	if !hostedRelayTargetValid(cfg.OrkaBaseURL, false) || !hostedRelayTargetValid(cfg.BrokerBaseURL, true) {
		return errHostedInvalid
	}
	return nil
}

func hostedRelayTargetValid(raw string, loopback bool) bool {
	u, err := url.Parse(raw)
	if err != nil || !acpSafeString(raw, 2048) || (u.Scheme != "http" && u.Scheme != "https") ||
		u.Host == "" || u.User != nil || u.Path != "" || u.RawPath != "" ||
		u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.Contains(raw, "#") {
		return false
	}
	if port := u.Port(); port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return false
		}
	}
	if loopback {
		ip := net.ParseIP(u.Hostname())
		return ip != nil && ip.IsLoopback()
	}
	// This is a fixed operator-configured destination, never selected by an
	// incoming HTTP request. In-cluster DNS and HTTPS endpoints are supported.
	return true
}

func hostedListenAddressValid(address string) bool {
	host, port, err := net.SplitHostPort(address)
	n, portErr := strconv.Atoi(port)
	return err == nil && portErr == nil && n >= 1 && n <= 65535 &&
		(host == "" || net.ParseIP(host) != nil)
}
