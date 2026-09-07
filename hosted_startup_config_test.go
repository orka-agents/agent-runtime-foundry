package main

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestHostedGatewayRejectsUnusableSigningKeyDuringSettingsLoad(t *testing.T) {
	for _, scenario := range []string{"valid", "zero-seed", "mismatched-seed"} {
		t.Run(scenario, func(t *testing.T) {
			f := newHostedProtocolFixture(t)
			key := bytes.Clone(f.key)
			defer clear(key)
			switch scenario {
			case "zero-seed":
				key = ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
				defer clear(key)
				f.config.SigningPublicKey = base64.RawURLEncoding.EncodeToString(key[ed25519.SeedSize:])
			case "mismatched-seed":
				key[0] ^= 1
			}
			cfg := hostedGatewayConfig{
				Protocol: hostedProtocol, Image: f.config,
				ContainerImage: "example.invalid/hosted@" + brokerSHA([]byte("fixture image")),
				SessionID:      f.hello.Challenge.SessionID, RuntimeProfileDigest: brokerSHA([]byte("fixture profile")),
				RuntimeEnvironment: maps.Clone(f.bootstrap.Environment),
				OrkaBaseURL:        "http://orka.test:8080", BrokerBaseURL: "http://127.0.0.1:8091",
			}
			if validateHostedGatewayConfig(cfg) != nil {
				t.Fatal("signing-key fixture has invalid public configuration")
			}
			dir := t.TempDir()
			write := func(name string, data []byte) string {
				t.Helper()
				path := filepath.Join(dir, name)
				if os.WriteFile(path, data, 0o600) != nil {
					t.Fatal("could not write local settings fixture")
				}
				return path
			}
			raw, err := json.Marshal(cfg)
			if err != nil {
				t.Fatal("could not marshal public settings fixture")
			}
			configPath := write("gateway.json", raw)
			env := map[string]string{
				"ORKA_FOUNDRY_GATEWAY_STATE_DIR":              filepath.Join(dir, "ledger"),
				"ORKA_FOUNDRY_GATEWAY_SIGNING_KEY_FILE":       write("signing-key", []byte(base64.RawURLEncoding.EncodeToString(key))),
				"ORKA_FOUNDRY_GATEWAY_CONTROLLER_TOKEN_FILE":  write("controller-token", []byte(f.bootstrap.ControllerToken)),
				"ORKA_FOUNDRY_GATEWAY_CAPABILITY_SECRET_FILE": write("capability-secret", []byte(f.bootstrap.CapabilitySecret)),
				"ORKA_FOUNDRY_GATEWAY_PROVIDER_TOKEN_FILE":    write("provider-token", []byte(f.bootstrap.ProviderToken)),
			}
			bootstrapReads := 0
			settings, err := loadHostedGatewaySettings(configPath, func(name string) string {
				if strings.HasSuffix(name, "_TOKEN_FILE") || strings.HasSuffix(name, "_SECRET_FILE") {
					bootstrapReads++
				}
				return env[name]
			})
			defer clear(settings.signingKey)
			if scenario != "valid" {
				if err == nil {
					t.Fatal("unusable signing key passed the pre-network settings boundary")
				}
				if bootstrapReads != 0 || hostedNonzeroBytes(settings.signingKey) {
					t.Fatal("invalid signing material was retained or bootstrap credentials were read")
				}
				return
			}
			if err != nil || bootstrapReads != 3 {
				t.Fatal("valid signing-key configuration was rejected")
			}
			signed, err := signHostedHello(f.hello, settings.signingKey)
			if err != nil || verifyHostedHello(signed, f.hello.Challenge, cfg.Image.SigningPublicKey, f.now) != nil {
				t.Fatal("loaded signing key could not authenticate the configured hosted image")
			}
			if _, err := os.Stat(env["ORKA_FOUNDRY_GATEWAY_STATE_DIR"]); !os.IsNotExist(err) {
				t.Fatal("settings validation created a gateway ownership ledger")
			}
		})
	}
}
