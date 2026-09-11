package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"maps"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

type hostedProtocolFixture struct {
	config    hostedImageConfig
	bootstrap hostedBootstrap
	hello     hostedHello
	key       ed25519.PrivateKey
	now       time.Time
}

func newHostedProtocolFixture(t *testing.T) hostedProtocolFixture {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal("could not generate test signing key")
	}
	cfg := hostedImageConfig{
		Protocol: hostedProtocol, DeploymentID: uuid.NewString(),
		Target: acpHostedTarget{
			ProjectEndpoint: "https://test-account.services.ai.azure.com/api/projects/test-project",
			AgentName:       "test-agent",
			AgentVersion:    "8",
		},
		SigningPublicKey:         base64.RawURLEncoding.EncodeToString(public),
		AgentConfigurationDigest: brokerSHA([]byte("test agent configuration")),
	}
	bootstrap := hostedBootstrap{
		ControllerToken:  strings.Repeat("test-controller-", 3),
		CapabilitySecret: strings.Repeat("test-capability-", 3),
		ProviderToken:    strings.Repeat("test-provider-", 3),
		Environment: map[string]string{
			"ORKA_ACP_PROVIDER":                   "foundry",
			"ORKA_ACP_MODEL":                      "test-model",
			"ORKA_ACP_FOUNDRY_ADAPTER_DIGEST":     brokerSHA([]byte("test adapter")),
			"ORKA_ACP_WORKSPACE_INTENT":           "read",
			"ORKA_ACP_AGENT_CONFIGURATION_DIGEST": cfg.AgentConfigurationDigest,
			"ORKA_ACP_TOOL_POLICY_DIGEST":         brokerSHA([]byte("test tool policy")),
			"ORKA_ACP_APPROVAL_POLICY_DIGEST":     brokerSHA([]byte("test approval policy")),
			"ORKA_ACP_MCP_CONFIGURATION_DIGEST":   brokerSHA([]byte("test MCP configuration")),
			"ORKA_ACP_PROXY_CREDENTIAL_ROLE":      "operator-managed",
			"ORKA_ACP_PROXY_CREDENTIAL_SCOPE":     "external-runtime",
			"ORKA_ACP_RESOURCE_CLASS":             "external",
			"ORKA_ACP_TRUST_NAMESPACE":            "test-namespace",
			"ORKA_ACP_CONTROLLER_EPOCH":           "1",
			"ORKA_ACP_RUNTIME_POOL_GENERATION":    "2",
			"ORKA_ACP_RUNTIME_POOL_UID":           uuid.NewString(),
		},
	}
	now := time.Unix(1_800_000_000, 0)
	hello := hostedHello{
		Challenge: hostedChallenge{
			Protocol: hostedProtocol, DeploymentID: cfg.DeploymentID,
			ConfigurationDigest: brokerJSONDigest(cfg), AgentName: cfg.Target.AgentName,
			AgentVersion: cfg.Target.AgentVersion, SessionID: uuid.NewString(), BootID: uuid.NewString(),
			Nonce: base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{7}, 32)),
		},
		PairID: uuid.NewString(), Role: "forward", BootstrapDigest: brokerJSONDigest(bootstrap),
		ExpiresAt: now.Add(time.Minute).Unix(),
	}
	return hostedProtocolFixture{config: cfg, bootstrap: bootstrap, hello: hello, key: private, now: now}
}

func (f hostedProtocolFixture) sign(t *testing.T, value hostedHello) hostedHello {
	t.Helper()
	signed, err := signHostedHello(value, f.key)
	if err != nil {
		t.Fatal("valid test handshake was not signed")
	}
	return signed
}

func TestHostedHelloAuthenticatesBothRoles(t *testing.T) {
	f := newHostedProtocolFixture(t)
	for _, role := range []string{"forward", "reverse"} {
		t.Run(role, func(t *testing.T) {
			value := f.hello
			value.Role = role
			signed := f.sign(t, value)
			if verifyHostedHello(signed, value.Challenge, f.config.SigningPublicKey, f.now) != nil {
				t.Fatal("valid signed role rejected")
			}
			// Verification is stateless. The hosted server consumes each role and
			// nonce under its own admission lock after cryptographic verification.
			if verifyHostedHello(signed, value.Challenge, f.config.SigningPublicKey, f.now) != nil {
				t.Fatal("signature verification unexpectedly consumed handshake state")
			}
		})
	}
}

func TestHostedHelloBindsEverySignedField(t *testing.T) {
	f := newHostedProtocolFixture(t)
	signed := f.sign(t, f.hello)
	for name, mutate := range map[string]func(*hostedHello){
		"protocol":      func(v *hostedHello) { v.Challenge.Protocol = "other-protocol" },
		"deployment":    func(v *hostedHello) { v.Challenge.DeploymentID = uuid.NewString() },
		"configuration": func(v *hostedHello) { v.Challenge.ConfigurationDigest = brokerSHA([]byte("other configuration")) },
		"agent":         func(v *hostedHello) { v.Challenge.AgentName = "other-agent" },
		"version":       func(v *hostedHello) { v.Challenge.AgentVersion = "9" },
		"session":       func(v *hostedHello) { v.Challenge.SessionID = uuid.NewString() },
		"boot":          func(v *hostedHello) { v.Challenge.BootID = uuid.NewString() },
		"nonce": func(v *hostedHello) {
			v.Challenge.Nonce = base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{8}, 32))
		},
		"pair":   func(v *hostedHello) { v.PairID = uuid.NewString() },
		"role":   func(v *hostedHello) { v.Role = "reverse" },
		"expiry": func(v *hostedHello) { v.ExpiresAt-- },
		"bootstrap body": func(v *hostedHello) {
			body := f.bootstrap
			body.Environment = maps.Clone(body.Environment)
			body.Environment["ORKA_ACP_WORKSPACE_INTENT"] = "write"
			v.BootstrapDigest = brokerJSONDigest(body)
		},
		"bootstrap credential": func(v *hostedHello) {
			body := f.bootstrap
			body.ControllerToken = strings.Repeat("different-test-controller-", 2)
			v.BootstrapDigest = brokerJSONDigest(body)
		},
	} {
		t.Run(name, func(t *testing.T) {
			changed := signed
			mutate(&changed)
			// Matching the modified challenge deliberately bypasses the equality
			// check, so valid field mutations must fail the signature itself.
			if verifyHostedHello(changed, changed.Challenge, f.config.SigningPublicKey, f.now) == nil {
				t.Fatal("changed signed field accepted")
			}
		})
	}
}

func TestHostedHelloRejectsDifferentExpectedChallenge(t *testing.T) {
	f := newHostedProtocolFixture(t)
	signed := f.sign(t, f.hello)
	for name, mutate := range map[string]func(*hostedChallenge){
		"deployment": func(c *hostedChallenge) { c.DeploymentID = uuid.NewString() },
		"session":    func(c *hostedChallenge) { c.SessionID = uuid.NewString() },
		"boot":       func(c *hostedChallenge) { c.BootID = uuid.NewString() },
		"nonce": func(c *hostedChallenge) {
			c.Nonce = base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{9}, 32))
		},
	} {
		t.Run(name, func(t *testing.T) {
			expected := signed.Challenge
			mutate(&expected)
			if verifyHostedHello(signed, expected, f.config.SigningPublicKey, f.now) == nil {
				t.Fatal("handshake authenticated against another challenge")
			}
		})
	}
}

func TestHostedHelloExpiryWindow(t *testing.T) {
	f := newHostedProtocolFixture(t)
	for _, test := range []struct {
		name   string
		offset time.Duration
		valid  bool
	}{
		{"expired", -time.Second, false},
		{"exactly now", 0, false},
		{"one second", time.Second, true},
		{"ninety seconds", 90 * time.Second, true},
		{"too far ahead", 91 * time.Second, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			value := f.hello
			value.ExpiresAt = f.now.Add(test.offset).Unix()
			signed := f.sign(t, value)
			if (verifyHostedHello(signed, signed.Challenge, f.config.SigningPublicKey, f.now) == nil) != test.valid {
				t.Fatal("expiry window enforced incorrectly")
			}
		})
	}
	signed := f.sign(t, f.hello)
	if verifyHostedHello(signed, signed.Challenge, f.config.SigningPublicKey,
		time.Unix(signed.ExpiresAt, 1)) == nil {
		t.Fatal("subsecond expiry was rounded into the acceptance window")
	}
}

func TestHostedHelloRejectsMalformedKeysAndSignatures(t *testing.T) {
	f := newHostedProtocolFixture(t)
	signed := f.sign(t, f.hello)
	corruptedKey := bytes.Clone(f.key)
	corruptedKey[len(corruptedKey)-1] ^= 1
	for name, key := range map[string]ed25519.PrivateKey{
		"missing": nil, "seed only": f.key[:ed25519.SeedSize],
		"short": f.key[:len(f.key)-1], "long": append(bytes.Clone(f.key), 1),
		"zero":                     make([]byte, ed25519.PrivateKeySize),
		"zero seed":                ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize)),
		"inconsistent public half": corruptedKey,
	} {
		t.Run("private "+name, func(t *testing.T) {
			if _, err := signHostedHello(f.hello, key); err == nil {
				t.Fatal("malformed private key accepted")
			}
		})
	}
	other := newHostedProtocolFixture(t)
	for name, key := range map[string]string{
		"missing": "", "different": other.config.SigningPublicKey,
		"zero":      base64.RawURLEncoding.EncodeToString(make([]byte, ed25519.PublicKeySize)),
		"short":     base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{1}, ed25519.PublicKeySize-1)),
		"long":      base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{1}, ed25519.PublicKeySize+1)),
		"padded":    f.config.SigningPublicKey + "=",
		"newline":   f.config.SigningPublicKey + "\n",
		"tail bits": hostedTestNoncanonicalBase64(f.config.SigningPublicKey),
	} {
		t.Run("public "+name, func(t *testing.T) {
			if verifyHostedHello(signed, signed.Challenge, key, f.now) == nil {
				t.Fatal("invalid or different public key accepted")
			}
		})
	}
	for name, signature := range map[string]string{
		"missing": "", "wrong alphabet": strings.Repeat("+", len(signed.Signature)),
		"zero":      base64.RawURLEncoding.EncodeToString(make([]byte, ed25519.SignatureSize)),
		"short":     base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{1}, ed25519.SignatureSize-1)),
		"long":      base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{1}, ed25519.SignatureSize+1)),
		"padded":    signed.Signature + "==",
		"newline":   signed.Signature + "\n",
		"tail bits": hostedTestNoncanonicalBase64(signed.Signature),
	} {
		t.Run("signature "+name, func(t *testing.T) {
			value := signed
			value.Signature = signature
			if verifyHostedHello(value, value.Challenge, f.config.SigningPublicKey, f.now) == nil {
				t.Fatal("malformed signature accepted")
			}
		})
	}
}

func hostedTestNoncanonicalBase64(value string) string {
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
	last := strings.IndexByte(alphabet, value[len(value)-1])
	return value[:len(value)-1] + string(alphabet[last+1])
}

func TestHostedHelloRejectsIdentityPublicKey(t *testing.T) {
	f := newHostedProtocolFixture(t)
	identity := make([]byte, ed25519.PublicKeySize)
	identity[0] = 1
	// For an identity public key, R = identity and S = 0 can satisfy the
	// standard Ed25519 verification equation for every handshake body.
	forged := make([]byte, ed25519.SignatureSize)
	forged[0] = 1
	f.config.SigningPublicKey = base64.RawURLEncoding.EncodeToString(identity)
	f.hello.Signature = base64.RawURLEncoding.EncodeToString(forged)
	if validateHostedImageConfig(f.config) == nil {
		t.Error("identity public key accepted in image configuration")
	}
	if verifyHostedHello(f.hello, f.hello.Challenge, f.config.SigningPublicKey, f.now) == nil {
		t.Error("handshake authenticated without knowledge of a private key")
	}
}

func TestHostedImageConfigRejectsInvalidPublicPoints(t *testing.T) {
	f := newHostedProtocolFixture(t)
	offCurve := make([]byte, ed25519.PublicKeySize)
	offCurve[0] = 2
	orderTwo := bytes.Repeat([]byte{0xff}, ed25519.PublicKeySize)
	orderTwo[0], orderTwo[31] = 0xec, 0x7f // y = p - 1
	orderFour := make([]byte, ed25519.PublicKeySize)
	orderFour[31] = 0x80 // y = 0, negative x
	negativeIdentity := make([]byte, ed25519.PublicKeySize)
	negativeIdentity[0], negativeIdentity[31] = 1, 0x80
	noncanonicalPoint := bytes.Repeat([]byte{0xff}, ed25519.PublicKeySize)
	noncanonicalPoint[0], noncanonicalPoint[31] = 0xf0, 0x7f // y = p + 3
	for name, key := range map[string][]byte{
		"off curve": offCurve, "order two": orderTwo, "order four": orderFour,
		"negative identity": negativeIdentity, "noncanonical point": noncanonicalPoint,
	} {
		t.Run(name, func(t *testing.T) {
			cfg := f.config
			cfg.SigningPublicKey = base64.RawURLEncoding.EncodeToString(key)
			if validateHostedImageConfig(cfg) == nil {
				t.Fatal("invalid curve point accepted as signing public key")
			}
		})
	}
}

func TestHostedHelloRejectsInvalidSigningInputs(t *testing.T) {
	f := newHostedProtocolFixture(t)
	for name, mutate := range map[string]func(*hostedHello){
		"unknown protocol": func(v *hostedHello) { v.Challenge.Protocol = "other" },
		"invalid deployment": func(v *hostedHello) {
			v.Challenge.DeploymentID = strings.ReplaceAll(v.Challenge.DeploymentID, "-", "")
		},
		"invalid configuration": func(v *hostedHello) { v.Challenge.ConfigurationDigest = "invalid" },
		"invalid agent":         func(v *hostedHello) { v.Challenge.AgentName = "../other" },
		"unpinned version":      func(v *hostedHello) { v.Challenge.AgentVersion = "@latest" },
		"invalid session":       func(v *hostedHello) { v.Challenge.SessionID = "session" },
		"invalid boot":          func(v *hostedHello) { v.Challenge.BootID = "boot" },
		"zero nonce": func(v *hostedHello) {
			v.Challenge.Nonce = base64.RawURLEncoding.EncodeToString(make([]byte, 32))
		},
		"padded nonce":      func(v *hostedHello) { v.Challenge.Nonce += "=" },
		"invalid pair":      func(v *hostedHello) { v.PairID = "pair" },
		"unknown role":      func(v *hostedHello) { v.Role = "control" },
		"invalid bootstrap": func(v *hostedHello) { v.BootstrapDigest = "invalid" },
		"zero expiry":       func(v *hostedHello) { v.ExpiresAt = 0 },
		"negative expiry":   func(v *hostedHello) { v.ExpiresAt = -1 },
	} {
		t.Run(name, func(t *testing.T) {
			value := f.hello
			mutate(&value)
			if _, err := signHostedHello(value, f.key); err == nil {
				t.Fatal("invalid handshake signed")
			}
		})
	}
}

func TestHostedHelloCanonicalJSONWire(t *testing.T) {
	f := newHostedProtocolFixture(t)
	signed := f.sign(t, f.hello)
	raw, err := json.Marshal(signed)
	if err != nil {
		t.Fatal("could not encode test handshake")
	}
	var reordered map[string]json.RawMessage
	if acpDecode(raw, &reordered, true) != nil {
		t.Fatal("could not decode test wire fields")
	}
	// Marshalling a map sorts the keys differently from the signing struct.
	raw, err = json.Marshal(reordered)
	if err != nil {
		t.Fatal("could not reorder test wire fields")
	}
	var decoded hostedHello
	if acpDecode(raw, &decoded, true) != nil ||
		verifyHostedHello(decoded, signed.Challenge, f.config.SigningPublicKey, f.now) != nil {
		t.Fatal("valid reordered wire representation rejected")
	}
	reordered["unexpected"] = json.RawMessage(`true`)
	raw, _ = json.Marshal(reordered)
	if acpDecode(raw, &decoded, true) == nil {
		t.Fatal("unexpected handshake field accepted by strict wire decoder")
	}
}

func TestHostedImageConfigRequiresExactTarget(t *testing.T) {
	f := newHostedProtocolFixture(t)
	if validateHostedImageConfig(f.config) != nil {
		t.Fatal("valid hosted image configuration rejected")
	}
	for _, endpoint := range []string{
		"http://test-account.services.ai.azure.com/api/projects/test-project",
		"https://test-account.services.ai.azure.com:443/api/projects/test-project",
		"https://TEST-ACCOUNT.services.ai.azure.com/api/projects/test-project",
		"https://test-account.SERVICES.ai.azure.com/api/projects/test-project",
		"https://nested.test-account.services.ai.azure.com/api/projects/test-project",
		"https://services.ai.azure.com/api/projects/test-project",
		"https://test-account.services.ai.azure.com.evil.invalid/api/projects/test-project",
		"https://test-account.azure.com/api/projects/test-project",
		"https://user@test-account.services.ai.azure.com/api/projects/test-project",
		"https://test-account.services.ai.azure.com/api/projects/test-project?",
		"https://test-account.services.ai.azure.com/api/projects/test-project?version=8",
		"https://test-account.services.ai.azure.com/api/projects/test-project#",
		"https://test-account.services.ai.azure.com/api/projects/test-project#fragment",
		"https://test-account.services.ai.azure.com/api/projects/%74est-project",
		"https://test-account.services.ai.azure.com/api/projects/test%2Fproject",
		"https://test-account.services.ai.azure.com/api/projects/../other",
		"https://test-account.services.ai.azure.com/api/projects/..",
		"https://test-account.services.ai.azure.com/api/projects/test-project/",
		"https://test-account.services.ai.azure.com/api/projects/test-project/agents",
		"https://test-account.services.ai.azure.com/api/projects/",
		"https://test-account.services.ai.azure.com/API/projects/test-project",
		"https://test-account.services.ai.azure.com/api/projects/test project",
		"https://test-account.services.ai.azure.com/api/projects/über",
		" https://test-account.services.ai.azure.com/api/projects/test-project",
		"https://test-account.services.ai.azure.com/api/projects/test-project ",
	} {
		cfg := f.config
		cfg.Target.ProjectEndpoint = endpoint
		if validateHostedImageConfig(cfg) == nil {
			t.Fatal("unsafe or noncanonical hosted endpoint accepted")
		}
	}
	for _, version := range []string{"", "0", "01", "+1", "-1", "1.0", "1e1", "latest", "@latest", "1 ", "18446744073709551616"} {
		cfg := f.config
		cfg.Target.AgentVersion = version
		if validateHostedImageConfig(cfg) == nil {
			t.Fatal("noncanonical or unpinned hosted version accepted")
		}
	}
	for name, mutate := range map[string]func(*hostedImageConfig){
		"protocol":         func(c *hostedImageConfig) { c.Protocol = "other" },
		"zero deployment":  func(c *hostedImageConfig) { c.DeploymentID = uuid.Nil.String() },
		"upper deployment": func(c *hostedImageConfig) { c.DeploymentID = "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA" },
		"short deployment": func(c *hostedImageConfig) { c.DeploymentID = strings.ReplaceAll(c.DeploymentID, "-", "") },
		"agent path":       func(c *hostedImageConfig) { c.Target.AgentName = "test/agent" },
		"empty digest":     func(c *hostedImageConfig) { c.AgentConfigurationDigest = "" },
		"uppercase digest": func(c *hostedImageConfig) { c.AgentConfigurationDigest = strings.ToUpper(c.AgentConfigurationDigest) },
		"zero public key": func(c *hostedImageConfig) {
			c.SigningPublicKey = base64.RawURLEncoding.EncodeToString(make([]byte, ed25519.PublicKeySize))
		},
		"padded public key": func(c *hostedImageConfig) { c.SigningPublicKey += "=" },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := f.config
			mutate(&cfg)
			if validateHostedImageConfig(cfg) == nil {
				t.Fatal("invalid hosted image configuration accepted")
			}
		})
	}
}

func TestHostedBootstrapAcceptsFrozenEnvironment(t *testing.T) {
	f := newHostedProtocolFixture(t)
	if validateHostedBootstrap(f.config, f.bootstrap) != nil {
		t.Fatal("valid bootstrap rejected")
	}
	f.bootstrap.Environment["ORKA_ACP_WORKSPACE_INTENT"] = "write"
	f.bootstrap.Environment["ORKA_ACP_MODEL_CONTEXT_LIMIT"] = "128000"
	f.bootstrap.Environment["ORKA_ACP_MODEL_OUTPUT_LIMIT"] = "16384"
	if validateHostedBootstrap(f.config, f.bootstrap) != nil {
		t.Fatal("valid model limits and write workspace rejected")
	}
	for _, size := range []int{32, 16 << 10} {
		for _, field := range []*string{&f.bootstrap.ControllerToken, &f.bootstrap.CapabilitySecret, &f.bootstrap.ProviderToken} {
			*field = strings.Repeat("x", size)
		}
		if validateHostedBootstrap(f.config, f.bootstrap) != nil {
			t.Fatal("valid credential size boundary rejected")
		}
	}
}

func TestHostedBootstrapRejectsMissingAndAdditionalAuthority(t *testing.T) {
	f := newHostedProtocolFixture(t)
	for key := range f.bootstrap.Environment {
		t.Run("missing "+key, func(t *testing.T) {
			body := f.bootstrap
			body.Environment = maps.Clone(body.Environment)
			delete(body.Environment, key)
			if validateHostedBootstrap(f.config, body) == nil {
				t.Fatal("required bootstrap environment key omitted")
			}
		})
	}
	for _, key := range []string{
		"HOME", "PATH", "LD_PRELOAD", "HTTP_PROXY", "AZURE_CLIENT_ID", "IDENTITY_ENDPOINT", "IDENTITY_HEADER",
		"FOUNDRY_AGENT_SESSION_ID", "ORKA_ACP_WORKSPACE_ROOT", "ORKA_ACP_EXEC_HELPER",
		"ORKA_ACP_PROCESS_COMMAND", "ORKA_ACP_LISTEN_ADDR", "ORKA_ACP_MCP_BROKER_URL",
		"ORKA_ACP_CONTROLLER_TOKEN", "ORKA_ACP_PROVIDER_TOKEN", "ORKA_ACP_CAPABILITY_SECRET",
		"ORKA_ACP_SUPERVISOR_BOOT_ID", "ORKA_ACP_RUNTIME_INSTANCE_ID", "ORKA_ACP_RUNTIME_SESSION_UID",
		"ORKA_FOUNDRY_ACP_PROVIDER_BASE_URL", "ORKA_FOUNDRY_ACP_PROVIDER_TOKEN", "orka_acp_model", "UNKNOWN",
	} {
		t.Run("additional "+key, func(t *testing.T) {
			body := f.bootstrap
			body.Environment = maps.Clone(body.Environment)
			body.Environment[key] = "synthetic-test-setting"
			if validateHostedBootstrap(f.config, body) == nil {
				t.Fatal("additional bootstrap environment authority accepted")
			}
		})
	}
	f.bootstrap.Environment = nil
	if validateHostedBootstrap(f.config, f.bootstrap) == nil {
		t.Fatal("missing bootstrap environment accepted")
	}
}

func TestHostedBootstrapRejectsInvalidValues(t *testing.T) {
	f := newHostedProtocolFixture(t)
	for key, values := range map[string][]string{
		"ORKA_ACP_PROVIDER":                   {"", "codex", "Foundry", "foundry "},
		"ORKA_ACP_MODEL":                      {"", " ", " model", "model ", "model\n", string([]byte{0xff}), strings.Repeat("m", 513)},
		"ORKA_ACP_WORKSPACE_INTENT":           {"", "Read", "read-write", "execute"},
		"ORKA_ACP_FOUNDRY_ADAPTER_DIGEST":     {"", "invalid", "sha256:" + strings.Repeat("A", 64)},
		"ORKA_ACP_AGENT_CONFIGURATION_DIGEST": {"", brokerSHA([]byte("other agent configuration"))},
		"ORKA_ACP_TOOL_POLICY_DIGEST":         {"", "invalid"},
		"ORKA_ACP_APPROVAL_POLICY_DIGEST":     {"", "invalid"},
		"ORKA_ACP_MCP_CONFIGURATION_DIGEST":   {"", "invalid"},
		"ORKA_ACP_PROXY_CREDENTIAL_ROLE":      {"", "task-managed", "operator-managed "},
		"ORKA_ACP_PROXY_CREDENTIAL_SCOPE":     {"", "task", "session"},
		"ORKA_ACP_RESOURCE_CLASS":             {"", "privileged", "internal"},
		"ORKA_ACP_TRUST_NAMESPACE":            {"", "Upper", "with.dot", "with_underscore", "-prefix", "suffix-", strings.Repeat("n", 64)},
		"ORKA_ACP_CONTROLLER_EPOCH":           {"", "0", "01", "+1", "-1", "1.0", "1e1", "18446744073709551616"},
		"ORKA_ACP_RUNTIME_POOL_GENERATION":    {"", "0", "01", "+1", "-1", "1.0", "1e1", "18446744073709551616"},
		"ORKA_ACP_RUNTIME_POOL_UID":           {"", "pool", uuid.Nil.String(), "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA"},
	} {
		t.Run(key, func(t *testing.T) {
			for _, value := range values {
				body := f.bootstrap
				body.Environment = maps.Clone(body.Environment)
				body.Environment[key] = value
				if validateHostedBootstrap(f.config, body) == nil {
					t.Fatal("invalid bootstrap environment value accepted")
				}
			}
		})
	}
	for _, limits := range [][2]string{
		{"1", ""}, {"", "1"}, {"0", "1"}, {"1", "0"}, {"01", "1"}, {"1", "01"},
		{"-1", "1"}, {"1", "+1"}, {"1.0", "1"}, {"1", "1e1"}, {"18446744073709551616", "1"},
	} {
		body := f.bootstrap
		body.Environment = maps.Clone(body.Environment)
		if limits[0] != "" {
			body.Environment["ORKA_ACP_MODEL_CONTEXT_LIMIT"] = limits[0]
		}
		if limits[1] != "" {
			body.Environment["ORKA_ACP_MODEL_OUTPUT_LIMIT"] = limits[1]
		}
		if validateHostedBootstrap(f.config, body) == nil {
			t.Fatal("invalid or partial model limits accepted")
		}
	}
	badConfig := f.config
	badConfig.AgentConfigurationDigest = "invalid"
	if validateHostedBootstrap(badConfig, f.bootstrap) == nil {
		t.Fatal("bootstrap validated against invalid image configuration")
	}
}

func TestHostedBootstrapRejectsMalformedCredentials(t *testing.T) {
	f := newHostedProtocolFixture(t)
	for name, set := range map[string]func(*hostedBootstrap, string){
		"controller": func(b *hostedBootstrap, value string) { b.ControllerToken = value },
		"capability": func(b *hostedBootstrap, value string) { b.CapabilitySecret = value },
		"provider":   func(b *hostedBootstrap, value string) { b.ProviderToken = value },
	} {
		t.Run(name, func(t *testing.T) {
			for _, value := range []string{
				"", strings.Repeat("x", 31), strings.Repeat("x", (16<<10)+1),
				strings.Repeat("x", 32) + " ", strings.Repeat("x", 32) + "\t",
				strings.Repeat("x", 32) + "\n", strings.Repeat("x", 32) + "\x00",
				strings.Repeat("x", 32) + "\u00a0", strings.Repeat("x", 32) + "\u0085",
				strings.Repeat("x", 32) + string([]byte{0xff}),
			} {
				body := f.bootstrap
				set(&body, value)
				if validateHostedBootstrap(f.config, body) == nil {
					t.Fatal("malformed bootstrap credential accepted")
				}
			}
		})
	}
}
