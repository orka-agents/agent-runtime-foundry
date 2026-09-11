package hosted

import (
	"crypto/ed25519"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"filippo.io/edwards25519"
	"github.com/google/uuid"
	"github.com/orka-agents/agent-runtime-foundry/internal/foundry"
)

const hostedProtocol = "orka.foundry.hosted.v1"

var errHostedInvalid = errors.New("invalid Foundry hosted configuration or handshake")

type hostedImageConfig struct {
	Protocol                 string               `json:"protocol"`
	DeploymentID             string               `json:"deploymentID"`
	Target                   foundry.HostedTarget `json:"target"`
	SigningPublicKey         string               `json:"signingPublicKey"`
	AgentConfigurationDigest string               `json:"agentConfigurationDigest"`
}

type hostedBootstrap struct {
	Environment      map[string]string `json:"environment"`
	ControllerToken  string            `json:"controllerToken"`
	CapabilitySecret string            `json:"capabilitySecret"`
	ProviderToken    string            `json:"providerToken"`
}

type hostedChallenge struct {
	Protocol            string `json:"protocol"`
	DeploymentID        string `json:"deploymentID"`
	ConfigurationDigest string `json:"configurationDigest"`
	AgentName           string `json:"agentName"`
	AgentVersion        string `json:"agentVersion"`
	SessionID           string `json:"sessionID"`
	BootID              string `json:"bootID"`
	Nonce               string `json:"nonce"`
}

type hostedHello struct {
	Challenge       hostedChallenge `json:"challenge"`
	PairID          string          `json:"pairID"`
	Role            string          `json:"role"`
	BootstrapDigest string          `json:"bootstrapDigest"`
	ExpiresAt       int64           `json:"expiresAt"`
	Signature       string          `json:"signature"`
}

type hostedAccepted struct {
	Protocol        string `json:"protocol"`
	PairID          string `json:"pairID"`
	Role            string `json:"role"`
	BootID          string `json:"bootID"`
	BootstrapDigest string `json:"bootstrapDigest"`
}

func validateHostedImageConfig(cfg hostedImageConfig) error {
	if cfg.Protocol != hostedProtocol || !hostedUUIDValid(cfg.DeploymentID) ||
		!foundry.DigestValid(cfg.AgentConfigurationDigest) || !hostedTargetValid(cfg.Target) {
		return errHostedInvalid
	}
	if _, ok := hostedPublicKey(cfg.SigningPublicKey); !ok {
		return errHostedInvalid
	}
	return nil
}

func hostedSigningKeyValid(key ed25519.PrivateKey) bool {
	if len(key) != ed25519.PrivateKeySize || !hostedNonzeroBytes(key[:ed25519.SeedSize]) {
		return false
	}
	derived := ed25519.NewKeyFromSeed(key[:ed25519.SeedSize])
	defer clear(derived)
	return subtle.ConstantTimeCompare(key, derived) == 1
}

func signHostedHello(value hostedHello, key ed25519.PrivateKey) (hostedHello, error) {
	if !hostedSigningKeyValid(key) {
		return hostedHello{}, errHostedInvalid
	}
	data, err := hostedHelloSigningBytes(value)
	if err != nil {
		return hostedHello{}, err
	}
	value.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(key, data))
	return value, nil
}

// Verification authenticates one exact challenge and its bootstrap digest.
// The caller must separately consume the nonce and enforce pair/role ownership.
func verifyHostedHello(value hostedHello, expected hostedChallenge, publicKey string, now time.Time) error {
	if value.Challenge != expected {
		return errHostedInvalid
	}
	expires := time.Unix(value.ExpiresAt, 0)
	if !expires.After(now) || expires.After(now.Add(90*time.Second)) {
		return errHostedInvalid
	}
	key, keyOK := hostedPublicKey(publicKey)
	signature, signatureOK := hostedCanonicalBytes(value.Signature, ed25519.SignatureSize)
	data, err := hostedHelloSigningBytes(value)
	if !keyOK || !signatureOK || err != nil || !ed25519.Verify(key, data, signature) {
		return errHostedInvalid
	}
	return nil
}

func hostedHelloSigningBytes(value hostedHello) ([]byte, error) {
	c := value.Challenge
	if c.Protocol != hostedProtocol || !hostedUUIDValid(c.DeploymentID) ||
		!foundry.DigestValid(c.ConfigurationDigest) || foundry.ValidateAgentName(c.AgentName) != nil ||
		!hostedPositiveUint(c.AgentVersion) || !hostedUUIDValid(c.SessionID) || !hostedUUIDValid(c.BootID) ||
		!hostedUUIDValid(value.PairID) || (value.Role != "forward" && value.Role != "reverse") ||
		!foundry.DigestValid(value.BootstrapDigest) || value.ExpiresAt <= 0 {
		return nil, errHostedInvalid
	}
	if _, ok := hostedCanonicalBytes(c.Nonce, 32); !ok {
		return nil, errHostedInvalid
	}
	// Keep the field order stable. Neither input JSON order nor the signature
	// itself participates in this domain-separated signing representation.
	unsigned := struct {
		Challenge       hostedChallenge `json:"challenge"`
		PairID          string          `json:"pairID"`
		Role            string          `json:"role"`
		BootstrapDigest string          `json:"bootstrapDigest"`
		ExpiresAt       int64           `json:"expiresAt"`
	}{value.Challenge, value.PairID, value.Role, value.BootstrapDigest, value.ExpiresAt}
	data, err := json.Marshal(unsigned)
	if err != nil {
		return nil, errHostedInvalid
	}
	return append([]byte(hostedProtocol+"\x00hello\x00"), data...), nil
}

func validateHostedBootstrap(cfg hostedImageConfig, value hostedBootstrap) error {
	if validateHostedImageConfig(cfg) != nil || !hostedCredentialValid(value.ControllerToken) ||
		!hostedCredentialValid(value.CapabilitySecret) || !hostedCredentialValid(value.ProviderToken) {
		return errHostedInvalid
	}
	required := 0
	for key, value := range value.Environment {
		valid := false
		switch key {
		case "ORKA_ACP_PROVIDER":
			valid = value == "foundry"
		case "ORKA_ACP_MODEL":
			valid = utf8.ValidString(value) && foundry.SafeString(value, 512) && strings.TrimSpace(value) == value
		case "ORKA_ACP_FOUNDRY_ADAPTER_DIGEST", "ORKA_ACP_TOOL_POLICY_DIGEST",
			"ORKA_ACP_APPROVAL_POLICY_DIGEST", "ORKA_ACP_MCP_CONFIGURATION_DIGEST":
			valid = foundry.DigestValid(value)
		case "ORKA_ACP_AGENT_CONFIGURATION_DIGEST":
			valid = value == cfg.AgentConfigurationDigest
		case "ORKA_ACP_WORKSPACE_INTENT":
			valid = value == "read" || value == "write"
		case "ORKA_ACP_PROXY_CREDENTIAL_ROLE":
			valid = value == "operator-managed"
		case "ORKA_ACP_PROXY_CREDENTIAL_SCOPE":
			valid = value == "external-runtime"
		case "ORKA_ACP_RESOURCE_CLASS":
			valid = value == "external"
		case "ORKA_ACP_TRUST_NAMESPACE":
			valid = hostedDNSLabelValid(value)
		case "ORKA_ACP_CONTROLLER_EPOCH", "ORKA_ACP_RUNTIME_POOL_GENERATION":
			valid = hostedPositiveUint(value)
		case "ORKA_ACP_RUNTIME_POOL_UID":
			valid = hostedUUIDValid(value)
		case "ORKA_ACP_MODEL_CONTEXT_LIMIT", "ORKA_ACP_MODEL_OUTPUT_LIMIT":
			if !hostedPositiveUint(value) {
				return errHostedInvalid
			}
			continue
		default:
			return errHostedInvalid
		}
		if !valid {
			return errHostedInvalid
		}
		required++
	}
	_, contextLimit := value.Environment["ORKA_ACP_MODEL_CONTEXT_LIMIT"]
	_, outputLimit := value.Environment["ORKA_ACP_MODEL_OUTPUT_LIMIT"]
	if required != 15 || contextLimit != outputLimit {
		return errHostedInvalid
	}
	return nil
}

func hostedTargetValid(target foundry.HostedTarget) bool {
	if foundry.ValidateAgentName(target.AgentName) != nil || !hostedPositiveUint(target.AgentVersion) ||
		!foundry.SafeString(target.ProjectEndpoint, 2048) || strings.ContainsAny(target.ProjectEndpoint, "%#") {
		return false
	}
	u, err := url.Parse(target.ProjectEndpoint)
	if err != nil || u.Scheme != "https" || u.Opaque != "" || u.User != nil || u.Host != u.Hostname() ||
		u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.RawPath != "" || u.String() != target.ProjectEndpoint {
		return false
	}
	const suffix = ".services.ai.azure.com"
	if !strings.HasSuffix(u.Host, suffix) || !hostedDNSLabelValid(strings.TrimSuffix(u.Host, suffix)) {
		return false
	}
	const prefix = "/api/projects/"
	if !strings.HasPrefix(u.Path, prefix) {
		return false
	}
	project := strings.TrimPrefix(u.Path, prefix)
	if len(project) == 0 || len(project) > 128 || !hostedASCIIAlphanumeric(project[0]) ||
		!hostedASCIIAlphanumeric(project[len(project)-1]) {
		return false
	}
	for i := range len(project) {
		if !hostedASCIIAlphanumeric(project[i]) && project[i] != '-' && project[i] != '_' && project[i] != '.' {
			return false
		}
	}
	return true
}

func hostedUUIDValid(value string) bool {
	id, err := uuid.Parse(value)
	return err == nil && id != uuid.Nil && id.String() == value
}

func hostedPositiveUint(value string) bool {
	number, err := strconv.ParseUint(value, 10, 64)
	return err == nil && number > 0 && strconv.FormatUint(number, 10) == value
}

func hostedCanonicalBytes(value string, size int) ([]byte, bool) {
	if len(value) != base64.RawURLEncoding.EncodedLen(size) {
		return nil, false
	}
	decoded, err := base64.RawURLEncoding.Strict().DecodeString(value)
	if err != nil || len(decoded) != size || base64.RawURLEncoding.EncodeToString(decoded) != value ||
		!hostedNonzeroBytes(decoded) {
		return nil, false
	}
	return decoded, true
}

func hostedPublicKey(value string) (ed25519.PublicKey, bool) {
	decoded, ok := hostedCanonicalBytes(value, ed25519.PublicKeySize)
	if !ok {
		return nil, false
	}
	point, err := new(edwards25519.Point).SetBytes(decoded)
	if err != nil || subtle.ConstantTimeCompare(point.Bytes(), decoded) != 1 {
		return nil, false
	}
	// A small-order public key can verify a signature without a private key.
	if new(edwards25519.Point).MultByCofactor(point).Equal(edwards25519.NewIdentityPoint()) == 1 {
		return nil, false
	}
	return ed25519.PublicKey(decoded), true
}

func hostedNonzeroBytes(value []byte) bool {
	var combined byte
	for _, b := range value {
		combined |= b
	}
	return combined != 0
}

func hostedCredentialValid(value string) bool {
	if len(value) < 32 || !utf8.ValidString(value) || !foundry.SafeString(value, 16<<10) {
		return false
	}
	return strings.IndexFunc(value, unicode.IsSpace) < 0
}

func hostedDNSLabelValid(value string) bool {
	if len(value) == 0 || len(value) > 63 || value != strings.ToLower(value) ||
		!hostedASCIIAlphanumeric(value[0]) || !hostedASCIIAlphanumeric(value[len(value)-1]) {
		return false
	}
	for i := range len(value) {
		if !hostedASCIIAlphanumeric(value[i]) && value[i] != '-' {
			return false
		}
	}
	return true
}

func hostedASCIIAlphanumeric(value byte) bool {
	return value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' || value >= '0' && value <= '9'
}
