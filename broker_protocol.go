package main

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"
)

const (
	brokerProtocol      = "orka.foundry.broker.v1"
	brokerContextHeader = "X-Orka-Foundry-Context"
	brokerMaxContext    = 16 << 10
	brokerResponsesPath = "/v1/responses"
	brokerRenewPath     = "/internal/v1/renew"
	brokerSettlePath    = "/internal/v1/settle"
	brokerRetirePath    = "/internal/v1/retire"
	brokerStatusPath    = "/internal/v1/status"
)

var (
	errBrokerInvalid   = errors.New("invalid Foundry broker request")
	errBrokerConflict  = errors.New("Foundry broker operation conflicts with durable ownership")
	errBrokerClosed    = errors.New("Foundry broker authority is closed")
	errBrokerPending   = errors.New("Foundry broker remote settlement is pending")
	errBrokerStorage   = errors.New("Foundry broker durable state is unavailable")
	errBrokerRemote    = errors.New("Foundry broker remote operation failed")
	errBrokerAmbiguous = errors.New("Foundry broker remote acceptance is unknown")
)

// Keep this field order and tags identical to Orka's v2.Fence. Both sides hash
// json.Marshal(Fence), rather than relying on untrusted JSON member order.
type brokerOwner struct {
	RuntimeInstanceID          string `json:"runtimeInstanceID"`
	SupervisorBootID           string `json:"supervisorBootID"`
	ControllerEpoch            uint64 `json:"controllerEpoch"`
	RuntimePoolUID             string `json:"runtimePoolUID"`
	RuntimePoolGeneration      uint64 `json:"runtimePoolGeneration"`
	RuntimeSessionUID          string `json:"runtimeSessionUID,omitempty"`
	RuntimeSessionGeneration   uint64 `json:"runtimeSessionGeneration,omitempty"`
	RuntimeProfileDigest       string `json:"runtimeProfileDigest"`
	ProfileDigestSchemaVersion uint32 `json:"profileDigestSchemaVersion"`
}

type brokerContext struct {
	Protocol                 string      `json:"protocol"`
	Owner                    brokerOwner `json:"owner"`
	AgentConfigurationDigest string      `json:"agentConfigurationDigest"`
	TaskUID                  string      `json:"taskUID,omitempty"`
	TaskAttempt              uint32      `json:"taskAttempt,omitempty"`
	PromptID                 string      `json:"promptID,omitempty"`
	PromptRequestDigest      string      `json:"promptRequestDigest,omitempty"`
	LeaseGeneration          uint64      `json:"leaseGeneration,omitempty"`
	LeaseExpiresAt           string      `json:"leaseExpiresAt,omitempty"`
	OperationID              string      `json:"operationID"`
	InvocationSequence       uint64      `json:"invocationSequence,omitempty"`
	BodySHA256               string      `json:"bodySHA256"`
}

type brokerControlResponse struct {
	Protocol             string `json:"protocol"`
	OwnerDigest          string `json:"ownerDigest"`
	OperationID          string `json:"operationID"`
	ContextSHA256        string `json:"contextSHA256"`
	State                string `json:"state"`
	SettlementProven     bool   `json:"settlementProven"`
	RetirementProven     bool   `json:"retirementProven"`
	ActiveInvocations    uint32 `json:"activeInvocations"`
	AmbiguousInvocations uint32 `json:"ambiguousInvocations"`
	CreatePending        bool   `json:"createPending"`
	RemoteSessionCreated bool   `json:"remoteSessionCreated"`
	LeaseGeneration      uint64 `json:"leaseGeneration"`
	LeaseExpiresAt       string `json:"leaseExpiresAt"`
	ProofDigest          string `json:"proofDigest"`
}

func brokerSHA(data []byte) string {
	digest := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func brokerJSONDigest(value any) string {
	data, _ := json.Marshal(value) // Only concrete, JSON-safe broker structs are used.
	return brokerSHA(data)
}

func brokerDigestValid(value string) bool {
	if len(value) != len("sha256:")+64 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	for _, char := range value[len("sha256:"):] {
		if !(char >= '0' && char <= '9') && !(char >= 'a' && char <= 'f') {
			return false
		}
	}
	return true
}

func (o brokerOwner) valid() bool {
	return acpSafeString(o.RuntimeInstanceID, 512) && acpSafeString(o.SupervisorBootID, 512) &&
		o.ControllerEpoch > 0 && acpSafeString(o.RuntimePoolUID, 512) && o.RuntimePoolGeneration > 0 &&
		acpSafeString(o.RuntimeSessionUID, 512) && o.RuntimeSessionGeneration > 0 &&
		brokerDigestValid(o.RuntimeProfileDigest) && o.ProfileDigestSchemaVersion == 1
}

func (c brokerContext) promptKey() string {
	return brokerJSONDigest(struct {
		TaskUID             string `json:"taskUID"`
		TaskAttempt         uint32 `json:"taskAttempt"`
		PromptID            string `json:"promptID"`
		PromptRequestDigest string `json:"promptRequestDigest"`
	}{c.TaskUID, c.TaskAttempt, c.PromptID, c.PromptRequestDigest})
}

func brokerParseContext(r *http.Request, body []byte, configDigest string, now time.Time) (brokerContext, string, error) {
	values := r.Header.Values(brokerContextHeader)
	if len(values) != 1 || len(values[0]) == 0 || len(values[0]) > brokerMaxContext {
		return brokerContext{}, "", errBrokerInvalid
	}
	raw, err := base64.RawURLEncoding.Strict().DecodeString(values[0])
	if err != nil || base64.RawURLEncoding.EncodeToString(raw) != values[0] {
		return brokerContext{}, "", errBrokerInvalid
	}
	var c brokerContext
	if acpDecode(raw, &c, true) != nil || c.Protocol != brokerProtocol || !c.Owner.valid() ||
		c.AgentConfigurationDigest != configDigest || !brokerDigestValid(configDigest) ||
		!acpSafeString(c.OperationID, 512) || c.BodySHA256 != brokerSHA(body) {
		return brokerContext{}, "", errBrokerInvalid
	}
	needsPrompt := r.URL.Path != brokerRetirePath && r.URL.Path != brokerStatusPath
	if needsPrompt {
		expiry, err := time.Parse(time.RFC3339Nano, c.LeaseExpiresAt)
		if !acpSafeString(c.TaskUID, 512) || c.TaskAttempt == 0 || !acpSafeString(c.PromptID, 512) ||
			!brokerDigestValid(c.PromptRequestDigest) || c.LeaseGeneration == 0 || err != nil {
			return brokerContext{}, "", errBrokerInvalid
		}
		if (r.URL.Path == brokerResponsesPath || r.URL.Path == brokerRenewPath) &&
			(!expiry.After(now) || expiry.After(now.Add(5*time.Minute))) {
			return brokerContext{}, "", errBrokerClosed
		}
	} else if c.TaskUID != "" || c.TaskAttempt != 0 || c.PromptID != "" || c.PromptRequestDigest != "" ||
		c.LeaseGeneration != 0 || c.LeaseExpiresAt != "" {
		return brokerContext{}, "", errBrokerInvalid
	}
	if (r.URL.Path == brokerResponsesPath) != (c.InvocationSequence > 0) {
		return brokerContext{}, "", errBrokerInvalid
	}
	return c, brokerSHA(raw), nil
}

func brokerOperationDigest(path string, c brokerContext) string {
	return brokerJSONDigest(struct {
		Path    string        `json:"path"`
		Context brokerContext `json:"context"`
	}{path, c})
}
