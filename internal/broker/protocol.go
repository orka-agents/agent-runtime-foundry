package broker

import (
	"encoding/base64"
	"errors"
	"net/http"
	"time"

	"github.com/orka-agents/agent-runtime-foundry/internal/brokerapi"
	"github.com/orka-agents/agent-runtime-foundry/internal/foundry"
	"github.com/orka-agents/agent-runtime-foundry/internal/strictjson"
)

const (
	brokerProtocol      = "orka.foundry.broker.v1"
	brokerContextHeader = "X-Orka-Foundry-Context"
	brokerMaxContext    = 16 << 10
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

func (o brokerOwner) valid() bool {
	return o.bootFence().validBootFence() && foundry.SafeString(o.RuntimeSessionUID, 512) && o.RuntimeSessionGeneration > 0
}

func (c brokerContext) promptKey() string {
	return foundry.JSONDigest(struct {
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
	if strictjson.Decode(raw, &c, true) != nil || c.Protocol != brokerProtocol || !c.Owner.valid() ||
		c.AgentConfigurationDigest != configDigest || !foundry.DigestValid(configDigest) ||
		!foundry.SafeString(c.OperationID, 512) || c.BodySHA256 != foundry.Digest(body) {
		return brokerContext{}, "", errBrokerInvalid
	}
	needsPrompt := r.URL.Path != brokerapi.RetirePath && r.URL.Path != brokerapi.StatusPath
	if needsPrompt {
		expiry, err := time.Parse(time.RFC3339Nano, c.LeaseExpiresAt)
		if !foundry.SafeString(c.TaskUID, 512) || c.TaskAttempt == 0 || !foundry.SafeString(c.PromptID, 512) ||
			!foundry.DigestValid(c.PromptRequestDigest) || c.LeaseGeneration == 0 || err != nil {
			return brokerContext{}, "", errBrokerInvalid
		}
		if (r.URL.Path == brokerapi.ResponsesPath || r.URL.Path == brokerapi.RenewPath) &&
			(!expiry.After(now) || expiry.After(now.Add(5*time.Minute))) {
			return brokerContext{}, "", errBrokerClosed
		}
	} else if c.TaskUID != "" || c.TaskAttempt != 0 || c.PromptID != "" || c.PromptRequestDigest != "" ||
		c.LeaseGeneration != 0 || c.LeaseExpiresAt != "" {
		return brokerContext{}, "", errBrokerInvalid
	}
	if (r.URL.Path == brokerapi.ResponsesPath) != (c.InvocationSequence > 0) {
		return brokerContext{}, "", errBrokerInvalid
	}
	return c, foundry.Digest(raw), nil
}

func brokerOperationDigest(path string, c brokerContext) string {
	return foundry.JSONDigest(struct {
		Path    string        `json:"path"`
		Context brokerContext `json:"context"`
	}{path, c})
}
