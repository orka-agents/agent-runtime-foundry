package broker

import (
	"encoding/json"
	"io"
	"net/http"
	"sort"

	"github.com/orka-agents/agent-runtime-foundry/internal/brokerapi"
	"github.com/orka-agents/agent-runtime-foundry/internal/foundry"
	"github.com/orka-agents/agent-runtime-foundry/internal/strictjson"
)

const brokerBootLimit = 4096

type brokerIdentityResponse struct {
	Protocol                 string `json:"protocol"`
	LedgerIdentityDigest     string `json:"ledgerIdentityDigest"`
	AgentConfigurationDigest string `json:"agentConfigurationDigest"`
}

type brokerRetireBootRequest struct {
	Protocol                 string      `json:"protocol"`
	LedgerIdentityDigest     string      `json:"ledgerIdentityDigest"`
	AgentConfigurationDigest string      `json:"agentConfigurationDigest"`
	RetiredFence             brokerOwner `json:"retiredFence"`
	OperationID              string      `json:"operationID"`
}

type brokerRetireBootResponse struct {
	Protocol                 string `json:"protocol"`
	LedgerIdentityDigest     string `json:"ledgerIdentityDigest"`
	AgentConfigurationDigest string `json:"agentConfigurationDigest"`
	RetiredFenceDigest       string `json:"retiredFenceDigest"`
	OperationID              string `json:"operationID"`
	ContextSHA256            string `json:"contextSHA256"`
	State                    string `json:"state"`
	Sealed                   bool   `json:"sealed"`
	SettlementProven         bool   `json:"settlementProven"`
	RetirementProven         bool   `json:"retirementProven"`
	OwnerCount               uint32 `json:"ownerCount"`
	OwnerSetDigest           string `json:"ownerSetDigest"`
	ActiveInvocations        uint32 `json:"activeInvocations"`
	AmbiguousInvocations     uint32 `json:"ambiguousInvocations"`
	PendingCreates           uint32 `json:"pendingCreates"`
	ProofDigest              string `json:"proofDigest"`
}

// Keep field order and JSON tags identical to Orka's canonical boot proof.
// Retry-specific operation and request digests bind the response separately.
func (r brokerRetireBootResponse) canonicalProofDigest() string {
	return foundry.JSONDigest(struct {
		Protocol                 string `json:"protocol"`
		LedgerIdentityDigest     string `json:"ledgerIdentityDigest"`
		AgentConfigurationDigest string `json:"agentConfigurationDigest"`
		RetiredFenceDigest       string `json:"retiredFenceDigest"`
		State                    string `json:"state"`
		Sealed                   bool   `json:"sealed"`
		SettlementProven         bool   `json:"settlementProven"`
		RetirementProven         bool   `json:"retirementProven"`
		OwnerCount               uint32 `json:"ownerCount"`
		OwnerSetDigest           string `json:"ownerSetDigest"`
		ActiveInvocations        uint32 `json:"activeInvocations"`
		AmbiguousInvocations     uint32 `json:"ambiguousInvocations"`
		PendingCreates           uint32 `json:"pendingCreates"`
	}{r.Protocol, r.LedgerIdentityDigest, r.AgentConfigurationDigest, r.RetiredFenceDigest, r.State, r.Sealed,
		r.SettlementProven, r.RetirementProven, r.OwnerCount, r.OwnerSetDigest, r.ActiveInvocations, r.AmbiguousInvocations, r.PendingCreates})
}

// The owner set is frozen with the seal before any cancellation or remote
// cleanup. No operation can subsequently create an owner under this boot.
type brokerBootSeal struct {
	RetiredFence   brokerOwner `json:"retiredFence"`
	OwnerCount     uint32      `json:"ownerCount"`
	OwnerSetDigest string      `json:"ownerSetDigest"`
}

func (o brokerOwner) bootFence() brokerOwner {
	o.RuntimeSessionUID, o.RuntimeSessionGeneration = "", 0
	return o
}

func (o brokerOwner) validBootFence() bool {
	return o.RuntimeSessionUID == "" && o.RuntimeSessionGeneration == 0 &&
		foundry.SafeString(o.RuntimeInstanceID, 512) && foundry.SafeString(o.SupervisorBootID, 512) &&
		o.ControllerEpoch > 0 && foundry.SafeString(o.RuntimePoolUID, 512) && o.RuntimePoolGeneration > 0 &&
		foundry.DigestValid(o.RuntimeProfileDigest) && o.ProfileDigestSchemaVersion == 1
}

func brokerBootOwners(ledger *brokerLedger) map[string][]string {
	boots := make(map[string][]string)
	for key, session := range ledger.Sessions {
		bootKey := foundry.JSONDigest(session.Owner.bootFence())
		boots[bootKey] = append(boots[bootKey], key)
	}
	for key := range boots {
		sort.Strings(boots[key])
	}
	return boots
}

func brokerBootCount(ledger *brokerLedger, owners map[string][]string) int {
	count := len(owners)
	for key := range ledger.SealedBoots {
		if _, exists := owners[key]; !exists {
			count++
		}
	}
	return count
}

func brokerOwnerSetDigest(owners []string) string {
	if owners == nil {
		owners = []string{}
	}
	return foundry.JSONDigest(owners)
}

func brokerBootSealsValid(ledger *brokerLedger) bool {
	if ledger.LedgerIdentityDigest == "" {
		return len(ledger.SealedBoots) == 0
	}
	if !foundry.DigestValid(ledger.LedgerIdentityDigest) || len(ledger.SealedBoots) > brokerBootLimit {
		return false
	}
	boots := brokerBootOwners(ledger)
	if brokerBootCount(ledger, boots) > brokerBootLimit {
		return false
	}
	for key, seal := range ledger.SealedBoots {
		if seal == nil || !seal.RetiredFence.validBootFence() || key != foundry.JSONDigest(seal.RetiredFence) ||
			seal.OwnerCount != uint32(len(boots[key])) || seal.OwnerSetDigest != brokerOwnerSetDigest(boots[key]) {
			return false
		}
		for _, owner := range boots[key] {
			if !ledger.Sessions[owner].Retiring {
				return false
			}
		}
	}
	return true
}

func (b *lifecycleBroker) serveBootRecovery(w http.ResponseWriter, r *http.Request) {
	identity := r.URL.Path == brokerapi.IdentityPath
	if r.URL.RawQuery != "" || r.URL.RawPath != "" || r.Header.Get("Content-Encoding") != "" ||
		len(r.Header.Values(brokerContextHeader)) != 0 ||
		(identity && r.Method != http.MethodGet) || (!identity && r.Method != http.MethodPost) {
		brokerWriteError(w, errBrokerInvalid)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, brokerMaxContext+1))
	if err != nil || len(body) > brokerMaxContext || (identity && len(body) != 0) {
		brokerWriteError(w, errBrokerInvalid)
		return
	}
	if identity {
		b.serveBrokerIdentity(w)
		return
	}
	var request brokerRetireBootRequest
	if strictjson.Decode(body, &request, true) != nil || request.Protocol != brokerProtocol ||
		!foundry.DigestValid(request.LedgerIdentityDigest) || request.AgentConfigurationDigest != b.cfg.configDigest ||
		!request.RetiredFence.validBootFence() || !foundry.SafeString(request.OperationID, 512) {
		brokerWriteError(w, errBrokerInvalid)
		return
	}
	owners, err := b.sealBoot(request)
	if err != nil {
		brokerWriteError(w, err)
		return
	}
	for _, owner := range owners {
		b.startReconcile(owner, true)
	}
	b.mu.Lock()
	response, err := b.retireBootResponseLocked(request, foundry.Digest(body))
	b.mu.Unlock()
	if err != nil {
		brokerWriteError(w, err)
		return
	}
	status := http.StatusConflict
	if response.RetirementProven {
		status = http.StatusOK
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(response)
}

func (b *lifecycleBroker) serveBrokerIdentity(w http.ResponseWriter) {
	b.mu.Lock()
	response := brokerIdentityResponse{brokerProtocol, b.ledger.LedgerIdentityDigest, b.ledger.ConfigDigest}
	healthy := b.storageError == nil && b.ctx.Err() == nil && foundry.DigestValid(response.LedgerIdentityDigest)
	b.mu.Unlock()
	if !healthy {
		brokerWriteError(w, errBrokerStorage)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

func (b *lifecycleBroker) sealBoot(request brokerRetireBootRequest) ([]string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.storageError != nil {
		return nil, b.storageError
	}
	if b.ctx.Err() != nil {
		return nil, errBrokerClosed
	}
	if request.LedgerIdentityDigest != b.ledger.LedgerIdentityDigest {
		return nil, errBrokerConflict
	}
	key := foundry.JSONDigest(request.RetiredFence)
	boots := brokerBootOwners(b.ledger)
	owners := boots[key]
	if b.ledger.SealedBoots[key] == nil {
		if len(owners) == 0 && brokerBootCount(b.ledger, boots) >= brokerBootLimit {
			return nil, errBrokerCapacity
		}
		err := b.commitCapacityLocked(len(owners) == 0, func(next *brokerLedger) error {
			if next.SealedBoots == nil {
				next.SealedBoots = make(map[string]*brokerBootSeal)
			}
			next.SealedBoots[key] = &brokerBootSeal{request.RetiredFence, uint32(len(owners)), brokerOwnerSetDigest(owners)}
			for _, owner := range owners {
				session := next.Sessions[owner]
				session.Retiring = true
				for _, prompt := range session.Prompts {
					prompt.Closing = true
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	for _, owner := range owners {
		if active, ok := b.active[owner]; ok {
			active.cancel()
		}
	}
	return owners, nil
}

func (b *lifecycleBroker) retireBootResponseLocked(request brokerRetireBootRequest, contextDigest string) (brokerRetireBootResponse, error) {
	if b.storageError != nil || b.ctx.Err() != nil {
		return brokerRetireBootResponse{}, errBrokerStorage
	}
	key := foundry.JSONDigest(request.RetiredFence)
	seal := b.ledger.SealedBoots[key]
	owners := brokerBootOwners(b.ledger)[key]
	if request.LedgerIdentityDigest != b.ledger.LedgerIdentityDigest || seal == nil ||
		seal.RetiredFence != request.RetiredFence || seal.OwnerCount != uint32(len(owners)) ||
		seal.OwnerSetDigest != brokerOwnerSetDigest(owners) {
		return brokerRetireBootResponse{}, errBrokerConflict
	}
	response := brokerRetireBootResponse{Protocol: brokerProtocol, LedgerIdentityDigest: b.ledger.LedgerIdentityDigest,
		AgentConfigurationDigest: b.ledger.ConfigDigest, RetiredFenceDigest: key, OperationID: request.OperationID,
		ContextSHA256: contextDigest, State: "retiring", Sealed: true, OwnerCount: seal.OwnerCount, OwnerSetDigest: seal.OwnerSetDigest}
	allRetired := true
	for _, owner := range owners {
		session := b.ledger.Sessions[owner]
		status := b.controlResponseLocked(brokerContext{Owner: session.Owner}, "")
		response.ActiveInvocations += status.ActiveInvocations
		if _, active := b.active[owner]; active && status.ActiveInvocations == 0 {
			response.ActiveInvocations++
		}
		response.AmbiguousInvocations += status.AmbiguousInvocations
		if status.CreatePending {
			response.PendingCreates++
		}
		if !status.RetirementProven || !status.SettlementProven || !foundry.DigestValid(status.ProofDigest) {
			allRetired = false
		}
	}
	if response.PendingCreates > 0 || response.AmbiguousInvocations > 0 {
		response.State = "blocked"
	}
	if allRetired && response.ActiveInvocations == 0 && response.AmbiguousInvocations == 0 && response.PendingCreates == 0 {
		response.State, response.RetirementProven, response.SettlementProven = "retired", true, true
		response.ProofDigest = response.canonicalProofDigest()
	}
	return response, nil
}
