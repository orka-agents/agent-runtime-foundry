package main

import (
	"strings"
	"time"

	"github.com/google/uuid"
)

// Recovery never interprets an unknown state or a broken response reference as
// quiescence. Content is deliberately absent; these are ownership records only.
func brokerLedgerValid(ledger *brokerLedger, digest string) bool {
	if ledger.Version != 1 || ledger.ConfigDigest != digest || ledger.Sessions == nil || len(ledger.Sessions) > 4096 ||
		(ledger.PrincipalDigest != "" && !brokerDigestValid(ledger.PrincipalDigest)) {
		return false
	}
	for key, session := range ledger.Sessions {
		if !brokerSessionValid(session, key, digest) || (session.RemoteID != "" && ledger.PrincipalDigest == "") {
			return false
		}
	}
	return true
}

func brokerSessionValid(session *brokerSession, key, digest string) bool {
	if session == nil || !session.Owner.valid() || key != brokerJSONDigest(session.Owner) ||
		session.Prompts == nil || session.Responses == nil || session.Operations == nil ||
		len(session.Prompts) > 4096 || len(session.Operations) > brokerRetirementOperationLimit {
		return false
	}
	switch session.CreateState {
	case "none":
		if session.RemoteID != "" || len(session.Responses) != 0 {
			return false
		}
	case "intent", "known", "deleted":
		id, err := uuid.Parse(session.RemoteID)
		if err != nil || id.String() != session.RemoteID {
			return false
		}
	default:
		return false
	}
	if session.Retired {
		if !session.Retiring || (session.CreateState != "none" && session.CreateState != "deleted") || !brokerDigestValid(session.ProofDigest) {
			return false
		}
	} else if session.CreateState == "deleted" || session.ProofDigest != "" {
		return false
	}
	if (len(session.Prompts) == 0) != (session.CurrentPrompt == "") ||
		(session.CurrentPrompt != "" && session.Prompts[session.CurrentPrompt] == nil) {
		return false
	}
	for id, operation := range session.Operations {
		if !acpSafeString(id, 512) || !brokerDigestValid(operation) {
			return false
		}
	}
	linked := make(map[string]bool, len(session.Responses))
	for promptKey, prompt := range session.Prompts {
		if !brokerPromptValid(prompt, promptKey, digest, session, linked) {
			return false
		}
	}
	return len(linked) == len(session.Responses)
}

func brokerPromptValid(prompt *brokerPrompt, key, digest string, session *brokerSession, linked map[string]bool) bool {
	if prompt == nil || prompt.Identity.promptKey() != key || prompt.Identity.Owner != session.Owner ||
		prompt.Identity.Protocol != brokerProtocol || prompt.Identity.AgentConfigurationDigest != digest ||
		!acpSafeString(prompt.Identity.TaskUID, 512) || prompt.Identity.TaskAttempt == 0 ||
		!acpSafeString(prompt.Identity.PromptID, 512) || !brokerDigestValid(prompt.Identity.PromptRequestDigest) ||
		!brokerDigestValid(prompt.Identity.BodySHA256) || session.Operations[prompt.Identity.OperationID] == "" ||
		prompt.Invocations == nil || prompt.Identity.LeaseGeneration == 0 || prompt.LeaseGeneration < prompt.Identity.LeaseGeneration {
		return false
	}
	expiry, err := time.Parse(time.RFC3339Nano, prompt.Identity.LeaseExpiresAt)
	if err != nil || prompt.LeaseExpiresAt.Before(expiry) || prompt.LeaseExpiresAt.IsZero() {
		return false
	}
	if prompt.Settled {
		if !prompt.Closing || !brokerDigestValid(prompt.ProofDigest) {
			return false
		}
	} else if prompt.ProofDigest != "" || session.Retired || key != session.CurrentPrompt {
		return false
	}
	if session.Retiring && !prompt.Closing {
		return false
	}
	var last, lastCompleted uint64
	lastAlias := ""
	for sequence, invocation := range prompt.Invocations {
		if !brokerInvocationValid(invocation, sequence, key, prompt, session, linked) {
			return false
		}
		if sequence > last {
			last = sequence
		}
		if invocation.State == "completed" && sequence > lastCompleted {
			lastCompleted, lastAlias = sequence, invocation.ResponseAlias
		}
	}
	return prompt.LastSequence == last && prompt.LastAlias == lastAlias
}

func brokerInvocationValid(invocation *brokerInvocation, sequence uint64, promptKey string, prompt *brokerPrompt, session *brokerSession, linked map[string]bool) bool {
	if invocation == nil || invocation.Sequence != sequence || sequence == 0 || !brokerDigestValid(invocation.BodyDigest) ||
		!acpSafeString(invocation.OperationID, 512) || session.Operations[invocation.OperationID] == "" {
		return false
	}
	switch invocation.State {
	case "reserved", "intent", "rejected", "uncertain":
		if invocation.ResponseID != "" || invocation.ResponseAlias != "" || prompt.Settled {
			return false
		}
	case "accepted", "completed":
		if invocation.ResponseID == "" || (prompt.Settled && invocation.State != "completed") {
			return false
		}
	case "settled":
		if !prompt.Settled {
			return false
		}
	default:
		return false
	}
	if invocation.ResponseID == "" {
		return invocation.ResponseAlias == ""
	}
	link, ok := session.Responses[invocation.ResponseAlias]
	if !ok || linked[invocation.ResponseAlias] || !brokerAliasValid(invocation.ResponseAlias, "fr_") ||
		validateProviderIdentifier("response", link.RemoteID) != nil || link.RemoteID != invocation.ResponseID ||
		link.PromptKey != promptKey || link.Completed != (invocation.State == "completed") ||
		link.HasFunctions != (len(link.CallIDs) > 0) || (!link.Completed && len(link.CallIDs) != 0) {
		return false
	}
	for alias, remote := range link.CallIDs {
		if !brokerAliasValid(alias, "fc_") || validateProviderIdentifier("call", remote) != nil {
			return false
		}
	}
	linked[invocation.ResponseAlias] = true
	return true
}

func brokerAliasValid(value, prefix string) bool {
	if !strings.HasPrefix(value, prefix) {
		return false
	}
	id, err := uuid.Parse(strings.TrimPrefix(value, prefix))
	return err == nil && prefix+id.String() == value
}
