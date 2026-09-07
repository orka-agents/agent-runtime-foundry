package main

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync"
	"time"
)

// Preserve cleanup receipts even for owners already at the ordinary operation
// limit. Settlement cannot consume the final slot reserved for retirement.
const (
	brokerOperationLimit           = 16384
	brokerSettlementOperationLimit = brokerOperationLimit + 1
	brokerRetirementOperationLimit = brokerSettlementOperationLimit + 1
)

type brokerActive struct {
	prompt string
	cancel context.CancelFunc
}

type lifecycleBroker struct {
	cfg           brokerConfiguration
	tokenProvider foundryTokenProvider
	httpClient    *http.Client
	store         *brokerStore
	mu            sync.Mutex
	ledger        *brokerLedger
	storageError  error
	active        map[string]brokerActive
	workers       map[string]bool
	nextReconcile map[string]time.Time
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
}

func newLifecycleBroker(ctx context.Context, cfg brokerConfiguration, provider foundryTokenProvider, client *http.Client) (*lifecycleBroker, error) {
	if provider == nil || len(cfg.bearer) < 32 || !brokerDigestValid(cfg.configDigest) {
		return nil, errBrokerInvalid
	}
	store, ledger, err := openBrokerStore(cfg.stateDir, cfg.configDigest)
	if err != nil {
		return nil, err
	}
	if cfg.operationTimeout <= 0 {
		cfg.operationTimeout = 45 * time.Second
	}
	if client == nil {
		client = newBrokerHTTPClient()
	} else {
		copy := *client
		copy.CheckRedirect = func(*http.Request, []*http.Request) error { return errBrokerRemote }
		client = &copy
	}
	ctx, cancel := context.WithCancel(ctx)
	b := &lifecycleBroker{cfg: cfg, tokenProvider: provider, httpClient: client, store: store, ledger: ledger,
		active: map[string]brokerActive{}, workers: map[string]bool{}, nextReconcile: map[string]time.Time{}, ctx: ctx, cancel: cancel}
	// A restarted broker never resumes an old request. Recovery only closes its
	// exact durable authority and obtains provider cleanup evidence.
	err = b.commitLocked(func(next *brokerLedger) error {
		for _, session := range next.Sessions {
			for _, prompt := range session.Prompts {
				if !prompt.Settled {
					prompt.Closing = true
					// Keep intent bytes unchanged: a valid older ledger may
					// have no room for a longer state name. Without its original
					// active request, intent is already ambiguous ownership.
				}
			}
		}
		return nil
	})
	if err != nil {
		cancel()
		store.close()
		return nil, err
	}
	b.wg.Add(1)
	go b.sweep()
	return b, nil
}

func (b *lifecycleBroker) close() {
	b.cancel()
	b.mu.Lock()
	for _, active := range b.active {
		active.cancel()
	}
	b.mu.Unlock()
	b.wg.Wait()
	b.store.close()
}

func (b *lifecycleBroker) commitLocked(change func(*brokerLedger) error) error {
	return b.commitCapacityLocked(false, change)
}

// Reserved growth may spend already retained space for original acceptance and
// cleanup. Ordinary admissions, renewals and completion mappings must replenish
// every owner's reserve. Recovery of an older ledger is not rejected merely
// because it predates that admission rule.
func (b *lifecycleBroker) commitCapacityLocked(ordinary bool, change func(*brokerLedger) error) error {
	if b.storageError != nil {
		return b.storageError
	}
	data, err := json.Marshal(b.ledger)
	var next brokerLedger
	if err != nil || json.Unmarshal(data, &next) != nil {
		return b.failStorageLocked()
	}
	if err := change(&next); err != nil {
		return err
	}
	nextData, err := json.Marshal(&next)
	if err != nil {
		return b.failStorageLocked()
	}
	limit := brokerMaxLedgerBytes
	if ordinary && !bytes.Equal(data, nextData) {
		limit -= brokerLedgerReserveBytes(&next)
	}
	// A definite capacity refusal precedes all disk I/O. Keep the old ledger
	// and its healthy writer available for acceptance and cleanup.
	if len(nextData) > limit {
		return errBrokerCapacity
	}
	if err := b.store.saveBytes(nextData); err != nil {
		return b.failStorageLocked()
	}
	b.ledger = &next
	return nil
}

func (b *lifecycleBroker) failStorageLocked() error {
	b.storageError = errBrokerStorage
	// Preserve the last durable owner and stop live requests. Cancellation is
	// containment only; failed storage cannot publish settlement or retirement.
	// Creation and cleanup outlive prompt cancellation, but cannot outlive
	// the broker's ability to retain their acknowledgements.
	b.cancel()
	for _, active := range b.active {
		active.cancel()
	}
	return b.storageError
}

func brokerEnsureSession(next *brokerLedger, c brokerContext) (*brokerSession, error) {
	key := brokerJSONDigest(c.Owner)
	if session := next.Sessions[key]; session != nil {
		return session, nil
	}
	if len(next.Sessions) >= 4096 {
		return nil, errBrokerStorage
	}
	session := &brokerSession{Owner: c.Owner, CreateState: "none", Prompts: map[string]*brokerPrompt{},
		Responses: map[string]brokerResponseID{}, Operations: map[string]string{}}
	next.Sessions[key] = session
	return session, nil
}

func brokerEnsurePrompt(session *brokerSession, c brokerContext) (*brokerPrompt, error) {
	key := c.promptKey()
	if prompt := session.Prompts[key]; prompt != nil {
		return prompt, nil
	}
	if session.Retiring || session.Retired {
		return nil, errBrokerClosed
	}
	for _, prompt := range session.Prompts {
		if prompt.Identity.PromptID == c.PromptID || (prompt.Identity.TaskUID == c.TaskUID && prompt.Identity.TaskAttempt == c.TaskAttempt) {
			return nil, errBrokerConflict
		}
	}
	if current := session.Prompts[session.CurrentPrompt]; current != nil && !current.Settled {
		return nil, errBrokerConflict
	}
	if len(session.Prompts) >= 4096 {
		return nil, errBrokerStorage
	}
	expires, err := time.Parse(time.RFC3339Nano, c.LeaseExpiresAt)
	if err != nil {
		return nil, errBrokerInvalid
	}
	prompt := &brokerPrompt{Identity: c, LeaseGeneration: c.LeaseGeneration, LeaseExpiresAt: expires,
		Invocations: map[uint64]*brokerInvocation{}}
	session.Prompts[key] = prompt
	session.CurrentPrompt = key
	return prompt, nil
}

func brokerRecordOperation(session *brokerSession, path string, c brokerContext) (bool, error) {
	digest := brokerOperationDigest(path, c)
	if previous, ok := session.Operations[c.OperationID]; ok {
		if previous != digest {
			return true, errBrokerConflict
		}
		return true, nil
	}
	limit := brokerOperationLimit
	switch path {
	case brokerSettlePath:
		limit = brokerSettlementOperationLimit
	case brokerRetirePath:
		limit = brokerRetirementOperationLimit
	}
	if len(session.Operations) >= limit {
		return false, errBrokerStorage
	}
	session.Operations[c.OperationID] = digest
	return false, nil
}

func (b *lifecycleBroker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if r.URL.Path == "/healthz" && r.Method == http.MethodGet {
		b.mu.Lock()
		healthy := b.storageError == nil && b.ctx.Err() == nil
		b.mu.Unlock()
		if !healthy {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		return
	}
	values := r.Header.Values("Authorization")
	if len(values) != 1 || subtle.ConstantTimeCompare([]byte(values[0]), []byte("Bearer "+b.cfg.bearer)) != 1 {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	validPath := r.URL.Path == brokerResponsesPath || r.URL.Path == brokerRenewPath || r.URL.Path == brokerSettlePath ||
		r.URL.Path == brokerRetirePath || r.URL.Path == brokerStatusPath
	if !validPath || r.URL.RawQuery != "" || r.URL.RawPath != "" || r.Header.Get("Content-Encoding") != "" ||
		(r.URL.Path == brokerStatusPath && r.Method != http.MethodGet) || (r.URL.Path != brokerStatusPath && r.Method != http.MethodPost) {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxFoundryPromptBytes+1))
	if err != nil || len(body) > maxFoundryPromptBytes {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if (r.URL.Path == brokerStatusPath && len(body) != 0) ||
		(r.URL.Path != brokerResponsesPath && r.URL.Path != brokerStatusPath && string(body) != "{}") {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	c, contextDigest, err := brokerParseContext(r, body, b.cfg.configDigest, time.Now())
	if err != nil {
		brokerWriteError(w, err)
		return
	}
	if r.URL.Path == brokerResponsesPath {
		b.serveResponses(w, r, c, body)
		return
	}
	if r.URL.Path != brokerStatusPath {
		if err := b.control(r.URL.Path, c); err != nil {
			brokerWriteError(w, err)
			return
		}
	}
	if r.URL.Path == brokerSettlePath || r.URL.Path == brokerRetirePath {
		b.startReconcile(brokerJSONDigest(c.Owner), true)
	}
	b.mu.Lock()
	response := b.controlResponseLocked(c, contextDigest)
	if r.URL.Path == brokerRenewPath && b.canAcknowledgeCreateRenewalLocked(c, response) {
		response.State = "open"
	}
	storageErr := b.storageError
	b.mu.Unlock()
	if storageErr != nil {
		brokerWriteError(w, storageErr)
		return
	}
	status := http.StatusOK
	if (r.URL.Path == brokerSettlePath && !response.SettlementProven) || (r.URL.Path == brokerRetirePath && !response.RetirementProven) {
		status = http.StatusConflict
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(response)
}

func brokerWriteError(w http.ResponseWriter, err error) {
	status := http.StatusServiceUnavailable
	switch {
	case errors.Is(err, errBrokerInvalid):
		status = http.StatusBadRequest
	case errors.Is(err, errBrokerConflict), errors.Is(err, errBrokerPending), errors.Is(err, errBrokerAmbiguous):
		status = http.StatusConflict
	case errors.Is(err, errBrokerClosed):
		status = http.StatusGone
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(`{"error":"foundry_broker_request_failed"}`))
}

func (b *lifecycleBroker) control(path string, c brokerContext) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	session := b.ledger.Sessions[brokerJSONDigest(c.Owner)]
	ordinary := path == brokerRenewPath || session == nil ||
		(path == brokerSettlePath && session.Prompts[c.promptKey()] == nil)
	err := b.commitCapacityLocked(ordinary, func(next *brokerLedger) error {
		session, err := brokerEnsureSession(next, c)
		if err != nil {
			return err
		}
		duplicate, err := brokerRecordOperation(session, path, c)
		if err != nil {
			return err
		}
		if path == brokerRetirePath {
			session.Retiring = true
			for _, prompt := range session.Prompts {
				prompt.Closing = true
			}
			return nil
		}
		if session.Retiring || session.Retired {
			if path == brokerRenewPath {
				return errBrokerClosed
			}
		}
		prompt, err := brokerEnsurePrompt(session, c)
		if err != nil {
			return err
		}
		if path == brokerSettlePath {
			prompt.Closing = true
			return nil
		}
		if duplicate {
			return nil
		}
		if prompt.Closing || prompt.Settled {
			return errBrokerClosed
		}
		expires, _ := time.Parse(time.RFC3339Nano, c.LeaseExpiresAt)
		// A renewal may establish an owner before inference. Its generation is
		// already authoritative and need not start at one.
		if prompt.Identity.OperationID == c.OperationID && prompt.LastSequence == 0 && prompt.LeaseGeneration == c.LeaseGeneration {
			return nil
		}
		if c.LeaseGeneration != prompt.LeaseGeneration+1 || !prompt.LeaseExpiresAt.After(time.Now()) || expires.Before(prompt.LeaseExpiresAt) {
			return errBrokerConflict
		}
		prompt.LeaseGeneration, prompt.LeaseExpiresAt = c.LeaseGeneration, expires
		return nil
	})
	if err == nil && (path == brokerSettlePath || path == brokerRetirePath) {
		if active, ok := b.active[brokerJSONDigest(c.Owner)]; ok && (path == brokerRetirePath || active.prompt == c.promptKey()) {
			active.cancel()
		}
	}
	return err
}

func (b *lifecycleBroker) controlResponseLocked(c brokerContext, contextDigest string) brokerControlResponse {
	response := brokerControlResponse{Protocol: brokerProtocol, OwnerDigest: brokerJSONDigest(c.Owner), OperationID: c.OperationID,
		ContextSHA256: contextDigest, State: "open"}
	session := b.ledger.Sessions[response.OwnerDigest]
	if session == nil {
		return response
	}
	response.CreatePending = session.CreateState == "intent"
	response.RemoteSessionCreated = session.CreateState == "known" || session.CreateState == "deleted"
	response.RetirementProven = session.Retired
	if session.Retiring {
		response.State = "retiring"
	}
	if session.Retired {
		response.State = "retired"
		response.SettlementProven = true
		response.ProofDigest = session.ProofDigest
	}
	for key, prompt := range session.Prompts {
		if c.PromptID != "" && key != c.promptKey() {
			continue
		}
		if c.PromptID != "" {
			response.LeaseGeneration = prompt.LeaseGeneration
			response.LeaseExpiresAt = prompt.LeaseExpiresAt.UTC().Format(time.RFC3339Nano)
			response.SettlementProven = prompt.Settled
			if prompt.Closing {
				response.State = "settling"
			}
			if prompt.Settled {
				response.State = "settled"
				response.ProofDigest = prompt.ProofDigest
			}
		}
		for _, invocation := range prompt.Invocations {
			switch invocation.State {
			case "uncertain":
				response.AmbiguousInvocations++
			case "intent":
				active, running := b.active[response.OwnerDigest]
				if running && active.prompt == key && invocation.Sequence == prompt.LastSequence {
					response.ActiveInvocations++
				} else {
					response.AmbiguousInvocations++
				}
			case "accepted", "reserved":
				if !prompt.Settled {
					response.ActiveInvocations++
				}
			}
		}
	}
	if response.CreatePending || response.AmbiguousInvocations > 0 {
		response.State = "blocked"
		response.SettlementProven = false
		response.RetirementProven = false
		response.ProofDigest = ""
	}
	return response
}

// A live original create may outlast a prompt's first lease. Acknowledge only
// the exact renewed lease while that request still owns the reserved invocation.
// Creation remains pending, and status, settlement and retirement remain blocked.
func (b *lifecycleBroker) canAcknowledgeCreateRenewalLocked(c brokerContext, response brokerControlResponse) bool {
	if b.ctx.Err() != nil || b.storageError != nil || response.State != "blocked" || !response.CreatePending ||
		response.AmbiguousInvocations != 0 || response.ActiveInvocations != 1 || response.LeaseGeneration != c.LeaseGeneration {
		return false
	}
	key, promptKey := brokerJSONDigest(c.Owner), c.promptKey()
	session := b.ledger.Sessions[key]
	if session == nil || session.CreateState != "intent" || session.Retiring || session.Retired || session.CurrentPrompt != promptKey {
		return false
	}
	prompt := session.Prompts[promptKey]
	expires, err := time.Parse(time.RFC3339Nano, c.LeaseExpiresAt)
	if err != nil || prompt == nil || prompt.Closing || prompt.Settled || prompt.LeaseGeneration != c.LeaseGeneration ||
		!prompt.LeaseExpiresAt.Equal(expires) || !prompt.LeaseExpiresAt.After(time.Now()) {
		return false
	}
	active, ok := b.active[key]
	if !ok || active.prompt != promptKey || len(prompt.Invocations) != 1 {
		return false
	}
	invocation := prompt.Invocations[prompt.LastSequence]
	return invocation != nil && invocation.State == "reserved"
}

func (b *lifecycleBroker) sweep() {
	defer b.wg.Done()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-b.ctx.Done():
			return
		case <-ticker.C:
		}
		b.mu.Lock()
		needsExpiry := false
		for _, session := range b.ledger.Sessions {
			for _, prompt := range session.Prompts {
				if !prompt.Closing && !prompt.LeaseExpiresAt.After(time.Now()) {
					needsExpiry = true
				}
			}
		}
		if needsExpiry {
			_ = b.commitLocked(func(next *brokerLedger) error {
				for _, session := range next.Sessions {
					for _, prompt := range session.Prompts {
						if !prompt.LeaseExpiresAt.After(time.Now()) {
							prompt.Closing = true
						}
					}
				}
				return nil
			})
		}
		keys := make([]string, 0, len(b.ledger.Sessions))
		for key, session := range b.ledger.Sessions {
			needsCleanup := session.Retiring && !session.Retired
			for promptKey, prompt := range session.Prompts {
				if prompt.Closing && !prompt.Settled {
					needsCleanup = true
					if active, ok := b.active[key]; ok && active.prompt == promptKey {
						active.cancel()
					}
				}
			}
			if needsCleanup {
				keys = append(keys, key)
			}
		}
		b.mu.Unlock()
		for _, key := range keys {
			b.startReconcile(key, false)
		}
	}
}

func (b *lifecycleBroker) startReconcile(key string, force bool) {
	b.mu.Lock()
	if b.ctx.Err() != nil || b.storageError != nil || b.workers[key] || (!force && time.Now().Before(b.nextReconcile[key])) {
		b.mu.Unlock()
		return
	}
	b.workers[key] = true
	b.wg.Add(1)
	b.mu.Unlock()
	go func() {
		defer b.wg.Done()
		ctx, cancel := context.WithTimeout(b.ctx, b.cfg.operationTimeout)
		defer cancel()
		b.reconcile(ctx, key)
		b.mu.Lock()
		delete(b.workers, key)
		b.nextReconcile[key] = time.Now().Add(time.Second)
		b.mu.Unlock()
	}()
}

func (b *lifecycleBroker) reconcile(ctx context.Context, key string) {
	b.mu.Lock()
	session := b.ledger.Sessions[key]
	_, active := b.active[key]
	if session == nil || session.Retired || active {
		b.mu.Unlock()
		return
	}
	// Capture exactly the closed prompts covered by this cleanup attempt. A
	// repeated settlement of an older turn cannot settle a concurrently added
	// turn using remote evidence obtained before that turn existed.
	closing := make([]string, 0, len(session.Prompts))
	ambiguous := false
	quiescent := true
	for promptKey, prompt := range session.Prompts {
		if prompt.Closing && !prompt.Settled {
			closing = append(closing, promptKey)
			for _, invocation := range prompt.Invocations {
				switch invocation.State {
				case "completed", "reserved", "rejected":
				default:
					quiescent = false
				}
			}
		}
		for _, invocation := range prompt.Invocations {
			if invocation.State == "uncertain" || (invocation.State == "intent" && invocation.ResponseID == "") {
				ambiguous = true
			}
		}
	}
	remoteID, createState, retiring := session.RemoteID, session.CreateState, session.Retiring
	b.mu.Unlock()
	if len(closing) == 0 && !retiring {
		return
	}
	if createState == "intent" {
		// A later GET cannot acknowledge the original CREATE or prove that it
		// has finished. Keep its owner unresolved even if the session appears;
		// only that original request's validated acknowledgement makes it known.
		return
	}
	kind := "never-created"
	if createState == "known" {
		if retiring && !ambiguous {
			// Deletion also terminates an acknowledged response. It remains
			// retryable if a previous DELETE succeeded before proof persistence.
			if b.remoteSessionDelete(ctx, remoteID) != nil {
				return
			}
			kind = "remote-delete-204-get-404"
		} else if quiescent && !ambiguous && len(closing) > 0 {
			// Completed responses have durable validated links. Empty prompts
			// and reserved/rejected invocations cannot add remote work once
			// their active request is gone. Preserve compute and its history.
			kind = "remote-responses-quiescent"
		} else {
			if b.remoteSessionStop(ctx, remoteID) != nil {
				return
			}
			kind = "remote-stop-idle"
		}
	}
	// Stop is useful containment, but does not prove an unacknowledged request
	// cannot arrive later. Keep its durable owner and never issue deletion.
	if ambiguous {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	_ = b.commitLocked(func(next *brokerLedger) error {
		session := next.Sessions[key]
		if _, active := b.active[key]; active {
			return errBrokerPending
		}
		for _, promptKey := range closing {
			prompt := session.Prompts[promptKey]
			prompt.Settled = true
			prompt.ProofDigest = brokerJSONDigest(struct {
				Owner, Prompt, Remote, Kind, At string
			}{key, promptKey, brokerSHA([]byte(remoteID)), kind, time.Now().UTC().Format(time.RFC3339Nano)})
			for _, invocation := range prompt.Invocations {
				if invocation.State != "completed" {
					invocation.State = "settled"
				}
			}
		}
		if !retiring {
			return nil
		}
		for _, prompt := range session.Prompts {
			if !prompt.Settled {
				return errBrokerPending
			}
		}
		if createState == "known" {
			session.CreateState = "deleted"
		}
		session.Retired = true
		session.ProofDigest = brokerJSONDigest(struct{ Owner, Remote, Kind, At string }{
			key, brokerSHA([]byte(remoteID)), kind, time.Now().UTC().Format(time.RFC3339Nano)})
		return nil
	})
}
