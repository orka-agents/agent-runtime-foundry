package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestBrokerCreationRequiresCurrentLeaseBeforeIntent(t *testing.T) {
	for _, renewed := range []bool{false, true} {
		name := "expired"
		if renewed {
			name = "renewed-during-preflight"
		}
		t.Run(name, func(t *testing.T) {
			f := newBrokerFixture(t, "success")
			cfg := brokerTestConfig(t, f)
			c := brokerTestContext(cfg)
			c.LeaseExpiresAt = time.Now().Add(-time.Second).UTC().Format(time.RFC3339Nano)
			body := brokerTestBody("")
			c.BodySHA256 = brokerSHA(body)
			var request acpResponseRequest
			if acpDecode(body, &request, true) != nil {
				t.Fatal("invalid inference fixture")
			}
			store, ledger, err := openBrokerStore(cfg.stateDir, cfg.configDigest)
			if err != nil {
				t.Fatal("could not open lease fixture store")
			}
			ctx, cancel := context.WithCancel(t.Context())
			b := &lifecycleBroker{cfg: cfg, store: store, ledger: ledger, ctx: ctx, cancel: cancel,
				httpClient: newBrokerHTTPClient(), active: map[string]brokerActive{},
				workers: map[string]bool{}, nextReconcile: map[string]time.Time{}}
			t.Cleanup(b.close)
			// Reproduce a previously admitted invocation whose lease expires before
			// CREATE preflight finishes. No sweep runs in this fixture: admission
			// must check the timestamp without waiting for background cleanup.
			err = b.commitLocked(func(next *brokerLedger) error {
				session, ensureErr := brokerEnsureSession(next, c)
				if ensureErr != nil {
					return ensureErr
				}
				if _, ensureErr = brokerRecordOperation(session, brokerResponsesPath, c); ensureErr != nil {
					return ensureErr
				}
				prompt, ensureErr := brokerEnsurePrompt(session, c)
				if ensureErr != nil {
					return ensureErr
				}
				prompt.LastSequence = c.InvocationSequence
				prompt.Invocations[c.InvocationSequence] = &brokerInvocation{Sequence: c.InvocationSequence,
					OperationID: c.OperationID, BodyDigest: c.BodySHA256, State: "reserved"}
				return nil
			})
			if err != nil || !brokerLedgerValid(b.ledger, cfg.configDigest) {
				t.Fatal("invalid admitted invocation fixture")
			}
			var tokens atomic.Int32
			b.tokenProvider = brokerEvidenceTokenProvider(func(context.Context) (string, error) {
				if tokens.Add(1) == 3 && renewed {
					b.mu.Lock()
					err := b.commitLocked(func(next *brokerLedger) error {
						prompt := next.Sessions[brokerJSONDigest(c.Owner)].Prompts[c.promptKey()]
						prompt.LeaseGeneration++
						prompt.LeaseExpiresAt = time.Now().Add(10 * time.Second)
						return nil
					})
					b.mu.Unlock()
					if err != nil {
						return "", err
					}
				}
				return brokerTestToken(), nil
			})
			_, err = b.invoke(ctx, c, request.foundryResponseRequest)
			creates, inferences, _, _ := f.counts()
			owner := b.ledger.Sessions[brokerJSONDigest(c.Owner)]
			if renewed {
				if err != nil || creates != 1 || inferences != 1 || owner.Prompts[c.promptKey()].LeaseGeneration != 2 {
					t.Fatal("current renewed lease did not authorize the original creation")
				}
			} else if !errors.Is(err, errBrokerClosed) || creates != 0 || inferences != 0 ||
				owner.CreateState != "none" || owner.RemoteID != "" {
				t.Fatalf("expired lease reached creation: creates=%d inferences=%d state=%s", creates, inferences, owner.CreateState)
			}
		})
	}
}
