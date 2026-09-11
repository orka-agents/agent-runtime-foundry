package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orka-agents/agent-runtime-foundry/internal/brokerapi"
	"github.com/orka-agents/agent-runtime-foundry/internal/foundry"
)

const brokerIdentityRemoteSession = "11111111-1111-4111-8111-111111111111"

func newBrokerResponseIdentityFixture(t *testing.T) (*lifecycleBroker, brokerContext) {
	t.Helper()
	cfg := brokerConfiguration{configDigest: foundry.Digest([]byte("identity-write-fixture")), stateDir: filepath.Join(t.TempDir(), "broker")}
	store, ledger, err := openBrokerStore(cfg.stateDir, cfg.configDigest)
	if err != nil {
		t.Fatal("could not initialize identity fixture store")
	}
	t.Cleanup(store.close)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	b := &lifecycleBroker{store: store, ledger: ledger, ctx: ctx, cancel: cancel, active: map[string]brokerActive{}}
	c := brokerTestContext(cfg)
	c.BodySHA256 = foundry.Digest([]byte("identity-write-input"))
	c.LeaseExpiresAt = time.Now().Add(time.Minute).UTC().Format(time.RFC3339Nano)
	err = b.commitLocked(func(next *brokerLedger) error {
		session, err := brokerEnsureSession(next, c)
		if err != nil {
			return err
		}
		next.PrincipalDigest = foundry.Digest([]byte("identity-write-principal"))
		session.RemoteID, session.CreateState = brokerIdentityRemoteSession, "known"
		if _, err := brokerRecordOperation(session, brokerapi.ResponsesPath, c); err != nil {
			return err
		}
		prompt, err := brokerEnsurePrompt(session, c)
		if err != nil {
			return err
		}
		prompt.LastSequence = c.InvocationSequence
		prompt.Invocations[c.InvocationSequence] = &brokerInvocation{Sequence: c.InvocationSequence,
			OperationID: c.OperationID, BodyDigest: c.BodySHA256, State: "intent"}
		return nil
	})
	if err != nil || !brokerLedgerValid(b.ledger, cfg.configDigest) {
		t.Fatal("identity fixture lacks valid durable invocation intent")
	}
	return b, c
}

// The store atomically replaces state.json on every save. Supplying exactly one
// SSE frame per Read lets the next Read count that replacement after its flush,
// without replacing the production store or relying on timestamp resolution.
type brokerIdentityEventReader struct {
	t      *testing.T
	path   string
	events []string
	index  int
	last   os.FileInfo
	writes int
}

func (r *brokerIdentityEventReader) observe() {
	r.t.Helper()
	info, err := os.Stat(r.path)
	if err != nil {
		r.t.Fatal("durable identity record unavailable")
	}
	if r.last != nil && !os.SameFile(r.last, info) {
		r.writes++
	}
	r.last = info
}

func (r *brokerIdentityEventReader) Read(p []byte) (int, error) {
	r.observe()
	if r.index == 1 && r.writes != 1 {
		r.t.Fatal("first acceptance was not persisted before reading the next event")
	}
	if r.index == len(r.events) {
		return 0, io.EOF
	}
	event := r.events[r.index]
	if len(p) < len(event) {
		return 0, io.ErrShortBuffer
	}
	r.index++
	return copy(p, event), nil
}

func brokerIdentityLedgerBytes(t *testing.T, b *lifecycleBroker) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(b.store.dir, "state.json"))
	if err != nil {
		t.Fatal("could not read durable identity fixture")
	}
	var ledger brokerLedger
	if json.Unmarshal(data, &ledger) != nil || !brokerLedgerValid(&ledger, b.ledger.ConfigDigest) ||
		foundry.JSONDigest(ledger) != foundry.JSONDigest(b.ledger) {
		t.Fatal("in-memory identity does not match valid durable ownership")
	}
	return data
}

func TestBrokerResponseIdentityWritesOncePerInvocation(t *testing.T) {
	for _, test := range []struct {
		name   string
		events int
		valid  bool
	}{
		{"repeated", 64, true},
		{"event-limit", foundry.DefaultMaxEvents, true},
		{"over-event-limit", foundry.DefaultMaxEvents + 1, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			b, c := newBrokerResponseIdentityFixture(t)
			events := make([]string, test.events)
			for i := range events {
				events[i] = testSSE(`{"type":"response.in_progress","response":{"id":"response-1","status":"in_progress"}}`)
			}
			events[0] = testSSE(`{"type":"response.created","response":{"id":"response-1","status":"in_progress"}}`)
			events[len(events)-1] = testSSE(`{"type":"response.completed","response":{"id":"response-1","status":"completed"}}`)
			reader := &brokerIdentityEventReader{t: t, path: filepath.Join(b.store.dir, "state.json"), events: events}
			data, err := b.readTrackedStream(reader, c, brokerIdentityRemoteSession)
			if err != nil {
				t.Fatal("coherent acceptance evidence was rejected")
			}
			if reader.writes != 1 {
				t.Fatalf("identity persistence count = %d for %d lifecycle events; want 1", reader.writes, test.events)
			}
			invocation := b.ledger.Sessions[foundry.JSONDigest(c.Owner)].Prompts[c.promptKey()].Invocations[c.InvocationSequence]
			if invocation.State != "accepted" || invocation.ResponseID != "response-1" || invocation.ResponseAlias == "" {
				t.Fatal("stream tracking did not preserve acceptance separately from completion")
			}
			_ = brokerIdentityLedgerBytes(t, b)
			summary, err := foundry.ParseStrictSSE(bytes.NewReader(data))
			if (err == nil) != test.valid {
				t.Fatal("identity deduplication changed stream event validation")
			}
			if test.valid {
				if _, err := b.commitCompletedResponse(c, summary); err != nil {
					t.Fatal("validated response could not persist completion")
				}
				reader.observe()
				if reader.writes != 2 {
					t.Fatal("completion did not perform its separate durable write")
				}
				_ = brokerIdentityLedgerBytes(t, b)
			}
		})
	}
}

func TestBrokerResponseIdentityDuplicateStillRejectsConflicts(t *testing.T) {
	b, c := newBrokerResponseIdentityFixture(t)
	accepted := foundry.Response{ID: "response-1", AgentSessionID: brokerIdentityRemoteSession}
	if err := b.recordResponseIdentity(c, accepted, brokerIdentityRemoteSession); err != nil {
		t.Fatal("initial response identity was rejected")
	}
	before := brokerIdentityLedgerBytes(t, b)
	for name, response := range map[string]foundry.Response{
		"changed-response": {ID: "response-2"},
		"missing-response": {},
		"invalid-response": {ID: "response/invalid"},
		"changed-session":  {ID: accepted.ID, AgentSessionID: "another-session"},
	} {
		t.Run(name, func(t *testing.T) {
			if err := b.recordResponseIdentity(c, response, brokerIdentityRemoteSession); !errors.Is(err, errBrokerConflict) {
				t.Fatal("conflicting response identity was accepted")
			}
			if !bytes.Equal(before, brokerIdentityLedgerBytes(t, b)) {
				t.Fatal("conflicting identity changed durable ownership")
			}
		})
	}
	if _, err := b.commitCompletedResponse(c, foundry.StreamSummary{ResponseID: accepted.ID}); err != nil {
		t.Fatal("could not prepare previous response identity")
	}
	nextContext := c
	nextContext.InvocationSequence++
	nextContext.OperationID = "identity-next-invocation"
	if err := b.commitLocked(func(next *brokerLedger) error {
		session := next.Sessions[foundry.JSONDigest(c.Owner)]
		if _, err := brokerRecordOperation(session, brokerapi.ResponsesPath, nextContext); err != nil {
			return err
		}
		prompt := session.Prompts[c.promptKey()]
		prompt.LastSequence = nextContext.InvocationSequence
		prompt.Invocations[nextContext.InvocationSequence] = &brokerInvocation{Sequence: nextContext.InvocationSequence,
			OperationID: nextContext.OperationID, BodyDigest: nextContext.BodySHA256, State: "intent"}
		return nil
	}); err != nil {
		t.Fatal("could not prepare next invocation")
	}
	before = brokerIdentityLedgerBytes(t, b)
	if err := b.recordResponseIdentity(nextContext, accepted, brokerIdentityRemoteSession); !errors.Is(err, errBrokerConflict) {
		t.Fatal("response identity from an earlier invocation was reused")
	}
	if !bytes.Equal(before, brokerIdentityLedgerBytes(t, b)) {
		t.Fatal("reused response changed durable ownership")
	}
}

func TestBrokerResponseIdentityConcurrentDuplicatesDoNotWrite(t *testing.T) {
	b, c := newBrokerResponseIdentityFixture(t)
	response := foundry.Response{ID: "response-1"}
	if err := b.recordResponseIdentity(c, response, brokerIdentityRemoteSession); err != nil {
		t.Fatal("initial response identity was rejected")
	}
	reader := &brokerIdentityEventReader{t: t, path: filepath.Join(b.store.dir, "state.json")}
	// Hold the inode so multiple incorrect rewrites cannot reuse it before the
	// final observation and hide a persistence call.
	record, err := os.Open(reader.path)
	if err != nil {
		t.Fatal("could not pin the durable identity record")
	}
	defer func() { _ = record.Close() }()
	reader.observe()
	before := brokerIdentityLedgerBytes(t, b)
	var group sync.WaitGroup
	for range 32 {
		group.Go(func() {
			if err := b.recordResponseIdentity(c, response, brokerIdentityRemoteSession); err != nil {
				t.Error("concurrent duplicate identity was rejected")
			}
		})
	}
	group.Wait()
	reader.observe()
	if reader.writes != 0 || !bytes.Equal(before, brokerIdentityLedgerBytes(t, b)) {
		t.Fatal("concurrent duplicate identities rewrote durable ownership")
	}
}

func TestBrokerResponseIdentityPersistenceFailureRemainsClosed(t *testing.T) {
	for _, existing := range []bool{false, true} {
		name := "first-acceptance"
		if existing {
			name = "duplicate-after-store-failure"
		}
		t.Run(name, func(t *testing.T) {
			b, c := newBrokerResponseIdentityFixture(t)
			response := foundry.Response{ID: "response-1"}
			if existing && b.recordResponseIdentity(c, response, brokerIdentityRemoteSession) != nil {
				t.Fatal("initial response identity was rejected")
			}
			before := brokerIdentityLedgerBytes(t, b)
			active, cancel := context.WithCancel(context.Background())
			defer cancel()
			b.active[foundry.JSONDigest(c.Owner)] = brokerActive{prompt: c.promptKey(), cancel: cancel}
			if err := os.Rename(b.store.dir, b.store.dir+"-unavailable"); err != nil {
				t.Fatal("could not inject identity persistence failure")
			}
			if existing && !errors.Is(b.commitLocked(func(*brokerLedger) error { return nil }), errBrokerStorage) {
				t.Fatal("store failure did not poison the broker")
			}
			if err := b.recordResponseIdentity(c, response, brokerIdentityRemoteSession); !errors.Is(err, errBrokerStorage) {
				t.Fatal("identity callback bypassed failed persistence")
			}
			if active.Err() != context.Canceled || !errors.Is(b.storageError, errBrokerStorage) {
				t.Fatal("persistence failure did not contain active work")
			}
			if foundry.Digest(before) != foundry.JSONDigest(b.ledger) {
				t.Fatal("failed persistence changed the last durable identity")
			}
			if err := os.Rename(b.store.dir+"-unavailable", b.store.dir); err != nil {
				t.Fatal("could not restore fixture store for readback")
			}
			if !bytes.Equal(before, brokerIdentityLedgerBytes(t, b)) {
				t.Fatal("failed identity write changed the durable record")
			}
		})
	}
}

func TestBrokerResponseIdentityTrackingStillRejectsMalformedTail(t *testing.T) {
	b, c := newBrokerResponseIdentityFixture(t)
	created := testSSE(`{"type":"response.created","response":{"id":"response-1","status":"in_progress"}}`)
	stream := created + testSSE(`{"type":"response.in_progress","response":{"id":"response-1","status":"completed"}}`)
	if _, err := b.readTrackedStream(strings.NewReader(stream), c, brokerIdentityRemoteSession); err == nil {
		t.Fatal("duplicate response identity bypassed lifecycle validation")
	}
	invocation := b.ledger.Sessions[foundry.JSONDigest(c.Owner)].Prompts[c.promptKey()].Invocations[c.InvocationSequence]
	if invocation.ResponseID != "response-1" || invocation.State != "accepted" {
		t.Fatal("malformed tail erased earlier durable acknowledgement")
	}
	_ = brokerIdentityLedgerBytes(t, b)
}
