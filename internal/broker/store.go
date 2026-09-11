package broker

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/orka-agents/agent-runtime-foundry/internal/durablestore"
	"github.com/orka-agents/agent-runtime-foundry/internal/foundry"
	"github.com/orka-agents/agent-runtime-foundry/internal/strictjson"
)

const brokerMaxLedgerBytes = 32 << 20

// The supervisor freezes one settlement operation per prompt and one
// retirement operation per owner, retrying those exact contexts. Before each
// new invocation or ordinary growth, retain space for every nonretired owner:
// two maximally escaped 4096-byte response IDs, two maximally escaped 512-byte
// cleanup keys with their digests, and UUID, alias, link, proof and state growth.
// Only one prompt (and one live invocation) can be unsettled for an owner.
// Completion call maps are not reserved; they must fit before output is exposed.
// Unbounded distinct cleanup IDs from a lifecycle bearer holder are outside this
// caller guarantee; their operation history is still retained and hard-capped.
const brokerOwnerReserveBytes = 64 << 10

const brokerPrincipalReserveBytes = 128

var errBrokerCapacity = errors.New("Foundry broker durable state is at capacity")

func brokerLedgerReserveBytes(ledger *brokerLedger) int {
	reserve := 0
	if ledger.PrincipalDigest == "" {
		reserve = brokerPrincipalReserveBytes
	}
	for _, session := range ledger.Sessions {
		if !session.Retired {
			reserve += brokerOwnerReserveBytes
		}
	}
	return reserve
}

type brokerLedger struct {
	Version         uint32                    `json:"version"`
	ConfigDigest    string                    `json:"configDigest"`
	PrincipalDigest string                    `json:"principalDigest,omitempty"`
	Sessions        map[string]*brokerSession `json:"sessions"`
}

type brokerSession struct {
	Owner         brokerOwner                 `json:"owner"`
	RemoteID      string                      `json:"remoteID,omitempty"`
	CreateState   string                      `json:"createState"`
	Retiring      bool                        `json:"retiring"`
	Retired       bool                        `json:"retired"`
	ProofDigest   string                      `json:"proofDigest,omitempty"`
	CurrentPrompt string                      `json:"currentPrompt,omitempty"`
	Prompts       map[string]*brokerPrompt    `json:"prompts"`
	Responses     map[string]brokerResponseID `json:"responses"`
	Operations    map[string]string           `json:"operations"`
}

type brokerPrompt struct {
	Identity        brokerContext                `json:"identity"`
	LeaseGeneration uint64                       `json:"leaseGeneration"`
	LeaseExpiresAt  time.Time                    `json:"leaseExpiresAt"`
	Closing         bool                         `json:"closing"`
	Settled         bool                         `json:"settled"`
	ProofDigest     string                       `json:"proofDigest,omitempty"`
	LastSequence    uint64                       `json:"lastSequence"`
	LastAlias       string                       `json:"lastAlias,omitempty"`
	Invocations     map[uint64]*brokerInvocation `json:"invocations"`
}

type brokerInvocation struct {
	Sequence      uint64 `json:"sequence"`
	OperationID   string `json:"operationID"`
	BodyDigest    string `json:"bodyDigest"`
	State         string `json:"state"`
	ResponseID    string `json:"responseID,omitempty"`
	ResponseAlias string `json:"responseAlias,omitempty"`
}

type brokerResponseID struct {
	RemoteID     string            `json:"remoteID"`
	PromptKey    string            `json:"promptKey"`
	Completed    bool              `json:"completed"`
	HasFunctions bool              `json:"hasFunctions"`
	CallIDs      map[string]string `json:"callIDs,omitempty"`
}

type brokerStore struct {
	dir  string
	lock *os.File
}

func openBrokerStore(dir, digest string) (*brokerStore, *brokerLedger, error) {
	if !filepath.IsAbs(dir) || filepath.Clean(dir) == string(filepath.Separator) || !foundry.DigestValid(digest) {
		return nil, nil, errBrokerStorage
	}
	lock, created, err := durablestore.OpenLock(dir, "broker.lock", durablestore.SyncDirectory)
	if err != nil {
		return nil, nil, errBrokerStorage
	}
	store := &brokerStore{dir: dir, lock: lock}
	ledger := &brokerLedger{Version: 1, ConfigDigest: digest, Sessions: map[string]*brokerSession{}}
	path := filepath.Join(dir, "state.json")
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		if !created {
			store.close()
			return nil, nil, errBrokerStorage
		}
		if err := store.save(ledger); err != nil {
			store.close()
			return nil, nil, err
		}
		return store, ledger, nil
	}
	if err != nil || !durablestore.PrivateFile(info) || info.Size() > brokerMaxLedgerBytes {
		store.close()
		return nil, nil, errBrokerStorage
	}
	data, err := os.ReadFile(path)
	if err != nil || strictjson.Decode(data, ledger, true) != nil || !brokerLedgerValid(ledger, digest) {
		store.close()
		return nil, nil, errBrokerStorage
	}
	return store, ledger, nil
}

func (s *brokerStore) save(ledger *brokerLedger) error {
	data, err := json.Marshal(ledger)
	if err != nil {
		return errBrokerStorage
	}
	return s.saveBytes(data)
}

func (s *brokerStore) saveBytes(data []byte) error {
	if len(data) > brokerMaxLedgerBytes {
		return errBrokerCapacity
	}
	name := filepath.Join(s.dir, ".state-"+uuid.NewString())
	file, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return errBrokerStorage
	}
	defer func() { _ = os.Remove(name) }()
	_, err = io.Copy(file, strings.NewReader(string(data)))
	if err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil || closeErr != nil || os.Rename(name, filepath.Join(s.dir, "state.json")) != nil {
		return errBrokerStorage
	}
	directory, err := os.Open(s.dir)
	if err != nil {
		return errBrokerStorage
	}
	err = directory.Sync()
	closeErr = directory.Close()
	if err != nil || closeErr != nil {
		return errBrokerStorage
	}
	return nil
}

func (s *brokerStore) close() {
	if s != nil && s.lock != nil {
		_ = syscall.Flock(int(s.lock.Fd()), syscall.LOCK_UN)
		_ = s.lock.Close()
	}
}
