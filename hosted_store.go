package main

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/google/uuid"
)

// The gateway records possible credential exposure BEFORE sending bootstrap.
// A replacement process cannot seed a fresh supervisor from a nonempty record.
// This deliberately blocks unknown ownership instead of replaying mutations.
type hostedGatewayLedger struct {
	Version          uint32          `json:"version"`
	ConfigDigest     string          `json:"configDigest"`
	SessionID        string          `json:"sessionID"`
	PrincipalDigest  string          `json:"principalDigest,omitempty"`
	CreateAttempted  bool            `json:"createAttempted"`
	SessionCreated   bool            `json:"sessionCreated"`
	ExposurePossible bool            `json:"exposurePossible"`
	Challenge        hostedChallenge `json:"challenge"`
	PairID           string          `json:"pairID,omitempty"`
	BootstrapDigest  string          `json:"bootstrapDigest,omitempty"`
	Ready            bool            `json:"ready"`
	Closed           bool            `json:"closed"`
}

type hostedGatewayStore struct {
	dir  string
	lock *os.File
}

func openHostedGatewayStore(dir string, cfg hostedGatewayConfig) (*hostedGatewayStore, hostedGatewayLedger, error) {
	var empty hostedGatewayLedger
	if !filepath.IsAbs(dir) || filepath.Clean(dir) == "/" {
		return nil, empty, errHostedInvalid
	}
	lock, created, err := openStoreLock(dir, "gateway.lock", syncStoreDirectory)
	if err != nil {
		return nil, empty, errHostedInvalid
	}
	store := &hostedGatewayStore{dir: dir, lock: lock}
	ledger := hostedGatewayLedger{Version: 1, ConfigDigest: brokerJSONDigest(cfg), SessionID: cfg.SessionID}
	path := filepath.Join(dir, "state.json")
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		if created && store.save(ledger) == nil {
			return store, ledger, nil
		}
	} else if err == nil && brokerPrivateFile(info) && info.Size() <= hostedMaxHandshakeBytes {
		data, readErr := readHostedFile(path, hostedMaxHandshakeBytes)
		var stored hostedGatewayLedger
		if readErr == nil && acpDecode(data, &stored, true) == nil && hostedGatewayLedgerValid(stored, cfg) {
			return store, stored, nil
		}
	}
	store.close()
	return nil, empty, errHostedInvalid
}

func hostedGatewayLedgerValid(value hostedGatewayLedger, cfg hostedGatewayConfig) bool {
	if value.Version != 1 || value.ConfigDigest != brokerJSONDigest(cfg) || value.SessionID != cfg.SessionID ||
		(value.PrincipalDigest != "" && !brokerDigestValid(value.PrincipalDigest)) ||
		(value.CreateAttempted && value.PrincipalDigest == "") ||
		(value.SessionCreated && !value.CreateAttempted) || (value.Ready && !value.ExposurePossible) {
		return false
	}
	if !value.ExposurePossible {
		return value.Challenge == (hostedChallenge{}) && value.PairID == "" && value.BootstrapDigest == "" && !value.Closed
	}
	_, nonceValid := hostedCanonicalBytes(value.Challenge.Nonce, 32)
	return value.SessionCreated && value.PrincipalDigest != "" && hostedUUIDValid(value.PairID) &&
		hostedUUIDValid(value.Challenge.BootID) && nonceValid &&
		brokerDigestValid(value.BootstrapDigest) && value.Challenge.SessionID == cfg.SessionID &&
		value.Challenge.ConfigurationDigest == brokerJSONDigest(cfg.Image) &&
		value.Challenge.DeploymentID == cfg.Image.DeploymentID && value.Challenge.Protocol == hostedProtocol &&
		value.Challenge.AgentName == cfg.Image.Target.AgentName && value.Challenge.AgentVersion == cfg.Image.Target.AgentVersion
}

func (s *hostedGatewayStore) save(value hostedGatewayLedger) error {
	data, err := json.Marshal(value)
	if err != nil || len(data) > hostedMaxHandshakeBytes {
		return errHostedInvalid
	}
	name := filepath.Join(s.dir, ".state-"+uuid.NewString())
	file, err := os.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return errHostedInvalid
	}
	defer os.Remove(name) //nolint:errcheck
	if n, err := file.Write(data); err != nil || n != len(data) {
		_ = file.Close()
		return io.ErrShortWrite
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return errHostedInvalid
	}
	if file.Close() != nil || os.Rename(name, filepath.Join(s.dir, "state.json")) != nil {
		return errHostedInvalid
	}
	directory, err := os.Open(s.dir)
	if err != nil {
		return errHostedInvalid
	}
	defer directory.Close() //nolint:errcheck
	return directory.Sync()
}

func (s *hostedGatewayStore) close() {
	if s != nil && s.lock != nil {
		_ = syscall.Flock(int(s.lock.Fd()), syscall.LOCK_UN)
		_ = s.lock.Close()
	}
}
