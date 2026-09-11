package main

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

type storeInitializationFixture struct {
	lockName string
	digest   string
	open     func(string) (func(), string, error)
	save     func(string) error
}

func storeInitializationFixtures(t *testing.T) map[string]storeInitializationFixture {
	t.Helper()
	digest := brokerSHA([]byte("durable-broker-fixture"))
	c := brokerTestContext(brokerConfiguration{configDigest: digest})
	brokerLedger := &brokerLedger{Version: 1, ConfigDigest: digest, Sessions: map[string]*brokerSession{
		brokerJSONDigest(c.Owner): {Owner: c.Owner, CreateState: "none", Retiring: true, Retired: true,
			ProofDigest: brokerSHA([]byte("retired-owner")), Prompts: map[string]*brokerPrompt{},
			Responses: map[string]brokerResponseID{}, Operations: map[string]string{}},
	}}
	if !brokerLedgerValid(brokerLedger, digest) {
		t.Fatal("invalid retired broker fixture")
	}
	cfg, exposed := hostedBoundaryLedgerFixture(t)
	return map[string]storeInitializationFixture{
		"broker": {
			lockName: "broker.lock", digest: brokerJSONDigest(brokerLedger),
			open: func(dir string) (func(), string, error) {
				store, ledger, err := openBrokerStore(dir, digest)
				if err != nil {
					return nil, "", err
				}
				return store.close, brokerJSONDigest(ledger), nil
			},
			save: func(dir string) error { return (&brokerStore{dir: dir}).save(brokerLedger) },
		},
		"gateway": {
			lockName: "gateway.lock", digest: brokerJSONDigest(exposed),
			open: func(dir string) (func(), string, error) {
				store, ledger, err := openHostedGatewayStore(dir, cfg)
				if err != nil {
					return nil, "", err
				}
				return store.close, brokerJSONDigest(ledger), nil
			},
			save: func(dir string) error { return (&hostedGatewayStore{dir: dir}).save(exposed) },
		},
	}
}

func TestDurableStoreMissingLedgerFailsClosed(t *testing.T) {
	for name, fixture := range storeInitializationFixtures(t) {
		t.Run(name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "ledger")
			closeStore, _, err := fixture.open(dir)
			if err != nil {
				t.Fatal("could not initialize ownership fixture")
			}
			err = fixture.save(dir)
			closeStore()
			if err != nil {
				t.Fatal("could not persist owned lifetime")
			}
			lockPath := filepath.Join(dir, fixture.lockName)
			lockInfo, err := os.Lstat(lockPath)
			if err != nil {
				t.Fatal("ownership witness is missing")
			}
			statePath := filepath.Join(dir, "state.json")
			retained := filepath.Join(t.TempDir(), "retained-state.json")
			if os.Rename(statePath, retained) != nil {
				t.Fatal("could not simulate missing ownership state")
			}
			for range 2 {
				closeStore, _, err = fixture.open(dir)
				if err == nil {
					closeStore()
					t.Fatal("missing ownership ledger was silently reinitialized")
				}
				if _, err := os.Lstat(statePath); !os.IsNotExist(err) {
					t.Fatal("rejected recovery recreated state")
				}
				after, err := os.Lstat(lockPath)
				if err != nil || !os.SameFile(lockInfo, after) {
					t.Fatal("rejected recovery replaced its initialization witness")
				}
			}
			if os.Rename(retained, statePath) != nil {
				t.Fatal("could not restore exact original ownership")
			}
			closeStore, restored, err := fixture.open(dir)
			if err != nil {
				t.Fatal("valid original ownership could not recover")
			}
			closeStore()
			if restored != fixture.digest {
				t.Fatal("recovery changed the owned lifetime")
			}
		})
	}
}

func TestDurableStoreEmptyDirectoryAndLegacyRecovery(t *testing.T) {
	for name, fixture := range storeInitializationFixtures(t) {
		t.Run(name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "ledger")
			if os.Mkdir(dir, 0o700) != nil {
				t.Fatal("could not create private pre-existing directory")
			}
			closeStore, _, err := fixture.open(dir)
			if err != nil {
				t.Fatal("pre-existing empty directory could not initialize")
			}
			err = fixture.save(dir)
			closeStore()
			if err != nil {
				t.Fatal("could not persist owned lifetime")
			}
			for _, missingLock := range []bool{false, true} {
				if missingLock && os.Remove(filepath.Join(dir, fixture.lockName)) != nil {
					t.Fatal("could not prepare valid ledger without a lock")
				}
				closeStore, restored, err := fixture.open(dir)
				if err != nil {
					t.Fatal("valid existing ownership could not recover")
				}
				closeStore()
				if restored != fixture.digest {
					t.Fatal("existing ownership was rebound during recovery")
				}
			}
		})
	}
}

func TestDurableStoreInitializerRace(t *testing.T) {
	for name, fixture := range storeInitializationFixtures(t) {
		t.Run(name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "ledger")
			if os.Mkdir(dir, 0o700) != nil {
				t.Fatal("could not create private initializer directory")
			}
			// Pause the original creator after exclusive lock-file creation,
			// before flock. A competing opener can acquire flock first, but
			// it must not claim the original creator's initialization rights.
			witness, err := os.OpenFile(filepath.Join(dir, fixture.lockName), os.O_CREATE|os.O_EXCL|os.O_RDWR|syscall.O_NOFOLLOW, 0o600)
			if err != nil {
				t.Fatal("could not pause original initialization")
			}
			defer witness.Close()
			closeStore, _, err := fixture.open(dir)
			if err == nil {
				closeStore()
				t.Fatal("competing opener initialized another process's store")
			}
			if _, err := os.Lstat(filepath.Join(dir, "state.json")); !os.IsNotExist(err) {
				t.Fatal("competing opener published a ledger")
			}
			// Resume the original creator using its same descriptor. The
			// rejected contender must release flock and preserve that inode.
			if syscall.Flock(int(witness.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) != nil {
				t.Fatal("rejected contender retained the initialization lock")
			}
			if fixture.save(dir) != nil {
				t.Fatal("original creator could not publish its ledger")
			}
			if syscall.Flock(int(witness.Fd()), syscall.LOCK_UN) != nil {
				t.Fatal("original creator could not release the store")
			}
			closeStore, restored, err := fixture.open(dir)
			if err != nil {
				t.Fatal("competing opener could not read the completed original store")
			}
			closeStore()
			if restored != fixture.digest {
				t.Fatal("competing opener replaced original ownership")
			}
		})
	}
}
