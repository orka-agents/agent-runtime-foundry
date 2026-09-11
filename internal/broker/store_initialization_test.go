package broker

import (
	"testing"

	"github.com/orka-agents/agent-runtime-foundry/internal/durablestore/storetest"
	"github.com/orka-agents/agent-runtime-foundry/internal/foundry"
)

func brokerStoreFixture(t *testing.T) storetest.Fixture {
	t.Helper()
	digest := foundry.Digest([]byte("durable-broker-fixture"))
	c := brokerTestContext(brokerConfiguration{configDigest: digest})
	ledger := &brokerLedger{Version: 1, ConfigDigest: digest, Sessions: map[string]*brokerSession{
		foundry.JSONDigest(c.Owner): {Owner: c.Owner, CreateState: "none", Retiring: true, Retired: true,
			ProofDigest: foundry.Digest([]byte("retired-owner")), Prompts: map[string]*brokerPrompt{},
			Responses: map[string]brokerResponseID{}, Operations: map[string]string{}},
	}}
	if !brokerLedgerValid(ledger, digest) {
		t.Fatal("invalid retired broker fixture")
	}
	return storetest.Fixture{
		LockName: "broker.lock", Digest: foundry.JSONDigest(ledger),
		Open: func(dir string) (func(), string, error) {
			store, ledger, err := openBrokerStore(dir, digest)
			if err != nil {
				return nil, "", err
			}
			return store.close, foundry.JSONDigest(ledger), nil
		},
		Save: func(dir string) error { return (&brokerStore{dir: dir}).save(ledger) },
	}
}

func TestDurableStoreMissingLedgerFailsClosed(t *testing.T) {
	storetest.MissingLedgerFailsClosed(t, brokerStoreFixture(t))
}

func TestDurableStoreEmptyDirectoryAndLegacyRecovery(t *testing.T) {
	storetest.EmptyDirectoryAndLegacyRecovery(t, brokerStoreFixture(t))
}

func TestDurableStoreInitializerRace(t *testing.T) {
	storetest.InitializerRace(t, brokerStoreFixture(t))
}

func TestStoreInitializerKeepsCreatorAcrossFlockContention(t *testing.T) {
	storetest.CreatorKeepsLock(t, brokerStoreFixture(t))
}

func TestStoreInitializerExistingWriterRemainsNonblocking(t *testing.T) {
	storetest.ExistingWriterNonblocking(t, brokerStoreFixture(t))
}

func TestStoreInitializerLegacyRecoveryRemainsNonblocking(t *testing.T) {
	storetest.LegacyRecoveryNonblocking(t, brokerStoreFixture(t))
}
