package hosted

import (
	"testing"

	"github.com/orka-agents/agent-runtime-foundry/internal/durablestore/storetest"
	"github.com/orka-agents/agent-runtime-foundry/internal/foundry"
)

func hostedStoreFixture(t *testing.T) storetest.Fixture {
	t.Helper()
	cfg, exposed := hostedBoundaryLedgerFixture(t)
	return storetest.Fixture{
		LockName: "gateway.lock", Digest: foundry.JSONDigest(exposed),
		Open: func(dir string) (func(), string, error) {
			store, ledger, err := openHostedGatewayStore(dir, cfg)
			if err != nil {
				return nil, "", err
			}
			return store.close, foundry.JSONDigest(ledger), nil
		},
		Save: func(dir string) error { return (&hostedGatewayStore{dir: dir}).save(exposed) },
	}
}

func TestDurableStoreMissingLedgerFailsClosed(t *testing.T) {
	storetest.MissingLedgerFailsClosed(t, hostedStoreFixture(t))
}

func TestDurableStoreEmptyDirectoryAndLegacyRecovery(t *testing.T) {
	storetest.EmptyDirectoryAndLegacyRecovery(t, hostedStoreFixture(t))
}

func TestDurableStoreInitializerRace(t *testing.T) {
	storetest.InitializerRace(t, hostedStoreFixture(t))
}

func TestStoreInitializerKeepsCreatorAcrossFlockContention(t *testing.T) {
	storetest.CreatorKeepsLock(t, hostedStoreFixture(t))
}

func TestStoreInitializerExistingWriterRemainsNonblocking(t *testing.T) {
	storetest.ExistingWriterNonblocking(t, hostedStoreFixture(t))
}

func TestStoreInitializerLegacyRecoveryRemainsNonblocking(t *testing.T) {
	storetest.LegacyRecoveryNonblocking(t, hostedStoreFixture(t))
}
