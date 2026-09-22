package broker

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orka-agents/agent-runtime-foundry/internal/brokerapi"
	"github.com/orka-agents/agent-runtime-foundry/internal/foundry"
)

func TestBrokerLegacyIdentityMigrationPreservesOwnership(t *testing.T) {
	f := newBrokerFixture(t, "success")
	cfg := brokerTestConfig(t, f)
	b, server := startBrokerTest(t, cfg)
	c := brokerTestContext(cfg)
	_ = brokerTestControl(t, server.URL, brokerapi.RetirePath, c)
	b.mu.Lock()
	err := b.commitLocked(func(next *brokerLedger) error { next.LedgerIdentityDigest = ""; return nil })
	ownersBefore := foundry.JSONDigest(b.ledger.Sessions)
	b.mu.Unlock()
	if err != nil {
		t.Fatal("could not create a valid legacy ledger fixture")
	}
	b.close()
	server.Close()
	recovered, restarted := startBrokerTest(t, cfg)
	identity := brokerReadIdentity(t, restarted.URL)
	data, err := os.ReadFile(filepath.Join(cfg.stateDir, "state.json"))
	var ledger brokerLedger
	if err != nil || json.Unmarshal(data, &ledger) != nil || ledger.LedgerIdentityDigest != identity.LedgerIdentityDigest ||
		foundry.JSONDigest(ledger.Sessions) != ownersBefore || !brokerLedgerValid(&ledger, cfg.configDigest) {
		t.Fatal("identity migration changed ownership or advertised an unpersisted identity")
	}
	recovered.close()
	restarted.Close()
	_, reopened := startBrokerTest(t, cfg)
	if brokerReadIdentity(t, reopened.URL) != identity {
		t.Fatal("upgraded ledger identity did not survive restart")
	}
}

func TestBrokerFullLegacyLedgerRemainsContainableWithoutRecoveryIdentity(t *testing.T) {
	f := newBrokerFixture(t, "success")
	cfg := brokerTestConfig(t, f)
	b, server := startBrokerTest(t, cfg)
	c := brokerTestContext(cfg)
	b.mu.Lock()
	err := b.commitLocked(func(next *brokerLedger) error { next.LedgerIdentityDigest = ""; return nil })
	b.mu.Unlock()
	if err != nil {
		t.Fatal("could not prepare legacy ledger")
	}
	brokerCapacityFillHistory(t, b, c, brokerMaxLedgerBytes, false)
	before := brokerIdentityLedgerBytes(t, b)
	b.close()
	server.Close()
	reopened, restarted := startBrokerTest(t, cfg)
	status, _ := brokerRecoveryHTTP(t, restarted.URL, brokerapi.IdentityPath, http.MethodGet, nil, true)
	if status != http.StatusServiceUnavailable || !bytes.Equal(before, brokerIdentityLedgerBytes(t, reopened)) {
		t.Fatal("full legacy ledger lost containment or claimed unpersisted identity")
	}
	c.Owner.RuntimeSessionUID = "capacity-retained-history"
	proof := brokerTestControl(t, restarted.URL, brokerapi.StatusPath, c)
	if !proof.RetirementProven || !proof.SettlementProven {
		t.Fatal("migration capacity refusal hid retained exact-owner cleanup evidence")
	}
}

func TestBrokerBootSealReserveCoversEscapedFence(t *testing.T) {
	cfg := brokerConfiguration{configDigest: foundry.Digest([]byte("boot-capacity-bounds"))}
	c := brokerTestContext(cfg)
	c.Owner.RuntimeInstanceID = strings.Repeat("<", 512)
	c.Owner.SupervisorBootID = strings.Repeat("<", 512)
	c.Owner.RuntimePoolUID = strings.Repeat("<", 512)
	ledger := &brokerLedger{Version: 1, ConfigDigest: cfg.configDigest,
		LedgerIdentityDigest: foundry.Digest([]byte("bound-ledger")), Sessions: map[string]*brokerSession{}}
	session, err := brokerEnsureSession(ledger, c)
	if err != nil {
		t.Fatal("could not establish bounded owner")
	}
	before, _ := json.Marshal(ledger)
	if got := brokerLedgerReserveBytes(ledger); got != brokerOwnerReserveBytes+brokerPrincipalReserveBytes+brokerBootReserveBytes {
		t.Fatal("new ownership did not reserve boot retirement growth")
	}
	session.Retiring, session.Retired = true, true
	session.ProofDigest = foundry.Digest([]byte("retired-fixture-owner"))
	if got := brokerLedgerReserveBytes(ledger); got != brokerPrincipalReserveBytes+brokerBootReserveBytes {
		t.Fatal("ordinary owner retirement released the pending boot seal reserve")
	}
	fence := c.Owner.bootFence()
	ledger.SealedBoots = map[string]*brokerBootSeal{foundry.JSONDigest(fence): {
		RetiredFence: fence, OwnerCount: 1, OwnerSetDigest: foundry.JSONDigest([]string{foundry.JSONDigest(c.Owner)}),
	}}
	after, _ := json.Marshal(ledger)
	if !brokerLedgerValid(ledger, cfg.configDigest) || len(after)-len(before) > brokerBootReserveBytes ||
		brokerLedgerReserveBytes(ledger) != brokerPrincipalReserveBytes {
		t.Fatal("escaped boot seal exceeded reserve or retained unnecessary growth allowance")
	}
}

func TestBrokerCorruptBootSealFailsClosedAtRestart(t *testing.T) {
	for _, field := range []string{"ledger-identity", "owner-count", "owner-set", "owner-fence", "reopened-owner"} {
		t.Run(field, func(t *testing.T) {
			f := newBrokerFixture(t, "success")
			cfg := brokerTestConfig(t, f)
			b, server := startBrokerTest(t, cfg)
			c := brokerTestContext(cfg)
			_ = brokerTestControl(t, server.URL, brokerapi.RenewPath, c)
			request := brokerBootRequest(t, server.URL, c)
			_ = brokerAwaitBootRetirement(t, server.URL, request)
			b.close()
			server.Close()
			path := filepath.Join(cfg.stateDir, "state.json")
			data, err := os.ReadFile(path)
			var ledger brokerLedger
			if err != nil || json.Unmarshal(data, &ledger) != nil {
				t.Fatal("could not load synthetic sealed ledger")
			}
			seal := ledger.SealedBoots[foundry.JSONDigest(request.RetiredFence)]
			switch field {
			case "ledger-identity":
				ledger.LedgerIdentityDigest = ""
			case "owner-count":
				seal.OwnerCount++
			case "owner-set":
				seal.OwnerSetDigest = foundry.Digest([]byte("wrong-owner-set"))
			case "owner-fence":
				seal.RetiredFence.SupervisorBootID = "wrong-boot"
			case "reopened-owner":
				owner := ledger.Sessions[foundry.JSONDigest(c.Owner)]
				owner.Retiring, owner.Retired, owner.ProofDigest = false, false, ""
			}
			data, _ = json.Marshal(ledger)
			if os.WriteFile(path, data, 0o600) != nil {
				t.Fatal("could not persist synthetic corruption")
			}
			store, _, err := openBrokerStore(cfg.stateDir, cfg.configDigest)
			if store != nil {
				store.close()
			}
			if err == nil {
				t.Fatal("broker accepted inconsistent durable boot authority")
			}
		})
	}
}
