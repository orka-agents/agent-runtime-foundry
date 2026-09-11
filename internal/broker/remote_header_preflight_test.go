package broker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/orka-agents/agent-runtime-foundry/internal/brokerapi"
	"github.com/orka-agents/agent-runtime-foundry/internal/foundry"
)

func invalidRemoteHeaderTokens() map[string]func(string) string {
	return map[string]func(string) string{
		"signature-nul": func(token string) string { return token + "\x00" },
		"signature-lf":  func(token string) string { return token + "\n" },
		"signature-cr":  func(token string) string { return token + "\r" },
		"header-del":    func(token string) string { return "\x7f" + token },
	}
}

func TestBrokerInvalidRemoteHeaderDoesNotReserveSubmission(t *testing.T) {
	for _, phase := range []string{"create", "inference"} {
		for name, corrupt := range invalidRemoteHeaderTokens() {
			t.Run(phase+"/"+name, func(t *testing.T) {
				f := newBrokerFixture(t, "success")
				cfg := brokerTestConfig(t, f)
				c := brokerTestContext(cfg)
				failAt, expectedPosts := int64(3), int64(0)
				if phase == "inference" {
					failAt, expectedPosts = 5, 1
				}
				var tokens, posts atomic.Int64
				provider := brokerEvidenceTokenProvider(func(context.Context) (string, error) {
					if tokens.Add(1) == failAt {
						return corrupt(brokerTestToken()), nil
					}
					return brokerTestToken(), nil
				})
				client := newBrokerHTTPClient()
				transport := client.Transport
				t.Cleanup(transport.(*http.Transport).CloseIdleConnections)
				client.Transport = brokerFixtureTransport(func(request *http.Request) (*http.Response, error) {
					if request.Method == http.MethodPost {
						posts.Add(1)
					}
					return transport.RoundTrip(request)
				})
				b, err := newLifecycleBroker(t.Context(), cfg, provider, client)
				if err != nil {
					t.Fatal("could not initialize header preflight fixture")
				}
				server := httptest.NewServer(b)
				t.Cleanup(func() { b.close(); server.Close() })
				status, _, err := brokerTestHTTP(t.Context(), server.URL, brokerapi.ResponsesPath, c, brokerTestBody(""))
				if err != nil || status == http.StatusOK || tokens.Load() < failAt {
					t.Fatal("fixture did not reach the invalid header boundary")
				}
				if posts.Load() != expectedPosts {
					t.Error("invalid authorization reached transport submission")
				}
				b.close()
				server.Close()
				raw, err := os.ReadFile(filepath.Join(cfg.stateDir, "state.json"))
				var ledger brokerLedger
				if err != nil || json.Unmarshal(raw, &ledger) != nil || !brokerLedgerValid(&ledger, cfg.configDigest) {
					t.Fatal("could not inspect durable header-failure ownership")
				}
				owner := ledger.Sessions[foundry.JSONDigest(c.Owner)]
				if owner == nil || owner.CreateState == "intent" ||
					(phase == "create" && (owner.CreateState != "none" || owner.RemoteID != "")) {
					t.Fatal("invalid authorization stranded an unsent creation")
				}
				invocation := owner.Prompts[c.promptKey()].Invocations[c.InvocationSequence]
				if invocation.State == "intent" || invocation.State == "uncertain" {
					t.Fatal("invalid authorization stranded an unsent inference")
				}
				_, restarted := startBrokerTest(t, cfg)
				proof := brokerTestControl(t, restarted.URL, brokerapi.SettlePath, c)
				if !proof.SettlementProven || proof.CreatePending || proof.AmbiguousInvocations != 0 {
					t.Fatal("restart could not settle definitely-unsent authorization failure")
				}
				proof = brokerTestControl(t, restarted.URL, brokerapi.RetirePath, c)
				creates, inferences, stops, deletes := f.counts()
				if !proof.RetirementProven || int64(creates) != expectedPosts || inferences != 0 || stops != 0 ||
					int64(deletes) != expectedPosts {
					t.Fatal("header-failure recovery replayed work or lost owned retirement")
				}
			})
		}
	}
}
