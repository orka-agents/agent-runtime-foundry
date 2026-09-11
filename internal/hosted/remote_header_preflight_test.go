package hosted

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
)

func TestHostedInvalidRemoteHeaderDoesNotReserveCreation(t *testing.T) {
	for name, corrupt := range invalidRemoteHeaderTokens() {
		t.Run(name, func(t *testing.T) {
			f := newHostedGatewayTestFixture(t, hostedGatewayTestOptions{})
			var posts atomic.Int64
			transport := f.gateway.httpClient.Transport
			f.gateway.httpClient.Transport = hostedGatewayTestRoundTripper(func(request *http.Request) (*http.Response, error) {
				if request.Method == http.MethodPost {
					posts.Add(1)
				}
				return transport.RoundTrip(request)
			})
			f.gateway.provider = hostedGatewayTestTokenProvider(func(context.Context) (string, error) {
				token := f.token.Load().(string)
				if f.tokenCalls.Add(1) == 4 {
					return corrupt(token), nil
				}
				return token, nil
			})
			if f.initialize() == nil || f.tokenCalls.Load() != 4 {
				t.Fatal("fixture did not reach the invalid creation header")
			}
			if posts.Load() != 0 || f.creates.Load() != 0 || f.dials.Load() != 0 || f.httpCalls.Load() != 3 {
				t.Error("invalid creation header reached transport or channel setup")
			}
			f.assertNoBootstrap(t)
			store, ledger, err := openHostedGatewayStore(f.settings.stateDir, f.settings.config)
			if err != nil {
				t.Fatal("could not reopen gateway after local header rejection")
			}
			defer store.close()
			if ledger.CreateAttempted || ledger.SessionCreated || ledger.ExposurePossible {
				t.Fatal("local header rejection stranded the hosted creation")
			}
			restarted := &hostedGateway{cfg: f.settings.config, provider: f.gateway.provider,
				httpClient: f.gateway.httpClient, store: store, ledger: ledger}
			if restarted.ensureSession(t.Context()) != nil || posts.Load() != 1 || f.creates.Load() != 1 || !restarted.ledger.SessionCreated {
				t.Fatal("definitely-unsent header rejection prevented a later owned creation")
			}
		})
	}
}

func invalidRemoteHeaderTokens() map[string]func(string) string {
	return map[string]func(string) string{
		"signature-nul": func(token string) string { return token + "\x00" },
		"signature-lf":  func(token string) string { return token + "\n" },
		"signature-cr":  func(token string) string { return token + "\r" },
		"header-del":    func(token string) string { return "\x7f" + token },
	}
}
