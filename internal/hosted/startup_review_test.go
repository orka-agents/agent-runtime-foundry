package hosted

import (
	"bytes"
	"context"
	"errors"
	"maps"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/orka-agents/agent-runtime-foundry/internal/foundry"
	"github.com/orka-agents/agent-runtime-foundry/internal/strictjson"
)

func hostedStartupGatewaySettings(t *testing.T) hostedGatewaySettings {
	t.Helper()
	f := newHostedProtocolFixture(t)
	settings := hostedGatewaySettings{
		config: hostedGatewayConfig{
			Protocol: hostedProtocol, Image: f.config,
			ContainerImage: "example.invalid/hosted@" + foundry.Digest([]byte("startup fixture image")),
			SessionID:      f.hello.Challenge.SessionID, RuntimeProfileDigest: foundry.Digest([]byte("startup fixture profile")),
			RuntimeEnvironment: maps.Clone(f.bootstrap.Environment),
			OrkaBaseURL:        "http://orka.test:8080", BrokerBaseURL: "http://127.0.0.1:8091",
		},
		bootstrap: f.bootstrap, signingKey: bytes.Clone(f.key), stateDir: filepath.Join(t.TempDir(), "gateway"),
	}
	t.Cleanup(func() { clear(settings.signingKey) })
	if validateHostedGatewayConfig(settings.config) != nil {
		t.Fatal("invalid gateway startup fixture")
	}
	return settings
}

func TestHostedStartupOccupiedAddressDoesNotInitializeGateway(t *testing.T) {
	reserved, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("could not reserve fixture address")
	}
	defer reserved.Close() //nolint:errcheck
	settings := hostedStartupGatewaySettings(t)
	settings.address = reserved.Addr().String()
	var tokens atomic.Int32
	provider := hostedGatewayTestTokenProvider(func(context.Context) (string, error) {
		tokens.Add(1)
		// No token is returned, so this fixture cannot make a remote call.
		return "", errHostedInvalid
	})
	if !errors.Is(serveHostedGateway(t.Context(), settings, provider), errHostedInvalid) {
		t.Fatal("occupied listener did not reject startup")
	}
	if tokens.Load() != 0 {
		t.Error("occupied listener reached gateway token acquisition")
	}
	if _, err := os.Stat(settings.stateDir); !os.IsNotExist(err) {
		t.Error("occupied listener created a gateway ownership ledger")
	}
	if hostedNonzeroBytes(settings.signingKey) {
		t.Error("failed listener retained signing material")
	}
}

func TestHostedStartupListenerHeldThroughInitializationAndReleasedOnFailure(t *testing.T) {
	reserved, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("could not select fixture address")
	}
	settings := hostedStartupGatewaySettings(t)
	settings.address = reserved.Addr().String()
	_ = reserved.Close()
	var tokens atomic.Int32
	provider := hostedGatewayTestTokenProvider(func(context.Context) (string, error) {
		tokens.Add(1)
		probe, err := net.Listen("tcp", settings.address)
		if err == nil {
			_ = probe.Close()
			t.Error("gateway initialized before acquiring its listener")
		}
		return "", errHostedInvalid
	})
	if !errors.Is(serveHostedGateway(t.Context(), settings, provider), errHostedInvalid) || tokens.Load() != 1 {
		t.Fatal("fixture did not reach the intended initialization failure")
	}
	listener, err := net.Listen("tcp", settings.address)
	if err != nil {
		t.Fatal("failed initialization retained the gateway listener")
	}
	_ = listener.Close()
	if hostedNonzeroBytes(settings.signingKey) {
		t.Error("failed initialization retained signing material")
	}
}

func TestHostedStartupServesRetainedListener(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("could not bind fixture listener")
	}
	defer listener.Close() //nolint:errcheck
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- serveHostedHTTPListener(ctx, listener, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		}))
	}()
	transport := newHostedLocalTransport()
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 2 * time.Second}
	response, err := client.Get("http://" + listener.Addr().String() + "/healthz")
	if err != nil {
		t.Fatal("retained listener did not serve HTTP")
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Error("retained listener changed the handler response")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Error("retained listener did not shut down cleanly")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("retained listener did not close after cancellation")
	}
	if _, err := listener.Accept(); !errors.Is(err, net.ErrClosed) {
		t.Fatal("cancelled server retained its listener")
	}
}

func TestHostedStartupSessionPreflightFailureDoesNotReserveCreate(t *testing.T) {
	for _, failure := range []string{"token-error", "malformed-token", "principal-drift", "cancelled-token", "cancelled-after-token"} {
		t.Run(failure, func(t *testing.T) {
			f := newHostedGatewayTestFixture(t, hostedGatewayTestOptions{})
			var submissions atomic.Int32
			transport := f.gateway.httpClient.Transport
			f.gateway.httpClient.Transport = hostedGatewayTestRoundTripper(func(request *http.Request) (*http.Response, error) {
				if request.Method == http.MethodPost {
					submissions.Add(1)
				}
				return transport.RoundTrip(request)
			})
			f.gateway.provider = hostedGatewayTestTokenProvider(func(ctx context.Context) (string, error) {
				call := f.tokenCalls.Add(1)
				if call == 4 { // Agent, version and exact-session GETs precede creation.
					switch failure {
					case "token-error":
						return "", errHostedInvalid
					case "malformed-token":
						return "invalid-fixture-identity", nil
					case "principal-drift":
						claims := maps.Clone(f.claims)
						claims["oid"] = uuid.NewString()
						return hostedGatewayTestJWT(claims), nil
					case "cancelled-token":
						f.gateway.cancel()
						return "", context.Canceled
					case "cancelled-after-token":
						f.gateway.cancel()
					}
				}
				if ctx.Err() != nil && failure != "cancelled-after-token" {
					return "", ctx.Err()
				}
				return f.token.Load().(string), nil
			})
			if f.initialize() == nil || f.tokenCalls.Load() != 4 {
				t.Fatal("fixture did not reject the create preflight")
			}
			if submissions.Load() != 0 || f.creates.Load() != 0 || f.dials.Load() != 0 || f.httpCalls.Load() != 3 {
				t.Error("failed create preflight reached transport submission or channel setup")
			}
			f.assertNoBootstrap(t)
			store, ledger, err := openHostedGatewayStore(f.settings.stateDir, f.settings.config)
			if err != nil {
				t.Fatal("could not reopen the preflight fixture ledger")
			}
			defer store.close()
			if ledger.CreateAttempted || ledger.SessionCreated || ledger.ExposurePossible || ledger.PrincipalDigest == "" {
				t.Fatal("definitely-unsent creation retained ambiguous ownership or lost its principal")
			}
			// A new startup may use the same pinned principal after a preflight
			// failure. It still performs the exact-session GET before one POST.
			restarted := &hostedGateway{cfg: f.settings.config, provider: f.gateway.provider,
				httpClient: f.gateway.httpClient, store: store, ledger: ledger}
			if restarted.ensureSession(t.Context()) != nil || submissions.Load() != 1 || f.creates.Load() != 1 || !restarted.ledger.SessionCreated {
				t.Fatal("definitely-unsent startup could not make one later owned creation")
			}
		})
	}
}

func TestHostedStartupCreateReservationPrecedesSubmission(t *testing.T) {
	f := newHostedGatewayTestFixture(t, hostedGatewayTestOptions{})
	transport := f.gateway.httpClient.Transport
	var submissions atomic.Int32
	f.gateway.httpClient.Transport = hostedGatewayTestRoundTripper(func(request *http.Request) (*http.Response, error) {
		if request.Method == http.MethodPost {
			submissions.Add(1)
			data, err := readHostedFile(filepath.Join(f.settings.stateDir, "state.json"), hostedMaxHandshakeBytes)
			var ledger hostedGatewayLedger
			if err != nil || strictjson.Decode(data, &ledger, true) != nil || !ledger.CreateAttempted || ledger.SessionCreated || ledger.ExposurePossible || ledger.PrincipalDigest == "" {
				t.Error("session creation reached transport before durable ownership")
			}
			if request.GetBody != nil {
				t.Error("session creation supplied a replayable request body")
			}
		}
		return transport.RoundTrip(request)
	})
	if f.gateway.ensureSession(t.Context()) != nil || submissions.Load() != 1 || f.creates.Load() != 1 {
		t.Fatal("owned session creation did not complete exactly once")
	}
}

func TestHostedStartupSubmittedCreateErrorRemainsReserved(t *testing.T) {
	f := newHostedGatewayTestFixture(t, hostedGatewayTestOptions{})
	transport := f.gateway.httpClient.Transport
	var submissions atomic.Int32
	f.gateway.httpClient.Transport = hostedGatewayTestRoundTripper(func(request *http.Request) (*http.Response, error) {
		if request.Method == http.MethodPost {
			submissions.Add(1)
			return nil, context.Canceled
		}
		return transport.RoundTrip(request)
	})
	if f.initialize() == nil || submissions.Load() != 1 || f.creates.Load() != 0 {
		t.Fatal("fixture did not fail at transport submission")
	}
	store, ledger, err := openHostedGatewayStore(f.settings.stateDir, f.settings.config)
	if err != nil {
		t.Fatal("could not reopen submitted-create ledger")
	}
	defer store.close()
	if !ledger.CreateAttempted || ledger.SessionCreated || ledger.ExposurePossible {
		t.Fatal("transport error erased an uncertain creation")
	}
	restarted := &hostedGateway{cfg: f.settings.config, provider: f.gateway.provider,
		httpClient: f.gateway.httpClient, store: store, ledger: ledger}
	tokens, requests := f.tokenCalls.Load(), f.httpCalls.Load()
	if restarted.ensureSession(t.Context()) == nil || f.tokenCalls.Load() != tokens || f.httpCalls.Load() != requests || submissions.Load() != 1 {
		t.Fatal("uncertain creation was replayed or adopted after restart")
	}
}

func TestHostedStartupRunnerFailureClassification(t *testing.T) {
	for _, cause := range []string{"uncancelled-error", "local-cancel", "forward-close", "reverse-close"} {
		t.Run(cause, func(t *testing.T) {
			f := newHostedServerTestFixture(t, false)
			entered, released := make(chan struct{}), make(chan struct{})
			release := sync.OnceFunc(func() { close(released) })
			t.Cleanup(release)
			var cancelled atomic.Bool
			f.server.runner = func(ctx context.Context, environment map[string]string) (<-chan error, error) {
				f.calls.Add(1)
				f.captured <- maps.Clone(environment)
				close(entered)
				<-released
				cancelled.Store(ctx.Err() != nil)
				// Match the production runner's rejection before command.Start.
				return nil, errHostedInvalid
			}
			pairID, digest := uuid.NewString(), foundry.JSONDigest(f.protocol.bootstrap)
			forward := f.claim(t, "forward", pairID, digest)
			reverse := hostedServerReverse(t, f.claim(t, "reverse", pairID, digest))
			f.sendBootstrap(t, forward)
			hostedTestDone(t, entered)
			switch cause {
			case "local-cancel":
				f.server.cancel()
			case "forward-close":
				hostedTestGoingAway(t, forward)
			case "reverse-close":
				hostedTestGoingAway(t, reverse.ws)
			}
			if cause != "uncancelled-error" {
				hostedTestDone(t, f.server.ctx.Done())
			}
			release()
			f.server.mu.Lock()
			pair := f.server.pair
			f.server.mu.Unlock()
			hostedTestDone(t, pair.stopped)
			err := f.server.close()
			if cause == "uncancelled-error" {
				if !errors.Is(err, errHostedInvalid) || cancelled.Load() {
					t.Error("actual runner startup failure was reported as clean cancellation")
				}
			} else if err != nil || !cancelled.Load() {
				t.Error("cancellation before runner launch was reported as a local failure")
			}
			if f.calls.Load() != 1 {
				t.Error("failed or cancelled runner launch was retried")
			}
		})
	}
}
