package hosted

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/orka-agents/agent-runtime-foundry/internal/foundry"
	"github.com/orka-agents/agent-runtime-foundry/internal/strictjson"
)

type hostedWebSocketDial func(context.Context, string, http.Header) (*websocket.Conn, *http.Response, error)

type hostedGateway struct {
	cfg                                    hostedGatewayConfig
	provider                               foundry.TokenProvider
	httpClient                             *http.Client
	dial                                   hostedWebSocketDial
	store                                  *hostedGatewayStore
	ledger                                 hostedGatewayLedger
	ctx                                    context.Context
	cancel                                 context.CancelFunc
	mu                                     sync.Mutex
	closeOnce                              sync.Once
	ready                                  atomic.Bool
	handler                                http.Handler
	forward, reverse                       *websocket.Conn
	forwardObservation, reverseObservation *hostedChannelObservation
	channelLog                             *log.Logger
}

func newHostedGateway(ctx context.Context, settings hostedGatewaySettings, provider foundry.TokenProvider) (*hostedGateway, error) {
	defer clear(settings.signingKey)
	if validateHostedGatewayConfig(settings.config) != nil || validateHostedBootstrap(settings.config.Image, settings.bootstrap) != nil || provider == nil {
		return nil, errHostedInvalid
	}
	store, ledger, err := openHostedGatewayStore(settings.stateDir, settings.config)
	if err != nil {
		return nil, err
	}
	// This check precedes Azure calls and channel setup, including after a
	// process/pod restart. No automatic replacement can inherit unknown work.
	if ledger.ExposurePossible {
		store.close()
		return nil, errHostedInvalid
	}
	lifetime, cancel := context.WithCancel(ctx)
	transport := newHostedLocalTransport()
	dialer := &websocket.Dialer{HandshakeTimeout: 120 * time.Second,
		NetDialContext: (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: -1}).DialContext,
		ReadBufferSize: 4096, WriteBufferSize: 4096}
	g := &hostedGateway{cfg: settings.config, provider: provider, store: store, ledger: ledger,
		ctx: lifetime, cancel: cancel, dial: dialer.DialContext,
		httpClient: &http.Client{Transport: transport, Timeout: 120 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error { return errHostedInvalid }}}
	if err := g.initialize(settings); err != nil {
		g.close()
		return nil, err
	}
	return g, nil
}

func (g *hostedGateway) initialize(settings hostedGatewaySettings) error {
	// JSON escaping can exceed the wire limit even when each raw field is valid.
	// Reject that configuration before creating or consuming a hosted lifetime.
	body, err := json.Marshal(settings.bootstrap)
	defer clear(body)
	if err != nil || len(body) > hostedMaxHandshakeBytes {
		return errHostedInvalid
	}
	ctx, cancel := context.WithTimeout(g.ctx, hostedInitializationWait)
	defer cancel()
	if g.validateRemote(ctx) != nil || g.ensureSession(ctx) != nil {
		return errHostedInvalid
	}
	forward, challenge, err := g.openChannel(ctx, "forward")
	if err != nil {
		return err
	}
	g.forward = forward
	reverse, otherChallenge, err := g.openChannel(ctx, "reverse")
	if err != nil {
		return err
	}
	g.reverse = reverse
	if otherChallenge != challenge {
		return errHostedInvalid
	}
	bootstrapDigest, pairID := foundry.Digest(body), uuid.NewString()
	for _, channel := range []struct {
		ws   *websocket.Conn
		role string
	}{{forward, "forward"}, {reverse, "reverse"}} {
		hello, err := signHostedHello(hostedHello{Challenge: challenge, PairID: pairID, Role: channel.role,
			BootstrapDigest: bootstrapDigest, ExpiresAt: time.Now().Add(60 * time.Second).Unix()}, settings.signingKey)
		_ = channel.ws.SetWriteDeadline(time.Now().Add(10 * time.Second))
		if err != nil || channel.ws.WriteJSON(hello) != nil {
			return errHostedInvalid
		}
		var ack hostedAccepted
		_ = channel.ws.SetReadDeadline(time.Now().Add(30 * time.Second))
		if readHostedWSJSON(channel.ws, &ack) != nil || ack != (hostedAccepted{Protocol: hostedProtocol,
			PairID: pairID, Role: channel.role, BootID: challenge.BootID, BootstrapDigest: bootstrapDigest}) {
			return errHostedInvalid
		}
	}
	reverseHandler, err := g.reverseHandler()
	if err != nil {
		return err
	}
	reverseConn := newHostedObservedWSConn(reverse, g.reverseObservation)
	go serveHostedHTTP2(g.ctx, reverseConn, reverseHandler)
	// Fsync this record BEFORE sending even the first bootstrap byte. Failure
	// after this point requires explicit retirement and a fresh deployment.
	next := g.ledger
	next.ExposurePossible, next.Challenge, next.PairID, next.BootstrapDigest = true, challenge, pairID, bootstrapDigest
	if g.store.save(next) != nil {
		return errHostedInvalid
	}
	g.ledger = next
	_ = forward.SetWriteDeadline(time.Now().Add(10 * time.Second))
	if forward.WriteMessage(websocket.TextMessage, body) != nil {
		return errHostedInvalid
	}
	_ = forward.SetReadDeadline(time.Now().Add(45 * time.Second))
	var ready hostedReady
	if readHostedWSJSON(forward, &ready) != nil || ready != (hostedReady{Protocol: hostedProtocol,
		PairID: pairID, BootID: challenge.BootID, Ready: true}) {
		return errHostedInvalid
	}
	forwardConn := newHostedObservedWSConn(forward, g.forwardObservation)
	client, err := newHostedHTTP2ClientConn(forwardConn)
	if err != nil {
		return err
	}
	if g.validateCapabilities(ctx, client) != nil {
		return errHostedInvalid
	}
	handler, err := newHostedProxy("http://supervisor", client, hostedV2Route)
	if err != nil {
		return err
	}
	g.handler = handler
	next = g.ledger
	next.Ready = true
	if g.store.save(next) != nil {
		return errHostedInvalid
	}
	g.ledger = next
	g.ready.Store(true)
	go func() {
		select {
		case <-g.ctx.Done():
		case <-forwardConn.Done():
		case <-reverseConn.Done():
		}
		g.close()
	}()
	return nil
}

func (g *hostedGateway) reverseHandler() (http.Handler, error) {
	broker, err := newHostedProxy(g.cfg.BrokerBaseURL, newHostedLocalTransport(), hostedBrokerRoute)
	if err != nil {
		return nil, err
	}
	orka, err := newHostedProxy(g.cfg.OrkaBaseURL, newHostedLocalTransport(), hostedOrkaRoute)
	if err != nil {
		return nil, err
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Host {
		case "broker":
			broker.ServeHTTP(w, r)
		case "orka":
			orka.ServeHTTP(w, r)
		default:
			http.Error(w, "hosted authority denied", http.StatusForbidden)
		}
	}), nil
}

func (g *hostedGateway) validateCapabilities(ctx context.Context, transport http.RoundTripper) error {
	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://supervisor/v2/capabilities", nil)
	response, err := transport.RoundTrip(request)
	if err != nil {
		return errHostedInvalid
	}
	defer response.Body.Close() //nolint:errcheck
	data, err := io.ReadAll(io.LimitReader(response.Body, hostedMaxHandshakeBytes+1))
	var value struct {
		Protocol             string            `json:"protocol"`
		Transport            string            `json:"transport"`
		RuntimeProfileDigest string            `json:"runtimeProfileDigest"`
		AdapterDigests       map[string]string `json:"adapterDigests"`
	}
	if err != nil || len(data) > hostedMaxHandshakeBytes || response.StatusCode != http.StatusOK ||
		strictjson.Decode(data, &value, false) != nil || value.Protocol != "orka.harness.v2" || value.Transport != "http+ndjson" ||
		value.RuntimeProfileDigest != g.cfg.RuntimeProfileDigest ||
		value.AdapterDigests["foundry-serve-acp"] != g.cfg.RuntimeEnvironment["ORKA_ACP_FOUNDRY_ADAPTER_DIGEST"] {
		return errHostedInvalid
	}
	return nil
}

func (g *hostedGateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !g.ready.Load() || g.ctx.Err() != nil {
		http.Error(w, "hosted lifetime unavailable; acceptance may be unknown", http.StatusServiceUnavailable)
		return
	}
	if r.Method == http.MethodGet && r.URL.Path == "/healthz" && r.URL.RawQuery == "" {
		w.WriteHeader(http.StatusOK)
		return
	}
	g.handler.ServeHTTP(w, r)
}

func (g *hostedGateway) close() {
	g.closeOnce.Do(func() {
		g.ready.Store(false)
		g.cancel()
		if g.forward != nil {
			_ = g.forward.Close()
		}
		if g.reverse != nil {
			_ = g.reverse.Close()
		}
		g.forwardObservation.finish()
		g.reverseObservation.finish()
		g.mu.Lock()
		if g.ledger.ExposurePossible {
			g.ledger.Closed = true
			_ = g.store.save(g.ledger) // ExposurePossible remains durable even if this write fails.
		}
		g.store.close()
		g.mu.Unlock()
		g.httpClient.CloseIdleConnections()
	})
}
