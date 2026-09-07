package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"golang.org/x/net/http2"
)

type hostedGatewayTestOptions struct {
	remote             func(string, map[string]any)
	challenge          func(int, *hostedChallenge)
	accepted           func(*hostedAccepted)
	ready              func(*hostedReady)
	capabilities       func(map[string]any)
	beforeReverseAck   func()
	supervisor         http.Handler
	callback           http.Handler
	preexistingSession bool
	ambiguousCreate    bool
	dropAfterBootstrap bool
}

type hostedGatewayTestTokenProvider func(context.Context) (string, error)

func (f hostedGatewayTestTokenProvider) AccessToken(ctx context.Context) (string, error) {
	return f(ctx)
}

type hostedGatewayTestRoundTripper func(*http.Request) (*http.Response, error)

func (f hostedGatewayTestRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

type hostedGatewayBootstrapWrite struct {
	ledger        hostedGatewayLedger
	persisted     bool
	containsToken bool
}

// The observer runs before the underlying socket writes any bootstrap bytes.
// Only the forward channel is wrapped, and the reverse acknowledgment arms it.
type hostedGatewayTestWriteObserver struct {
	net.Conn
	armed  *atomic.Bool
	before func()
}

func (c *hostedGatewayTestWriteObserver) Write(data []byte) (int, error) {
	if c.armed.CompareAndSwap(true, false) {
		c.before()
	}
	return c.Conn.Write(data)
}

type hostedGatewayTestFixture struct {
	t           *testing.T
	settings    hostedGatewaySettings
	gateway     *hostedGateway
	options     hostedGatewayTestOptions
	server      *httptest.Server
	claims      map[string]any
	token       atomic.Value
	tokenCalls  atomic.Int64
	httpCalls   atomic.Int64
	creates     atomic.Int64
	dials       atomic.Int64
	bootstraps  atomic.Int64
	callbacks   atomic.Int64
	writeArmed  atomic.Bool
	writes      chan hostedGatewayBootstrapWrite
	reverse     chan *http2.ClientConn
	mu          sync.Mutex
	exists      bool
	requests    []string
	hellos      []hostedHello
	peers       []*websocket.Conn
	channelLogs *hostedTestChannelLog
}

func newHostedGatewayTestFixture(t *testing.T, options hostedGatewayTestOptions) *hostedGatewayTestFixture {
	t.Helper()
	p := newHostedProtocolFixture(t)
	f := &hostedGatewayTestFixture{
		t: t, options: options, exists: options.preexistingSession,
		writes: make(chan hostedGatewayBootstrapWrite, 1), reverse: make(chan *http2.ClientConn, 1),
		claims: map[string]any{"aud": "https://ai.azure.com", "tid": uuid.NewString(),
			"oid": uuid.NewString(), "appid": uuid.NewString()},
	}
	f.token.Store(hostedGatewayTestJWT(f.claims))
	f.server = httptest.NewServer(http.HandlerFunc(f.serveHTTP))
	local, err := url.Parse(f.server.URL)
	if err != nil {
		t.Fatal("could not prepare local gateway fixture")
	}
	f.settings = hostedGatewaySettings{
		config: hostedGatewayConfig{
			Protocol: hostedProtocol, Image: p.config,
			ContainerImage: "example.invalid/hosted@" + brokerSHA([]byte("gateway fixture image")),
			SessionID:      p.hello.Challenge.SessionID, RuntimeProfileDigest: brokerSHA([]byte("gateway fixture profile")),
			RuntimeEnvironment: maps.Clone(p.bootstrap.Environment),
			OrkaBaseURL:        f.server.URL, BrokerBaseURL: f.server.URL,
		},
		bootstrap: p.bootstrap, signingKey: bytes.Clone(p.key), stateDir: filepath.Join(t.TempDir(), "gateway"),
	}
	if validateHostedGatewayConfig(f.settings.config) != nil {
		t.Fatal("gateway fixture configuration is invalid")
	}
	store, ledger, err := openHostedGatewayStore(f.settings.stateDir, f.settings.config)
	if err != nil {
		t.Fatal("could not open gateway fixture store")
	}
	ctx, cancel := context.WithCancel(context.Background())
	provider := hostedGatewayTestTokenProvider(func(ctx context.Context) (string, error) {
		f.tokenCalls.Add(1)
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		return f.token.Load().(string), nil
	})
	transport := newHostedLocalTransport()
	remote, _ := url.Parse(f.settings.config.Image.Target.ProjectEndpoint)
	client := &http.Client{Timeout: 5 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return errHostedInvalid },
		Transport: hostedGatewayTestRoundTripper(func(r *http.Request) (*http.Response, error) {
			if r.URL.Scheme != "https" || r.URL.Host != remote.Host || r.GetBody != nil {
				t.Error("gateway changed the configured authority or exposed a replayable request body")
				return nil, errHostedInvalid
			}
			copy := r.Clone(r.Context())
			copy.URL.Scheme, copy.URL.Host, copy.Host = local.Scheme, local.Host, remote.Host
			return transport.RoundTrip(copy)
		})}
	f.gateway = &hostedGateway{cfg: f.settings.config, provider: provider, httpClient: client,
		store: store, ledger: ledger, ctx: ctx, cancel: cancel}
	f.channelLogs = newHostedTestChannelLog()
	f.gateway.channelLog = f.channelLogs.logger
	f.gateway.dial = func(ctx context.Context, endpoint string, headers http.Header) (*websocket.Conn, *http.Response, error) {
		index := f.dials.Add(1)
		expected := "wss://" + remote.Host + remote.Path + "/agents/" + p.config.Target.AgentName +
			"/endpoint/protocols/invocations_ws?api-version=v1&agent_session_id=" + f.settings.config.SessionID
		if endpoint != expected || headers.Get("Authorization") != "Bearer "+f.token.Load().(string) ||
			headers.Get("Foundry-Features") != "HostedAgents=V1Preview" {
			t.Error("gateway WebSocket route, exact session or Azure authorization changed")
			return nil, nil, errHostedInvalid
		}
		dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second,
			NetDialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
				if address != local.Host {
					return nil, errHostedInvalid
				}
				conn, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, network, address)
				if err != nil || index != 1 {
					return conn, err
				}
				return &hostedGatewayTestWriteObserver{Conn: conn, armed: &f.writeArmed, before: f.observeBootstrapWrite}, nil
			}}
		return dialer.DialContext(ctx, "ws://"+local.Host+strings.TrimPrefix(endpoint, "wss://"+remote.Host), headers)
	}
	t.Cleanup(func() {
		f.gateway.close()
		f.mu.Lock()
		peers := append([]*websocket.Conn(nil), f.peers...)
		f.mu.Unlock()
		for _, peer := range peers {
			_ = peer.Close()
		}
		transport.CloseIdleConnections()
		f.server.Close()
		clear(f.settings.signingKey)
	})
	return f
}

func hostedGatewayTestJWT(claims map[string]any) string {
	data, _ := json.Marshal(claims)
	return "fixture-header." + base64.RawURLEncoding.EncodeToString(data) + ".fixture-signature"
}

func (f *hostedGatewayTestFixture) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == brokerStatusPath || r.URL.Path == "/internal/v2/acp/mcp/tools/call" {
		f.callbacks.Add(1)
		if r.Header.Get("Authorization") != "Bearer fixture-operation-authorization" {
			f.t.Error("reverse channel changed caller authorization")
		}
		if f.options.callback != nil {
			f.options.callback.ServeHTTP(w, r)
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}
	target := f.settings.config.Image.Target
	base := "/api/projects/test-project/agents/" + target.AgentName
	if r.URL.Path == base+"/endpoint/protocols/invocations_ws" {
		f.serveWebSocket(w, r)
		return
	}
	f.httpCalls.Add(1)
	f.mu.Lock()
	f.requests = append(f.requests, r.Method+" "+r.URL.RequestURI())
	exists := f.exists
	f.mu.Unlock()
	if r.Header.Get("Authorization") != "Bearer "+f.token.Load().(string) ||
		r.Header.Get("Foundry-Features") != "HostedAgents=V1Preview" || r.URL.RawQuery != "api-version=v1" {
		f.t.Error("gateway HTTP routing or Azure authorization changed")
		http.Error(w, "fixture request rejected", http.StatusBadRequest)
		return
	}
	stage, status := "", http.StatusOK
	var body map[string]any
	switch {
	case r.Method == http.MethodGet && r.URL.Path == base:
		stage = "agent"
		body = map[string]any{"name": target.AgentName, "agent_endpoint": map[string]any{
			"authorization_schemes": []any{map[string]any{"type": "entra"}}}}
	case r.Method == http.MethodGet && r.URL.Path == base+"/versions/"+target.AgentVersion:
		stage = "version"
		body = map[string]any{"name": target.AgentName, "version": target.AgentVersion, "status": "active",
			"definition": map[string]any{"kind": "hosted",
				"container_configuration": map[string]any{"image": f.settings.config.ContainerImage},
				"protocol_versions":       []any{map[string]any{"protocol": "invocations_ws", "version": "2.0.0"}}}}
	case r.Method == http.MethodGet && r.URL.Path == base+brokerSessionSuffix(f.settings.config.SessionID):
		stage, body = "session-get", f.sessionBody()
		if !exists {
			status, body = http.StatusNotFound, map[string]any{}
		}
	case r.Method == http.MethodPost && r.URL.Path == base+"/endpoint/sessions":
		f.creates.Add(1)
		data, err := io.ReadAll(io.LimitReader(r.Body, hostedMaxHandshakeBytes+1))
		var request brokerRemoteSession
		if err != nil || acpDecode(data, &request, true) != nil ||
			request.ID != f.settings.config.SessionID || request.Version.Type != "version_ref" || request.Version.Version != target.AgentVersion {
			f.t.Error("session create omitted the exact session or concrete version")
			http.Error(w, "fixture create rejected", http.StatusBadRequest)
			return
		}
		f.mu.Lock()
		f.exists = true
		f.mu.Unlock()
		if f.options.ambiguousCreate {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				_ = conn.Close()
			}
			return
		}
		stage, status, body = "session-create", http.StatusCreated, f.sessionBody()
	default:
		f.t.Error("gateway called an unexpected Foundry API route")
		http.Error(w, "fixture route rejected", http.StatusNotFound)
		return
	}
	if f.options.remote != nil && status != http.StatusNotFound {
		f.options.remote(stage, body)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func (f *hostedGatewayTestFixture) sessionBody() map[string]any {
	return map[string]any{"agent_session_id": f.settings.config.SessionID, "status": "active",
		"version_indicator": map[string]any{"type": "version_ref", "agent_version": f.settings.config.Image.Target.AgentVersion}}
}

func (f *hostedGatewayTestFixture) serveWebSocket(w http.ResponseWriter, r *http.Request) {
	upgrader := websocket.Upgrader{}
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer ws.Close()
	f.mu.Lock()
	f.peers = append(f.peers, ws)
	index := len(f.peers)
	f.mu.Unlock()
	_ = ws.SetReadDeadline(time.Now().Add(5 * time.Second))
	_ = ws.SetWriteDeadline(time.Now().Add(5 * time.Second))
	challenge := hostedChallenge{
		Protocol: hostedProtocol, DeploymentID: f.settings.config.Image.DeploymentID,
		ConfigurationDigest: brokerJSONDigest(f.settings.config.Image),
		AgentName:           f.settings.config.Image.Target.AgentName, AgentVersion: f.settings.config.Image.Target.AgentVersion,
		SessionID: f.settings.config.SessionID, BootID: f.settings.config.Image.DeploymentID,
		Nonce: base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{11}, 32)),
	}
	if f.options.challenge != nil {
		f.options.challenge(index, &challenge)
	}
	if ws.WriteJSON(challenge) != nil {
		return
	}
	var hello hostedHello
	if readHostedWSJSON(ws, &hello) != nil {
		return
	}
	if verifyHostedHello(hello, challenge, f.settings.config.Image.SigningPublicKey, time.Now()) != nil {
		f.t.Error("gateway sent an invalid signed handshake")
		return
	}
	f.mu.Lock()
	f.hellos = append(f.hellos, hello)
	f.mu.Unlock()
	role := "forward"
	if index == 2 {
		role = "reverse"
	}
	if hello.Role != role {
		f.t.Error("gateway did not assign distinct forward and reverse roles")
		return
	}
	ack := hostedAccepted{Protocol: hostedProtocol, PairID: hello.PairID, Role: role,
		BootID: challenge.BootID, BootstrapDigest: hello.BootstrapDigest}
	if f.options.accepted != nil {
		f.options.accepted(&ack)
	}
	if role == "reverse" {
		f.writeArmed.Store(true)
		if f.options.beforeReverseAck != nil {
			f.options.beforeReverseAck()
		}
	}
	if ws.WriteJSON(ack) != nil {
		return
	}
	if role == "reverse" {
		conn := newHostedWSConn(ws)
		client, err := newHostedHTTP2ClientConn(conn)
		if err != nil {
			return
		}
		defer client.Close()
		f.reverse <- client
		select {
		case <-f.gateway.ctx.Done():
		case <-conn.Done():
		}
		return
	}
	kind, data, err := ws.ReadMessage()
	if err != nil {
		return
	}
	f.bootstraps.Add(1)
	var bootstrap hostedBootstrap
	valid := kind == websocket.TextMessage && brokerSHA(data) == hello.BootstrapDigest &&
		acpDecode(data, &bootstrap, true) == nil && reflect.DeepEqual(bootstrap, f.settings.bootstrap)
	clear(data)
	if !valid {
		f.t.Error("gateway changed the signed bootstrap body")
		return
	}
	if f.options.dropAfterBootstrap {
		return
	}
	ready := hostedReady{Protocol: hostedProtocol, PairID: hello.PairID, BootID: challenge.BootID, Ready: true}
	if f.options.ready != nil {
		f.options.ready(&ready)
	}
	if ws.WriteJSON(ready) != nil {
		return
	}
	serveHostedHTTP2(f.gateway.ctx, newHostedWSConn(ws), http.HandlerFunc(f.serveSupervisor))
}

func (f *hostedGatewayTestFixture) serveSupervisor(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/v2/capabilities" {
		value := map[string]any{"protocol": "orka.harness.v2", "transport": "http+ndjson",
			"runtimeProfileDigest": f.settings.config.RuntimeProfileDigest,
			"adapterDigests":       map[string]string{"foundry-serve-acp": f.settings.config.RuntimeEnvironment["ORKA_ACP_FOUNDRY_ADAPTER_DIGEST"]}}
		if f.options.capabilities != nil {
			f.options.capabilities(value)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(value)
		return
	}
	if f.options.supervisor != nil {
		f.options.supervisor.ServeHTTP(w, r)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (f *hostedGatewayTestFixture) observeBootstrapWrite() {
	data, err := os.ReadFile(filepath.Join(f.settings.stateDir, "state.json"))
	var observed hostedGatewayBootstrapWrite
	observed.persisted = err == nil && acpDecode(data, &observed.ledger, true) == nil &&
		hostedGatewayLedgerValid(observed.ledger, f.settings.config)
	for _, token := range []string{f.settings.bootstrap.ControllerToken, f.settings.bootstrap.CapabilitySecret, f.settings.bootstrap.ProviderToken} {
		observed.containsToken = observed.containsToken || bytes.Contains(data, []byte(token))
	}
	f.writes <- observed
}

func (f *hostedGatewayTestFixture) initialize() error {
	err := f.gateway.initialize(f.settings)
	if err != nil {
		f.gateway.close()
	}
	return err
}

func (f *hostedGatewayTestFixture) assertNoBootstrap(t *testing.T) {
	t.Helper()
	if f.bootstraps.Load() != 0 {
		t.Fatal("gateway disclosed bootstrap after failed admission")
	}
	select {
	case <-f.writes:
		t.Fatal("gateway wrote bootstrap bytes after failed admission")
	default:
	}
}

func TestHostedGatewayChecksEncodedBootstrapBeforeRemoteIO(t *testing.T) {
	for _, test := range []struct {
		name  string
		field int
		fits  bool
	}{
		{name: "escaped controller", field: 0},
		{name: "escaped capability", field: 1},
		{name: "escaped provider", field: 2},
		{name: "maximum raw credentials without expansion", fits: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newHostedGatewayTestFixture(t, hostedGatewayTestOptions{})
			fields := []*string{&f.settings.bootstrap.ControllerToken, &f.settings.bootstrap.CapabilitySecret, &f.settings.bootstrap.ProviderToken}
			if test.fits {
				for _, field := range fields {
					*field = strings.Repeat("x", 16<<10)
				}
			} else {
				*fields[test.field] = strings.Repeat("&", 16<<10)
			}
			if validateHostedBootstrap(f.settings.config.Image, f.settings.bootstrap) != nil {
				t.Fatal("fixture did not supply valid raw bootstrap fields")
			}
			body, err := json.Marshal(f.settings.bootstrap)
			if err != nil || (len(body) <= hostedMaxHandshakeBytes) != test.fits {
				t.Fatal("fixture did not exercise the encoded handshake bound")
			}
			clear(body)
			path := filepath.Join(f.settings.stateDir, "state.json")
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal("could not capture the initial ownership ledger")
			}
			err = f.initialize()
			if test.fits {
				if err != nil || !f.gateway.ready.Load() || f.creates.Load() != 1 || f.dials.Load() != 2 || f.bootstraps.Load() != 1 {
					t.Fatal("valid encoded bootstrap did not establish exactly one lifetime")
				}
				return
			}
			if !errors.Is(err, errHostedInvalid) {
				t.Fatal("oversized encoded bootstrap was not rejected")
			}
			if f.tokenCalls.Load() != 0 || f.httpCalls.Load() != 0 || f.creates.Load() != 0 || f.dials.Load() != 0 {
				t.Error("oversized encoded bootstrap reached remote I/O")
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(before, after) {
				t.Error("oversized encoded bootstrap changed durable ownership")
			}
			f.assertNoBootstrap(t)
		})
	}
}

func TestHostedGatewayBindsLifetimeAndPersistsBeforeBootstrap(t *testing.T) {
	f := newHostedGatewayTestFixture(t, hostedGatewayTestOptions{})
	if f.initialize() != nil || !f.gateway.ready.Load() {
		t.Fatal("valid local hosted lifetime did not become ready")
	}
	var observed hostedGatewayBootstrapWrite
	select {
	case observed = <-f.writes:
	default:
		t.Fatal("bootstrap write was not observed")
	}
	if !observed.persisted || observed.containsToken || !observed.ledger.ExposurePossible ||
		observed.ledger.Ready || observed.ledger.Closed || !observed.ledger.CreateAttempted || !observed.ledger.SessionCreated ||
		observed.ledger.PrincipalDigest == "" || observed.ledger.BootstrapDigest != brokerJSONDigest(f.settings.bootstrap) {
		t.Fatal("complete private exposure record was not durable before the first bootstrap write")
	}
	f.mu.Lock()
	hellos, requests := append([]hostedHello(nil), f.hellos...), append([]string(nil), f.requests...)
	f.mu.Unlock()
	if len(hellos) != 2 || hellos[0].Role != "forward" || hellos[1].Role != "reverse" ||
		hellos[0].PairID != hellos[1].PairID || hellos[0].Challenge != hellos[1].Challenge ||
		hellos[0].BootstrapDigest != hellos[1].BootstrapDigest || observed.ledger.Challenge != hellos[0].Challenge ||
		observed.ledger.PairID != hellos[0].PairID || f.dials.Load() != 2 || f.bootstraps.Load() != 1 {
		t.Fatal("forward and reverse channels did not bind one exact lifetime and bootstrap")
	}
	base := "/api/projects/test-project/agents/" + f.settings.config.Image.Target.AgentName
	want := []string{"GET " + base + "?api-version=v1",
		"GET " + base + "/versions/" + f.settings.config.Image.Target.AgentVersion + "?api-version=v1",
		"GET " + base + brokerSessionSuffix(f.settings.config.SessionID) + "?api-version=v1",
		"POST " + base + "/endpoint/sessions?api-version=v1",
		"GET " + base + brokerSessionSuffix(f.settings.config.SessionID) + "?api-version=v1"}
	if !reflect.DeepEqual(requests, want) || f.creates.Load() != 1 {
		t.Fatal("gateway did not validate, create and confirm exactly the configured session")
	}
	front := httptest.NewServer(f.gateway)
	defer front.Close()
	client := &http.Client{Timeout: 5 * time.Second}
	for path, status := range map[string]int{"/healthz": http.StatusOK, "/v2/health": http.StatusNoContent} {
		response, err := client.Get(front.URL + path)
		if err != nil {
			t.Fatal("ready gateway did not serve its HTTP interface")
		}
		_ = response.Body.Close()
		if response.StatusCode != status {
			t.Fatal("gateway changed the supervisor HTTP status")
		}
	}
	var reverse *http2.ClientConn
	select {
	case reverse = <-f.reverse:
	case <-time.After(5 * time.Second):
		t.Fatal("reverse HTTP/2 channel did not connect")
	}
	for _, endpoint := range []struct{ method, url string }{
		{http.MethodGet, "http://broker" + brokerStatusPath},
		{http.MethodPost, "http://orka/internal/v2/acp/mcp/tools/call"},
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		request, _ := http.NewRequestWithContext(ctx, endpoint.method, endpoint.url, nil)
		request.Header.Set("Authorization", "Bearer fixture-operation-authorization")
		response, err := reverse.RoundTrip(request)
		if err != nil {
			cancel()
			t.Fatal("reverse callback did not reach the fixed upstream")
		}
		_ = response.Body.Close()
		cancel()
		if response.StatusCode != http.StatusNoContent {
			t.Fatal("reverse callback changed the upstream status")
		}
	}
	if f.callbacks.Load() != 2 {
		t.Fatal("reverse callback was lost or replayed")
	}
}

func TestHostedGatewayRejectsRemoteTargetDriftBeforeChannels(t *testing.T) {
	for _, test := range []struct {
		name, stage string
		mutate      func(map[string]any)
	}{
		{"agent name", "agent", func(v map[string]any) { v["name"] = "other-agent" }},
		{"anonymous endpoint", "agent", func(v map[string]any) { v["agent_endpoint"] = map[string]any{"authorization_schemes": []any{}} }},
		{"non-Entra endpoint", "agent", func(v map[string]any) {
			v["agent_endpoint"] = map[string]any{"authorization_schemes": []any{map[string]any{"type": "key"}}}
		}},
		{"version name", "version", func(v map[string]any) { v["name"] = "other-agent" }},
		{"version number", "version", func(v map[string]any) { v["version"] = "9" }},
		{"inactive version", "version", func(v map[string]any) { v["status"] = "inactive" }},
		{"definition kind", "version", func(v map[string]any) { v["definition"].(map[string]any)["kind"] = "prompt" }},
		{"container digest", "version", func(v map[string]any) {
			v["definition"].(map[string]any)["container_configuration"] = map[string]any{"image": "example.invalid/hosted@" + brokerSHA([]byte("other image"))}
		}},
		{"missing WebSocket", "version", func(v map[string]any) { v["definition"].(map[string]any)["protocol_versions"] = []any{} }},
		{"WebSocket protocol version", "version", func(v map[string]any) {
			v["definition"].(map[string]any)["protocol_versions"] = []any{map[string]any{"protocol": "invocations_ws", "version": "1.0.0"}}
		}},
		{"duplicate WebSocket protocol", "version", func(v map[string]any) {
			v["definition"].(map[string]any)["protocol_versions"] = []any{map[string]any{"protocol": "invocations_ws", "version": "2.0.0"}, map[string]any{"protocol": "invocations_ws", "version": "2.0.0"}}
		}},
		{"created session ID", "session-create", func(v map[string]any) { v["agent_session_id"] = uuid.NewString() }},
		{"created session version", "session-create", func(v map[string]any) { v["version_indicator"].(map[string]any)["agent_version"] = "9" }},
		{"session version selector", "session-get", func(v map[string]any) { v["version_indicator"].(map[string]any)["type"] = "latest" }},
		{"confirmed session ID", "session-get", func(v map[string]any) { v["agent_session_id"] = uuid.NewString() }},
		{"inactive session", "session-get", func(v map[string]any) { v["status"] = "inactive" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newHostedGatewayTestFixture(t, hostedGatewayTestOptions{remote: func(stage string, body map[string]any) {
				if stage == test.stage {
					test.mutate(body)
				}
			}})
			if f.initialize() == nil || f.gateway.ready.Load() || f.dials.Load() != 0 {
				t.Fatal("changed remote target reached hosted channel admission")
			}
			f.assertNoBootstrap(t)
		})
	}
	t.Run("preexisting session", func(t *testing.T) {
		f := newHostedGatewayTestFixture(t, hostedGatewayTestOptions{preexistingSession: true})
		if f.initialize() == nil || f.creates.Load() != 0 || f.dials.Load() != 0 {
			t.Fatal("gateway adopted or replaced an existing session without creation ownership")
		}
		f.assertNoBootstrap(t)
	})
}

func TestHostedGatewayRejectsChallengeAndChannelBindingDrift(t *testing.T) {
	for name, mutate := range map[string]func(*hostedChallenge){
		"protocol":      func(c *hostedChallenge) { c.Protocol = "other" },
		"deployment":    func(c *hostedChallenge) { c.DeploymentID = uuid.NewString() },
		"configuration": func(c *hostedChallenge) { c.ConfigurationDigest = brokerSHA([]byte("other configuration")) },
		"agent":         func(c *hostedChallenge) { c.AgentName = "other-agent" },
		"version":       func(c *hostedChallenge) { c.AgentVersion = "9" },
		"session":       func(c *hostedChallenge) { c.SessionID = uuid.NewString() },
		"invalid boot":  func(c *hostedChallenge) { c.BootID = uuid.Nil.String() },
		"invalid nonce": func(c *hostedChallenge) { c.Nonce += "=" },
	} {
		t.Run(name, func(t *testing.T) {
			f := newHostedGatewayTestFixture(t, hostedGatewayTestOptions{challenge: func(_ int, c *hostedChallenge) { mutate(c) }})
			if f.initialize() == nil || f.dials.Load() != 1 {
				t.Fatal("gateway admitted a challenge outside the exact configured target")
			}
			f.assertNoBootstrap(t)
		})
	}
	for name, mutate := range map[string]func(*hostedChallenge){
		"cross-boot pair":  func(c *hostedChallenge) { c.BootID = uuid.NewString() },
		"cross-nonce pair": func(c *hostedChallenge) { c.Nonce = base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{12}, 32)) },
	} {
		t.Run(name, func(t *testing.T) {
			f := newHostedGatewayTestFixture(t, hostedGatewayTestOptions{challenge: func(index int, c *hostedChallenge) {
				if index == 2 {
					mutate(c)
				}
			}})
			if f.initialize() == nil || f.dials.Load() != 2 {
				t.Fatal("gateway paired channels from different hosted challenges")
			}
			f.mu.Lock()
			hellos := len(f.hellos)
			f.mu.Unlock()
			if hellos != 0 {
				t.Fatal("gateway signed a role before matching both hosted challenges")
			}
			f.assertNoBootstrap(t)
		})
	}
	for name, mutate := range map[string]func(*hostedAccepted){
		"protocol":  func(a *hostedAccepted) { a.Protocol = "other" },
		"pair":      func(a *hostedAccepted) { a.PairID = uuid.NewString() },
		"role":      func(a *hostedAccepted) { a.Role = "other" },
		"boot":      func(a *hostedAccepted) { a.BootID = uuid.NewString() },
		"bootstrap": func(a *hostedAccepted) { a.BootstrapDigest = brokerSHA([]byte("other bootstrap")) },
	} {
		for _, role := range []string{"forward", "reverse"} {
			t.Run(role+" acknowledgment "+name, func(t *testing.T) {
				f := newHostedGatewayTestFixture(t, hostedGatewayTestOptions{accepted: func(a *hostedAccepted) {
					if a.Role == role {
						mutate(a)
					}
				}})
				if f.initialize() == nil {
					t.Fatal("gateway accepted a changed signed-role acknowledgment")
				}
				f.assertNoBootstrap(t)
			})
		}
	}
}

func TestHostedGatewayAmbiguousCreateIsNeverReplayedOrAdopted(t *testing.T) {
	f := newHostedGatewayTestFixture(t, hostedGatewayTestOptions{ambiguousCreate: true})
	if f.initialize() == nil || f.creates.Load() != 1 || f.dials.Load() != 0 {
		t.Fatal("gateway continued after losing the create response")
	}
	f.assertNoBootstrap(t)
	store, ledger, err := openHostedGatewayStore(f.settings.stateDir, f.settings.config)
	if err != nil || !ledger.CreateAttempted || ledger.SessionCreated || ledger.ExposurePossible {
		t.Fatal("ambiguous creation intent did not survive restart")
	}
	defer store.close()
	httpBefore, tokensBefore := f.httpCalls.Load(), f.tokenCalls.Load()
	restarted := &hostedGateway{cfg: f.settings.config, provider: f.gateway.provider,
		httpClient: f.gateway.httpClient, store: store, ledger: ledger}
	for range 2 {
		if restarted.ensureSession(context.Background()) == nil {
			t.Fatal("gateway adopted a remotely active session after an ambiguous create")
		}
	}
	if f.httpCalls.Load() != httpBefore || f.tokenCalls.Load() != tokensBefore || f.creates.Load() != 1 {
		t.Fatal("ambiguous create caused a token request, discovery request or replay")
	}
}

func TestHostedGatewayPersistenceFailurePreventsBootstrapWrite(t *testing.T) {
	f := newHostedGatewayTestFixture(t, hostedGatewayTestOptions{})
	f.options.beforeReverseAck = func() {
		if os.Rename(f.settings.stateDir, f.settings.stateDir+"-unavailable") != nil {
			t.Error("could not inject exposure persistence failure")
		}
	}
	if f.initialize() == nil || f.gateway.ready.Load() {
		t.Fatal("gateway became ready after failing to persist exposure")
	}
	f.assertNoBootstrap(t)
}

func TestHostedGatewayPrincipalDriftIsRejectedBeforeNetwork(t *testing.T) {
	for _, claim := range []string{"aud", "tid", "oid", "appid", "azp"} {
		t.Run(claim, func(t *testing.T) {
			f := newHostedGatewayTestFixture(t, hostedGatewayTestOptions{})
			if f.gateway.validateRemote(context.Background()) != nil {
				t.Fatal("valid initial principal was not enrolled")
			}
			changed := maps.Clone(f.claims)
			changed[claim] = uuid.NewString()
			if claim == "aud" {
				changed[claim] = "https://other.invalid"
			}
			f.token.Store(hostedGatewayTestJWT(changed))
			before := f.httpCalls.Load()
			if f.gateway.validateRemote(context.Background()) == nil || f.httpCalls.Load() != before || f.dials.Load() != 0 {
				t.Fatal("a changed Azure principal reached remote HTTP or WebSocket calls")
			}
		})
	}
	t.Run("same principal token refresh", func(t *testing.T) {
		f := newHostedGatewayTestFixture(t, hostedGatewayTestOptions{})
		if f.gateway.validateRemote(context.Background()) != nil {
			t.Fatal("valid initial principal was not enrolled")
		}
		refreshed := maps.Clone(f.claims)
		refreshed["iat"], refreshed["exp"] = time.Now().Unix(), time.Now().Add(time.Hour).Unix()
		f.token.Store(hostedGatewayTestJWT(refreshed))
		if f.gateway.validateRemote(context.Background()) != nil || f.httpCalls.Load() != 4 {
			t.Fatal("normal token refresh changed the enrolled principal")
		}
	})
}

func TestHostedGatewayExposureBlocksRestartAndCredentialChangesBeforeNetwork(t *testing.T) {
	f := newHostedGatewayTestFixture(t, hostedGatewayTestOptions{dropAfterBootstrap: true})
	if f.initialize() == nil || f.bootstraps.Load() != 1 {
		t.Fatal("fixture did not lose transport after bootstrap exposure")
	}
	for name, mutate := range map[string]func(*hostedGatewaySettings){
		"unchanged restart":     func(*hostedGatewaySettings) {},
		"controller credential": func(s *hostedGatewaySettings) { s.bootstrap.ControllerToken = strings.Repeat("changed-controller-", 3) },
		"capability credential": func(s *hostedGatewaySettings) {
			s.bootstrap.CapabilitySecret = strings.Repeat("changed-capability-", 3)
		},
		"provider credential": func(s *hostedGatewaySettings) { s.bootstrap.ProviderToken = strings.Repeat("changed-provider-", 3) },
		"configuration":       func(s *hostedGatewaySettings) { s.config.RuntimeEnvironment["ORKA_ACP_CONTROLLER_EPOCH"] = "2" },
	} {
		t.Run(name, func(t *testing.T) {
			settings := f.settings
			settings.signingKey = bytes.Clone(f.settings.signingKey)
			settings.config.RuntimeEnvironment = maps.Clone(f.settings.config.RuntimeEnvironment)
			settings.bootstrap.Environment = maps.Clone(f.settings.bootstrap.Environment)
			mutate(&settings)
			var called atomic.Int64
			provider := hostedGatewayTestTokenProvider(func(context.Context) (string, error) {
				called.Add(1)
				return "", errors.New("fixture forbids external network")
			})
			gateway, err := newHostedGateway(context.Background(), settings, provider)
			if gateway != nil {
				gateway.close()
			}
			if err == nil || called.Load() != 0 {
				t.Fatal("exposed ownership or credential drift reached Azure token acquisition")
			}
		})
	}
}

func TestHostedGatewayRejectsReadyAndCapabilityDriftAfterExposure(t *testing.T) {
	for name, mutate := range map[string]func(*hostedReady){
		"protocol":  func(r *hostedReady) { r.Protocol = "other" },
		"pair":      func(r *hostedReady) { r.PairID = uuid.NewString() },
		"boot":      func(r *hostedReady) { r.BootID = uuid.NewString() },
		"not ready": func(r *hostedReady) { r.Ready = false },
	} {
		t.Run("ready "+name, func(t *testing.T) {
			f := newHostedGatewayTestFixture(t, hostedGatewayTestOptions{ready: mutate})
			if f.initialize() == nil || f.gateway.ready.Load() || f.bootstraps.Load() != 1 {
				t.Fatal("gateway became ready after a changed hosted-ready acknowledgment")
			}
		})
	}
	for name, mutate := range map[string]func(map[string]any){
		"protocol":  func(v map[string]any) { v["protocol"] = "orka.harness.v1" },
		"transport": func(v map[string]any) { v["transport"] = "other" },
		"profile":   func(v map[string]any) { v["runtimeProfileDigest"] = brokerSHA([]byte("other profile")) },
		"adapter": func(v map[string]any) {
			v["adapterDigests"] = map[string]string{"foundry-serve-acp": brokerSHA([]byte("other adapter"))}
		},
	} {
		t.Run("capabilities "+name, func(t *testing.T) {
			f := newHostedGatewayTestFixture(t, hostedGatewayTestOptions{capabilities: mutate})
			if f.initialize() == nil || f.gateway.ready.Load() || f.bootstraps.Load() != 1 {
				t.Fatal("gateway became ready with a different supervisor profile or adapter")
			}
		})
	}
}

func TestHostedGatewayStreamsWhileOtherRequestsProgress(t *testing.T) {
	release := make(chan struct{})
	var streams atomic.Int64
	f := newHostedGatewayTestFixture(t, hostedGatewayTestOptions{supervisor: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v2/events" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		streams.Add(1)
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.Header().Set("Trailer", "X-Stream-Complete")
		w.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(w, "{\"part\":1}\n")
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		_, _ = io.WriteString(w, "{\"part\":2}\n")
		w.Header().Set("X-Stream-Complete", "yes")
	})})
	if f.initialize() != nil {
		t.Fatal("streaming fixture gateway did not become ready")
	}
	front := httptest.NewServer(f.gateway)
	defer front.Close()
	client := &http.Client{Timeout: 5 * time.Second}
	response, err := client.Get(front.URL + "/v2/events")
	if err != nil {
		t.Fatal("gateway buffered the stream before delivering response headers")
	}
	defer response.Body.Close()
	reader := bufio.NewReader(response.Body)
	first, err := reader.ReadString('\n')
	if err != nil || first != "{\"part\":1}\n" || response.StatusCode != http.StatusAccepted {
		t.Fatal("gateway changed or buffered the first stream record")
	}
	other, err := client.Get(front.URL + "/v2/health")
	if err != nil {
		t.Fatal("open stream blocked another HTTP/2 request")
	}
	_ = other.Body.Close()
	if other.StatusCode != http.StatusNoContent {
		t.Fatal("concurrent request did not reach the supervisor")
	}
	close(release)
	rest, err := io.ReadAll(reader)
	if err != nil || string(rest) != "{\"part\":2}\n" || response.Trailer.Get("X-Stream-Complete") != "yes" || streams.Load() != 1 {
		t.Fatal("gateway lost stream completion, trailers or request ownership")
	}
}

func TestHostedGatewayLostMutationClosesLifetimeWithoutReplay(t *testing.T) {
	var accepted atomic.Int64
	var f *hostedGatewayTestFixture
	f = newHostedGatewayTestFixture(t, hostedGatewayTestOptions{supervisor: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		accepted.Add(1)
		f.mu.Lock()
		forward := f.peers[0]
		f.mu.Unlock()
		_ = forward.Close()
	})})
	if f.initialize() != nil {
		t.Fatal("mutation fixture gateway did not become ready")
	}
	httpBefore, tokensBefore := f.httpCalls.Load(), f.tokenCalls.Load()
	front := httptest.NewServer(f.gateway)
	defer front.Close()
	client := &http.Client{Timeout: 5 * time.Second}
	response, err := client.Post(front.URL+"/v2/sessions/fixture/operations", "application/json", strings.NewReader(`{"operation":"fixture"}`))
	if err != nil {
		t.Fatal("gateway did not report the failed mutation transport")
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusBadGateway || accepted.Load() != 1 {
		t.Fatal("ambiguous mutation was replayed or reported as successful")
	}
	select {
	case <-f.gateway.ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("lost hosted channel did not close the gateway lifetime")
	}
	f.gateway.close()
	denied := httptest.NewRecorder()
	f.gateway.ServeHTTP(denied, httptest.NewRequest(http.MethodPost, "/v2/sessions/fixture/operations", strings.NewReader(`{}`)))
	if denied.Code != http.StatusServiceUnavailable || accepted.Load() != 1 || f.httpCalls.Load() != httpBefore ||
		f.tokenCalls.Load() != tokensBefore || f.dials.Load() != 2 || f.bootstraps.Load() != 1 {
		t.Fatal("closed gateway retried work, reconnected or admitted a replacement request")
	}
	var ledger hostedGatewayLedger
	if readHostedConfig(filepath.Join(f.settings.stateDir, "state.json"), &ledger) != nil || !ledger.ExposurePossible || !ledger.Closed {
		t.Fatal("lost channel erased durable possible exposure")
	}
}

func hostedGatewayTestClosedWithoutReplay(t *testing.T, f *hostedGatewayTestFixture, httpBefore, tokensBefore, callbacks int64) {
	t.Helper()
	hostedTestDone(t, f.gateway.ctx.Done())
	f.gateway.close()
	denied := httptest.NewRecorder()
	f.gateway.ServeHTTP(denied, httptest.NewRequest(http.MethodPost, "/v2/sessions/fixture/operations", strings.NewReader(`{}`)))
	if denied.Code != http.StatusServiceUnavailable || f.callbacks.Load() != callbacks || f.httpCalls.Load() != httpBefore ||
		f.tokenCalls.Load() != tokensBefore || f.creates.Load() != 1 || f.dials.Load() != 2 || f.bootstraps.Load() != 1 {
		t.Fatal("platform-closed gateway retried, reconnected or admitted a new operation")
	}
	var ledger hostedGatewayLedger
	if readHostedConfig(filepath.Join(f.settings.stateDir, "state.json"), &ledger) != nil || !ledger.ExposurePossible || !ledger.Closed {
		t.Fatal("platform closure lost durable possible exposure")
	}
	settings := f.settings
	settings.signingKey = bytes.Clone(f.settings.signingKey)
	var tokens atomic.Int64
	provider := hostedGatewayTestTokenProvider(func(context.Context) (string, error) {
		tokens.Add(1)
		return "", errHostedInvalid
	})
	restarted, err := newHostedGateway(context.Background(), settings, provider)
	if restarted != nil {
		restarted.close()
	}
	if err == nil || restarted != nil || tokens.Load() != 0 {
		t.Fatal("platform-closed lifetime could reseed or reach remote authentication after restart")
	}
}

func TestHostedGatewayIdleGoingAwayClosesLifetimeWithoutReseed(t *testing.T) {
	for index, role := range []string{"forward", "reverse"} {
		t.Run(role, func(t *testing.T) {
			f := newHostedGatewayTestFixture(t, hostedGatewayTestOptions{})
			earliest := time.Now()
			if f.initialize() != nil {
				t.Fatal("idle gateway fixture did not become ready")
			}
			latest := time.Now()
			httpBefore, tokensBefore := f.httpCalls.Load(), f.tokenCalls.Load()
			f.mu.Lock()
			peer := f.peers[index]
			f.mu.Unlock()
			hostedTestGoingAway(t, peer)
			hostedGatewayTestClosedWithoutReplay(t, f, httpBefore, tokensBefore, 0)
			hostedTestClosedChannelLogs(t, f.channelLogs, role, earliest, latest)
		})
	}
}

func TestHostedGatewayReverseGoingAwayAfterAcceptanceDoesNotReplay(t *testing.T) {
	var accepted atomic.Int64
	var f *hostedGatewayTestFixture
	f = newHostedGatewayTestFixture(t, hostedGatewayTestOptions{callback: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			t.Error("synthetic tool request body was not accepted")
			return
		}
		accepted.Add(1)
		f.mu.Lock()
		peer := f.peers[1]
		f.mu.Unlock()
		if peer.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseGoingAway, hostedTestCloseText), time.Now().Add(time.Second)) != nil {
			t.Error("could not close the reverse channel after synthetic tool acceptance")
			return
		}
		// Never return a successful response before transport loss is observed.
		select {
		case <-r.Context().Done():
		case <-time.After(3 * time.Second):
			t.Error("reverse transport loss did not cancel the accepted callback")
		}
	})})
	earliest := time.Now()
	if f.initialize() != nil {
		t.Fatal("reverse acceptance fixture did not become ready")
	}
	latest := time.Now()
	httpBefore, tokensBefore := f.httpCalls.Load(), f.tokenCalls.Load()
	var reverse *http2.ClientConn
	select {
	case reverse = <-f.reverse:
	case <-time.After(3 * time.Second):
		t.Fatal("reverse fixture was not established")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for attempt := range 2 {
		request, _ := http.NewRequestWithContext(ctx, http.MethodPost, "http://orka/internal/v2/acp/mcp/tools/call", strings.NewReader(`{"tool":"synthetic"}`))
		request.GetBody = nil
		request.Header.Set("Authorization", "Bearer fixture-operation-authorization")
		response, err := reverse.RoundTrip(request)
		if response != nil {
			_ = response.Body.Close()
		}
		if err == nil || response != nil || accepted.Load() != 1 {
			t.Fatal("ambiguous reverse mutation succeeded or was replayed")
		}
		if attempt == 0 {
			hostedGatewayTestClosedWithoutReplay(t, f, httpBefore, tokensBefore, 1)
		}
	}
	if f.callbacks.Load() != 1 {
		t.Fatal("a second reverse callback reached the backend after channel closure")
	}
	hostedTestClosedChannelLogs(t, f.channelLogs, "reverse", earliest, latest)
}
