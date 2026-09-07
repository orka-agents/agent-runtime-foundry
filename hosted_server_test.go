package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
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

type hostedServerTestFixture struct {
	protocol    hostedProtocolFixture
	server      *hostedServer
	http        *httptest.Server
	calls       atomic.Int32
	captured    chan map[string]string
	stopped     chan struct{}
	runnerOK    bool
	channelLogs *hostedTestChannelLog
}

func newHostedServerTestFixture(t *testing.T, runnerOK bool) *hostedServerTestFixture {
	t.Helper()
	f := &hostedServerTestFixture{protocol: newHostedProtocolFixture(t), captured: make(chan map[string]string, 8),
		stopped: make(chan struct{}, 8), runnerOK: runnerOK}
	agent, err := json.Marshal(acpAgentConfiguration{Model: "test-model", ToolSchemaMode: toolSchemaModeRequest,
		HostedTarget: f.protocol.config.Target})
	if err != nil {
		t.Fatal("could not prepare hosted agent fixture")
	}
	f.protocol.config.AgentConfigurationDigest = brokerSHA(agent)
	f.protocol.bootstrap.Environment["ORKA_ACP_AGENT_CONFIGURATION_DIGEST"] = brokerSHA(agent)
	f.protocol.hello.Challenge.ConfigurationDigest = brokerJSONDigest(f.protocol.config)
	f.protocol.hello.BootstrapDigest = brokerJSONDigest(f.protocol.bootstrap)
	if validateHostedBootstrap(f.protocol.config, f.protocol.bootstrap) != nil {
		t.Fatal("invalid canonical bootstrap fixture")
	}
	if _, err := decodeACPAgentConfiguration(agent, f.protocol.config.AgentConfigurationDigest, "test-model"); err != nil {
		t.Fatal("invalid canonical agent configuration fixture")
	}
	ctx, cancel := context.WithCancel(context.Background())
	f.server = &hostedServer{
		cfg: f.protocol.config, challenge: f.protocol.hello.Challenge, agentConfig: agent,
		adapterDigest: f.protocol.bootstrap.Environment["ORKA_ACP_FOUNDRY_ADAPTER_DIGEST"], ctx: ctx, cancel: cancel,
	}
	f.channelLogs = newHostedTestChannelLog()
	f.server.channelLog = f.channelLogs.logger
	f.server.runner = func(ctx context.Context, env map[string]string) (<-chan error, error) {
		f.calls.Add(1)
		f.captured <- maps.Clone(env)
		if !f.runnerOK {
			return nil, errors.New("fixture runner deliberately unavailable")
		}
		done := make(chan error, 1)
		go func() {
			<-ctx.Done()
			done <- ctx.Err()
			close(done)
			f.stopped <- struct{}{}
		}()
		return done, nil
	}
	var handlers sync.WaitGroup
	f.http = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlers.Add(1)
		defer handlers.Done()
		f.server.ServeHTTP(w, r)
	}))
	t.Cleanup(func() {
		cancel()
		f.server.mu.Lock()
		pair := f.server.pair
		f.server.mu.Unlock()
		if pair != nil {
			pair.close()
		}
		f.http.Close()
		finished := make(chan struct{})
		go func() { handlers.Wait(); close(finished) }()
		hostedTestDone(t, finished)
		for len(f.captured) > 0 {
			env := <-f.captured
			hostedServerRemoveTestDirectory(t, env["ORKA_ACP_SESSION_BASE_DIR"])
		}
		if f.calls.Load() > 0 {
			// Pair closure can precede run's deferred listener closes. Wait only
			// for the two fixture-owned relay listeners before the next test.
			for _, address := range []string{hostedBrokerRelayAddr, hostedOrkaRelayAddr} {
				hostedServerWait(t, func() bool {
					listener, err := net.Listen("tcp", address)
					if err != nil {
						return false
					}
					_ = listener.Close()
					return true
				})
			}
		}
	})
	return f
}

func hostedServerRemoveTestDirectory(t *testing.T, path string) {
	t.Helper()
	if filepath.Dir(path) != "/tmp" || !strings.HasPrefix(filepath.Base(path), "orka-hosted-sessions-") {
		t.Error("runner did not receive a dedicated temporary session directory")
		return
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		t.Error("could not remove empty test session directory")
	}
}

func hostedServerWait(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("hosted fixture state did not settle")
		case <-tick.C:
		}
	}
}

func (f *hostedServerTestFixture) connect(t *testing.T) *websocket.Conn {
	t.Helper()
	ws, response, err := (&websocket.Dialer{HandshakeTimeout: 3 * time.Second}).Dial(
		"ws"+strings.TrimPrefix(f.http.URL, "http")+"/invocations_ws", nil)
	if response != nil && response.Body != nil {
		_ = response.Body.Close()
	}
	if err != nil {
		t.Fatal("localhost hosted WebSocket upgrade failed")
	}
	t.Cleanup(func() { _ = ws.Close() })
	_ = ws.SetReadDeadline(time.Now().Add(3 * time.Second))
	var challenge hostedChallenge
	if readHostedWSJSON(ws, &challenge) != nil || challenge != f.server.challenge {
		t.Fatal("hosted server did not send its exact challenge")
	}
	return ws
}

func (f *hostedServerTestFixture) hello(t *testing.T, role, pairID, digest string) hostedHello {
	t.Helper()
	return f.protocol.sign(t, hostedHello{Challenge: f.server.challenge, PairID: pairID, Role: role,
		BootstrapDigest: digest, ExpiresAt: time.Now().Add(time.Minute).Unix()})
}

func (f *hostedServerTestFixture) sendBootstrap(t *testing.T, ws *websocket.Conn) {
	t.Helper()
	data, err := json.Marshal(f.protocol.bootstrap)
	if err != nil || brokerSHA(data) != f.protocol.hello.BootstrapDigest || ws.WriteMessage(websocket.TextMessage, data) != nil {
		t.Fatal("could not send exact signed bootstrap bytes")
	}
}

func (f *hostedServerTestFixture) claim(t *testing.T, role, pairID, digest string) *websocket.Conn {
	t.Helper()
	ws := f.connect(t)
	if ws.WriteJSON(f.hello(t, role, pairID, digest)) != nil {
		t.Fatal("could not send signed fixture hello")
	}
	var ack hostedAccepted
	if readHostedWSJSON(ws, &ack) != nil || ack != (hostedAccepted{Protocol: hostedProtocol,
		PairID: pairID, Role: role, BootID: f.server.challenge.BootID, BootstrapDigest: digest}) {
		t.Fatal("authenticated role was not acknowledged exactly")
	}
	return ws
}

func hostedServerRejected(t *testing.T, ws *websocket.Conn) {
	t.Helper()
	_ = ws.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, _, err := ws.ReadMessage()
	var networkError net.Error
	if err == nil || errors.As(err, &networkError) && networkError.Timeout() {
		t.Fatal("invalid handshake was acknowledged or left pending")
	}
}

func (f *hostedServerTestFixture) state(condition func(*hostedPair) bool) bool {
	f.server.mu.Lock()
	defer f.server.mu.Unlock()
	return condition(f.server.pair)
}

func (f *hostedServerTestFixture) receiveRunner(t *testing.T) map[string]string {
	t.Helper()
	select {
	case env := <-f.captured:
		t.Cleanup(func() { hostedServerRemoveTestDirectory(t, env["ORKA_ACP_SESSION_BASE_DIR"]) })
		return env
	case <-time.After(3 * time.Second):
		t.Fatal("authenticated pair did not reach the fixture runner")
		return nil
	}
}

func hostedServerReverse(t *testing.T, ws *websocket.Conn) *hostedWSConn {
	t.Helper()
	conn := newHostedWSConn(ws)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		serveHostedHTTP2(ctx, conn, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		}))
	}()
	t.Cleanup(func() {
		cancel()
		_ = conn.Close()
		hostedTestDone(t, done)
	})
	return conn
}

func TestHostedServerUnauthenticatedInputCannotReservePair(t *testing.T) {
	for _, name := range []string{"missing signature", "invalid signature", "different boot", "expired", "binary hello", "bootstrap before hello", "malformed JSON"} {
		t.Run(name, func(t *testing.T) {
			f := newHostedServerTestFixture(t, false)
			ws := f.connect(t)
			hello := f.hello(t, "forward", uuid.NewString(), brokerJSONDigest(f.protocol.bootstrap))
			kind := websocket.TextMessage
			switch name {
			case "missing signature":
				hello.Signature = ""
			case "invalid signature":
				hello.Signature = strings.Repeat("a", len(hello.Signature))
			case "different boot":
				hello.Challenge.BootID = uuid.NewString()
				hello = f.protocol.sign(t, hello)
			case "expired":
				hello.ExpiresAt = time.Now().Add(-time.Second).Unix()
				hello = f.protocol.sign(t, hello)
			case "binary hello":
				kind = websocket.BinaryMessage
			}
			data, _ := json.Marshal(hello)
			if name == "bootstrap before hello" {
				data, _ = json.Marshal(f.protocol.bootstrap)
			} else if name == "malformed JSON" {
				data = []byte("{")
			}
			if ws.WriteMessage(kind, data) != nil {
				t.Fatal("could not send invalid admission fixture")
			}
			hostedServerRejected(t, ws)
			if !f.state(func(p *hostedPair) bool { return p == nil }) || f.calls.Load() != 0 || f.server.ctx.Err() != nil {
				t.Fatal("unauthenticated input reserved, launched or consumed the hosted lifetime")
			}
		})
	}
}

func TestHostedServerConflictingClaimsCannotReplaceAuthenticatedRole(t *testing.T) {
	for _, name := range []string{"forward replay", "different pair", "different bootstrap"} {
		t.Run(name, func(t *testing.T) {
			f := newHostedServerTestFixture(t, false)
			pairID, digest := uuid.NewString(), brokerJSONDigest(f.protocol.bootstrap)
			_ = f.claim(t, "forward", pairID, digest)
			hostedServerWait(t, func() bool { return f.state(func(p *hostedPair) bool { return p != nil && p.forwardAcked }) })
			f.server.mu.Lock()
			original := f.server.pair.forward
			f.server.mu.Unlock()
			role, otherPair, otherDigest := "reverse", pairID, digest
			switch name {
			case "forward replay":
				role = "forward"
			case "different pair":
				otherPair = uuid.NewString()
			case "different bootstrap":
				otherDigest = brokerSHA([]byte("different bootstrap"))
			}
			ws := f.connect(t)
			if ws.WriteJSON(f.hello(t, role, otherPair, otherDigest)) != nil {
				t.Fatal("could not send conflicting fixture claim")
			}
			hostedServerRejected(t, ws)
			if !f.state(func(p *hostedPair) bool {
				return p.id == pairID && p.bootstrapDigest == digest && p.forward == original && p.reverse == nil && p.bootstrap == nil && !p.running
			}) || f.calls.Load() != 0 || f.server.ctx.Err() != nil {
				t.Fatal("conflicting claim replaced or disturbed admitted ownership")
			}
		})
	}
}

func TestHostedServerConcurrentRoleReplayHasSingleWinner(t *testing.T) {
	f := newHostedServerTestFixture(t, false)
	pairID, digest := uuid.NewString(), brokerJSONDigest(f.protocol.bootstrap)
	hello := f.hello(t, "forward", pairID, digest)
	const count = 8
	results := make(chan *websocket.Conn, count)
	for range count {
		go func() {
			ws, response, err := (&websocket.Dialer{HandshakeTimeout: 3 * time.Second}).Dial(
				"ws"+strings.TrimPrefix(f.http.URL, "http")+"/invocations_ws", nil)
			if response != nil && response.Body != nil {
				_ = response.Body.Close()
			}
			if err != nil {
				results <- nil
				return
			}
			_ = ws.SetReadDeadline(time.Now().Add(3 * time.Second))
			var challenge hostedChallenge
			var ack hostedAccepted
			if readHostedWSJSON(ws, &challenge) == nil && challenge == hello.Challenge && ws.WriteJSON(hello) == nil &&
				readHostedWSJSON(ws, &ack) == nil && ack.PairID == pairID && ack.Role == "forward" {
				results <- ws
				return
			}
			_ = ws.Close()
			results <- nil
		}()
	}
	winners := 0
	for range count {
		select {
		case ws := <-results:
			if ws != nil {
				winners++
				t.Cleanup(func() { _ = ws.Close() })
			}
		case <-time.After(5 * time.Second):
			t.Fatal("concurrent claims did not finish")
		}
	}
	if winners != 1 || f.calls.Load() != 0 || !f.state(func(p *hostedPair) bool {
		return p != nil && p.id == pairID && p.forward != nil && p.reverse == nil && p.bootstrap == nil && !p.running
	}) {
		t.Fatal("concurrent replay admitted more than one role or launched early")
	}
}

func TestHostedServerRejectsMalformedOrUntrustedBootstrapBeforeLaunch(t *testing.T) {
	for _, name := range []string{"wrong digest", "binary bootstrap", "unknown member", "duplicate member", "missing controller token", "model mismatch", "adapter mismatch", "inherited identity", "supervisor override", "invalid agent config"} {
		t.Run(name, func(t *testing.T) {
			f := newHostedServerTestFixture(t, false)
			bootstrap := f.protocol.bootstrap
			bootstrap.Environment = maps.Clone(bootstrap.Environment)
			switch name {
			case "missing controller token":
				bootstrap.ControllerToken = ""
			case "model mismatch":
				bootstrap.Environment["ORKA_ACP_MODEL"] = "other-model"
			case "adapter mismatch":
				bootstrap.Environment["ORKA_ACP_FOUNDRY_ADAPTER_DIGEST"] = brokerSHA([]byte("other adapter"))
			case "inherited identity":
				bootstrap.Environment["IDENTITY_HEADER"] = "fixture-untrusted-identity"
			case "supervisor override":
				bootstrap.Environment["ORKA_ACP_SUPERVISOR_BOOT_ID"] = uuid.NewString()
			case "invalid agent config":
				f.server.agentConfig = append(bytes.Clone(f.server.agentConfig), '\n')
			}
			data, _ := json.Marshal(bootstrap)
			if name == "unknown member" {
				data = append([]byte(`{"injected":true,`), data[1:]...)
			} else if name == "duplicate member" {
				data = append([]byte(`{"environment":{},`), data[1:]...)
			}
			digest := brokerSHA(data)
			if name == "wrong digest" {
				digest = brokerSHA([]byte("other bytes"))
			}
			pairID := uuid.NewString()
			forward := f.claim(t, "forward", pairID, digest)
			_ = f.claim(t, "reverse", pairID, digest)
			kind := websocket.TextMessage
			if name == "binary bootstrap" {
				kind = websocket.BinaryMessage
			}
			if forward.WriteMessage(kind, data) != nil {
				t.Fatal("could not send invalid bootstrap fixture")
			}
			hostedTestDone(t, f.server.ctx.Done())
			if f.calls.Load() != 0 || !f.state(func(p *hostedPair) bool { return p != nil && p.bootstrap == nil && !p.running }) {
				t.Fatal("untrusted bootstrap reached the supervisor runner")
			}
		})
	}
}

func TestHostedServerRequiresBothRolesAndExactConstructedEnvironment(t *testing.T) {
	for _, order := range []string{"forward first", "reverse first"} {
		t.Run(order, func(t *testing.T) {
			for _, name := range []string{"IDENTITY_HEADER", "IDENTITY_ENDPOINT", "AZURE_CLIENT_ID", "AZURE_TENANT_ID", "AZURE_CLIENT_SECRET", "AZURE_FEDERATED_TOKEN_FILE", "MSI_SECRET"} {
				t.Setenv(name, "fixture-parent-only")
			}
			f := newHostedServerTestFixture(t, false)
			pairID, digest := uuid.NewString(), brokerJSONDigest(f.protocol.bootstrap)
			var forward, reverse *websocket.Conn
			if order == "forward first" {
				forward = f.claim(t, "forward", pairID, digest)
				f.sendBootstrap(t, forward)
				hostedServerWait(t, func() bool { return f.state(func(p *hostedPair) bool { return p.bootstrap != nil }) })
			} else {
				reverse = f.claim(t, "reverse", pairID, digest)
				hostedServerWait(t, func() bool { return f.state(func(p *hostedPair) bool { return p.reverseAcked }) })
			}
			if f.calls.Load() != 0 || !f.state(func(p *hostedPair) bool { return p != nil && !p.running }) {
				t.Fatal("a single authenticated role launched the supervisor")
			}
			if order == "forward first" {
				reverse = f.claim(t, "reverse", pairID, digest)
			} else {
				forward = f.claim(t, "forward", pairID, digest)
				hostedServerWait(t, func() bool { return f.state(func(p *hostedPair) bool { return p.forwardAcked && p.reverseAcked }) })
				if f.calls.Load() != 0 || !f.state(func(p *hostedPair) bool { return p.bootstrap == nil && !p.running }) {
					t.Fatal("authenticated roles launched without the signed bootstrap")
				}
			}
			_ = hostedServerReverse(t, reverse)
			if order == "reverse first" {
				f.sendBootstrap(t, forward)
			}
			env := f.receiveRunner(t)
			expected := maps.Clone(f.protocol.bootstrap.Environment)
			for key, value := range map[string]string{
				"PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin", "HOME": "/tmp",
				"ORKA_ACP_LISTEN_ADDRESS":               hostedSupervisorAddr,
				"ORKA_ACP_RUNTIME_INSTANCE_ID":          "foundry-hosted." + f.server.challenge.BootID,
				"ORKA_ACP_SUPERVISOR_BOOT_ID":           f.server.challenge.BootID,
				"ORKA_ACP_PROVIDER_PROXY_BASE_URL":      "http://" + hostedBrokerRelayAddr + "/v1",
				"ORKA_ACP_MCP_BROKER_URL":               "http://" + hostedOrkaRelayAddr,
				"ORKA_ACP_ARTIFACT_API_URL":             "http://" + hostedOrkaRelayAddr,
				"ORKA_ACP_WORKSPACE_MAX_ARTIFACT_BYTES": "536870912",
				"ORKA_ACP_SESSION_BASE_DIR":             env["ORKA_ACP_SESSION_BASE_DIR"],
				"ORKA_ACP_CONTROLLER_TOKEN_BOOTSTRAP":   f.protocol.bootstrap.ControllerToken,
				"ORKA_ACP_CAPABILITY_SECRET_BOOTSTRAP":  f.protocol.bootstrap.CapabilitySecret,
				"ORKA_ACP_PROVIDER_TOKEN_BOOTSTRAP":     f.protocol.bootstrap.ProviderToken,
			} {
				expected[key] = value
			}
			if !reflect.DeepEqual(env, expected) {
				t.Fatal("supervisor environment inherited authority or changed its pinned configuration")
			}
			info, err := os.Lstat(env["ORKA_ACP_SESSION_BASE_DIR"])
			if err != nil || !info.IsDir() || info.Mode().Perm() != 0o711 {
				t.Fatal("supervisor session directory lacks its explicit isolation permissions")
			}
			hostedTestDone(t, f.server.ctx.Done())
			if f.calls.Load() != 1 {
				t.Fatal("fixture runner failure retried the supervisor")
			}
		})
	}
}

func hostedServerTestHealth(t *testing.T, status int) <-chan struct{} {
	t.Helper()
	called := make(chan struct{}, 1)
	listener, err := net.Listen("tcp", hostedSupervisorAddr)
	if err != nil {
		t.Fatal("fixed supervisor fixture port is unavailable")
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v2/health" {
			http.NotFound(w, r)
			return
		}
		select {
		case called <- struct{}{}:
		default:
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"protocol":"orka.harness.v2","status":"ok"}`))
	}))
	_ = server.Listener.Close()
	server.Listener = listener
	server.Start()
	t.Cleanup(server.Close)
	return called
}

func hostedServerTestRunningPair(t *testing.T, f *hostedServerTestFixture) (*hostedWSConn, *hostedWSConn, *http2.ClientConn) {
	t.Helper()
	pairID, digest := uuid.NewString(), brokerJSONDigest(f.protocol.bootstrap)
	forward := f.claim(t, "forward", pairID, digest)
	reverse := hostedServerReverse(t, f.claim(t, "reverse", pairID, digest))
	f.sendBootstrap(t, forward)
	_ = f.receiveRunner(t)
	var ready hostedReady
	if readHostedWSJSON(forward, &ready) != nil || ready != (hostedReady{Protocol: hostedProtocol,
		PairID: pairID, BootID: f.server.challenge.BootID, Ready: true}) {
		t.Fatal("running fixture did not report exact readiness")
	}
	forwardConn := newHostedWSConn(forward)
	client, err := newHostedHTTP2ClientConn(forwardConn)
	if err != nil {
		t.Fatal("could not establish forward HTTP/2 fixture")
	}
	t.Cleanup(func() { _ = client.Close(); _ = forwardConn.Close() })
	return forwardConn, reverse, client
}

func TestHostedServerRunningLifetimeCannotLaunchSecondSupervisor(t *testing.T) {
	hostedServerTestHealth(t, http.StatusOK)
	f := newHostedServerTestFixture(t, true)
	_, _, client := hostedServerTestRunningPair(t, f)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://supervisor/v2/health", nil)
	response, err := client.RoundTrip(request)
	if err != nil {
		t.Fatal("authenticated pair did not carry a forward HTTP/2 request")
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatal("forward proxy changed supervisor health status")
	}
	for _, role := range []string{"forward", "reverse"} {
		ws := f.connect(t)
		if ws.WriteJSON(f.hello(t, role, uuid.NewString(), brokerJSONDigest(f.protocol.bootstrap))) != nil {
			t.Fatal("could not send second-launch fixture")
		}
		hostedServerRejected(t, ws)
	}
	if f.calls.Load() != 1 || f.server.ctx.Err() != nil {
		t.Fatal("second launch was admitted or disrupted the authenticated lifetime")
	}
}

func TestHostedServerEitherChannelLossClosesLifetime(t *testing.T) {
	for _, role := range []string{"forward", "reverse"} {
		t.Run(role, func(t *testing.T) {
			hostedServerTestHealth(t, http.StatusOK)
			f := newHostedServerTestFixture(t, true)
			forward, reverse, _ := hostedServerTestRunningPair(t, f)
			if role == "forward" {
				_ = forward.Close()
			} else {
				_ = reverse.Close()
			}
			hostedTestDone(t, f.server.ctx.Done())
			hostedTestDone(t, forward.Done())
			hostedTestDone(t, reverse.Done())
			select {
			case <-f.stopped:
			case <-time.After(3 * time.Second):
				t.Fatal("channel loss did not cancel the supervisor runner")
			}
			response := httptest.NewRecorder()
			f.server.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/readiness", nil))
			if response.Code != http.StatusServiceUnavailable || f.calls.Load() != 1 {
				t.Fatal("closed lifetime remained ready or replayed its supervisor")
			}
			ws, reply, err := (&websocket.Dialer{HandshakeTimeout: 3 * time.Second}).Dial(
				"ws"+strings.TrimPrefix(f.http.URL, "http")+"/invocations_ws", nil)
			if ws != nil {
				_ = ws.Close()
			}
			if reply != nil && reply.Body != nil {
				_ = reply.Body.Close()
			}
			if err == nil || reply == nil || reply.StatusCode != http.StatusServiceUnavailable {
				t.Fatal("closed lifetime accepted a replayed channel")
			}
		})
	}
}

func TestHostedServerIdleGoingAwayClosesLifetime(t *testing.T) {
	for _, role := range []string{"forward", "reverse"} {
		t.Run(role, func(t *testing.T) {
			hostedServerTestHealth(t, http.StatusOK)
			f := newHostedServerTestFixture(t, true)
			earliest := time.Now()
			forward, reverse, _ := hostedServerTestRunningPair(t, f)
			latest := time.Now()
			channel := forward
			if role == "reverse" {
				channel = reverse
			}
			hostedTestGoingAway(t, channel.ws)
			hostedTestDone(t, f.server.ctx.Done())
			hostedTestDone(t, forward.Done())
			hostedTestDone(t, reverse.Done())
			f.server.mu.Lock()
			stopped := f.server.pair.stopped
			f.server.mu.Unlock()
			hostedTestDone(t, stopped)
			response := httptest.NewRecorder()
			f.server.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/readiness", nil))
			if response.Code != http.StatusServiceUnavailable || f.calls.Load() != 1 {
				t.Fatal("idle platform closure left a ready runtime or relaunched its supervisor")
			}
			replay := httptest.NewRecorder()
			f.server.ServeHTTP(replay, httptest.NewRequest(http.MethodGet, "/invocations_ws", nil))
			if replay.Code != http.StatusServiceUnavailable || f.calls.Load() != 1 {
				t.Fatal("platform-closed lifetime admitted a replacement channel")
			}
			hostedTestClosedChannelLogs(t, f.channelLogs, role, earliest, latest)
		})
	}
}

func TestHostedServerEitherChannelLossWhileStartingCancelsRunner(t *testing.T) {
	for _, role := range []string{"forward", "reverse", "forward after early preface"} {
		t.Run(role, func(t *testing.T) {
			healthCalled := hostedServerTestHealth(t, http.StatusServiceUnavailable)
			f := newHostedServerTestFixture(t, true)
			pairID, digest := uuid.NewString(), brokerJSONDigest(f.protocol.bootstrap)
			forward := f.claim(t, "forward", pairID, digest)
			reverse := hostedServerReverse(t, f.claim(t, "reverse", pairID, digest))
			f.sendBootstrap(t, forward)
			_ = f.receiveRunner(t)
			select {
			case <-healthCalled:
			case <-time.After(3 * time.Second):
				t.Fatal("fixture did not reach supervisor readiness polling")
			}
			if role == "reverse" {
				_ = reverse.Close()
			} else {
				if role == "forward after early preface" {
					if forward.WriteMessage(websocket.BinaryMessage, []byte(http2.ClientPreface)) != nil {
						t.Fatal("could not send the early HTTP/2 preface")
					}
				}
				_ = forward.Close()
			}
			select {
			case <-f.server.ctx.Done():
			case <-time.After(time.Second):
				t.Fatal("channel loss left the starting supervisor alive")
			}
			select {
			case <-f.stopped:
			case <-time.After(3 * time.Second):
				t.Fatal("lost startup channel did not cancel the runner")
			}
			if f.calls.Load() != 1 {
				t.Fatal("lost startup channel replayed the supervisor")
			}
		})
	}
}
