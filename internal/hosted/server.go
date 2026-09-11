package hosted

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/orka-agents/agent-runtime-foundry/internal/foundry"
	"github.com/orka-agents/agent-runtime-foundry/internal/strictjson"
)

type hostedReady struct {
	Protocol string `json:"protocol"`
	PairID   string `json:"pairID"`
	BootID   string `json:"bootID"`
	Ready    bool   `json:"ready"`
}

type hostedSupervisorRunner func(context.Context, map[string]string) (<-chan error, error)

const (
	// Both challenges must match before either role is signed. Allow the
	// gateway's bounded initialization budget for the other channel's dial
	// and handshake, then bound authenticated pairing by one shared deadline.
	hostedInitializationWait = 4 * time.Minute

	// Orka allows 45 seconds for HTTP shutdown, then 45 seconds for session
	// cleanup. Give that owner time to reap its separate ACP UID scopes.
	hostedSupervisorStopGrace    = 95 * time.Second
	hostedSupervisorKillWait     = 5 * time.Second
	hostedSupervisorShutdownWait = hostedSupervisorStopGrace + hostedSupervisorKillWait + 5*time.Second
)

type hostedServer struct {
	cfg           hostedImageConfig
	challenge     hostedChallenge
	adapterDigest string
	agentConfig   []byte
	runner        hostedSupervisorRunner
	ctx           context.Context
	cancel        context.CancelFunc
	mu            sync.Mutex
	pair          *hostedPair
	channelLog    *log.Logger
}

type hostedPair struct {
	server                                 *hostedServer
	id, bootstrapDigest                    string
	forward, reverse                       *websocket.Conn
	forwardObservation, reverseObservation *hostedChannelObservation
	forwardAcked, reverseAcked             bool
	setupDeadline                          time.Time
	bootstrap                              *hostedBootstrap
	started                                chan struct{}
	done                                   chan struct{}
	stopped                                chan struct{}
	shutdownErr                            error
	once                                   sync.Once
	running                                bool
}

func newHostedServer(ctx context.Context, cfg hostedImageConfig, getenv func(string) string, runner hostedSupervisorRunner) (*hostedServer, error) {
	if validateHostedImageConfig(cfg) != nil || os.Geteuid() != 0 ||
		strings.TrimRight(getenv("FOUNDRY_PROJECT_ENDPOINT"), "/") != cfg.Target.ProjectEndpoint ||
		getenv("FOUNDRY_AGENT_NAME") != cfg.Target.AgentName || getenv("FOUNDRY_AGENT_VERSION") != cfg.Target.AgentVersion {
		return nil, errHostedInvalid
	}
	sid, err := uuid.Parse(getenv("FOUNDRY_AGENT_SESSION_ID"))
	if err != nil || !hostedUUIDValid(getenv("FOUNDRY_AGENT_SESSION_ID")) {
		return nil, errHostedInvalid
	}
	digest := getenv("ORKA_ACP_FOUNDRY_ADAPTER_DIGEST")
	agent, err := readHostedFile(foundry.AgentConfigPath, foundry.MaxAgentConfigBytes)
	if err != nil || foundry.Digest(agent) != cfg.AgentConfigurationDigest || !foundry.DigestValid(digest) {
		return nil, errHostedInvalid
	}
	var nonce [32]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, errHostedInvalid
	}
	lifetime, cancel := context.WithCancel(ctx)
	return &hostedServer{cfg: cfg, adapterDigest: digest, agentConfig: agent, runner: runner,
		ctx: lifetime, cancel: cancel, challenge: hostedChallenge{Protocol: hostedProtocol,
			DeploymentID: cfg.DeploymentID, ConfigurationDigest: foundry.JSONDigest(cfg),
			AgentName: cfg.Target.AgentName, AgentVersion: cfg.Target.AgentVersion,
			SessionID: sid.String(), BootID: uuid.NewString(), Nonce: base64.RawURLEncoding.EncodeToString(nonce[:])}}, nil
}

func (s *hostedServer) close() error {
	s.cancel()
	s.mu.Lock()
	pair := s.pair
	running := pair != nil && pair.running
	s.mu.Unlock()
	if pair == nil {
		return nil
	}
	if !running {
		pair.close()
		return nil
	}
	timer := time.NewTimer(hostedSupervisorShutdownWait + time.Second)
	defer timer.Stop()
	select {
	case <-pair.stopped:
		s.mu.Lock()
		defer s.mu.Unlock()
		return pair.shutdownErr
	case <-timer.C:
		pair.close()
		return errHostedInvalid
	}
}

func (s *hostedServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/readiness" && r.Method == http.MethodGet && r.URL.RawQuery == "" {
		if s.ctx.Err() != nil {
			http.Error(w, "hosted lifetime closed", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"ready":true}`)
		return
	}
	if r.URL.Path != "/invocations_ws" || r.Method != http.MethodGet || r.URL.RawPath != "" || !hostedRoutingQuery(r) {
		http.NotFound(w, r)
		return
	}
	if s.ctx.Err() != nil {
		http.Error(w, "hosted lifetime closed", http.StatusServiceUnavailable)
		return
	}
	upgrader := websocket.Upgrader{HandshakeTimeout: 10 * time.Second,
		ReadBufferSize: 4096, WriteBufferSize: 4096,
		CheckOrigin: func(request *http.Request) bool { return request.Header.Get("Origin") == "" }}
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	openedAt := time.Now()
	defer ws.Close() //nolint:errcheck
	ws.SetReadLimit(hostedMaxHandshakeBytes)
	_ = ws.SetWriteDeadline(time.Now().Add(10 * time.Second))
	if ws.WriteJSON(s.challenge) != nil {
		return
	}
	var hello hostedHello
	_ = ws.SetReadDeadline(time.Now().Add(hostedInitializationWait))
	if readHostedWSJSON(ws, &hello) != nil || verifyHostedHello(hello, s.challenge, s.cfg.SigningPublicKey, time.Now()) != nil {
		_ = ws.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "hosted authentication failed"), time.Now().Add(time.Second))
		return
	}
	observation := observeHostedChannel(ws, hello.Role, openedAt, s.channelLog)
	defer observation.finish()
	pair, err := s.claim(hello, ws, observation)
	if err != nil {
		_ = ws.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "hosted pair conflicts"), time.Now().Add(time.Second))
		return
	}
	ack := hostedAccepted{Protocol: hostedProtocol, PairID: pair.id, Role: hello.Role,
		BootID: s.challenge.BootID, BootstrapDigest: pair.bootstrapDigest}
	_ = ws.SetWriteDeadline(time.Now().Add(10 * time.Second))
	if ws.WriteJSON(ack) != nil {
		pair.close()
		return
	}
	s.mu.Lock()
	if hello.Role == "forward" {
		pair.forwardAcked = true
	} else {
		pair.reverseAcked = true
	}
	s.mu.Unlock()
	if hello.Role == "forward" {
		_ = ws.SetReadDeadline(pair.setupDeadline)
		kind, data, err := ws.ReadMessage()
		var bootstrap hostedBootstrap
		if err != nil || kind != websocket.TextMessage || foundry.Digest(data) != pair.bootstrapDigest ||
			strictjson.Decode(data, &bootstrap, true) != nil || validateHostedBootstrap(s.cfg, bootstrap) != nil ||
			bootstrap.Environment["ORKA_ACP_FOUNDRY_ADAPTER_DIGEST"] != s.adapterDigest {
			clear(data)
			pair.close()
			return
		}
		clear(data)
		if _, err := foundry.DecodeAgentConfig(s.agentConfig, s.cfg.AgentConfigurationDigest, bootstrap.Environment["ORKA_ACP_MODEL"]); err != nil {
			pair.close()
			return
		}
		s.mu.Lock()
		pair.bootstrap = &bootstrap
		s.mu.Unlock()
	}
	pair.maybeStart()
	timer := time.NewTimer(time.Until(pair.setupDeadline))
	defer timer.Stop()
	select {
	case <-pair.started:
	case <-timer.C:
		pair.close()
	case <-s.ctx.Done():
		// A running supervisor owns bounded shutdown. Keep its healthy
		// transports available until cleanup completes.
		s.mu.Lock()
		running := pair.running
		s.mu.Unlock()
		if !running {
			pair.close()
		}
	}
	<-pair.done
}

func hostedRoutingQuery(r *http.Request) bool {
	if len(r.URL.RawQuery) > 2048 {
		return false
	}
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return false
	}
	for key, values := range query {
		if (key != "api-version" && key != "agent_session_id") || len(values) != 1 || len(values[0]) > 512 || !foundry.SafeString(values[0], 512) {
			return false
		}
	}
	return true
}

func readHostedWSJSON(ws *websocket.Conn, value any) error {
	kind, data, err := ws.ReadMessage()
	if err != nil || kind != websocket.TextMessage || len(data) > hostedMaxHandshakeBytes || strictjson.Decode(data, value, true) != nil {
		return errHostedInvalid
	}
	return nil
}

func (s *hostedServer) claim(hello hostedHello, ws *websocket.Conn, observation *hostedChannelObservation) (*hostedPair, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx.Err() != nil {
		return nil, errHostedInvalid
	}
	if s.pair == nil {
		s.pair = &hostedPair{server: s, id: hello.PairID, bootstrapDigest: hello.BootstrapDigest,
			setupDeadline: time.Now().Add(hostedInitializationWait),
			started:       make(chan struct{}), done: make(chan struct{}), stopped: make(chan struct{})}
	}
	pair := s.pair
	if pair.id != hello.PairID || pair.bootstrapDigest != hello.BootstrapDigest || pair.running {
		return nil, errHostedInvalid
	}
	switch hello.Role {
	case "forward":
		if pair.forward != nil {
			return nil, errHostedInvalid
		}
		pair.forward = ws
		pair.forwardObservation = observation
	case "reverse":
		if pair.reverse != nil {
			return nil, errHostedInvalid
		}
		pair.reverse = ws
		pair.reverseObservation = observation
	default:
		return nil, errHostedInvalid
	}
	return pair, nil
}

func (p *hostedPair) maybeStart() {
	p.server.mu.Lock()
	defer p.server.mu.Unlock()
	if !p.running && p.forwardAcked && p.reverseAcked && p.bootstrap != nil &&
		p.server.ctx.Err() == nil && time.Now().Before(p.setupDeadline) {
		p.running = true
		close(p.started)
		go p.run()
	}
}

func (p *hostedPair) close() {
	p.once.Do(func() {
		p.server.cancel()
		p.server.mu.Lock()
		forward, reverse := p.forward, p.reverse
		forwardObservation, reverseObservation := p.forwardObservation, p.reverseObservation
		p.server.mu.Unlock()
		if forward != nil {
			_ = forward.Close()
		}
		if reverse != nil {
			_ = reverse.Close()
		}
		forwardObservation.finish()
		reverseObservation.finish()
		close(p.done)
	})
}

func (p *hostedPair) run() {
	defer close(p.stopped)
	defer p.close()
	s := p.server
	// Local shutdown stops admission and the supervisor first. Its cleanup
	// still needs the reverse relay, and the gateway requires both channels.
	// Actual channel loss continues to close the pair immediately.
	transportContext, stopTransport := context.WithCancel(context.WithoutCancel(s.ctx))
	defer stopTransport()
	_ = p.forward.SetReadDeadline(time.Time{})
	forward := newHostedObservedWSConn(p.forward, p.forwardObservation)
	bufferedForward := &hostedBufferedConn{Conn: forward, reader: bufio.NewReader(forward)}
	forwardReadable := make(chan struct{})
	var forwardReady atomic.Bool
	// Observe a lost forward channel while the supervisor is starting. The
	// gateway must wait for readiness before sending HTTP/2; premature bytes
	// close the lifetime. Hand off valid bytes only after Peek finishes.
	go func() {
		if _, err := bufferedForward.reader.Peek(1); err != nil || !forwardReady.Load() {
			p.close()
		}
		close(forwardReadable)
	}()
	_ = p.reverse.SetReadDeadline(time.Time{})
	_ = p.reverse.SetWriteDeadline(time.Time{})
	reverse := newHostedObservedWSConn(p.reverse, p.reverseObservation)
	client, err := newHostedHTTP2ClientConn(reverse)
	if err != nil {
		return
	}
	defer client.Close() //nolint:errcheck
	go func() {
		select {
		case <-reverse.Done():
			p.close()
		case <-p.done:
		}
	}()
	for _, relay := range []struct {
		address, target string
		allow           func(*http.Request) bool
	}{
		{hostedBrokerRelayAddr, "http://broker", hostedBrokerRoute},
		{hostedOrkaRelayAddr, "http://orka", hostedOrkaRoute},
	} {
		handler, err := newHostedProxy(relay.target, client, relay.allow)
		if err != nil {
			p.localFailure()
			return
		}
		listener, err := net.Listen("tcp", relay.address)
		if err != nil {
			p.localFailure()
			return
		}
		server := newHostedHTTPServer(handler)
		defer server.Close() //nolint:errcheck
		go func() { _ = server.Serve(listener) }()
	}
	sessionDir, err := os.MkdirTemp("/tmp", "orka-hosted-sessions-")
	if err != nil || os.Chmod(sessionDir, 0o711) != nil {
		p.localFailure()
		return
	}
	// /tmp is outside the Foundry Session Files surface. Each wrapper lifetime
	// launches exactly one supervisor; neither process is silently restarted.
	env := make(map[string]string, len(p.bootstrap.Environment)+12)
	for key, value := range p.bootstrap.Environment {
		env[key] = value
	}
	for key, value := range map[string]string{
		"PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin", "HOME": "/tmp",
		"ORKA_ACP_LISTEN_ADDRESS":               hostedSupervisorAddr,
		"ORKA_ACP_RUNTIME_INSTANCE_ID":          "foundry-hosted." + s.challenge.BootID,
		"ORKA_ACP_SUPERVISOR_BOOT_ID":           s.challenge.BootID,
		"ORKA_ACP_PROVIDER_PROXY_BASE_URL":      "http://" + hostedBrokerRelayAddr + "/v1",
		"ORKA_ACP_MCP_BROKER_URL":               "http://" + hostedOrkaRelayAddr,
		"ORKA_ACP_ARTIFACT_API_URL":             "http://" + hostedOrkaRelayAddr,
		"ORKA_ACP_WORKSPACE_MAX_ARTIFACT_BYTES": "536870912",
		"ORKA_ACP_SESSION_BASE_DIR":             sessionDir,
		"ORKA_ACP_CONTROLLER_TOKEN_BOOTSTRAP":   p.bootstrap.ControllerToken,
		"ORKA_ACP_CAPABILITY_SECRET_BOOTSTRAP":  p.bootstrap.CapabilitySecret,
		"ORKA_ACP_PROVIDER_TOKEN_BOOTSTRAP":     p.bootstrap.ProviderToken,
	} {
		env[key] = value
	}
	processDone, err := s.runner(s.ctx, env)
	if err != nil {
		if s.ctx.Err() == nil {
			p.localFailure()
		}
		return
	}
	processResult := make(chan error, 1)
	go func() {
		err, ok := <-processDone
		// Classify startup completion before close cancels the lifetime. A
		// zero exit before health readiness is still a failed startup unless
		// local or peer cancellation was already requested.
		if !ok || (err == nil && !forwardReady.Load() && s.ctx.Err() == nil) {
			err = errHostedInvalid
		}
		processResult <- err
		p.close()
	}()
	defer p.joinSupervisor(processResult)
	if waitHostedSupervisor(s.ctx) != nil {
		if s.ctx.Err() == nil {
			p.localFailure()
		}
		return
	}
	forwardReady.Store(true)
	_ = p.forward.SetWriteDeadline(time.Now().Add(10 * time.Second))
	if p.forward.WriteJSON(hostedReady{Protocol: hostedProtocol, PairID: p.id, BootID: s.challenge.BootID, Ready: true}) != nil {
		p.close()
		return
	}
	_ = p.forward.SetWriteDeadline(time.Time{})
	transport := newHostedLocalTransport()
	defer transport.CloseIdleConnections()
	handler, err := newHostedProxy("http://"+hostedSupervisorAddr, transport, hostedV2Route)
	if err != nil {
		p.localFailure()
		return
	}
	admission := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.ctx.Err() != nil {
			http.Error(w, "hosted lifetime closed", http.StatusServiceUnavailable)
			return
		}
		handler.ServeHTTP(w, r)
	})
	go func() {
		select {
		case <-forwardReadable:
			serveHostedHTTP2(transportContext, bufferedForward, admission)
		case <-transportContext.Done():
		}
		p.close()
	}()
	select {
	case <-s.ctx.Done():
	case <-forward.Done():
		p.close()
	case <-reverse.Done():
		p.close()
	}
}

func (p *hostedPair) localFailure() {
	p.server.mu.Lock()
	p.shutdownErr = errHostedInvalid
	p.server.mu.Unlock()
}

func (p *hostedPair) joinSupervisor(result <-chan error) {
	p.server.cancel()
	defer p.close()
	timer := time.NewTimer(hostedSupervisorShutdownWait)
	defer timer.Stop()
	var err error
	select {
	case err = <-result:
	case <-timer.C:
		err = errHostedInvalid
	}
	p.server.mu.Lock()
	if err != nil {
		p.shutdownErr = errHostedInvalid
	}
	p.server.mu.Unlock()
}

type hostedBufferedConn struct {
	net.Conn
	reader *bufio.Reader
}

func (c *hostedBufferedConn) Read(data []byte) (int, error) { return c.reader.Read(data) }

func waitHostedSupervisor(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	transport := newHostedLocalTransport()
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 2 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return errHostedInvalid }}
	for {
		request, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+hostedSupervisorAddr+"/v2/health", nil)
		response, err := client.Do(request)
		if err == nil {
			data, readErr := io.ReadAll(io.LimitReader(response.Body, 4097))
			_ = response.Body.Close()
			var health struct {
				Protocol string `json:"protocol"`
				Status   string `json:"status"`
			}
			if readErr == nil && len(data) <= 4096 && response.StatusCode == http.StatusOK && json.Unmarshal(data, &health) == nil && health.Protocol == "orka.harness.v2" && health.Status == "ok" {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return errHostedInvalid
		case <-time.After(100 * time.Millisecond):
		}
	}
}
