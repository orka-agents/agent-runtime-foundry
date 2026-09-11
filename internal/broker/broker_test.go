package broker

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orka-agents/agent-runtime-foundry/internal/brokerapi"
	"github.com/orka-agents/agent-runtime-foundry/internal/foundry"
)

const brokerFixtureBearer = "test-broker-bearer-with-more-than-thirty-two-bytes"

type brokerFixtureToken struct{ value string }

func (p brokerFixtureToken) AccessToken(context.Context) (string, error) { return p.value, nil }

type brokerFixtureTransport func(*http.Request) (*http.Response, error)

func (f brokerFixtureTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func brokerTestToken() string {
	return "fixture." + base64.RawURLEncoding.EncodeToString([]byte(`{"aud":"https://ai.azure.com","tid":"test-tenant","oid":"test-principal","appid":"test-client"}`)) + ".fixture"
}

type brokerFixture struct {
	t           *testing.T
	server      *httptest.Server
	mu          sync.Mutex
	sessions    map[string]string
	creates     int
	inferences  int
	stops       int
	deletes     int
	mode        string
	started     chan struct{}
	release     chan struct{}
	startOnce   sync.Once
	releaseOnce sync.Once
	requests    []foundry.ResponseRequest
	createCheck func(string)
}

func newBrokerFixture(t *testing.T, mode string) *brokerFixture {
	t.Helper()
	f := &brokerFixture{t: t, sessions: map[string]string{}, mode: mode, started: make(chan struct{}), release: make(chan struct{})}
	f.server = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(func() { f.unblock(); f.server.Close() })
	return f
}

func (f *brokerFixture) unblock() { f.releaseOnce.Do(func() { close(f.release) }) }

func (f *brokerFixture) counts() (int, int, int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.creates, f.inferences, f.stops, f.deletes
}

func (f *brokerFixture) serve(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != "Bearer "+brokerTestToken() {
		f.t.Error("provider authentication mismatch")
		w.WriteHeader(401)
		return
	}
	if r.URL.Query().Get("api-version") != "v1" {
		f.t.Error("provider API version mismatch")
		w.WriteHeader(400)
		return
	}
	prefix := "/api/projects/fixture/agents/fixture"
	w.Header().Set("Content-Type", "application/json")
	suffix := strings.TrimPrefix(r.URL.Path, prefix)
	if suffix == "" && r.Method == http.MethodGet {
		_, _ = io.WriteString(w, `{"name":"fixture","agent_endpoint":{"authorization_schemes":[{"type":"entra"}]}}`)
		return
	}
	if suffix == "/versions/3" && r.Method == http.MethodGet {
		_, _ = io.WriteString(w, `{"name":"fixture","version":"3","status":"active","definition":{"kind":"hosted"}}`)
		return
	}
	if suffix == "/endpoint/sessions" && r.Method == http.MethodPost {
		var request foundry.RemoteSession
		if json.NewDecoder(r.Body).Decode(&request) != nil || request.ID == "" || request.Version.Type != "version_ref" || request.Version.Version != "3" {
			f.t.Error("session creation did not contain exact chosen identity and version")
			w.WriteHeader(400)
			return
		}
		f.mu.Lock()
		f.creates++
		check := f.createCheck
		f.mu.Unlock()
		if check != nil {
			check(request.ID)
		}
		if strings.HasPrefix(f.mode, "create-rejected") {
			status := http.StatusTooManyRequests
			switch f.mode {
			case "create-rejected-server":
				status = http.StatusServiceUnavailable
			case "create-rejected-conflict":
				status = http.StatusConflict
			case "create-rejected-truncated":
				w.Header().Set("Content-Length", "1024")
			}
			w.WriteHeader(status)
			if f.mode == "create-rejected-oversized" {
				_, _ = io.WriteString(w, strings.Repeat("x", foundry.MaxAgentConfigBytes+1))
				return
			}
			_, _ = io.WriteString(w, `{"error":"fixture creation rejection"}`)
			return
		}
		if f.mode == "late-create" {
			// Lose the original acknowledgement independently of prompt
			// cancellation, then let that same request create the session later.
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				f.t.Error("could not drop creation acknowledgement")
				return
			}
			_ = conn.Close()
			f.startOnce.Do(func() { close(f.started) })
			<-f.release
		}
		f.mu.Lock()
		if _, exists := f.sessions[request.ID]; exists {
			f.mu.Unlock()
			w.WriteHeader(409)
			return
		}
		f.sessions[request.ID] = "active"
		f.mu.Unlock()
		if f.mode == "late-create" {
			return
		}
		if f.mode == "hold-create" {
			f.startOnce.Do(func() { close(f.started) })
			<-f.release
		}
		w.WriteHeader(201)
		f.sessionJSON(w, request.ID, "active")
		return
	}
	if suffix == "/endpoint/protocols/openai/responses" && r.Method == http.MethodPost {
		var request foundry.ResponseRequest
		if json.NewDecoder(r.Body).Decode(&request) != nil {
			w.WriteHeader(400)
			return
		}
		f.mu.Lock()
		_, exists := f.sessions[request.AgentSessionID]
		if !exists {
			f.mu.Unlock()
			w.WriteHeader(404)
			return
		}
		f.sessions[request.AgentSessionID] = "active"
		f.inferences++
		count := f.inferences
		f.requests = append(f.requests, request)
		f.mu.Unlock()
		responseID := fmt.Sprintf("provider-response-%d", count)
		if f.mode == "hold-known" || f.mode == "hold-unknown" || f.mode == "truncated" {
			w.Header().Set("Content-Type", "text/event-stream")
			if f.mode != "hold-unknown" {
				frame, _ := json.Marshal(map[string]any{"type": "response.created", "response": map[string]any{
					"id": responseID, "status": "in_progress", "agent_session_id": request.AgentSessionID, "output": []any{}}})
				_, _ = fmt.Fprintf(w, "data: %s\n\n", frame)
			}
			w.(http.Flusher).Flush()
			f.startOnce.Do(func() { close(f.started) })
			if f.mode == "truncated" {
				return
			}
			select {
			case <-r.Context().Done():
				return
			case <-f.release:
			}
			frame, _ := json.Marshal(map[string]any{"type": "response.completed", "response": map[string]any{
				"id": responseID, "status": "completed", "agent_session_id": request.AgentSessionID,
				"output": []any{map[string]any{"type": "message", "role": "assistant", "content": []any{map[string]any{"type": "output_text", "text": "fixture-terminal-do-not-persist"}}}}}})
			_, _ = fmt.Fprintf(w, "data: %s\n\n", frame)
			return
		}
		if strings.HasPrefix(f.mode, "rejected") {
			status := http.StatusTooManyRequests
			if f.mode == "rejected-server" {
				status = http.StatusServiceUnavailable
			}
			if f.mode == "rejected-truncated" {
				w.Header().Set("Content-Length", "1024")
			}
			w.WriteHeader(status)
			if f.mode == "rejected-oversized" {
				_, _ = io.WriteString(w, strings.Repeat("x", foundry.MaxAgentConfigBytes+1))
				return
			}
			_, _ = io.WriteString(w, `{"error":"fixture rejection"}`)
			return
		}
		if f.mode == "functions" && count == 1 {
			_ = json.NewEncoder(w).Encode(map[string]any{"id": responseID, "status": "completed", "agent_session_id": request.AgentSessionID,
				"output": []any{map[string]any{"id": "provider-item-id", "type": "function_call", "call_id": "provider-call-id", "name": "fixture_echo", "arguments": "{\"text\":\"fixture\"}"}}})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"id": responseID, "status": "completed", "agent_session_id": request.AgentSessionID,
			"output": []any{map[string]any{"id": "provider-item-id", "type": "message", "role": "assistant", "content": []any{map[string]any{"type": "output_text", "text": "fixture-terminal-do-not-persist"}}}}})
		return
	}
	if strings.HasPrefix(suffix, "/endpoint/sessions/") {
		id := strings.TrimPrefix(suffix, "/endpoint/sessions/")
		stop := strings.HasSuffix(id, ":stop")
		id = strings.TrimSuffix(id, ":stop")
		f.mu.Lock()
		state, exists := f.sessions[id]
		if stop && r.Method == http.MethodPost {
			f.stops++
			if !exists {
				f.mu.Unlock()
				w.WriteHeader(404)
				return
			}
			f.sessions[id] = "idle"
			f.mu.Unlock()
			if state == "idle" {
				w.WriteHeader(409)
			} else {
				w.WriteHeader(204)
			}
			return
		}
		if r.Method == http.MethodDelete {
			f.deletes++
			delete(f.sessions, id)
			f.mu.Unlock()
			w.WriteHeader(204)
			return
		}
		f.mu.Unlock()
		if r.Method == http.MethodGet {
			if !exists {
				w.WriteHeader(404)
				return
			}
			f.sessionJSON(w, id, state)
			return
		}
	}
	w.WriteHeader(404)
}

func (f *brokerFixture) sessionJSON(w http.ResponseWriter, id, state string) {
	_ = json.NewEncoder(w).Encode(map[string]any{"agent_session_id": id, "version_indicator": map[string]string{"type": "version_ref", "agent_version": "3"}, "status": state})
}

func brokerTestConfig(t *testing.T, fixture *brokerFixture) brokerConfiguration {
	t.Helper()
	agent := foundry.AgentConfig{Model: "fixture-model", ToolSchemaMode: foundry.ToolSchemaModeProviderStatic,
		HostedTarget: foundry.HostedTarget{ProjectEndpoint: fixture.server.URL + "/api/projects/fixture", AgentName: "fixture", AgentVersion: "3"}}
	raw, _ := json.Marshal(agent)
	return brokerConfiguration{agent: agent, configDigest: foundry.Digest(raw), stateDir: filepath.Join(t.TempDir(), "broker"), bearer: brokerFixtureBearer, operationTimeout: 2 * time.Second}
}

func startBrokerTest(t *testing.T, cfg brokerConfiguration) (*lifecycleBroker, *httptest.Server) {
	t.Helper()
	return startBrokerTestWithClient(t, cfg, nil)
}

func startBrokerTestWithClient(t *testing.T, cfg brokerConfiguration, client *http.Client) (*lifecycleBroker, *httptest.Server) {
	t.Helper()
	b, err := newLifecycleBroker(context.Background(), cfg, brokerFixtureToken{brokerTestToken()}, client)
	if err != nil {
		t.Fatalf("new broker: %v", err)
	}
	server := httptest.NewServer(b)
	t.Cleanup(func() { b.close(); server.Close() })
	return b, server
}

func brokerTestContext(cfg brokerConfiguration) brokerContext {
	return brokerContext{Protocol: brokerProtocol, AgentConfigurationDigest: cfg.configDigest,
		Owner: brokerOwner{RuntimeInstanceID: "fixture-instance", SupervisorBootID: "fixture-boot", ControllerEpoch: 11,
			RuntimePoolUID: "fixture-pool", RuntimePoolGeneration: 2, RuntimeSessionUID: "fixture-session", RuntimeSessionGeneration: 3,
			RuntimeProfileDigest: "sha256:" + strings.Repeat("1", 64), ProfileDigestSchemaVersion: 1},
		TaskUID: "fixture-task", TaskAttempt: 1, PromptID: "fixture-prompt", PromptRequestDigest: "sha256:" + strings.Repeat("2", 64),
		LeaseGeneration: 1, LeaseExpiresAt: time.Now().Add(10 * time.Second).UTC().Format(time.RFC3339Nano), OperationID: "fixture-inference", InvocationSequence: 1}
}

func brokerTestBody(previous string) []byte {
	request := foundry.ModelResponseRequest{Model: "fixture-model", ResponseRequest: foundry.ResponseRequest{
		Input: "fixture-input-do-not-persist", Stream: true, Store: true, PreviousResponseID: previous}}
	data, _ := json.Marshal(request)
	return data
}

func brokerTestHTTP(ctx context.Context, base, path string, c brokerContext, body []byte) (int, []byte, error) {
	c.BodySHA256 = foundry.Digest(body)
	raw, _ := json.Marshal(c)
	method := http.MethodPost
	if path == brokerapi.StatusPath {
		method = http.MethodGet
	}
	request, err := http.NewRequestWithContext(ctx, method, base+path, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	request.Header.Set("Authorization", "Bearer "+brokerFixtureBearer)
	request.Header.Set(brokerContextHeader, base64.RawURLEncoding.EncodeToString(raw))
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close() //nolint:errcheck
	data, err := io.ReadAll(response.Body)
	return response.StatusCode, data, err
}

func brokerTestControlContext(path string, c brokerContext) (brokerContext, []byte) {
	c.InvocationSequence = 0
	c.OperationID = "control-" + strings.TrimPrefix(path, "/internal/v1/") + "-" + c.PromptID
	if path == brokerapi.RetirePath || path == brokerapi.StatusPath {
		c.TaskUID = ""
		c.TaskAttempt = 0
		c.PromptID = ""
		c.PromptRequestDigest = ""
		c.LeaseGeneration = 0
		c.LeaseExpiresAt = ""
	}
	body := []byte("{}")
	if path == brokerapi.StatusPath {
		body = nil
	}
	return c, body
}

func brokerTestControl(t *testing.T, base, path string, c brokerContext) brokerControlResponse {
	t.Helper()
	c, body := brokerTestControlContext(path, c)
	deadline := time.Now().Add(4 * time.Second)
	for {
		status, data, err := brokerTestHTTP(context.Background(), base, path, c, body)
		var proof brokerControlResponse
		if err != nil || json.Unmarshal(data, &proof) != nil {
			t.Fatalf("control response unreadable, status=%d", status)
		}
		if status == 200 {
			return proof
		}
		if status != 409 || time.Now().After(deadline) {
			t.Fatalf("control did not settle: status=%d state=%s pending=%v ambiguous=%d", status, proof.State, proof.CreatePending, proof.AmbiguousInvocations)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func brokerAwait(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("bounded condition did not become true")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestBrokerLifecycleOwnershipContinuationAndRetirement(t *testing.T) {
	f := newBrokerFixture(t, "success")
	cfg := brokerTestConfig(t, f)
	b, server := startBrokerTest(t, cfg)
	f.createCheck = func(id string) {
		data, err := os.ReadFile(filepath.Join(cfg.stateDir, "state.json"))
		if err != nil || !bytes.Contains(data, []byte(id)) || !bytes.Contains(data, []byte(`"createState":"intent"`)) {
			t.Error("remote create preceded durable caller-chosen ownership")
		}
	}
	c := brokerTestContext(cfg)
	status, data, err := brokerTestHTTP(context.Background(), server.URL, brokerapi.ResponsesPath, c, brokerTestBody(""))
	var first foundry.Response
	if err != nil || status != 200 || json.Unmarshal(data, &first) != nil {
		t.Fatalf("first response failed: %d", status)
	}
	if !strings.HasPrefix(first.ID, "fr_") || first.AgentSessionID != "" || bytes.Contains(data, []byte("provider-")) {
		t.Fatal("provider identity escaped into ACP response")
	}
	proof := brokerTestControl(t, server.URL, brokerapi.SettlePath, c)
	if !proof.SettlementProven || proof.ActiveInvocations != 0 || proof.AmbiguousInvocations != 0 || !foundry.DigestValid(proof.ProofDigest) {
		t.Fatal("first settlement lacked exact proof")
	}
	c.TaskUID = "fixture-task-two"
	c.PromptID = "fixture-prompt-two"
	c.OperationID = "inference-two"
	status, _, err = brokerTestHTTP(context.Background(), server.URL, brokerapi.ResponsesPath, c, brokerTestBody(first.ID))
	if err != nil || status != 200 {
		t.Fatalf("continued response failed: %d", status)
	}
	_ = brokerTestControl(t, server.URL, brokerapi.SettlePath, c)
	proof = brokerTestControl(t, server.URL, brokerapi.RetirePath, c)
	if !proof.RetirementProven || !proof.SettlementProven || proof.CreatePending || proof.State != "retired" {
		t.Fatal("retirement lacked proof")
	}
	duplicate := brokerTestControl(t, server.URL, brokerapi.RetirePath, c)
	if duplicate.ProofDigest != proof.ProofDigest {
		t.Fatal("retirement proof changed on duplicate")
	}
	creates, inferences, stops, deletes := f.counts()
	if creates != 1 || inferences != 2 || stops != 0 || deletes != 1 {
		t.Fatalf("unexpected operation counts: %d %d %d %d", creates, inferences, stops, deletes)
	}
	f.mu.Lock()
	if f.requests[0].AgentSessionID != f.requests[1].AgentSessionID || f.requests[1].PreviousResponseID != "provider-response-1" {
		t.Error("continuation did not preserve remote ownership")
	}
	f.mu.Unlock()
	ledger, err := os.ReadFile(filepath.Join(cfg.stateDir, "state.json"))
	if err != nil || bytes.Contains(ledger, []byte("fixture-input-do-not-persist")) || bytes.Contains(ledger, []byte("fixture-terminal-do-not-persist")) || bytes.Contains(ledger, []byte(brokerTestToken())) {
		t.Fatal("ledger retained content or credentials")
	}
	b.mu.Lock()
	retired := b.ledger.Sessions[foundry.JSONDigest(c.Owner)].Retired
	b.mu.Unlock()
	if !retired {
		t.Fatal("retirement was not durable")
	}
}

func TestBrokerNoInferenceCleanupRejectsDelayedPOST(t *testing.T) {
	f := newBrokerFixture(t, "success")
	cfg := brokerTestConfig(t, f)
	_, server := startBrokerTest(t, cfg)
	c := brokerTestContext(cfg)
	c.LeaseExpiresAt = time.Now().Add(-time.Second).UTC().Format(time.RFC3339Nano)
	proof := brokerTestControl(t, server.URL, brokerapi.SettlePath, c)
	if !proof.SettlementProven || proof.RemoteSessionCreated {
		t.Fatal("empty prompt did not obtain never-created proof")
	}
	c.LeaseExpiresAt = time.Now().Add(10 * time.Second).UTC().Format(time.RFC3339Nano)
	c.OperationID = "delayed-inference"
	status, _, err := brokerTestHTTP(context.Background(), server.URL, brokerapi.ResponsesPath, c, brokerTestBody(""))
	if err != nil || status != 410 {
		t.Fatalf("delayed inference was not fenced: %d", status)
	}
	proof = brokerTestControl(t, server.URL, brokerapi.RetirePath, c)
	if !proof.RetirementProven || proof.RemoteSessionCreated {
		t.Fatal("never-created retirement missing")
	}
	creates, inferences, stops, deletes := f.counts()
	if creates+inferences+stops+deletes != 0 {
		t.Fatal("empty cleanup performed remote mutations")
	}
}

func TestBrokerRenewBeforeInference(t *testing.T) {
	f := newBrokerFixture(t, "success")
	cfg := brokerTestConfig(t, f)
	_, server := startBrokerTest(t, cfg)
	c := brokerTestContext(cfg)
	c.LeaseGeneration = 4
	proof := brokerTestControl(t, server.URL, brokerapi.RenewPath, c)
	if proof.LeaseGeneration != 4 || proof.State != "open" {
		t.Fatal("early renewal was not established")
	}
	status, _, err := brokerTestHTTP(context.Background(), server.URL, brokerapi.ResponsesPath, c, brokerTestBody(""))
	if err != nil || status != 200 {
		t.Fatalf("first inference after renewal failed: %d", status)
	}
	c.LeaseGeneration = 5
	c.LeaseExpiresAt = time.Now().Add(20 * time.Second).UTC().Format(time.RFC3339Nano)
	c.OperationID = "renew-five"
	cc := c
	cc.InvocationSequence = 0
	cc.BodySHA256 = foundry.Digest([]byte("{}"))
	status, _, err = brokerTestHTTP(context.Background(), server.URL, brokerapi.RenewPath, cc, []byte("{}"))
	if err != nil || status != 200 {
		t.Fatalf("later renewal failed: %d", status)
	}
	c.LeaseGeneration = 4
	c.LeaseExpiresAt = time.Now().Add(-time.Second).UTC().Format(time.RFC3339Nano)
	proof = brokerTestControl(t, server.URL, brokerapi.SettlePath, c)
	if !proof.SettlementProven || proof.LeaseGeneration != 5 {
		t.Fatal("cleanup with old lease failed or reopened authority")
	}
	_ = brokerTestControl(t, server.URL, brokerapi.RetirePath, c)
}

func TestBrokerUnknownInferenceNeverClaimsCleanup(t *testing.T) {
	f := newBrokerFixture(t, "hold-unknown")
	cfg := brokerTestConfig(t, f)
	b, server := startBrokerTest(t, cfg)
	c := brokerTestContext(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _, _ = brokerTestHTTP(ctx, server.URL, brokerapi.ResponsesPath, c, brokerTestBody(""))
	}()
	select {
	case <-f.started:
	case <-time.After(3 * time.Second):
		t.Fatal("inference not started")
	}
	cancel()
	<-done
	brokerAwait(t, func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		p := b.ledger.Sessions[foundry.JSONDigest(c.Owner)].Prompts[c.promptKey()]
		return p.Invocations[1].State == "uncertain"
	})
	cc := c
	cc.InvocationSequence = 0
	cc.OperationID = "unknown-settle"
	for range 3 {
		status, data, err := brokerTestHTTP(context.Background(), server.URL, brokerapi.SettlePath, cc, []byte("{}"))
		var proof brokerControlResponse
		if err != nil || status != 409 || json.Unmarshal(data, &proof) != nil || proof.AmbiguousInvocations != 1 || proof.SettlementProven || proof.ProofDigest != "" {
			t.Fatal("ambiguous inference produced a cleanup proof")
		}
		time.Sleep(30 * time.Millisecond)
	}
	cc.TaskUID = ""
	cc.TaskAttempt = 0
	cc.PromptID = ""
	cc.PromptRequestDigest = ""
	cc.LeaseGeneration = 0
	cc.LeaseExpiresAt = ""
	cc.OperationID = "unknown-retire"
	status, _, err := brokerTestHTTP(context.Background(), server.URL, brokerapi.RetirePath, cc, []byte("{}"))
	if err != nil || status != 409 {
		t.Fatal("ambiguous inference was retired")
	}
	_, inferences, _, deletes := f.counts()
	if inferences != 1 || deletes != 0 {
		t.Fatal("ambiguous inference replayed or lost its remote cleanup target")
	}
}
