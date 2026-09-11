package acp

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orka-agents/agent-runtime-foundry/internal/foundry"
)

type acpTestPeer struct {
	t       *testing.T
	in      *io.PipeWriter
	out     *io.PipeReader
	decoder *json.Decoder
	done    chan error
	events  []map[string]any
	writeMu sync.Mutex
	session string
	id      int
}

type acpTestMCP struct {
	tools   func() []map[string]any
	execute func(http.ResponseWriter, *http.Request, json.RawMessage, string, json.RawMessage)
	lists   atomic.Int32
	calls   atomic.Int32
	badAuth atomic.Bool
}

func (m *acpTestMCP) handler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || r.URL.Path != "/mcp" || r.Header.Get("Authorization") != "Bearer test-only-mcp-token" ||
		r.Header.Get("MCP-Protocol-Version") != acpMCPVersion {
		m.badAuth.Store(true)
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	var request acpRequest
	if json.NewDecoder(r.Body).Decode(&request) != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	switch request.Method {
	case "initialize":
		acpTestMCPResult(w, request.ID, map[string]any{
			"protocolVersion": acpMCPVersion, "capabilities": map[string]any{"tools": map[string]any{}},
		})
	case "notifications/initialized":
		w.WriteHeader(http.StatusAccepted)
	case "tools/list":
		m.lists.Add(1)
		tools := []map[string]any{}
		if m.tools != nil {
			tools = m.tools()
		}
		acpTestMCPResult(w, request.ID, map[string]any{"tools": tools})
	case "tools/call":
		var params struct {
			Name string          `json:"name"`
			Args json.RawMessage `json:"arguments"`
		}
		if json.Unmarshal(request.Params, &params) != nil || m.execute == nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		m.calls.Add(1)
		m.execute(w, r, request.ID, params.Name, params.Args)
	default:
		w.WriteHeader(http.StatusBadRequest)
	}
}

func acpTestMCPResult(w http.ResponseWriter, id json.RawMessage, result any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": id, "result": result})
}

func acpTestToolResult(w http.ResponseWriter, id json.RawMessage, text string, isError bool) {
	acpTestMCPResult(w, id, map[string]any{"content": []map[string]string{{"type": "text", "text": text}}, "isError": isError})
}

func acpTestTools(names ...string) []map[string]any {
	tools := make([]map[string]any, 0, len(names))
	for _, name := range names {
		tools = append(tools, map[string]any{"name": name, "description": "Test tool", "inputSchema": map[string]any{"type": "object", "additionalProperties": true}})
	}
	return tools
}

func newACPTestPeer(t *testing.T, mode string, provider http.HandlerFunc, mcp *acpTestMCP) *acpTestPeer {
	t.Helper()
	providerServer := httptest.NewServer(provider)
	mcpServer := httptest.NewServer(http.HandlerFunc(mcp.handler))
	data := acpTestConfigBytes(mode)
	env := acpTestEnvironment(data)
	env[acpProviderBaseEnv] = providerServer.URL + "/v1"
	cfg, err := verifyACPConfiguration(data, func(key string) string { return env[key] })
	if err != nil {
		t.Fatal(err)
	}
	in, input := io.Pipe()
	output, out := io.Pipe()
	p := &acpTestPeer{t: t, in: input, out: output, decoder: json.NewDecoder(output), done: make(chan error, 1)}
	go func() { p.done <- serveACP(context.Background(), cfg, in, out) }()
	t.Cleanup(func() {
		_ = p.in.Close()
		_ = p.out.Close()
		select {
		case <-p.done:
		case <-time.After(5 * time.Second):
			t.Error("ACP child did not join after transport close")
		}
		providerServer.Close()
		mcpServer.Close()
		if mcp.badAuth.Load() {
			t.Error("MCP request lost scoped authentication or protocol metadata")
		}
	})
	response := p.call("initialize", map[string]any{"protocolVersion": 1, "clientCapabilities": map[string]any{}})
	result, ok := response["result"].(map[string]any)
	if !ok || result["protocolVersion"] != float64(1) {
		t.Fatal("ACP initialize failed")
	}
	caps := result["agentCapabilities"].(map[string]any)
	if caps["loadSession"] != false || caps["mcpCapabilities"].(map[string]any)["http"] != true ||
		caps["promptCapabilities"].(map[string]any)["image"] != false {
		t.Fatal("ACP capabilities claim unsupported functionality")
	}
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	response = p.call("session/new", map[string]any{
		"cwd": cwd,
		"mcpServers": []map[string]any{{"type": "http", "name": "broker", "url": mcpServer.URL + "/mcp",
			"headers": []map[string]string{{"name": "Authorization", "value": "Bearer test-only-mcp-token"}}}},
	})
	result, ok = response["result"].(map[string]any)
	if !ok {
		t.Fatal("ACP session/new failed")
	}
	p.session, ok = result["sessionId"].(string)
	if !ok || !strings.HasPrefix(p.session, "foundry-") {
		t.Fatal("ACP session identity missing")
	}
	return p
}

func (p *acpTestPeer) send(message any) {
	p.t.Helper()
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	if err := json.NewEncoder(p.in).Encode(message); err != nil {
		p.t.Fatal("ACP request write failed")
	}
}

func (p *acpTestPeer) read() map[string]any {
	p.t.Helper()
	type outcome struct {
		value map[string]any
		err   error
	}
	done := make(chan outcome, 1)
	go func() {
		var value map[string]any
		err := p.decoder.Decode(&value)
		done <- outcome{value, err}
	}()
	select {
	case result := <-done:
		if result.err != nil {
			p.t.Fatal("ACP response read failed")
		}
		if result.value["jsonrpc"] != "2.0" {
			p.t.Fatal("invalid ACP response version")
		}
		if result.value["method"] == "session/update" {
			params := result.value["params"].(map[string]any)
			if params["sessionId"] != p.session {
				p.t.Fatal("event belongs to another session")
			}
			p.events = append(p.events, params["update"].(map[string]any))
		}
		return result.value
	case <-time.After(5 * time.Second):
		p.t.Fatal("ACP response did not settle")
		return nil
	}
}

func (p *acpTestPeer) start(method string, params any) int {
	p.id++
	p.send(map[string]any{"jsonrpc": "2.0", "id": p.id, "method": method, "params": params})
	return p.id
}

func (p *acpTestPeer) reply(id int) map[string]any {
	p.t.Helper()
	for {
		message := p.read()
		if message["id"] == float64(id) {
			return message
		}
		if message["method"] != "session/update" {
			p.t.Fatal("unexpected ACP response identity")
		}
	}
}

func (p *acpTestPeer) call(method string, params any) map[string]any {
	p.t.Helper()
	return p.reply(p.start(method, params))
}

func (p *acpTestPeer) prompt(text string) int {
	return p.start("session/prompt", map[string]any{"sessionId": p.session, "prompt": []map[string]string{{"type": "text", "text": text}}})
}

func acpTestCompleted(w http.ResponseWriter, id, text string, calls ...foundry.OutputItem) {
	output := append([]foundry.OutputItem(nil), calls...)
	if text != "" {
		output = append(output, foundry.OutputItem{Type: "message", Content: []foundry.OutputContent{{Type: "output_text", Text: text}}})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(foundry.Response{ID: id, Status: "completed", Output: output})
}

func acpTestReadProvider(t *testing.T, r *http.Request) map[string]json.RawMessage {
	t.Helper()
	if r.Method != http.MethodPost || r.URL.Path != "/v1/responses" || r.Header.Get("Authorization") != "Bearer test-only-proxy-token" {
		t.Error("provider request escaped scoped endpoint or authentication")
	}
	var body map[string]json.RawMessage
	if json.NewDecoder(r.Body).Decode(&body) != nil {
		t.Error("invalid provider request")
	}
	for _, field := range []string{"agent_session_id", "conversation", "background"} {
		if _, present := body[field]; present {
			t.Error("child supplied privileged provider state")
		}
	}
	if string(body["model"]) != `"test-model"` || string(body["stream"]) != "true" || string(body["store"]) != "true" {
		t.Error("provider request lost pinned model or Responses settings")
	}
	return body
}

func acpAssertStop(t *testing.T, response map[string]any, stop string) {
	t.Helper()
	result, ok := response["result"].(map[string]any)
	if !ok || result["stopReason"] != stop || response["error"] != nil {
		t.Fatalf("expected ACP stop reason %s", stop)
	}
}

func acpAssertFailure(t *testing.T, response map[string]any) {
	t.Helper()
	err, ok := response["error"].(map[string]any)
	if !ok || err["code"] != float64(-32603) || response["result"] != nil || err["message"] != acpInternalError.Message {
		t.Fatal("expected generic fatal ACP prompt error")
	}
}

func acpAssertToolEvents(t *testing.T, events []map[string]any, completed, failed int) {
	t.Helper()
	pending := make(map[string]bool)
	seen := make(map[string]bool)
	idPattern := regexp.MustCompile(`^tool-[a-f0-9]{32}$`)
	gotCompleted, gotFailed := 0, 0
	for _, event := range events {
		kind := event["sessionUpdate"]
		if kind != "tool_call" && kind != "tool_call_update" {
			continue
		}
		id, _ := event["toolCallId"].(string)
		if !idPattern.MatchString(id) || event["kind"] != "other" || len(event) != 5 {
			t.Fatal("tool lifecycle metadata is invalid or carries payload fields")
		}
		if kind == "tool_call" {
			if event["status"] != "in_progress" || seen[id] {
				t.Fatal("duplicate or invalid tool start")
			}
			pending[id], seen[id] = true, true
			continue
		}
		if !pending[id] {
			t.Fatal("tool terminal has no unique matching start")
		}
		delete(pending, id)
		switch event["status"] {
		case "completed":
			gotCompleted++
		case "failed":
			gotFailed++
		default:
			t.Fatal("invalid tool terminal status")
		}
	}
	if len(pending) != 0 || gotCompleted != completed || gotFailed != failed {
		t.Fatalf("tool lifecycle pairing differs: completed=%d failed=%d pending=%d", gotCompleted, gotFailed, len(pending))
	}
}

func acpOutput(events []map[string]any) string {
	var text strings.Builder
	for _, event := range events {
		if event["sessionUpdate"] == "agent_message_chunk" {
			text.WriteString(event["content"].(map[string]any)["text"].(string))
		}
	}
	return text.String()
}
