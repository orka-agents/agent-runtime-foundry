package broker

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orka-agents/agent-runtime-foundry/internal/acp"
	"github.com/orka-agents/agent-runtime-foundry/internal/brokerapi"
	"github.com/orka-agents/agent-runtime-foundry/internal/foundry"
)

// This opt-in test runs AgentKit's production hosted server and model loop,
// Foundry's ACP entrypoint over real pipes, and the real lifecycle broker.
// Only the model, MCP backend, supervisor context stamping, and Azure session
// management API are fixtures. It does not establish Azure ingress support.
func TestBrokerAgentKitHostedIntegration(t *testing.T) {
	source := os.Getenv("AGENTKIT_SOURCE_DIR")
	if source == "" {
		t.Skip("set AGENTKIT_SOURCE_DIR and AGENTKIT_PYTHON to test an AgentKit checkout")
	}
	for _, mode := range []string{"success", "tool_error", "authorization_denied", "gateway_strips_proof", "approval_approved", "approval_declined", "approval_expired", "approval_execution_failed"} {
		t.Run(mode, func(t *testing.T) {
			var models, calls, invocations, executions atomic.Int32
			pendingReview, decision, reviewChecked := make(chan struct{}), make(chan struct{}), make(chan struct{})
			heldApproval := strings.HasPrefix(mode, "approval_")
			approvalCode := ""
			switch mode {
			case "approval_declined", "approval_expired":
				approvalCode = mode
			case "approval_execution_failed":
				approvalCode = "tool_execution_failed"
			}
			model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				if r.Method != http.MethodPost || r.URL.Path != "/v1/chat/completions" ||
					bytes.Contains(body, []byte(brokerAgentKitFixtureProof)) || r.Header.Get(brokerAgentKitProofHeader) != "" {
					t.Error("model request used the wrong route or exposed continuation credentials")
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				var request struct {
					Messages []map[string]any `json:"messages"`
					Tools    []map[string]any `json:"tools"`
				}
				if json.Unmarshal(body, &request) != nil || len(request.Tools) != 2 {
					t.Error("hosted model loop lost its static tool schemas")
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				n := models.Add(1)
				if n > 1 {
					last := request.Messages[len(request.Messages)-1]
					text, _ := last["content"].(string)
					var result struct {
						Approved *bool           `json:"approved"`
						Output   json.RawMessage `json:"output"`
						Error    map[string]any  `json:"error"`
					}
					approved := n != 2 || (mode != "tool_error" && approvalCode == "")
					code := "brokered_tool_error"
					if n == 2 && approvalCode != "" {
						code = approvalCode
					}
					if last["role"] != "tool" || json.Unmarshal([]byte(text), &result) != nil ||
						result.Approved == nil || *result.Approved != approved ||
						(!approved && (result.Output != nil || result.Error["code"] != code)) || strings.Contains(text, "private-review-") {
						t.Error("real model resume lost the governed tool result or converted an error to approval")
						w.WriteHeader(http.StatusBadRequest)
						return
					}
				}
				message := map[string]any{"role": "assistant", "content": "Checked the work order and inventory."}
				if n < 3 {
					name, arguments := "lookup_work_order", `{"id":"WO-7"}`
					if n == 2 {
						name, arguments = "lookup_inventory", `{"part":"FBR-7"}`
					}
					message = map[string]any{"role": "assistant", "content": nil, "tool_calls": []any{map[string]any{
						"id": "model-chosen-id", "type": "function", "function": map[string]string{"name": name, "arguments": arguments},
					}}}
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": message}}})
			}))
			t.Cleanup(model.Close)
			hostedURL, stateFile := brokerStartAgentKit(t, source, model.URL, false)
			f := newBrokerFixture(t, "success")
			cfg := brokerTestConfig(t, f)
			cfg.agentKitProof = brokerAgentKitFixtureProof
			var remoteMu sync.Mutex
			var remoteRequests []map[string]json.RawMessage
			client := &http.Client{Transport: brokerFixtureTransport(func(r *http.Request) (*http.Response, error) {
				if !strings.HasSuffix(r.URL.Path, "/endpoint/protocols/openai/responses") {
					return http.DefaultTransport.RoundTrip(r)
				}
				body, err := io.ReadAll(r.Body)
				if err != nil {
					return nil, err
				}
				_ = r.Body.Close()
				var fields map[string]json.RawMessage
				if json.Unmarshal(body, &fields) != nil {
					return nil, errBrokerInvalid
				}
				remoteMu.Lock()
				remoteRequests = append(remoteRequests, fields)
				remoteMu.Unlock()
				if mode == "gateway_strips_proof" {
					var forwarded map[string]json.RawMessage
					_ = json.Unmarshal(body, &forwarded)
					delete(forwarded, "brokered_continuation_proof")
					body, _ = json.Marshal(forwarded)
				}
				request := r.Clone(r.Context())
				request.URL, _ = url.Parse(hostedURL + "/responses")
				request.Host = request.URL.Host
				request.Body = io.NopCloser(bytes.NewReader(body))
				request.ContentLength = int64(len(body))
				return http.DefaultTransport.RoundTrip(request)
			})}
			b, brokerServer := startBrokerTestWithClient(t, cfg, client)
			c := brokerTestContext(cfg)
			c.LeaseExpiresAt = time.Now().Add(time.Minute).UTC().Format(time.RFC3339Nano)
			proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				if r.Header.Get("Authorization") != "Bearer fixture-acp-proxy" ||
					bytes.Contains(body, []byte("brokered_continuation_proof")) || bytes.Contains(body, []byte(cfg.agentKitProof)) {
					t.Error("ACP request acquired broker authority")
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				next := c
				next.InvocationSequence = uint64(invocations.Add(1))
				next.OperationID = fmt.Sprintf("agentkit-integration-%d", next.InvocationSequence)
				next.BodySHA256 = foundry.Digest(body)
				raw, _ := json.Marshal(next)
				r.Header.Set("Authorization", "Bearer "+brokerFixtureBearer)
				r.Header.Set(brokerContextHeader, base64.RawURLEncoding.EncodeToString(raw))
				r.Body = io.NopCloser(bytes.NewReader(body))
				b.ServeHTTP(w, r)
			}))
			t.Cleanup(proxy.Close)
			mcp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer fixture-mcp" || r.Header.Get("MCP-Protocol-Version") != "2025-06-18" {
					t.Error("MCP request lost its scoped authentication")
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				var request struct {
					ID     json.RawMessage `json:"id"`
					Method string          `json:"method"`
					Params struct {
						Name      string          `json:"name"`
						Arguments json.RawMessage `json:"arguments"`
					} `json:"params"`
				}
				if json.NewDecoder(r.Body).Decode(&request) != nil {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				var result any
				switch request.Method {
				case "initialize":
					result = map[string]any{"protocolVersion": "2025-06-18", "capabilities": map[string]any{"tools": map[string]any{}}}
				case "notifications/initialized":
					w.WriteHeader(http.StatusAccepted)
					return
				case "tools/list":
					var tools []map[string]any
					for _, name := range []string{"lookup_work_order", "lookup_inventory"} {
						tools = append(tools, map[string]any{"name": name, "inputSchema": map[string]any{"type": "object"}})
					}
					result = map[string]any{"tools": tools}
				case "tools/call":
					n := calls.Add(1)
					if n == 1 && mode == "authorization_denied" {
						w.Header().Set("Content-Type", "application/json")
						_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": request.ID,
							"error": map[string]any{"code": -32001, "message": "MCP tool call is not authorized"}})
						return
					}
					name, arguments := "lookup_work_order", `{"id":"WO-7"}`
					if n == 2 {
						name, arguments = "lookup_inventory", `{"part":"FBR-7"}`
					}
					if n > 2 || request.Params.Name != name || string(request.Params.Arguments) != arguments {
						t.Error("tool sequence changed or repeated a call")
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					if n == 1 && heldApproval {
						close(pendingReview)
						select {
						case <-decision:
						case <-r.Context().Done():
							return
						}
					}
					if n != 1 || approvalCode == "" || approvalCode == "tool_execution_failed" {
						executions.Add(1)
					}
					failed := n == 1 && (mode == "tool_error" || approvalCode != "")
					structured := map[string]any{"part": "FBR-7", "quantity": 7}
					if failed {
						structured = map[string]any{"isError": true, "error": "MCP tool execution failed"}
						if approvalCode != "" {
							structured["code"] = approvalCode
							structured["error"] = "private-review-note"
						}
					}
					text, _ := json.Marshal(structured)
					result = map[string]any{"content": []map[string]string{{"type": "text", "text": string(text)}},
						"isError": failed, "structuredContent": structured, "_meta": map[string]any{"private": "not-model-visible"}}
				default:
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": result})
			}))
			t.Cleanup(mcp.Close)
			peer := brokerStartAgentKitACP(t, cfg, proxy.URL)
			peer.call("initialize", map[string]any{"protocolVersion": 1, "clientCapabilities": map[string]any{}})
			cwd, err := os.Getwd()
			if err != nil {
				t.Fatal("could not read the ACP working directory")
			}
			session := peer.call("session/new", map[string]any{
				"cwd": cwd, "mcpServers": []any{map[string]any{"type": "http", "name": "governed", "url": mcp.URL + "/mcp",
					"headers": []any{map[string]string{"name": "Authorization", "value": "Bearer fixture-mcp"}}}},
			})
			result, _ := session["result"].(map[string]any)
			if result["sessionId"] == nil {
				t.Fatal("real ACP session creation failed")
			}
			if heldApproval {
				go func() {
					defer close(reviewChecked)
					select {
					case <-pendingReview:
					case <-t.Context().Done():
						return
					}
					if models.Load() != 1 || calls.Load() != 1 || executions.Load() != 0 || invocations.Load() != 1 {
						t.Error("held review executed a tool or continued the real hosted model")
					}
					b.mu.Lock()
					owner := b.ledger.Sessions[foundry.JSONDigest(c.Owner)]
					prompt := owner.Prompts[c.promptKey()]
					owned := !prompt.Closing && prompt.LastSequence == 1 && len(owner.Responses[prompt.LastAlias].CallIDs) == 1
					b.mu.Unlock()
					if !owned {
						t.Error("held review lost its durable original call ownership")
					}
					close(decision)
				}()
			}
			terminal := peer.call("session/prompt", map[string]any{"sessionId": result["sessionId"],
				"prompt": []any{map[string]string{"type": "text", "text": "Check the work order, then check inventory."}}})
			blocked := mode == "authorization_denied" || mode == "gateway_strips_proof"
			if blocked {
				if terminal["error"] == nil || models.Load() != 1 || calls.Load() != 1 || peer.text.String() != "" {
					t.Fatal("denied MCP call or missing continuation proof did not stop before model resume")
				}
			} else {
				result, _ = terminal["result"].(map[string]any)
				if result["stopReason"] != "end_turn" || models.Load() != 3 || calls.Load() != 2 ||
					peer.text.String() != "Checked the work order and inventory." {
					t.Fatal("two governed tool rounds did not complete through the real hosted AgentKit and ACP path")
				}
				remoteMu.Lock()
				if len(remoteRequests) != 3 || bytes.Equal(remoteRequests[1]["previous_response_id"], remoteRequests[2]["previous_response_id"]) {
					t.Error("chained tool results reused a response identity")
				}
				remoteMu.Unlock()
			}
			if heldApproval {
				<-reviewChecked
				wantExecutions := int32(2)
				if approvalCode == "approval_declined" || approvalCode == "approval_expired" {
					wantExecutions = 1
				}
				if executions.Load() != wantExecutions {
					t.Fatal("human decision did not control the counted execution")
				}
			}
			_ = brokerTestControl(t, brokerServer.URL, brokerapi.SettlePath, c)
			_ = brokerTestControl(t, brokerServer.URL, brokerapi.RetirePath, c)
			for _, path := range []string{stateFile, filepath.Join(cfg.stateDir, "state.json")} {
				data, err := os.ReadFile(path)
				if err != nil || bytes.Contains(data, []byte(cfg.agentKitProof)) {
					t.Fatal("durable state was missing or retained continuation credentials")
				}
			}
		})
	}
}

func brokerStartAgentKit(t *testing.T, source, modelURL string, native bool) (string, string) {
	t.Helper()
	source, err := filepath.Abs(source)
	if err != nil {
		t.Fatal("invalid AgentKit checkout path")
	}
	python := os.Getenv("AGENTKIT_PYTHON")
	if native {
		python = os.Getenv("AGENTKIT_MAF_PYTHON")
	}
	if python == "" {
		python = "python3"
	}
	dir := t.TempDir()
	config, stateFile := filepath.Join(dir, "agent.yaml"), filepath.Join(dir, "responses.json")
	var tools []map[string]any
	for _, name := range []string{"lookup_work_order", "lookup_inventory"} {
		tools = append(tools, map[string]any{"name": name, "description": "Read operational data.", "brokeredClass": "read",
			"parameters": map[string]any{"type": "object", "properties": map[string]any{"id": map[string]any{"type": "string"}, "part": map[string]any{"type": "string"}}}})
	}
	spec := map[string]any{"abiVersion": "v0", "metadata": map[string]string{"name": "foundry-integration"},
		"model":        map[string]string{"provider": "openai-compatible", "baseURL": modelURL + "/v1", "name": "fixture-model"},
		"instructions": "Use the operational tools in sequence.", "tools": []any{}, "brokeredTools": tools, "expose": map[string]any{"openai": true, "port": 8088}}
	if native {
		delete(spec, "brokeredTools")
		spec["instructions"] = "Respond to the requested text task."
	}
	body, _ := json.Marshal(spec)
	if os.WriteFile(config, body, 0600) != nil {
		t.Fatal("could not write local AgentKit configuration")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("could not reserve local hosted port")
	}
	address := listener.Addr().String()
	_, port, _ := net.SplitHostPort(address)
	_ = listener.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	arguments := []string{"-m", "agentkit_serve_common.foundry_brokered_cli", "--config", config,
		"--host", "127.0.0.1", "--port", port}
	pythonPaths := []string{filepath.Join(source, "runtimes", "common")}
	if native {
		arguments = []string{"-c", `import sys, uvicorn
from agentkit_serve_common.config import load
from agentkit_serve_common.foundry import create_foundry_app
from agentkit_serve import agent_factory
app = create_foundry_app(load(sys.argv[1]), agent_factory)
uvicorn.run(app, host="127.0.0.1", port=int(sys.argv[2]))
`, config, port}
		pythonPaths = append(pythonPaths, filepath.Join(source, "runtimes", "microsoft-agent-framework"))
	}
	command := exec.CommandContext(ctx, python, arguments...)
	// No inherited model/Azure credentials, and no proof in the command line.
	command.Env = []string{"PATH=" + os.Getenv("PATH"), "PYTHONPATH=" + strings.Join(pythonPaths, string(os.PathListSeparator)),
		"PYTHONDONTWRITEBYTECODE=1", "AGENTKIT_FOUNDRY_BROKERED_MODEL_LOOP=1", "AGENTKIT_FOUNDRY_RESPONSE_STATE_FILE=" + stateFile,
		"AGENTKIT_FOUNDRY_BROKERED_CONTINUATION_PROOF=" + brokerAgentKitFixtureProof}
	var logs bytes.Buffer
	command.Stdout, command.Stderr = &logs, &logs
	if command.Start() != nil {
		cancel()
		t.Fatal("could not start AgentKit Python; set AGENTKIT_PYTHON to an environment with runtimes/common installed")
	}
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	t.Cleanup(func() {
		cancel()
		<-done
		if bytes.Contains(logs.Bytes(), []byte(brokerAgentKitFixtureProof)) {
			t.Error("hosted AgentKit logged continuation credentials")
		}
	})
	base := "http://" + address
	deadline := time.Now().Add(15 * time.Second)
	client := &http.Client{Timeout: time.Second}
	for time.Now().Before(deadline) {
		response, err := client.Get(base + "/readiness")
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return base, stateFile
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("hosted AgentKit did not become ready; check the checkout and common-package Python dependencies")
	return "", ""
}

type brokerAgentKitACP struct {
	t       *testing.T
	input   *io.PipeWriter
	decoder *json.Decoder
	nextID  int
	text    strings.Builder
}

func brokerStartAgentKitACP(t *testing.T, cfg brokerConfiguration, proxyURL string) *brokerAgentKitACP {
	t.Helper()
	body, _ := json.Marshal(cfg.agent)
	path := filepath.Join(t.TempDir(), "foundry.json")
	if os.WriteFile(path, body, 0600) != nil {
		t.Fatal("could not write local ACP configuration")
	}
	t.Setenv(foundry.ModelEnv, cfg.agent.Model)
	t.Setenv(foundry.AgentConfigDigestEnv, foundry.Digest(body))
	t.Setenv("ORKA_FOUNDRY_ACP_PROVIDER_BASE_URL", proxyURL+"/v1")
	t.Setenv("ORKA_FOUNDRY_ACP_PROVIDER_TOKEN", "fixture-acp-proxy")
	input, send := io.Pipe()
	receive, output := io.Pipe()
	done := make(chan error, 1)
	go func() {
		_, err := acp.MaybeServe([]string{"--protocol", "acp", "--config", path}, input, output)
		done <- err
	}()
	t.Cleanup(func() {
		_ = send.Close()
		_ = receive.Close()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("ACP did not join after its pipes closed")
		}
	})
	return &brokerAgentKitACP{t: t, input: send, decoder: json.NewDecoder(receive)}
}

func (p *brokerAgentKitACP) call(method string, params any) map[string]any {
	p.t.Helper()
	p.nextID++
	if json.NewEncoder(p.input).Encode(map[string]any{"jsonrpc": "2.0", "id": p.nextID, "method": method, "params": params}) != nil {
		p.t.Fatal("could not write to real ACP entrypoint")
	}
	for {
		type result struct {
			message map[string]any
			err     error
		}
		done := make(chan result, 1)
		go func() {
			var message map[string]any
			err := p.decoder.Decode(&message)
			done <- result{message, err}
		}()
		select {
		case next := <-done:
			if next.err != nil {
				p.t.Fatal("real ACP entrypoint closed before replying")
			}
			body, _ := json.Marshal(next.message)
			if bytes.Contains(body, []byte(brokerAgentKitFixtureProof)) || bytes.Contains(body, []byte("caresp_")) ||
				bytes.Contains(body, []byte("not-model-visible")) {
				p.t.Fatal("ACP output leaked continuation credentials, hosted identities, or MCP metadata")
			}
			if next.message["id"] == float64(p.nextID) {
				return next.message
			}
			if next.message["method"] != "session/update" {
				p.t.Fatal("ACP reply had an unexpected identity")
			}
			params, _ := next.message["params"].(map[string]any)
			update, _ := params["update"].(map[string]any)
			if update["sessionUpdate"] == "agent_message_chunk" {
				content, _ := update["content"].(map[string]any)
				text, _ := content["text"].(string)
				p.text.WriteString(text)
			}
		case <-time.After(10 * time.Second):
			p.t.Fatal("real ACP entrypoint did not settle")
		}
	}
}
