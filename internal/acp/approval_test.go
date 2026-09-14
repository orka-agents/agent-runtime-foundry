package acp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/orka-agents/agent-runtime-foundry/internal/foundry"
)

// Real HTTP encoding and the production transport run over net.Pipe, so virtual
// time can exercise the actual two- and fifteen-minute client deadlines.
func TestACPHeldToolCallTimeouts(t *testing.T) {
	for _, tc := range []struct {
		name       string
		method     string
		delay      time.Duration
		cancelAt   time.Duration
		wantTime   time.Duration
		wantResult bool
	}{
		{"review_over_two_minutes", "tools/call", 130 * time.Second, 0, 130 * time.Second, true},
		{"review_and_execution", "tools/call", 840 * time.Second, 0, 840 * time.Second, true},
		{"tool_hard_limit", "tools/call", 901 * time.Second, 0, 900 * time.Second, false},
		{"task_cancelled", "tools/call", 840 * time.Second, 130 * time.Second, 130 * time.Second, false},
		{"discovery_keeps_limit", "tools/list", 121 * time.Second, 0, 120 * time.Second, false},
		{"model_keeps_limit", "model", 121 * time.Second, 0, 120 * time.Second, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				if tc.cancelAt != 0 {
					go func() { time.Sleep(tc.cancelAt); cancel() }()
				}
				client := newACPHTTPClient()
				defer client.CloseIdleConnections()
				var wg sync.WaitGroup
				var calls atomic.Int32
				client.Transport.(*http.Transport).DialContext = func(context.Context, string, string) (net.Conn, error) {
					left, right := net.Pipe()
					wg.Go(func() {
						defer right.Close() //nolint:errcheck
						r, err := http.ReadRequest(bufio.NewReader(right))
						if err != nil {
							return
						}
						_, _ = io.Copy(io.Discard, r.Body)
						_ = r.Body.Close()
						calls.Add(1)
						time.Sleep(tc.delay)
						body := `{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"counted once"}],"isError":false}}`
						response := &http.Response{StatusCode: http.StatusOK, Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
							Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(body)),
							ContentLength: int64(len(body)), Close: true}
						_ = response.Write(right)
					})
					return left, nil
				}
				started := time.Now()
				var err error
				if tc.method == "model" {
					request, _ := http.NewRequestWithContext(ctx, http.MethodPost, "http://127.0.0.1/responses", strings.NewReader("{}"))
					response, requestErr := client.Do(request)
					err = requestErr
					if response != nil {
						_ = response.Body.Close()
					}
				} else {
					mcp := &acpMCPClient{url: "http://127.0.0.1/mcp", client: client}
					_, err = mcp.call(ctx, tc.method, map[string]any{"name": "counted_action", "arguments": map[string]any{}})
				}
				if (err == nil) != tc.wantResult || time.Since(started) != tc.wantTime || calls.Load() != 1 {
					t.Errorf("held request: success=%v elapsed=%s calls=%d", err == nil, time.Since(started), calls.Load())
				}
				if client.Timeout != 120*time.Second {
					t.Error("tool wait changed the shared model/discovery client")
				}
				wg.Wait()
			})
		})
	}
}

func TestACPHeldApprovalsNeedSeparateDecisionsAndLeaveOtherSessionsAvailable(t *testing.T) {
	var models, executions atomic.Int32
	pending := make(chan int, 2)
	decisions := make(chan bool, 2)
	mcp := &acpTestMCP{tools: func() []map[string]any { return acpTestTools("counted_action") }}
	mcp.execute = func(w http.ResponseWriter, r *http.Request, id json.RawMessage, _ string, args json.RawMessage) {
		var proposed struct {
			Number int `json:"number"`
		}
		if json.Unmarshal(args, &proposed) != nil {
			t.Error("invalid proposed action")
			return
		}
		pending <- proposed.Number
		select {
		case <-r.Context().Done():
			return
		case approved := <-decisions:
			if !approved || r.Context().Err() != nil {
				return
			}
		}
		executions.Add(1)
		acpTestToolResult(w, id, `{"executed":true}`, false)
	}
	peer := newACPTestPeer(t, foundry.ToolSchemaModeRequest, func(w http.ResponseWriter, r *http.Request) {
		body := acpTestReadProvider(t, r)
		switch models.Add(1) {
		case 1:
			acpTestCompleted(w, "first-proposal", "", acpTestCall("counted_action", "first-call", `{"number":1}`))
		case 2:
			acpAssertApprovalContinuation(t, body, "first-proposal", "first-call", false)
			acpTestCompleted(w, "second-proposal", "", acpTestCall("counted_action", "second-call", `{"number":2}`))
		case 3:
			acpAssertApprovalContinuation(t, body, "second-proposal", "second-call", false)
			acpTestCompleted(w, "final", "Both actions completed.")
		default:
			t.Error("model submission repeated")
		}
	}, mcp)
	id := peer.prompt("Propose two counted actions.")
	peer.read() // Consume the first tool-start event while its review stays held.
	if <-pending != 1 || models.Load() != 1 || executions.Load() != 0 {
		t.Fatal("action or continuation ran before its first decision")
	}
	other := newACPTestPeer(t, foundry.ToolSchemaModeRequest, func(w http.ResponseWriter, _ *http.Request) {
		acpTestCompleted(w, "unrelated-response", "An independent session progressed.")
	}, &acpTestMCP{})
	acpAssertStop(t, other.reply(other.prompt("Continue independently.")), "end_turn")
	if models.Load() != 1 || executions.Load() != 0 {
		t.Fatal("unrelated progress released the held action")
	}
	decisions <- true
	peer.read() // First action completed.
	peer.read() // Second action proposed.
	if <-pending != 2 || executions.Load() != 1 || models.Load() != 2 {
		t.Fatal("the first approval authorized a later action")
	}
	decisions <- true
	acpAssertStop(t, peer.reply(id), "end_turn")
	if executions.Load() != 2 || mcp.calls.Load() != 2 || models.Load() != 3 {
		t.Fatal("approved actions did not execute and continue exactly once")
	}
}

func acpAssertApprovalContinuation(t *testing.T, body map[string]json.RawMessage, previous, call string, isError bool) {
	t.Helper()
	var outputs []foundry.FunctionOutput
	var previousID string
	if json.Unmarshal(body["previous_response_id"], &previousID) != nil || previousID != previous ||
		json.Unmarshal(body["input"], &outputs) != nil || len(outputs) != 1 || outputs[0].CallID != call {
		t.Error("approval result did not continue its original response and pending call")
		return
	}
	var result struct {
		IsError bool `json:"isError"`
	}
	if json.Unmarshal([]byte(outputs[0].Output), &result) != nil || result.IsError != isError {
		t.Error("approval result changed the execution outcome")
	}
}

func TestACPHeldApprovalFinalErrors(t *testing.T) {
	for _, code := range []string{"approval_declined", "approval_expired", "approval_cancelled", "approval_stale", "tool_execution_failed", "tool_outcome_unknown"} {
		t.Run(code, func(t *testing.T) {
			var models, executions atomic.Int32
			pending, decision := make(chan struct{}), make(chan struct{})
			mcp := &acpTestMCP{tools: func() []map[string]any { return acpTestTools("counted_action") }}
			mcp.execute = func(w http.ResponseWriter, r *http.Request, id json.RawMessage, _ string, _ json.RawMessage) {
				close(pending)
				select {
				case <-r.Context().Done():
					return
				case <-decision:
				}
				if code == "tool_execution_failed" || code == "tool_outcome_unknown" {
					executions.Add(1)
				}
				acpTestMCPResult(w, id, map[string]any{
					"content": []map[string]string{{"type": "text", "text": "Safe final outcome."}}, "isError": true,
					"structuredContent": map[string]any{"isError": true, "code": code},
					"_meta":             map[string]any{"reviewer": "private-reviewer"},
				})
			}
			peer := newACPTestPeer(t, foundry.ToolSchemaModeRequest, func(w http.ResponseWriter, r *http.Request) {
				body := acpTestReadProvider(t, r)
				if models.Add(1) == 1 {
					acpTestCompleted(w, "held-response", "", acpTestCall("counted_action", "held-call", `{}`))
					return
				}
				acpAssertApprovalContinuation(t, body, "held-response", "held-call", true)
				if !bytes.Contains(body["input"], []byte(code)) || bytes.Contains(body["input"], []byte("private-reviewer")) {
					t.Error("final outcome lost its code or exposed review metadata")
				}
				acpTestCompleted(w, "final-response", "The final outcome was received.")
			}, mcp)
			id := peer.prompt("Propose a counted action.")
			peer.read()
			<-pending
			if models.Load() != 1 || executions.Load() != 0 {
				t.Fatal("pending approval executed or resumed the model")
			}
			close(decision)
			acpAssertStop(t, peer.reply(id), "end_turn")
			wantExecutions := int32(0)
			if code == "tool_execution_failed" || code == "tool_outcome_unknown" {
				wantExecutions = 1
			}
			if models.Load() != 2 || executions.Load() != wantExecutions || mcp.calls.Load() != 1 {
				t.Fatal("final error repeated an action or a model continuation")
			}
		})
	}
}

func TestACPHeldApprovalCancellationAndLostResultNeverReplay(t *testing.T) {
	for _, mode := range []string{"cancelled", "result_lost"} {
		t.Run(mode, func(t *testing.T) {
			var models, executions atomic.Int32
			pending, decision, disconnected := make(chan struct{}), make(chan struct{}), make(chan struct{})
			mcp := &acpTestMCP{tools: func() []map[string]any { return acpTestTools("counted_action") }}
			mcp.execute = func(w http.ResponseWriter, r *http.Request, _ json.RawMessage, _ string, _ json.RawMessage) {
				close(pending)
				select {
				case <-r.Context().Done():
					close(disconnected)
					return
				case <-decision:
				}
				executions.Add(1)
				connection, _, err := w.(http.Hijacker).Hijack()
				if err == nil {
					_ = connection.Close()
				}
			}
			peer := newACPTestPeer(t, foundry.ToolSchemaModeRequest, func(w http.ResponseWriter, _ *http.Request) {
				models.Add(1)
				acpTestCompleted(w, "held-response", "", acpTestCall("counted_action", "held-call", `{}`))
			}, mcp)
			id := peer.prompt("Propose a counted action.")
			peer.read()
			<-pending
			if mode == "cancelled" {
				peer.send(map[string]any{"jsonrpc": "2.0", "method": "session/cancel", "params": map[string]string{"sessionId": peer.session}})
				acpAssertStop(t, peer.reply(id), "cancelled")
				<-disconnected
				close(decision) // A decision after cancellation has no waiting action.
			} else {
				close(decision)
				acpAssertFailure(t, peer.reply(id))
			}
			if peer.reply(peer.prompt("Try the same session again."))["error"] == nil {
				t.Fatal("a poisoned approval session accepted another prompt")
			}
			wantExecutions := int32(0)
			if mode == "result_lost" {
				wantExecutions = 1
			}
			if models.Load() != 1 || executions.Load() != wantExecutions || mcp.calls.Load() != 1 {
				t.Fatal("cancelled or uncertain action was repeated or continued")
			}
		})
	}
}
