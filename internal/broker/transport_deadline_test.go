package broker

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/orka-agents/agent-runtime-foundry/internal/brokerapi"
	"github.com/orka-agents/agent-runtime-foundry/internal/foundry"
)

// These tests use the production HTTP transport and real HTTP encoding over
// net.Pipe. No socket, DNS, identity provider, or live service is used. Fake
// time preserves the actual 30/45-second deadline relationship.
type brokerDeadlineRemote struct {
	mu                                  sync.Mutex
	wg                                  sync.WaitGroup
	closed                              bool
	createDelay                         time.Duration
	responseDelay                       time.Duration
	getStatus                           string
	responseStatus                      int
	sessions                            map[string]bool
	creates, inferences, deletes, stops int
	createAckWritten                    bool
}

func (f *brokerDeadlineRemote) dial(context.Context, string, string) (net.Conn, error) {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return nil, net.ErrClosed
	}
	f.wg.Add(1)
	f.mu.Unlock()
	left, right := net.Pipe()
	go func() {
		defer f.wg.Done()
		defer right.Close()
		r, err := http.ReadRequest(bufio.NewReader(right))
		if err != nil {
			return
		}
		body, err := io.ReadAll(r.Body)
		_ = r.Body.Close()
		if err != nil {
			return
		}
		status, value, created := f.reply(r, body)
		raw, _ := json.Marshal(value)
		response := &http.Response{StatusCode: status, Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
			Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(bytes.NewReader(raw)),
			ContentLength: int64(len(raw)), Close: true}
		err = response.Write(right)
		if created && err == nil {
			f.mu.Lock()
			f.createAckWritten = true
			f.mu.Unlock()
		}
	}()
	return left, nil
}

func (f *brokerDeadlineRemote) close() {
	// Transport dial goroutines can outlive the cancelled request. Close
	// admission before joining so a late dial cannot race the first Add
	// against Wait after the previous handlers have all exited.
	f.mu.Lock()
	f.closed = true
	f.mu.Unlock()
	f.wg.Wait()
}

func (f *brokerDeadlineRemote) reply(r *http.Request, body []byte) (int, any, bool) {
	p := strings.TrimPrefix(r.URL.Path, "/api/projects/fixture/agents/fixture")
	if p == "" && r.Method == http.MethodGet {
		// Separate a renewal at +30s from the header timeout at +30.2s.
		time.Sleep(200 * time.Millisecond)
		return 200, map[string]any{"name": "fixture", "agent_endpoint": map[string]any{
			"authorization_schemes": []any{map[string]string{"type": "entra"}}}}, false
	}
	if p == "/versions/3" {
		return 200, map[string]any{"name": "fixture", "version": "3", "status": "active", "definition": map[string]string{"kind": "hosted"}}, false
	}
	if p == "/endpoint/sessions" && r.Method == http.MethodPost {
		var request foundry.RemoteSession
		_ = json.Unmarshal(body, &request)
		f.mu.Lock()
		f.creates++
		f.sessions[request.ID] = true
		f.mu.Unlock()
		time.Sleep(f.createDelay)
		return 201, brokerDeadlineSession(request.ID, "active"), true
	}
	if p == "/endpoint/protocols/openai/responses" && r.Method == http.MethodPost {
		var request foundry.ResponseRequest
		_ = json.Unmarshal(body, &request)
		f.mu.Lock()
		f.inferences++
		f.mu.Unlock()
		time.Sleep(f.responseDelay)
		if f.responseStatus != 200 {
			return f.responseStatus, map[string]string{"error": "synthetic admission rejection"}, false
		}
		return 200, map[string]any{"id": "synthetic-response", "status": "completed", "agent_session_id": request.AgentSessionID,
			"output": []any{map[string]any{"id": "synthetic-item", "type": "message", "role": "assistant",
				"content": []any{map[string]string{"type": "output_text", "text": "ok"}}}}}, false
	}
	if strings.HasPrefix(p, "/endpoint/sessions/") {
		id := strings.TrimPrefix(p, "/endpoint/sessions/")
		stop := strings.HasSuffix(id, ":stop")
		id = strings.TrimSuffix(id, ":stop")
		f.mu.Lock()
		defer f.mu.Unlock()
		if stop && r.Method == http.MethodPost {
			f.stops++
			return 204, nil, false
		}
		if r.Method == http.MethodDelete {
			f.deletes++
			delete(f.sessions, id)
			return 204, nil, false
		}
		if !f.sessions[id] {
			return 404, nil, false
		}
		status := f.getStatus
		if f.stops > 0 {
			status = "idle"
		}
		return 200, brokerDeadlineSession(id, status), false
	}
	return 404, nil, false
}

func brokerDeadlineSession(id, status string) map[string]any {
	return map[string]any{"agent_session_id": id, "version_indicator": map[string]string{"type": "version_ref", "agent_version": "3"}, "status": status}
}

func brokerDeadlineRequest(b *lifecycleBroker, path string, c brokerContext, body []byte) *httptest.ResponseRecorder {
	c.BodySHA256 = foundry.Digest(body)
	raw, _ := json.Marshal(c)
	r := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+brokerFixtureBearer)
	r.Header.Set(brokerContextHeader, base64.RawURLEncoding.EncodeToString(raw))
	w := httptest.NewRecorder()
	b.ServeHTTP(w, r)
	return w
}

func brokerDeadlineControl(t *testing.T, b *lifecycleBroker, path string, c brokerContext) brokerControlResponse {
	t.Helper()
	c, body := brokerTestControlContext(path, c)
	for i := 0; i < 100; i++ {
		w := brokerDeadlineRequest(b, path, c, body)
		var proof brokerControlResponse
		if json.Unmarshal(w.Body.Bytes(), &proof) != nil {
			t.Fatal("synthetic control has no valid receipt")
		}
		if w.Code == 200 {
			return proof
		}
		if w.Code != 409 {
			t.Fatalf("unexpected control status %d", w.Code)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("synthetic cleanup did not reach a receipt")
	return brokerControlResponse{}
}

func TestBrokerRemoteHeaderDeadline(t *testing.T) {
	for _, tc := range []struct {
		name                            string
		create, response, wantElapsed   time.Duration
		getStatus                       string
		responseStatus, wantInvocations int
		wantSuccess, wantAmbiguity      bool
		wantCreatePending               bool
	}{
		{"create_ack_inside_operation_budget", 31 * time.Second, 0, 31200 * time.Millisecond, "active", 200, 1, true, false, false},
		{"create_operation_deadline_still_bounds_original", 46 * time.Second, 0, 45200 * time.Millisecond, "active", 200, 0, false, false, true},
		{"response_headers_timeout_is_ambiguous", 0, 46 * time.Second, 45200 * time.Millisecond, "active", 200, 1, false, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				started := time.Now()
				f := &brokerDeadlineRemote{createDelay: tc.create, responseDelay: tc.response, getStatus: tc.getStatus,
					responseStatus: tc.responseStatus, sessions: map[string]bool{}}
				client := newBrokerHTTPClient()
				transport := client.Transport.(*http.Transport)
				transport.DialContext = f.dial
				transport.DisableKeepAlives = true
				agent := foundry.AgentConfig{Model: "fixture-model", ToolSchemaMode: foundry.ToolSchemaModeProviderStatic,
					HostedTarget: foundry.HostedTarget{ProjectEndpoint: "http://synthetic.invalid/api/projects/fixture", AgentName: "fixture", AgentVersion: "3"}}
				raw, _ := json.Marshal(agent)
				cfg := brokerConfiguration{agent: agent, configDigest: foundry.Digest(raw), stateDir: filepath.Join(t.TempDir(), "broker"), bearer: brokerFixtureBearer, operationTimeout: 45 * time.Second}
				b, err := newLifecycleBroker(context.Background(), cfg, brokerFixtureToken{brokerTestToken()}, client)
				if err != nil {
					t.Fatal("synthetic broker did not initialize")
				}
				shutdown := sync.OnceFunc(func() {
					b.close()
					transport.CloseIdleConnections()
					f.close()
				})
				defer shutdown()
				c := brokerTestContext(cfg)
				c.LeaseExpiresAt = started.Add(30 * time.Second).UTC().Format(time.RFC3339Nano)
				done := make(chan *httptest.ResponseRecorder, 1)
				go func() { done <- brokerDeadlineRequest(b, brokerapi.ResponsesPath, c, brokerTestBody("")) }()
				for generation := uint64(2); generation <= 3; generation++ {
					time.Sleep(time.Until(started.Add(time.Duration(generation-1) * 15 * time.Second)))
					renew := c
					renew.LeaseGeneration = generation
					renew.LeaseExpiresAt = started.Add(time.Duration(generation+1) * 15 * time.Second).UTC().Format(time.RFC3339Nano)
					renew.OperationID = fmt.Sprintf("synthetic-renew-%d", generation)
					renew.InvocationSequence = 0
					w := brokerDeadlineRequest(b, brokerapi.RenewPath, renew, []byte("{}"))
					var proof brokerControlResponse
					if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &proof) != nil || proof.State != "open" || proof.LeaseGeneration != generation {
						t.Fatal("original invocation did not acknowledge exact renewal")
					}
				}
				w := <-done
				if (w.Code == http.StatusOK) != tc.wantSuccess {
					t.Fatalf("unexpected invocation HTTP %d", w.Code)
				}
				elapsed := time.Since(started)
				if elapsed != tc.wantElapsed {
					t.Fatalf("unexpected fake-time failure/completion duration %s", elapsed)
				}
				if tc.wantAmbiguity {
					brokerAwait(t, func() bool {
						b.mu.Lock()
						defer b.mu.Unlock()
						return b.ledger.Sessions[foundry.JSONDigest(c.Owner)].Prompts[c.promptKey()].Invocations[1].State == "uncertain"
					})
				}
				pending := tc.wantAmbiguity || tc.wantCreatePending
				if pending {
					for _, path := range []string{brokerapi.SettlePath, brokerapi.RetirePath} {
						cc, body := brokerTestControlContext(path, c)
						w := brokerDeadlineRequest(b, path, cc, body)
						var proof brokerControlResponse
						if w.Code != http.StatusConflict || json.Unmarshal(w.Body.Bytes(), &proof) != nil ||
							proof.CreatePending != tc.wantCreatePending || proof.SettlementProven || proof.RetirementProven || proof.ProofDigest != "" {
							t.Fatal("unacknowledged work lost pending ownership or authorized cleanup proof")
						}
					}
				} else {
					proof := brokerDeadlineControl(t, b, brokerapi.SettlePath, c)
					if !proof.SettlementProven {
						t.Fatal("exact synthetic prompt settlement missing")
					}
					proof = brokerDeadlineControl(t, b, brokerapi.RetirePath, c)
					if !proof.RetirementProven {
						t.Fatal("known exact owner retirement missing")
					}
				}
				// Stop broker workers and fixture admission, then join the original
				// delayed HTTP write before inspecting the final evidence.
				shutdown()
				b.mu.Lock()
				owner := b.ledger.Sessions[foundry.JSONDigest(c.Owner)]
				prompt := owner.Prompts[c.promptKey()]
				inv := prompt.Invocations[1]
				if prompt.LeaseGeneration != 3 || !prompt.LeaseExpiresAt.Equal(started.Add(60*time.Second)) || len(prompt.Invocations) != 1 {
					t.Fatal("renewal or invocation identity changed")
				}
				if !tc.wantSuccess && (inv.ResponseID != "" || len(owner.Responses) != 0) {
					t.Fatal("unexpected response acceptance evidence")
				}
				if owner.Retired == pending || tc.wantCreatePending && owner.CreateState != "intent" {
					t.Fatal("retirement classification mismatch")
				}
				state, createState := inv.State, owner.CreateState
				b.mu.Unlock()
				f.mu.Lock()
				defer f.mu.Unlock()
				if f.creates != 1 || f.inferences != tc.wantInvocations || f.deletes != map[bool]int{true: 0, false: 1}[pending] {
					t.Fatal("replay, incorrect submission, or incorrect deletion")
				}
				if tc.create > cfg.operationTimeout && f.createAckWritten {
					t.Fatal("original Session POST acknowledgment was unexpectedly delivered")
				}
				t.Logf("elapsed=%s leaseGeneration=3 expiryFromInitial=30s creates=%d inferenceSubmissions=%d deletes=%d originalCreateAckWritten=%v state=%s createState=%s", elapsed, f.creates, f.inferences, f.deletes, f.createAckWritten, state, createState)
			})
		})
	}
}

func TestBrokerDeadlineRemoteShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := &brokerDeadlineRemote{}
		original, err := f.dial(context.Background(), "tcp", "synthetic.invalid")
		if err != nil {
			t.Fatal(err)
		}
		defer original.Close()

		// A transport dial may already be queued when its request is cancelled.
		queued, release := make(chan struct{}), make(chan struct{})
		type dialResult struct {
			conn net.Conn
			err  error
		}
		late := make(chan dialResult, 1)
		go func() {
			close(queued)
			<-release
			conn, err := f.dial(context.Background(), "tcp", "synthetic.invalid")
			late <- dialResult{conn, err}
		}()
		<-queued
		closed := make(chan struct{})
		go func() {
			f.close()
			close(closed)
		}()
		synctest.Wait()
		select {
		case <-closed:
			t.Fatal("fixture shutdown did not join its admitted handler")
		default:
		}

		close(release)
		result := <-late
		if result.conn != nil {
			_ = result.conn.Close()
			t.Fatal("fixture admitted a queued dial after shutdown")
		}
		if result.err != net.ErrClosed {
			t.Fatalf("late dial error = %v, want closed admission", result.err)
		}
		_ = original.Close()
		<-closed
	})
}
