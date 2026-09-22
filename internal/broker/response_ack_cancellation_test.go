package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/orka-agents/agent-runtime-foundry/internal/brokerapi"
	"github.com/orka-agents/agent-runtime-foundry/internal/foundry"
)

// Hold the original provider acknowledgement after dispatch, before the broker
// can read either headers or response.created. Prompt revocation must close new
// admission immediately without discarding that one request's cleanup evidence.
func TestBrokerCancellationPreservesOriginalResponseAcknowledgement(t *testing.T) {
	for _, shape := range []string{"success", "hold-known"} {
		for _, trigger := range []string{"disconnect", "settle", "retire", "lease-expiry"} {
			t.Run(shape+"/"+trigger, func(t *testing.T) {
				f := newBrokerFixture(t, shape)
				t.Cleanup(f.unblock)
				cfg := brokerTestConfig(t, f)
				received := make(chan context.Context, 1)
				release := make(chan struct{})
				unblock := sync.OnceFunc(func() { close(release) })
				t.Cleanup(unblock)
				client := &http.Client{Transport: brokerFixtureTransport(func(r *http.Request) (*http.Response, error) {
					response, err := http.DefaultTransport.RoundTrip(r)
					if err != nil || !strings.HasSuffix(r.URL.Path, "/endpoint/protocols/openai/responses") {
						return response, err
					}
					received <- r.Context()
					select {
					case <-release:
						return response, nil
					case <-r.Context().Done():
						_ = response.Body.Close()
						return nil, r.Context().Err()
					}
				})}
				b, server := startBrokerTestWithClient(t, cfg, client)
				c := brokerTestContext(cfg)
				if trigger == "lease-expiry" {
					c.LeaseExpiresAt = time.Now().Add(500 * time.Millisecond).UTC().Format(time.RFC3339Nano)
				}
				ctx, cancel := context.WithCancel(context.Background())
				t.Cleanup(cancel)
				done := brokerAsyncInference(ctx, server.URL, c, brokerTestBody(""))
				var original context.Context
				select {
				case original = <-received:
				case <-time.After(3 * time.Second):
					t.Fatal("original inference did not reach the held acknowledgement")
				}
				if brokerInvocationState(b, c) != "intent" {
					t.Fatal("provider-only acknowledgement was treated as durable broker evidence")
				}
				switch trigger {
				case "disconnect":
					cancel()
				case "settle", "retire":
					path := brokerapi.SettlePath
					if trigger == "retire" {
						path = brokerapi.RetirePath
					}
					control, body := brokerTestControlContext(path, c)
					status, _, err := brokerTestHTTP(context.Background(), server.URL, path, control, body)
					if err != nil || status != http.StatusConflict {
						t.Fatal("cleanup claimed success before the original acknowledgement")
					}
				}
				brokerAwait(t, func() bool {
					b.mu.Lock()
					defer b.mu.Unlock()
					return b.ledger.Sessions[foundry.JSONDigest(c.Owner)].Prompts[c.promptKey()].Closing
				})
				if original.Err() != nil {
					t.Fatal("prompt cancellation discarded the original response acknowledgement")
				}
				statusContext, body := brokerTestControlContext(brokerapi.StatusPath, c)
				status, raw, err := brokerTestHTTP(context.Background(), server.URL, brokerapi.StatusPath, statusContext, body)
				var pending brokerControlResponse
				if err != nil || status != http.StatusOK || json.Unmarshal(raw, &pending) != nil ||
					pending.ActiveInvocations != 1 || pending.AmbiguousInvocations != 0 || pending.SettlementProven || pending.RetirementProven {
					t.Fatal("original unacknowledged request lost its active owner or fabricated cleanup")
				}
				later := c
				later.OperationID, later.InvocationSequence = "after-cancellation", c.InvocationSequence+1
				status, _, err = brokerTestHTTP(context.Background(), server.URL, brokerapi.ResponsesPath, later, brokerTestBody(""))
				if err != nil || status == http.StatusOK {
					t.Fatal("closed prompt admitted another inference")
				}
				unblock()
				if result := brokerWaitInference(t, done); result.err == nil && result.status == http.StatusOK {
					t.Fatal("cancelled prompt exposed successful output")
				}
				proof := brokerTestControl(t, server.URL, brokerapi.SettlePath, c)
				if !proof.SettlementProven || proof.ActiveInvocations != 0 || proof.AmbiguousInvocations != 0 || !foundry.DigestValid(proof.ProofDigest) {
					t.Fatal("original acknowledgement did not permit genuine settlement")
				}
				proof = brokerTestControl(t, server.URL, brokerapi.RetirePath, c)
				creates, inferences, _, deletes := f.counts()
				if !proof.RetirementProven || creates != 1 || inferences != 1 || deletes != 1 {
					t.Fatal("acknowledgement recovery replayed inference or skipped retirement")
				}
			})
		}
	}
}

type brokerDelayedResponseBody struct {
	ctx    context.Context
	delay  time.Duration
	reader io.Reader
}

func (b *brokerDelayedResponseBody) Read(p []byte) (int, error) {
	if b.delay > 0 {
		timer := time.NewTimer(b.delay)
		b.delay = 0
		defer timer.Stop()
		select {
		case <-b.ctx.Done():
			return 0, b.ctx.Err()
		case <-timer.C:
		}
	}
	return b.reader.Read(p)
}

// The acknowledgement budget must bound a silent body after HTTP headers, but
// it must not turn into a new timeout on an already acknowledged model stream.
func TestBrokerResponseAcknowledgementDeadline(t *testing.T) {
	for _, accepted := range []bool{false, true} {
		name := "missing-acknowledgement-remains-ambiguous"
		if accepted {
			name = "acknowledged-stream-outlives-acknowledgement-budget"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				remote := &brokerDeadlineRemote{sessions: map[string]bool{}, getStatus: "active", responseStatus: http.StatusOK}
				client := &http.Client{Transport: brokerFixtureTransport(func(r *http.Request) (*http.Response, error) {
					raw, err := io.ReadAll(r.Body)
					if err != nil {
						return nil, err
					}
					_ = r.Body.Close()
					status, data, _ := remote.reply(r, raw)
					encoded, _ := json.Marshal(data)
					body := io.Reader(bytes.NewReader(encoded))
					media := "application/json"
					if strings.HasSuffix(r.URL.Path, "/endpoint/protocols/openai/responses") {
						media = "text/event-stream"
						var request foundry.ResponseRequest
						if json.Unmarshal(raw, &request) != nil {
							return nil, errBrokerInvalid
						}
						complete := "data: {\"type\":\"response.completed\",\"response\":" + string(encoded) + "}\n\n"
						body = &brokerDelayedResponseBody{ctx: r.Context(), delay: time.Minute, reader: strings.NewReader(complete)}
						if accepted {
							created, _ := json.Marshal(map[string]any{"type": "response.created", "response": map[string]any{
								"id": "synthetic-response", "status": "in_progress", "agent_session_id": request.AgentSessionID}})
							body = io.MultiReader(strings.NewReader("data: "+string(created)+"\n\n"), body)
						}
					}
					return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": []string{media}}, Body: io.NopCloser(body)}, nil
				})}
				agent := foundry.AgentConfig{Model: "fixture-model", ToolSchemaMode: foundry.ToolSchemaModeProviderStatic,
					HostedTarget: foundry.HostedTarget{ProjectEndpoint: "http://synthetic.invalid/api/projects/fixture", AgentName: "fixture", AgentVersion: "3"}}
				raw, _ := json.Marshal(agent)
				cfg := brokerConfiguration{agent: agent, configDigest: foundry.Digest(raw), stateDir: filepath.Join(t.TempDir(), "broker"),
					bearer: brokerFixtureBearer, operationTimeout: 45 * time.Second}
				b, err := newLifecycleBroker(context.Background(), cfg, brokerFixtureToken{brokerTestToken()}, client)
				if err != nil {
					t.Fatal("could not create synthetic broker")
				}
				defer b.close()
				c := brokerTestContext(cfg)
				c.LeaseExpiresAt = time.Now().Add(2 * time.Minute).UTC().Format(time.RFC3339Nano)
				started := time.Now()
				w := brokerDeadlineRequest(b, brokerapi.ResponsesPath, c, brokerTestBody(""))
				wantElapsed := 45*time.Second + 200*time.Millisecond
				wantState, wantStatus := "uncertain", http.StatusConflict
				if accepted {
					wantElapsed = time.Minute + 200*time.Millisecond
					wantState, wantStatus = "completed", http.StatusOK
				}
				if time.Since(started) != wantElapsed || w.Code != wantStatus || brokerInvocationState(b, c) != wantState {
					t.Fatalf("acknowledgement deadline: elapsed=%s status=%d state=%s", time.Since(started), w.Code, brokerInvocationState(b, c))
				}
				remote.mu.Lock()
				creates, inferences, deletes := remote.creates, remote.inferences, remote.deletes
				remote.mu.Unlock()
				if creates != 1 || inferences != 1 || deletes != 0 {
					t.Fatal("acknowledgement deadline replayed work or deleted an unresolved session")
				}
			})
		})
	}
}
