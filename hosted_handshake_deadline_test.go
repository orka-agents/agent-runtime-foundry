package main

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

// Real Gorilla handshakes over net.Pipe exercise the production 10/30-second
// deadlines without an external network service.
func hostedHandshakePipeDial(t *testing.T, ctx context.Context, handler http.Handler, endpoint string, headers http.Header) (*websocket.Conn, *http.Response, error) {
	t.Helper()
	serverConn, clientConn := net.Pipe()
	listener := &hostedPipeListener{connection: serverConn, closed: make(chan struct{})}
	server := &http.Server{Handler: handler}
	served := make(chan struct{})
	go func() { _ = server.Serve(listener); close(served) }()
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
		_ = server.Close()
		<-served
	})
	dialer := &websocket.Dialer{HandshakeTimeout: time.Minute,
		NetDialContext: func(context.Context, string, string) (net.Conn, error) { return clientConn, nil }}
	return dialer.DialContext(ctx, strings.Replace(endpoint, "wss://", "ws://", 1), headers)
}

func TestHostedGatewayHandshakeDeadlinesArePerPhase(t *testing.T) {
	for _, test := range []struct {
		name                               string
		secondChallenge, firstAck, lastAck time.Duration
	}{
		{name: "second challenge after original write deadline", secondChallenge: 11 * time.Second},
		{name: "bootstrap after slow reverse acknowledgment", lastAck: 11 * time.Second},
		{name: "acknowledgment after original read deadline", secondChallenge: 21 * time.Second, firstAck: 11 * time.Second},
	} {
		t.Run(test.name, func(t *testing.T) {
			// The gateway reaches HTTP/2, whose shared channel pool cannot cross
			// synctest bubbles. Exercise these independent deadlines in parallel
			// using real time; the server-only cases below remain on fake time.
			t.Parallel()
			var f *hostedGatewayTestFixture
			f = newHostedGatewayTestFixture(t, hostedGatewayTestOptions{
				challenge: func(index int, _ *hostedChallenge) {
					f.mu.Lock()
					peer := f.peers[index-1]
					f.mu.Unlock()
					// The fixture's original five-second deadline must not mask
					// the production per-phase limits being exercised here.
					_ = peer.SetReadDeadline(time.Now().Add(time.Minute))
					_ = peer.SetWriteDeadline(time.Now().Add(time.Minute))
					if index == 2 {
						time.Sleep(test.secondChallenge)
					}
				},
				accepted: func(ack *hostedAccepted) {
					if ack.Role == "forward" {
						time.Sleep(test.firstAck)
					}
				},
				beforeReverseAck: func() { time.Sleep(test.lastAck) },
			})
			f.gateway.cancel()
			f.gateway.ctx, f.gateway.cancel = context.WithCancel(t.Context())
			defer f.gateway.close()
			// Reuse the remote metadata and WebSocket fixtures in memory;
			// no DNS, network service, or Azure is used.
			f.gateway.httpClient.Transport = hostedGatewayTestRoundTripper(func(r *http.Request) (*http.Response, error) {
				response := httptest.NewRecorder()
				f.serveHTTP(response, r)
				return response.Result(), nil
			})
			f.gateway.dial = func(ctx context.Context, endpoint string, headers http.Header) (*websocket.Conn, *http.Response, error) {
				f.dials.Add(1)
				return hostedHandshakePipeDial(t, ctx, http.HandlerFunc(f.serveWebSocket), endpoint, headers)
			}
			start := time.Now()
			if f.initialize() != nil {
				t.Fatalf("valid delayed handshake rejected: elapsed=%s exposure=%t bootstrapDeliveries=%d",
					time.Since(start), f.gateway.ledger.ExposurePossible, f.bootstraps.Load())
			}
			if time.Since(start) < test.secondChallenge+test.firstAck+test.lastAck || !f.gateway.ready.Load() ||
				!f.gateway.ledger.ExposurePossible || !f.gateway.ledger.Ready ||
				f.creates.Load() != 1 || f.dials.Load() != 2 || f.bootstraps.Load() != 1 {
				t.Fatal("slow handshake skipped its delay, retried, or failed to establish one lifetime")
			}
		})
	}
}

func TestHostedServerHandshakeDeadlinesArePerPhase(t *testing.T) {
	for _, test := range []struct {
		name                            string
		challengeRead, hello, bootstrap time.Duration
	}{
		{name: "hello after original write deadline", hello: 11 * time.Second},
		{name: "hello after slow challenge write", challengeRead: 9 * time.Second, hello: 22 * time.Second},
		{name: "bootstrap after original read deadline", hello: 2 * time.Second, bootstrap: 29 * time.Second},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newHostedServerTestFixture(t, false)
			synctest.Test(t, func(t *testing.T) {
				f.server.cancel()
				f.server.ctx, f.server.cancel = context.WithCancel(t.Context())
				defer f.server.close() //nolint:errcheck
				ws, response, err := hostedHandshakePipeDial(t, t.Context(), f.server, "ws://fixture.invalid/invocations_ws", nil)
				if response != nil && response.Body != nil {
					_ = response.Body.Close()
				}
				if err != nil {
					t.Fatal("could not establish in-memory hosted handshake")
				}
				defer ws.Close() //nolint:errcheck
				_ = ws.SetReadDeadline(time.Now().Add(time.Minute))
				start := time.Now()
				time.Sleep(test.challengeRead)
				var challenge hostedChallenge
				if readHostedWSJSON(ws, &challenge) != nil || challenge != f.server.challenge {
					t.Fatal("valid delayed challenge was rejected")
				}
				time.Sleep(test.hello)
				pairID, digest := uuid.NewString(), f.protocol.hello.BootstrapDigest
				if ws.WriteJSON(f.hello(t, "forward", pairID, digest)) != nil {
					t.Fatalf("valid hello could not be delivered after %s", time.Since(start))
				}
				var ack hostedAccepted
				if readHostedWSJSON(ws, &ack) != nil || ack != (hostedAccepted{Protocol: hostedProtocol,
					PairID: pairID, Role: "forward", BootID: f.server.challenge.BootID, BootstrapDigest: digest}) {
					t.Fatalf("valid hello was not acknowledged after %s", time.Since(start))
				}
				if test.bootstrap != 0 {
					time.Sleep(test.bootstrap)
					f.sendBootstrap(t, ws)
					synctest.Wait()
					if !f.state(func(pair *hostedPair) bool { return pair != nil && pair.bootstrap != nil }) {
						t.Fatal("bootstrap within its own read window was rejected")
					}
				}
				if f.calls.Load() != 0 || f.server.ctx.Err() != nil {
					t.Fatal("incomplete pair launched a supervisor or closed its lifetime")
				}
			})
		})
	}
}
