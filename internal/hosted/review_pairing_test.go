package hosted

import (
	"context"
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

func TestHostedReviewServerAllowsHelloAfterOtherChannelSetup(t *testing.T) {
	f := newHostedServerTestFixture(t, false)
	synctest.Test(t, func(t *testing.T) {
		f.server.cancel()
		f.server.ctx, f.server.cancel = context.WithCancel(t.Context())
		defer f.server.close() //nolint:errcheck
		pairID, digest := uuid.NewString(), f.protocol.hello.BootstrapDigest
		forward := hostedReviewPipeConnect(t, f)
		// The gateway verifies both challenges before signing. A second dial
		// may take up to 120 seconds, exceeding the old first-hello window.
		time.Sleep(31 * time.Second)
		reverse := hostedReviewPipeConnect(t, f)
		if forward.WriteJSON(f.hello(t, "forward", pairID, digest)) != nil {
			t.Fatal("valid first hello could not be delivered after slow second-channel setup")
		}
		hostedReviewReadAck(t, f, forward, "forward", pairID)
		if reverse.WriteJSON(f.hello(t, "reverse", pairID, digest)) != nil {
			t.Fatal("valid second hello could not be delivered")
		}
		hostedReviewReadAck(t, f, reverse, "reverse", pairID)
		synctest.Wait()
		if f.server.ctx.Err() != nil || f.calls.Load() != 0 || !f.state(func(p *hostedPair) bool {
			return p != nil && p.forwardAcked && p.reverseAcked && p.bootstrap == nil && !p.running
		}) {
			t.Fatal("slow channel pairing expired or launched before bootstrap")
		}
	})
}

func hostedReviewPipeConnect(t *testing.T, f *hostedServerTestFixture) *websocket.Conn {
	t.Helper()
	ws, response, err := hostedHandshakePipeDial(t, t.Context(), f.server, "ws://fixture.invalid/invocations_ws", nil)
	if response != nil && response.Body != nil {
		_ = response.Body.Close()
	}
	if err != nil {
		t.Fatal("could not establish the in-memory hosted channel")
	}
	t.Cleanup(func() { _ = ws.Close() })
	_ = ws.SetReadDeadline(time.Now().Add(5 * time.Minute))
	var challenge hostedChallenge
	if readHostedWSJSON(ws, &challenge) != nil || challenge != f.server.challenge {
		t.Fatal("server did not send the exact in-memory challenge")
	}
	return ws
}

func hostedReviewReadAck(t *testing.T, f *hostedServerTestFixture, ws *websocket.Conn, role, pairID string) {
	t.Helper()
	var ack hostedAccepted
	if readHostedWSJSON(ws, &ack) != nil || ack != (hostedAccepted{Protocol: hostedProtocol,
		PairID: pairID, Role: role, BootID: f.server.challenge.BootID, BootstrapDigest: f.protocol.hello.BootstrapDigest}) {
		t.Fatal("valid authenticated role was not acknowledged")
	}
}

func TestHostedReviewServerAllowsBootstrapAfterSlowOtherRole(t *testing.T) {
	for _, test := range []struct {
		name       string
		hello, ack time.Duration
	}{
		{name: "reverse hello near its deadline", hello: 29 * time.Second},
		{name: "reverse acknowledgment near its deadline", hello: 22 * time.Second, ack: 9 * time.Second},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newHostedServerTestFixture(t, false)
			synctest.Test(t, func(t *testing.T) {
				f.server.cancel()
				f.server.ctx, f.server.cancel = context.WithCancel(t.Context())
				defer f.server.close() //nolint:errcheck
				pairID, digest := uuid.NewString(), f.protocol.hello.BootstrapDigest
				forward := hostedReviewPipeConnect(t, f)
				if forward.WriteJSON(f.hello(t, "forward", pairID, digest)) != nil {
					t.Fatal("could not send the first authenticated role")
				}
				hostedReviewReadAck(t, f, forward, "forward", pairID)
				time.Sleep(2 * time.Second)
				reverse := hostedReviewPipeConnect(t, f)
				time.Sleep(test.hello)
				if reverse.WriteJSON(f.hello(t, "reverse", pairID, digest)) != nil {
					t.Fatal("reverse hello inside its own window could not be delivered")
				}
				time.Sleep(test.ack)
				hostedReviewReadAck(t, f, reverse, "reverse", pairID)
				reverseConn := newHostedWSConn(reverse)
				go serveHostedHTTP2(f.server.ctx, reverseConn, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(http.StatusNoContent)
				}))
				f.sendBootstrap(t, forward)
				<-f.server.ctx.Done() // The deliberate fixture runner failure ends this lifetime.
				f.server.mu.Lock()
				pair := f.server.pair
				f.server.mu.Unlock()
				<-pair.stopped
				if f.calls.Load() != 1 || !f.state(func(p *hostedPair) bool { return p.running && p.bootstrap != nil }) {
					t.Fatal("valid slow pairing failed to deliver exactly one bootstrap to the runner")
				}
			})
		})
	}
}

func TestHostedReviewAuthenticatedPairingHasOneBound(t *testing.T) {
	for _, role := range []string{"forward", "reverse"} {
		t.Run(role, func(t *testing.T) {
			f := newHostedServerTestFixture(t, false)
			synctest.Test(t, func(t *testing.T) {
				f.server.cancel()
				f.server.ctx, f.server.cancel = context.WithCancel(t.Context())
				defer f.server.close() //nolint:errcheck
				pairID := uuid.NewString()
				ws := hostedReviewPipeConnect(t, f)
				if ws.WriteJSON(f.hello(t, role, pairID, f.protocol.hello.BootstrapDigest)) != nil {
					t.Fatal("could not send the authenticated role")
				}
				hostedReviewReadAck(t, f, ws, role, pairID)
				synctest.Wait()
				time.Sleep(4*time.Minute - time.Second)
				if f.server.ctx.Err() != nil || f.calls.Load() != 0 {
					t.Fatal("incomplete authenticated pair expired before its setup window or launched early")
				}
				time.Sleep(time.Second)
				synctest.Wait()
				if f.server.ctx.Err() == nil || f.calls.Load() != 0 {
					t.Fatal("incomplete pair outlived its single setup window or launched a supervisor")
				}
			})
		})
	}
}

func TestHostedReviewUnauthenticatedPairingWaitIsBounded(t *testing.T) {
	f := newHostedServerTestFixture(t, false)
	synctest.Test(t, func(t *testing.T) {
		f.server.cancel()
		f.server.ctx, f.server.cancel = context.WithCancel(t.Context())
		defer f.server.close() //nolint:errcheck
		ws := hostedReviewPipeConnect(t, f)
		time.Sleep(4 * time.Minute)
		synctest.Wait()
		if readHostedWSJSON(ws, &hostedAccepted{}) == nil || f.server.ctx.Err() != nil || f.calls.Load() != 0 ||
			!f.state(func(p *hostedPair) bool { return p == nil }) {
			t.Fatal("expired unauthenticated channel stayed open or consumed authenticated ownership")
		}
	})
}
