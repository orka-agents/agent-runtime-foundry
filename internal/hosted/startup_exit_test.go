package hosted

import (
	"context"
	"errors"
	"maps"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/orka-agents/agent-runtime-foundry/internal/foundry"
)

func TestHostedStartupProcessCompletionBeforeReadiness(t *testing.T) {
	for _, cause := range []string{"unexpected-nil", "unexpected-error", "missing-result", "local-cancel", "forward-close", "reverse-close"} {
		t.Run(cause, func(t *testing.T) {
			healthCalled := hostedServerTestHealth(t, http.StatusServiceUnavailable)
			f := newHostedServerTestFixture(t, true)
			processDone := make(chan error, 1)
			f.server.runner = func(_ context.Context, environment map[string]string) (<-chan error, error) {
				f.calls.Add(1)
				f.captured <- maps.Clone(environment)
				return processDone, nil
			}
			pairID, digest := uuid.NewString(), foundry.JSONDigest(f.protocol.bootstrap)
			forward := f.claim(t, "forward", pairID, digest)
			reverse := hostedServerReverse(t, f.claim(t, "reverse", pairID, digest))
			f.sendBootstrap(t, forward)
			_ = f.receiveRunner(t)
			hostedTestDone(t, healthCalled)
			if f.server.ctx.Err() != nil {
				t.Fatal("fixture lifetime closed before the supervisor exit")
			}
			returned := make(chan error, 1)
			go func() { returned <- serveHostedHTTP(f.server.ctx, "127.0.0.1:0", f.server) }()
			alreadyCancelled := false
			switch cause {
			case "local-cancel":
				f.server.cancel()
				alreadyCancelled = true
			case "forward-close":
				hostedTestGoingAway(t, forward)
				alreadyCancelled = true
			case "reverse-close":
				hostedTestGoingAway(t, reverse.ws)
				alreadyCancelled = true
			}
			if alreadyCancelled {
				hostedTestDone(t, f.server.ctx.Done())
				select {
				case <-returned:
					t.Fatal("entrypoint returned before the cancelled supervisor was joined")
				default:
				}
			}
			switch cause {
			case "missing-result":
				close(processDone)
			case "unexpected-error":
				processDone <- errHostedInvalid
				close(processDone)
			default:
				processDone <- nil
				close(processDone)
			}
			select {
			case err := <-returned:
				if alreadyCancelled {
					if err != nil {
						t.Error("already-cancelled startup was reported as local process failure")
					}
				} else if !errors.Is(err, errHostedInvalid) {
					t.Error("supervisor exited before health became ready but the entrypoint reported success")
				}
			case <-time.After(3 * time.Second):
				t.Fatal("entrypoint did not join the completed supervisor")
			}
			f.server.mu.Lock()
			pair := f.server.pair
			f.server.mu.Unlock()
			hostedTestDone(t, pair.stopped)
			hostedTestDone(t, pair.done)
			hostedTestDone(t, reverse.Done())
			hostedServerRejected(t, forward)
			for _, path := range []string{"/readiness", "/invocations_ws"} {
				reply := httptest.NewRecorder()
				f.server.ServeHTTP(reply, httptest.NewRequest(http.MethodGet, path, nil))
				if reply.Code != http.StatusServiceUnavailable {
					t.Error("completed startup retained readiness or admitted another channel")
				}
			}
			if f.calls.Load() != 1 {
				t.Error("completed startup relaunched its supervisor")
			}
		})
	}
}
