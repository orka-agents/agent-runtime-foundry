package hosted

import (
	"context"
	"errors"
	"maps"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/orka-agents/agent-runtime-foundry/internal/brokerapi"
	"github.com/orka-agents/agent-runtime-foundry/internal/foundry"
)

func TestHostedReviewLocalStopPreservesCleanupRelays(t *testing.T) {
	hostedServerTestHealth(t, http.StatusOK)
	f := newHostedServerTestFixture(t, true)
	stopRequested, releaseStop := make(chan struct{}), make(chan struct{})
	cleanupOK := make(chan bool, 1)
	release := sync.OnceFunc(func() { close(releaseStop) })
	defer release()
	f.server.runner = func(ctx context.Context, environment map[string]string) (<-chan error, error) {
		f.calls.Add(1)
		f.captured <- maps.Clone(environment)
		done := make(chan error, 1)
		go func() {
			<-ctx.Done()
			close(stopRequested)
			<-releaseStop
			transport := newHostedLocalTransport()
			defer transport.CloseIdleConnections()
			client := &http.Client{Transport: transport, Timeout: 2 * time.Second}
			ok := true
			for _, target := range []string{
				"http://" + hostedBrokerRelayAddr + brokerapi.SettlePath,
				"http://" + hostedOrkaRelayAddr + "/internal/v2/acp/artifact-authorizations",
			} {
				request, _ := http.NewRequest(http.MethodPost, target, nil)
				response, err := client.Do(request)
				if err != nil {
					ok = false
					continue
				}
				ok = ok && response.StatusCode == http.StatusNoContent
				_ = response.Body.Close()
			}
			cleanupOK <- ok
			if ok {
				done <- nil
			} else {
				done <- errHostedInvalid
			}
			close(done)
		}()
		return done, nil
	}
	forward, reverse, client := hostedServerTestRunningPair(t, f)
	returned := make(chan error, 1)
	go func() { returned <- serveHostedHTTP(f.server.ctx, "127.0.0.1:0", f.server) }()
	f.server.cancel()
	hostedTestDone(t, stopRequested)
	select {
	case <-returned:
		t.Fatal("entry point did not join the supervisor's pending cleanup")
	case <-time.After(25 * time.Millisecond):
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://supervisor/v2/health", nil)
	response, err := client.RoundTrip(request)
	if err != nil {
		t.Error("local shutdown destroyed the healthy forward transport before cleanup")
	} else {
		_ = response.Body.Close()
		if response.StatusCode != http.StatusServiceUnavailable {
			t.Error("stopping lifetime admitted a new forward request")
		}
	}
	release()
	select {
	case ok := <-cleanupOK:
		if !ok {
			t.Error("ordinary local shutdown destroyed a healthy relay needed by supervisor cleanup")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("bounded fixture cleanup did not return")
	}
	select {
	case err := <-returned:
		if err != nil {
			t.Error("clean joined supervisor shutdown was rejected")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("entry point did not finish after supervisor cleanup")
	}
	hostedTestDone(t, forward.Done())
	hostedTestDone(t, reverse.Done())
	if f.calls.Load() != 1 {
		t.Fatal("local shutdown relaunched its supervisor")
	}
}

func TestHostedReviewLocalRelayStartupFailureIsNotCleanExit(t *testing.T) {
	for _, address := range []string{hostedBrokerRelayAddr, hostedOrkaRelayAddr} {
		name := "broker relay"
		if address == hostedOrkaRelayAddr {
			name = "Orka relay"
		}
		t.Run(name, func(t *testing.T) {
			reserved, err := net.Listen("tcp", address)
			if err != nil {
				t.Fatal("could not reserve the local fixture relay port")
			}
			defer reserved.Close() //nolint:errcheck
			f := newHostedServerTestFixture(t, false)
			pairID, digest := uuid.NewString(), foundry.JSONDigest(f.protocol.bootstrap)
			forward := f.claim(t, "forward", pairID, digest)
			_ = hostedServerReverse(t, f.claim(t, "reverse", pairID, digest))
			f.sendBootstrap(t, forward)
			hostedTestDone(t, f.server.ctx.Done())
			f.server.mu.Lock()
			pair := f.server.pair
			f.server.mu.Unlock()
			hostedTestDone(t, pair.stopped)
			if !errors.Is(f.server.close(), errHostedInvalid) {
				t.Error("local relay bind failure was reported as a clean hosted exit")
			}
			if f.calls.Load() != 0 {
				t.Fatal("failed local startup launched or retried a supervisor")
			}
		})
	}
}

func TestHostedReviewPeerCancellationWhileStartingIsNotLocalFailure(t *testing.T) {
	for _, role := range []string{"forward", "reverse"} {
		t.Run(role, func(t *testing.T) {
			healthCalled := hostedServerTestHealth(t, http.StatusServiceUnavailable)
			f := newHostedServerTestFixture(t, true)
			f.server.runner = func(ctx context.Context, environment map[string]string) (<-chan error, error) {
				f.calls.Add(1)
				f.captured <- maps.Clone(environment)
				done := make(chan error, 1)
				go func() { <-ctx.Done(); done <- nil; close(done) }()
				return done, nil
			}
			pairID, digest := uuid.NewString(), foundry.JSONDigest(f.protocol.bootstrap)
			forward := f.claim(t, "forward", pairID, digest)
			reverse := hostedServerReverse(t, f.claim(t, "reverse", pairID, digest))
			f.sendBootstrap(t, forward)
			_ = f.receiveRunner(t)
			hostedTestDone(t, healthCalled)
			if role == "forward" {
				hostedTestGoingAway(t, forward)
			} else {
				hostedTestGoingAway(t, reverse.ws)
			}
			hostedTestDone(t, f.server.ctx.Done())
			if f.server.close() != nil || f.calls.Load() != 1 {
				t.Fatal("peer-canceled startup was classified as local failure or relaunched")
			}
		})
	}
}

func TestHostedReviewChannelLossDuringCleanupClosesPair(t *testing.T) {
	for _, role := range []string{"forward", "reverse"} {
		t.Run(role, func(t *testing.T) {
			hostedServerTestHealth(t, http.StatusOK)
			f := newHostedServerTestFixture(t, true)
			stopRequested, releaseStop := make(chan struct{}), make(chan struct{})
			release := sync.OnceFunc(func() { close(releaseStop) })
			defer release()
			f.server.runner = func(ctx context.Context, environment map[string]string) (<-chan error, error) {
				f.calls.Add(1)
				f.captured <- maps.Clone(environment)
				done := make(chan error, 1)
				go func() {
					<-ctx.Done()
					close(stopRequested)
					<-releaseStop
					done <- nil
					close(done)
				}()
				return done, nil
			}
			forward, reverse, _ := hostedServerTestRunningPair(t, f)
			returned := make(chan error, 1)
			go func() { returned <- f.server.close() }()
			hostedTestDone(t, stopRequested)
			channel := forward
			if role == "reverse" {
				channel = reverse
			}
			hostedTestGoingAway(t, channel.ws)
			hostedTestDone(t, forward.Done())
			hostedTestDone(t, reverse.Done())
			select {
			case <-returned:
				t.Error("channel loss returned before joining pending supervisor cleanup")
			default:
			}
			release()
			select {
			case err := <-returned:
				if err != nil || f.calls.Load() != 1 {
					t.Fatal("channel loss during local cleanup failed to join or replayed the supervisor")
				}
			case <-time.After(3 * time.Second):
				t.Fatal("channel loss during cleanup left the lifetime pending")
			}
		})
	}
}
