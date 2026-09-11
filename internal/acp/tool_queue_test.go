package acp

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/orka-agents/agent-runtime-foundry/internal/foundry"
)

func TestACPFatalToolBatchDoesNotAdmitQueuedCalls(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		started, release := make(chan struct{}), make(chan struct{})
		var dispatched atomic.Int32
		client := &http.Client{Transport: acpTestTransport(func(r *http.Request) (*http.Response, error) {
			if dispatched.Add(1) == 2 {
				close(started)
			}
			<-release
			return &http.Response{StatusCode: http.StatusServiceUnavailable,
				Header: http.Header{"Content-Type": {"application/json"}},
				Body:   io.NopCloser(strings.NewReader("{}")), Request: r}, nil
		})}
		s := &acpServer{ctx: ctx, cancel: cancel, writes: make(chan acpWrite, 32)}
		writerDone := make(chan struct{})
		go func() {
			defer close(writerDone)
			for {
				select {
				case write := <-s.writes:
					write.done <- nil
				case <-ctx.Done():
					return
				}
			}
		}()
		session := &acpSession{id: "queue-test", mcp: &acpMCPClient{
			url: "http://127.0.0.1/mcp", client: client,
		}}
		calls := make([]foundry.OutputItem, 32)
		for i := range calls {
			calls[i] = foundry.OutputItem{Name: "probe", CallID: fmt.Sprintf("call-%d", i), Arguments: []byte("{}")}
		}
		done := make(chan error, 1)
		go func() {
			_, err := s.executeTools(ctx, session, calls)
			done <- err
		}()
		<-started
		// Both admitted requests are held while the other calls queue. Let
		// the fatal responses complete only after that state is established.
		synctest.Wait()
		close(release)
		if err := <-done; err == nil {
			t.Fatal("fatal batch returned success")
		}
		if got := dispatched.Load(); got != 2 {
			t.Fatalf("fatal batch dispatched %d calls, want only the two originally admitted calls", got)
		}
		cancel()
		<-writerDone
	})
}
