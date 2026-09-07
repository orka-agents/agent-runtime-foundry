package main

import (
	"bufio"
	"context"
	"errors"
	"io"
	"maps"
	"net"
	"net/http"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

func TestHostedHTTPWaitsForSupervisorShutdown(t *testing.T) {
	for _, role := range []string{"forward", "reverse"} {
		t.Run(role, func(t *testing.T) {
			hostedServerTestHealth(t, http.StatusOK)
			f := newHostedServerTestFixture(t, true)
			stopRequested, releaseStop := make(chan struct{}), make(chan struct{})
			release := sync.OnceFunc(func() { close(releaseStop) })
			t.Cleanup(release)
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
			go func() { returned <- serveHostedHTTP(f.server.ctx, "127.0.0.1:0", f.server) }()
			channel := forward
			if role == "reverse" {
				channel = reverse
			}
			hostedTestGoingAway(t, channel.ws)
			hostedTestDone(t, stopRequested)
			select {
			case <-returned:
				t.Fatal("hosted entry point returned before the supervisor finished shutdown")
			case <-time.After(25 * time.Millisecond):
			}
			release()
			select {
			case err := <-returned:
				if err != nil {
					t.Fatal("clean supervisor shutdown was rejected")
				}
			case <-time.After(3 * time.Second):
				t.Fatal("hosted entry point did not join supervisor shutdown")
			}
			if f.calls.Load() != 1 {
				t.Fatal("platform closure relaunched the joined supervisor")
			}
		})
	}
}
func TestHostedHTTPStreamsBodyBeyondThirtySeconds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		serverConn, clientConn := net.Pipe()
		listener := &hostedPipeListener{connection: serverConn, closed: make(chan struct{})}
		server := newHostedHTTPServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, err := io.ReadAll(io.LimitReader(r.Body, 4))
			if err != nil || string(body) != "abc" {
				http.Error(w, "stream interrupted", http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusCreated)
		}))
		connectionClosed := make(chan struct{})
		server.ConnState = func(_ net.Conn, state http.ConnState) {
			if state == http.StateClosed {
				close(connectionClosed)
			}
		}
		served := make(chan struct{})
		go func() { _ = server.Serve(listener); close(served) }()
		written := make(chan struct{})
		var writeErr error
		defer func() {
			_ = server.Close()
			_ = clientConn.Close()
			<-served
			<-written
			<-connectionClosed
		}()
		go func() {
			_, err := io.WriteString(clientConn, "PUT /artifact HTTP/1.1\r\nHost: fixture\r\nContent-Length: 3\r\n\r\na")
			if err == nil {
				time.Sleep(31 * time.Second)
				_, err = io.WriteString(clientConn, "bc")
			}
			writeErr = err
			close(written)
		}()
		response, err := http.ReadResponse(bufio.NewReader(clientConn), &http.Request{Method: http.MethodPut})
		if err != nil {
			t.Fatal("streaming request did not receive its response")
		}
		defer response.Body.Close() //nolint:errcheck
		if response.StatusCode != http.StatusCreated {
			t.Fatalf("valid slow body status = %d, want 201", response.StatusCode)
		}
		<-written
		if writeErr != nil {
			t.Fatal("valid slow body write failed")
		}
	})
}

func TestHostedSupervisorJoinReportsUnprovenShutdown(t *testing.T) {
	for _, timeout := range []bool{false, true} {
		name := "supervisor cleanup failed"
		if timeout {
			name = "supervisor did not finish"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				server := &hostedServer{ctx: ctx, cancel: cancel}
				pair := &hostedPair{server: server, running: true, done: make(chan struct{}), stopped: make(chan struct{})}
				server.pair = pair
				result := make(chan error, 1)
				if !timeout {
					result <- errHostedInvalid
				}
				started := time.Now()
				go func() { pair.joinSupervisor(result); close(pair.stopped) }()
				if err := server.close(); !errors.Is(err, errHostedInvalid) {
					t.Fatal("unproven supervisor shutdown was accepted")
				}
				if timeout && time.Since(started) != hostedSupervisorShutdownWait {
					t.Fatal("supervisor join did not use its bounded shutdown window")
				}
			})
		})
	}
}

func TestHostedHTTPStreamingRetainsHeaderAndIdleBounds(t *testing.T) {
	server := newHostedHTTPServer(http.NotFoundHandler())
	if server.ReadTimeout != 0 || server.WriteTimeout != 0 {
		t.Fatal("hosted proxy applies a whole-stream timeout")
	}
	if server.ReadHeaderTimeout != 10*time.Second || server.IdleTimeout != 2*time.Minute || server.MaxHeaderBytes != 32<<10 {
		t.Fatal("hosted proxy lost its header or idle bounds")
	}
}

type hostedPipeListener struct {
	connection net.Conn
	accepted   bool
	closed     chan struct{}
	once       sync.Once
}

func (l *hostedPipeListener) Accept() (net.Conn, error) {
	if !l.accepted {
		l.accepted = true
		return l.connection, nil
	}
	<-l.closed
	return nil, net.ErrClosed
}

func (l *hostedPipeListener) Close() error {
	l.once.Do(func() { close(l.closed) })
	return nil
}

func (*hostedPipeListener) Addr() net.Addr { return &net.TCPAddr{} }
