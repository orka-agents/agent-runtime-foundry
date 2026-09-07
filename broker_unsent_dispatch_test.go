package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
)

type brokerDispatchBody struct {
	io.Reader
	closed bool
}

func (b *brokerDispatchBody) Close() error { b.closed = true; return nil }

func TestBrokerCancelledBeforeDispatchIsUnsent(t *testing.T) {
	for _, phase := range []string{"create", "inference"} {
		t.Run(phase, func(t *testing.T) {
			f := newBrokerFixture(t, "success")
			cfg := brokerTestConfig(t, f)
			var attempts atomic.Int32
			client := &http.Client{Transport: brokerFixtureTransport(func(*http.Request) (*http.Response, error) {
				attempts.Add(1)
				return nil, context.Canceled
			})}
			b, _ := startBrokerTestWithClient(t, cfg, client)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			request, err := b.prepareRemoteSessionCreate(ctx, brokerIdentityRemoteSession)
			if err != nil {
				t.Fatal("request preparation failed")
			}
			body := &brokerDispatchBody{Reader: strings.NewReader("{}")}
			request.Body = body
			cancel()
			if phase == "create" {
				created, createErr := b.remoteSessionCreate(request, brokerIdentityRemoteSession)
				err = createErr
				if created {
					t.Fatal("uncalled creation fabricated acceptance")
				}
			} else {
				_, err = b.sendRemoteRequest(request)
			}
			if !errors.Is(err, errBrokerRequestUnsent) || errors.Is(err, errBrokerAmbiguous) || attempts.Load() != 0 || !body.closed {
				t.Fatalf("pre-dispatch cancellation was not proven unsent: attempts=%d unsent=%v ambiguous=%v bodyClosed=%v", attempts.Load(), errors.Is(err, errBrokerRequestUnsent), errors.Is(err, errBrokerAmbiguous), body.closed)
			}
		})
	}
}

func TestBrokerDispatchErrorsRemainAmbiguous(t *testing.T) {
	for _, transportError := range []error{context.Canceled, context.DeadlineExceeded, errBrokerRequestUnsent} {
		t.Run(transportError.Error(), func(t *testing.T) {
			var attempts atomic.Int32
			b := &lifecycleBroker{httpClient: &http.Client{Transport: brokerFixtureTransport(func(request *http.Request) (*http.Response, error) {
				attempts.Add(1)
				_ = request.Body.Close()
				return nil, transportError
			})}}
			request, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "http://synthetic.invalid/", strings.NewReader("{}"))
			if err != nil {
				t.Fatal("request construction failed")
			}
			request.GetBody = nil
			_, err = b.sendRemoteRequest(request)
			if !errors.Is(err, errBrokerAmbiguous) || errors.Is(err, errBrokerRequestUnsent) || attempts.Load() != 1 {
				t.Fatal("transport-returned error allowed unsent classification or replay")
			}
		})
	}
}
