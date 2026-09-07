package main

import (
	"context"
	"io"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
)

func TestHostedRejectedCreateAllowsOneLaterStartup(t *testing.T) {
	for _, status := range []int{400, 401, 403, 404, 405, 413, 415, 422, 429} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			f := newHostedGatewayTestFixture(t, hostedGatewayTestOptions{})
			transport := f.gateway.httpClient.Transport
			var submissions atomic.Int32
			f.gateway.httpClient.Transport = hostedGatewayTestRoundTripper(func(request *http.Request) (*http.Response, error) {
				if request.Method == http.MethodPost && submissions.Add(1) == 1 {
					return &http.Response{StatusCode: status, Header: make(http.Header),
						Body: io.NopCloser(strings.NewReader("{}")), Request: request}, nil
				}
				return transport.RoundTrip(request)
			})
			if f.initialize() == nil || submissions.Load() != 1 || f.creates.Load() != 0 {
				t.Fatal("fixture did not reach one complete creation rejection")
			}
			f.assertNoBootstrap(t)
			store, ledger, err := openHostedGatewayStore(f.settings.stateDir, f.settings.config)
			if err != nil {
				t.Fatal("could not reopen rejected-create ledger")
			}
			defer store.close()
			if ledger.CreateAttempted || ledger.SessionCreated || ledger.ExposurePossible || ledger.PrincipalDigest == "" {
				t.Fatal("complete admission rejection retained possibly-sent creation")
			}
			restarted := &hostedGateway{cfg: f.settings.config, provider: f.gateway.provider,
				httpClient: f.gateway.httpClient, store: store, ledger: ledger}
			if restarted.ensureSession(t.Context()) != nil || submissions.Load() != 2 || f.creates.Load() != 1 ||
				!restarted.ledger.SessionCreated || restarted.ledger.PrincipalDigest != ledger.PrincipalDigest {
				t.Fatal("later explicit startup did not retain identity and make exactly one creation")
			}
		})
	}
}

type hostedIncompleteRejectionBody struct{}

func (hostedIncompleteRejectionBody) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }
func (hostedIncompleteRejectionBody) Close() error             { return nil }

func TestHostedAmbiguousCreateCannotClearIntent(t *testing.T) {
	for _, failure := range []string{"conflict", "server-error", "incomplete-rejection", "rollback-storage"} {
		t.Run(failure, func(t *testing.T) {
			f := newHostedGatewayTestFixture(t, hostedGatewayTestOptions{})
			transport := f.gateway.httpClient.Transport
			var submissions atomic.Int32
			f.gateway.httpClient.Transport = hostedGatewayTestRoundTripper(func(request *http.Request) (*http.Response, error) {
				if request.Method != http.MethodPost {
					return transport.RoundTrip(request)
				}
				submissions.Add(1)
				status := http.StatusForbidden
				var body io.ReadCloser = io.NopCloser(strings.NewReader("{}"))
				switch failure {
				case "conflict":
					status = http.StatusConflict
				case "server-error":
					status = http.StatusInternalServerError
				case "incomplete-rejection":
					body = hostedIncompleteRejectionBody{}
				case "rollback-storage":
					f.gateway.store.dir = filepath.Join(f.settings.stateDir, "missing")
				}
				return &http.Response{StatusCode: status, Header: make(http.Header), Body: body, Request: request}, nil
			})
			if f.initialize() == nil || submissions.Load() != 1 || f.creates.Load() != 0 {
				t.Fatal("fixture did not reach the selected submitted-create failure")
			}
			if !f.gateway.ledger.CreateAttempted || f.gateway.ledger.SessionCreated || f.gateway.ledger.ExposurePossible {
				t.Fatal("failed creation cleared in-memory ownership without durable proof")
			}
			store, ledger, err := openHostedGatewayStore(f.settings.stateDir, f.settings.config)
			if err != nil {
				t.Fatal("could not reopen submitted-create ledger")
			}
			defer store.close()
			if !ledger.CreateAttempted || ledger.SessionCreated || ledger.ExposurePossible {
				t.Fatal("failed creation lost its original durable intent")
			}
			restarted := &hostedGateway{cfg: f.settings.config, provider: f.gateway.provider,
				httpClient: f.gateway.httpClient, store: store, ledger: ledger}
			requests, tokens := f.httpCalls.Load(), f.tokenCalls.Load()
			if restarted.ensureSession(context.Background()) == nil || submissions.Load() != 1 ||
				f.httpCalls.Load() != requests || f.tokenCalls.Load() != tokens {
				t.Fatal("uncertain creation was retried or adopted")
			}
		})
	}
}
