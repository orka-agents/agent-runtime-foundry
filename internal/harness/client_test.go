package harness

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestAuthenticatedStatusErrorRedactsOpaqueBearer(t *testing.T) {
	const bearer = "opaque-bearer-value-12345"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, bearer, http.StatusBadRequest)
	}))
	defer server.Close()
	client, err := NewClient(server.URL, WithBearerToken(bearer))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	_, err = client.Health(context.Background())
	if err == nil {
		t.Fatal("Health succeeded")
	}
	if strings.Contains(err.Error(), bearer) || !strings.Contains(err.Error(), "400 Bad Request") {
		t.Fatalf("error = %q", err)
	}
}

func TestClientRejectsRedirectsWithoutLeakingAuthorization(t *testing.T) {
	const bearer = "redirect-bearer-value-12345"
	var targetAuthorization string
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetAuthorization = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer target.Close()
	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer redirector.Close()
	client, err := NewClient(redirector.URL, WithBearerToken(bearer))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	_, err = client.Health(context.Background())
	if err == nil {
		t.Fatal("Health followed redirect")
	}
	if targetAuthorization != "" {
		t.Fatalf("redirect target authorization = %q", targetAuthorization)
	}
}

func TestWithHTTPClientDoesNotMutateCallerRedirectPolicy(t *testing.T) {
	callerPolicy := func(_ *http.Request, _ []*http.Request) error { return nil }
	source := &http.Client{CheckRedirect: callerPolicy}
	client, err := NewClient("https://example.com", WithHTTPClient(source))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if source.CheckRedirect == nil {
		t.Fatal("caller client was mutated")
	}
	if client.httpClient == source {
		t.Fatal("client did not clone caller HTTP client")
	}
}

func TestStartTurnMalformedAcceptedResponseMarksRemoteAccepted(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"version":`))
	}))
	defer server.Close()
	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	request := StartTurnRequest{
		Version:          ProtocolVersion,
		Namespace:        "default",
		TaskName:         "malformed-accepted",
		SessionName:      "malformed-accepted",
		RuntimeSessionID: "runtime-a",
		TurnID:           "turn-a",
		CorrelationID:    "corr-a",
		Deadline:         time.Now().UTC().Add(time.Minute),
		AuthIdentity:     AuthIdentity{Subject: "task:default/malformed-accepted"},
	}
	_, err = client.StartTurn(context.Background(), request)
	var clientErr ClientError
	if !errors.As(err, &clientErr) || !clientErr.RemoteAccepted || clientErr.StatusCode != http.StatusAccepted {
		t.Fatalf("StartTurn error = %#v, want accepted client error", err)
	}
}
