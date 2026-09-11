package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestBrokerFoldedSessionEvidenceCannotSettleActiveOwner(t *testing.T) {
	f := newBrokerFixture(t, "hold-known")
	var cleanupEvidence atomic.Bool
	transport := http.DefaultTransport.(*http.Transport).Clone()
	t.Cleanup(transport.CloseIdleConnections)
	client := &http.Client{Transport: brokerFixtureTransport(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, ":stop") {
			// The original remote inference stays active. A 204 alone cannot
			// prove idle, and the following GET is deliberately contradictory.
			f.mu.Lock()
			f.stops++
			f.mu.Unlock()
			return &http.Response{StatusCode: http.StatusNoContent, Body: http.NoBody, Header: make(http.Header), Request: r}, nil
		}
		response, err := transport.RoundTrip(r)
		if err != nil || !cleanupEvidence.Load() || r.Method != http.MethodGet || !strings.Contains(r.URL.Path, "/endpoint/sessions/") || response.StatusCode != http.StatusOK {
			return response, err
		}
		data, err := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if err != nil {
			return nil, err
		}
		var session brokerRemoteSession
		if json.Unmarshal(data, &session) != nil {
			t.Error("fixture session evidence unreadable")
		}
		data = []byte(fmt.Sprintf(`{"agent_session_id":%q,"version_indicator":{"type":"version_ref","agent_version":"3"},"status":"active","Status":"idle"}`, session.ID))
		response.Body = io.NopCloser(strings.NewReader(string(data)))
		response.ContentLength = int64(len(data))
		return response, nil
	})}
	cfg := brokerTestConfig(t, f)
	b, server := startBrokerTestWithClient(t, cfg, client)
	c := brokerTestContext(cfg)
	key := brokerJSONDigest(c.Owner)
	finished := make(chan struct{})
	go func() {
		_, _, _ = brokerTestHTTP(context.Background(), server.URL, brokerResponsesPath, c, brokerTestBody(""))
		close(finished)
	}()
	brokerAwait(t, func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		session := b.ledger.Sessions[key]
		return session != nil && session.Prompts[c.promptKey()].Invocations[1].ResponseID != ""
	})
	cleanupEvidence.Store(true)
	control, body := brokerTestControlContext(brokerSettlePath, c)
	status, _, err := brokerTestHTTP(context.Background(), server.URL, brokerSettlePath, control, body)
	if err != nil || (status != http.StatusOK && status != http.StatusConflict) {
		t.Fatalf("original settlement request failed: status=%d", status)
	}
	select {
	case <-finished:
	case <-time.After(3 * time.Second):
		t.Fatal("original local invocation did not stop")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b.reconcile(ctx, key)
	b.mu.Lock()
	proof := b.controlResponseLocked(c, "")
	remoteID := b.ledger.Sessions[key].RemoteID
	b.mu.Unlock()
	data, err := os.ReadFile(filepath.Join(cfg.stateDir, "state.json"))
	var ledger brokerLedger
	if err != nil || json.Unmarshal(data, &ledger) != nil {
		t.Fatal("durable evidence unreadable")
	}
	f.mu.Lock()
	active := f.sessions[remoteID] == "active"
	f.mu.Unlock()
	creates, inferences, stops, deletes := f.counts()
	durable := ledger.Sessions[key].Prompts[c.promptKey()].Settled
	if !active || creates != 1 || inferences != 1 || deletes != 0 {
		t.Fatal("fixture did not preserve the one original active remote request")
	}
	if proof.SettlementProven || durable || proof.ProofDigest != "" {
		t.Fatalf("malformed Session metadata proved cleanup: active=%v settled=%v durable=%v stops=%d", active, proof.SettlementProven, durable, stops)
	}
}

func TestBrokerFoldedSessionIdentityRejected(t *testing.T) {
	for name, document := range map[string]string{
		"session identity":      `{"agent_session_id":"wrong","Agent_Session_ID":"owned","version_indicator":{"type":"version_ref","agent_version":"3"},"status":"idle"}`,
		"nested version":        `{"agent_session_id":"owned","version_indicator":{"type":"version_ref","agent_version":"wrong","Agent_Version":"3"},"status":"idle"}`,
		"nested type":           `{"agent_session_id":"owned","version_indicator":{"type":"wrong","Type":"version_ref","agent_version":"3"},"status":"idle"}`,
		"merged version object": `{"agent_session_id":"owned","version_indicator":{"type":"version_ref","agent_version":"wrong"},"Version_Indicator":{"agent_version":"3"},"status":"idle"}`,
		"Unicode status":        `{"agent_session_id":"owned","version_indicator":{"type":"version_ref","agent_version":"3"},"status":"active","ſtatus":"idle"}`,
		"escaped status":        `{"agent_session_id":"owned","version_indicator":{"type":"version_ref","agent_version":"3"},"status":"active","\u0053tatus":"idle"}`,
	} {
		t.Run(name, func(t *testing.T) {
			f := newBrokerFixture(t, "success")
			cfg := brokerTestConfig(t, f)
			client := &http.Client{Transport: brokerFixtureTransport(func(r *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(document)), Request: r}, nil
			})}
			b, _ := startBrokerTestWithClient(t, cfg, client)
			_, _, err := b.remoteSessionGet(context.Background(), "owned")
			if err == nil {
				t.Fatal("ambiguous remote identity or state accepted")
			}
		})
	}
}
