package broker

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/orka-agents/agent-runtime-foundry/internal/foundry"
	"github.com/orka-agents/agent-runtime-foundry/internal/strictjson"
	"golang.org/x/net/http/httpguts"
)

var errBrokerRequestUnsent = errors.New("Foundry broker request was not sent")

// Complete fallible authentication and request preparation before a caller
// records possible submission. Only sendRemoteRequest crosses that boundary.
func (b *lifecycleBroker) prepareRemoteRequest(ctx context.Context, method, suffix string, body []byte) (*http.Request, error) {
	token, err := b.tokenProvider.AccessToken(ctx)
	if err != nil {
		return nil, errors.Join(errBrokerRequestUnsent, errBrokerRemote)
	}
	if err := b.pinPrincipal(token); err != nil {
		return nil, errors.Join(errBrokerRequestUnsent, err)
	}
	endpoint := strings.TrimRight(b.cfg.agent.HostedTarget.ProjectEndpoint, "/") + "/agents/" +
		url.PathEscape(b.cfg.agent.HostedTarget.AgentName) + suffix + "?api-version=v1"
	request, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, errors.Join(errBrokerRequestUnsent, errBrokerRemote)
	}
	// Disable replayable bodies and redirects. A network error never authorizes
	// resubmission of a create or inference intent.
	request.GetBody = nil
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Foundry-Features", "HostedAgents=V1Preview")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json, text/event-stream")
	if ctx.Err() != nil {
		return nil, errors.Join(errBrokerRequestUnsent, errBrokerClosed)
	}
	return request, nil
}

func (b *lifecycleBroker) sendRemoteRequest(request *http.Request) (*http.Response, error) {
	if request.Context().Err() != nil {
		// Only skipping Do proves non-submission. A cancellation observed
		// after dispatch may follow accepted work and remains ambiguous.
		if request.Body != nil {
			_ = request.Body.Close()
		}
		return nil, errors.Join(errBrokerRequestUnsent, errBrokerClosed)
	}
	response, err := b.httpClient.Do(request)
	if err != nil {
		return nil, errBrokerAmbiguous
	}
	return response, nil
}

func newBrokerHTTPClient() *http.Client {
	dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	// Session provisioning has a 45-second operation deadline. Do not abandon
	// its one acknowledgement earlier; shorter request contexts still win.
	return &http.Client{Transport: &http.Transport{Proxy: nil, DialContext: dialer.DialContext,
		TLSHandshakeTimeout: 10 * time.Second, ResponseHeaderTimeout: 45 * time.Second,
		MaxIdleConns: 16, MaxIdleConnsPerHost: 8, IdleConnTimeout: 30 * time.Second},
		CheckRedirect: func(*http.Request, []*http.Request) error { return errBrokerRemote }}
}

func (b *lifecycleBroker) pinPrincipal(token string) error {
	if !httpguts.ValidHeaderFieldValue(token) {
		return errBrokerRemote
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return errBrokerRemote
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	var claims struct {
		Audience string `json:"aud"`
		Tenant   string `json:"tid"`
		Object   string `json:"oid"`
		App      string `json:"appid"`
		AZP      string `json:"azp"`
	}
	if err != nil || strictjson.DecodeStruct(raw, &claims, false) != nil || strings.TrimRight(claims.Audience, "/") != "https://ai.azure.com" ||
		!foundry.SafeString(claims.Tenant, 512) || !foundry.SafeString(claims.Object, 512) {
		return errBrokerRemote
	}
	digest := foundry.JSONDigest(claims)
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.ledger.PrincipalDigest == digest {
		return b.storageError
	}
	if b.ledger.PrincipalDigest != "" {
		return errBrokerConflict
	}
	return b.commitLocked(func(next *brokerLedger) error { next.PrincipalDigest = digest; return nil })
}

func (b *lifecycleBroker) remoteJSON(ctx context.Context, method, suffix string, body []byte, target any) (int, error) {
	request, err := b.prepareRemoteRequest(ctx, method, suffix, body)
	if err != nil {
		return 0, err
	}
	return b.remoteJSONRequest(request, target)
}

func (b *lifecycleBroker) remoteJSONRequest(request *http.Request, target any) (int, error) {
	response, err := b.sendRemoteRequest(request)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close() //nolint:errcheck
	data, err := io.ReadAll(io.LimitReader(response.Body, foundry.MaxAgentConfigBytes+1))
	if err != nil || len(data) > foundry.MaxAgentConfigBytes {
		return response.StatusCode, errBrokerRemote
	}
	if target != nil && response.StatusCode >= 200 && response.StatusCode < 300 {
		if strictjson.DecodeStruct(data, target, false) != nil {
			return response.StatusCode, errBrokerRemote
		}
	}
	return response.StatusCode, nil
}

func (b *lifecycleBroker) validateRemoteTarget(ctx context.Context) error {
	var agent struct {
		Name     string `json:"name"`
		Endpoint struct {
			Schemes []json.RawMessage `json:"authorization_schemes"`
		} `json:"agent_endpoint"`
	}
	status, err := b.remoteJSON(ctx, http.MethodGet, "", nil, &agent)
	if err != nil || status != http.StatusOK || agent.Name != b.cfg.agent.HostedTarget.AgentName || len(agent.Endpoint.Schemes) == 0 {
		return errBrokerRemote
	}
	// Entra isolation is the verified first increment. Unknown/header isolation
	// is not silently adopted from the endpoint or inferred from a broad list.
	for _, scheme := range agent.Endpoint.Schemes {
		var value struct {
			Type string `json:"type"`
		}
		if strictjson.DecodeStruct(scheme, &value, true) != nil || !strings.EqualFold(value.Type, "entra") {
			return errBrokerRemote
		}
	}
	var version struct {
		Name       string `json:"name"`
		Version    string `json:"version"`
		Status     string `json:"status"`
		Definition struct {
			Kind string `json:"kind"`
		} `json:"definition"`
	}
	status, err = b.remoteJSON(ctx, http.MethodGet, "/versions/"+url.PathEscape(b.cfg.agent.HostedTarget.AgentVersion), nil, &version)
	if err != nil || status != http.StatusOK || version.Name != b.cfg.agent.HostedTarget.AgentName ||
		version.Version != b.cfg.agent.HostedTarget.AgentVersion || version.Status != "active" || version.Definition.Kind != "hosted" {
		return errBrokerRemote
	}
	return nil
}

func (b *lifecycleBroker) remoteSessionMatches(value foundry.RemoteSession, id string) bool {
	return value.ID == id && value.Version.Type == "version_ref" && value.Version.Version == b.cfg.agent.HostedTarget.AgentVersion
}

func (b *lifecycleBroker) remoteSessionGet(ctx context.Context, id string) (foundry.RemoteSession, int, error) {
	var session foundry.RemoteSession
	status, err := b.remoteJSON(ctx, http.MethodGet, foundry.SessionSuffix(id), nil, &session)
	if err == nil && status == http.StatusOK && !b.remoteSessionMatches(session, id) {
		err = errBrokerConflict
	}
	return session, status, err
}

func (b *lifecycleBroker) prepareRemoteSessionCreate(ctx context.Context, id string) (*http.Request, error) {
	body, _ := json.Marshal(map[string]any{"agent_session_id": id,
		"version_indicator": map[string]string{"type": "version_ref", "agent_version": b.cfg.agent.HostedTarget.AgentVersion}})
	return b.prepareRemoteRequest(ctx, http.MethodPost, "/endpoint/sessions", body)
}

// A false, nil result proves complete admission rejection. Every possibly sent,
// unacknowledged outcome retains the durable creation intent; an absent-session
// observation cannot clear it. Authentication was completed before the intent.
func (b *lifecycleBroker) remoteSessionCreate(request *http.Request, id string) (bool, error) {
	var session foundry.RemoteSession
	status, err := b.remoteJSONRequest(request, &session)
	if errors.Is(err, errBrokerRequestUnsent) {
		return false, err
	}
	if err == nil && foundry.DefiniteRejection(status) {
		return false, nil
	}
	if err != nil || status != http.StatusCreated || !b.remoteSessionMatches(session, id) {
		return false, errBrokerAmbiguous
	}
	return true, nil
}

func (b *lifecycleBroker) remoteSessionStop(ctx context.Context, id string) error {
	current, status, err := b.remoteSessionGet(ctx, id)
	if err != nil || status != http.StatusOK {
		return errBrokerPending
	}
	status, err = b.remoteJSON(ctx, http.MethodPost, foundry.SessionSuffix(id)+":stop", nil, nil)
	if err != nil || (status != http.StatusNoContent && status != http.StatusConflict) {
		return errBrokerPending
	}
	// The deployed API returned409 for a duplicate stop. Neither204 nor409 is
	// sufficient alone: use exact authenticated identity/version + idle status.
	for {
		current, status, err = b.remoteSessionGet(ctx, id)
		if err != nil || status != http.StatusOK {
			return errBrokerPending
		}
		if current.Status == "idle" {
			return nil
		}
		select {
		case <-ctx.Done():
			return errBrokerPending
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (b *lifecycleBroker) remoteSessionDelete(ctx context.Context, id string) error {
	// Known ownership permits retrying a lost DELETE acknowledgement. An absent
	// record does not skip DELETE204 + GET404, and this path is never called for
	// an unresolved create or inference intent.
	_, status, err := b.remoteSessionGet(ctx, id)
	if err != nil || (status != http.StatusOK && status != http.StatusNotFound) {
		return errBrokerPending
	}
	status, err = b.remoteJSON(ctx, http.MethodDelete, foundry.SessionSuffix(id), nil, nil)
	if err != nil || status != http.StatusNoContent {
		return errBrokerPending
	}
	_, status, err = b.remoteSessionGet(ctx, id)
	if err != nil || status != http.StatusNotFound {
		return errBrokerPending
	}
	return nil
}
