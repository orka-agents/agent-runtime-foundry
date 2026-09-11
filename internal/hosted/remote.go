package hosted

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/orka-agents/agent-runtime-foundry/internal/foundry"
	"github.com/orka-agents/agent-runtime-foundry/internal/strictjson"
	"golang.org/x/net/http/httpguts"
)

// Azure authorizes the exact hosted session. The in-band signature separately
// authorizes the supervisor bootstrap; Foundry strips ordinary custom headers.
func (g *hostedGateway) accessToken(ctx context.Context) (string, error) {
	token, err := g.provider.AccessToken(ctx)
	if err != nil || !httpguts.ValidHeaderFieldValue(token) {
		return "", errHostedInvalid
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return "", errHostedInvalid
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	defer clear(raw)
	var claims struct {
		Audience string `json:"aud"`
		Tenant   string `json:"tid"`
		Object   string `json:"oid"`
		App      string `json:"appid"`
		AZP      string `json:"azp"`
	}
	if err != nil || strictjson.DecodeStruct(raw, &claims, false) != nil ||
		strings.TrimRight(claims.Audience, "/") != "https://ai.azure.com" ||
		!hostedUUIDValid(claims.Tenant) || !hostedUUIDValid(claims.Object) {
		return "", errHostedInvalid
	}
	digest := foundry.JSONDigest(claims)
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.ledger.PrincipalDigest == "" {
		next := g.ledger
		next.PrincipalDigest = digest
		if g.store.save(next) != nil {
			return "", errHostedInvalid
		}
		g.ledger = next
	}
	if g.ledger.PrincipalDigest != digest {
		return "", errHostedInvalid
	}
	return token, nil
}

func (g *hostedGateway) remoteURL(suffix string) string {
	target := g.cfg.Image.Target
	return target.ProjectEndpoint + "/agents/" + url.PathEscape(target.AgentName) + suffix + "?api-version=v1"
}

func (g *hostedGateway) remoteJSON(ctx context.Context, method, suffix string, body []byte, target any) (int, error) {
	request, err := g.prepareRemoteJSON(ctx, method, suffix, body)
	if err != nil {
		return 0, err
	}
	return g.sendRemoteJSON(request, target)
}

func (g *hostedGateway) prepareRemoteJSON(ctx context.Context, method, suffix string, body []byte) (*http.Request, error) {
	if ctx.Err() != nil {
		return nil, errHostedInvalid
	}
	token, err := g.accessToken(ctx)
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, method, g.remoteURL(suffix), bytes.NewReader(body))
	if err != nil {
		return nil, errHostedInvalid
	}
	request.GetBody = nil
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Foundry-Features", "HostedAgents=V1Preview")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	if ctx.Err() != nil {
		return nil, errHostedInvalid
	}
	return request, nil
}

func (g *hostedGateway) sendRemoteJSON(request *http.Request, target any) (int, error) {
	response, err := g.httpClient.Do(request)
	if err != nil {
		return 0, errHostedInvalid
	}
	defer response.Body.Close() //nolint:errcheck
	data, err := io.ReadAll(io.LimitReader(response.Body, foundry.MaxAgentConfigBytes+1))
	defer clear(data)
	if err != nil || len(data) > foundry.MaxAgentConfigBytes {
		return response.StatusCode, errHostedInvalid
	}
	if target != nil && response.StatusCode >= 200 && response.StatusCode < 300 && strictjson.DecodeStruct(data, target, false) != nil {
		return response.StatusCode, errHostedInvalid
	}
	return response.StatusCode, nil
}

func (g *hostedGateway) validateRemote(ctx context.Context) error {
	var agent struct {
		Name     string `json:"name"`
		Endpoint struct {
			Schemes []json.RawMessage `json:"authorization_schemes"`
		} `json:"agent_endpoint"`
	}
	status, err := g.remoteJSON(ctx, http.MethodGet, "", nil, &agent)
	if err != nil || status != http.StatusOK || agent.Name != g.cfg.Image.Target.AgentName || len(agent.Endpoint.Schemes) == 0 {
		return errHostedInvalid
	}
	for _, scheme := range agent.Endpoint.Schemes {
		var value struct {
			Type string `json:"type"`
		}
		if strictjson.DecodeStruct(scheme, &value, true) != nil || !strings.EqualFold(value.Type, "entra") {
			return errHostedInvalid
		}
	}
	var version struct {
		Name       string `json:"name"`
		Version    string `json:"version"`
		Status     string `json:"status"`
		Definition struct {
			Kind      string `json:"kind"`
			Container struct {
				Image string `json:"image"`
			} `json:"container_configuration"`
			Protocols []struct {
				Protocol string `json:"protocol"`
				Version  string `json:"version"`
			} `json:"protocol_versions"`
		} `json:"definition"`
	}
	status, err = g.remoteJSON(ctx, http.MethodGet, "/versions/"+g.cfg.Image.Target.AgentVersion, nil, &version)
	if err != nil || status != http.StatusOK || version.Name != g.cfg.Image.Target.AgentName ||
		version.Version != g.cfg.Image.Target.AgentVersion || version.Status != "active" ||
		version.Definition.Kind != "hosted" || version.Definition.Container.Image != g.cfg.ContainerImage {
		return errHostedInvalid
	}
	ws := 0
	for _, protocol := range version.Definition.Protocols {
		if protocol.Protocol == "invocations_ws" {
			if protocol.Version != "2.0.0" {
				return errHostedInvalid
			}
			ws++
		}
	}
	if ws != 1 {
		return errHostedInvalid
	}
	return nil
}

func (g *hostedGateway) ensureSession(ctx context.Context) error {
	// An uncertain create is never replayed or adopted from a subsequent GET.
	if g.ledger.CreateAttempted && !g.ledger.SessionCreated {
		return errHostedInvalid
	}
	var session foundry.RemoteSession
	status, err := g.remoteJSON(ctx, http.MethodGet, foundry.SessionSuffix(g.cfg.SessionID), nil, &session)
	if err != nil {
		return err
	}
	if !g.ledger.CreateAttempted {
		if status != http.StatusNotFound {
			return errHostedInvalid
		}
		body, _ := json.Marshal(map[string]any{"agent_session_id": g.cfg.SessionID,
			"version_indicator": map[string]string{"type": "version_ref", "agent_version": g.cfg.Image.Target.AgentVersion}})
		request, prepareErr := g.prepareRemoteJSON(ctx, http.MethodPost, "/endpoint/sessions", body)
		if prepareErr != nil || ctx.Err() != nil {
			return errHostedInvalid
		}
		// Authentication and local request preparation cannot strand an
		// unsent creation. Once this intent is durable, submission errors
		// remain ambiguous and never authorize retry or adoption.
		next := g.ledger
		next.CreateAttempted = true
		if g.store.save(next) != nil {
			return errHostedInvalid
		}
		g.ledger = next
		status, err = g.sendRemoteJSON(request, &session)
		if err == nil && foundry.DefiniteRejection(status) {
			// A complete admission rejection proves that this attempt created no
			// session. Persist that result before permitting a later startup.
			// Transport errors and incomplete responses retain the original intent.
			next = g.ledger
			next.CreateAttempted = false
			if g.store.save(next) != nil {
				return errHostedInvalid
			}
			g.ledger = next
			return errHostedInvalid
		}
		if err != nil || status != http.StatusCreated || !g.sessionMatches(session) {
			return errHostedInvalid
		}
		next = g.ledger
		next.SessionCreated = true
		if g.store.save(next) != nil {
			return errHostedInvalid
		}
		g.ledger = next
		status, err = g.remoteJSON(ctx, http.MethodGet, foundry.SessionSuffix(g.cfg.SessionID), nil, &session)
	}
	if err != nil || status != http.StatusOK || !g.sessionMatches(session) || session.Status != "active" {
		return errHostedInvalid
	}
	return nil
}

func (g *hostedGateway) sessionMatches(value foundry.RemoteSession) bool {
	return value.ID == g.cfg.SessionID && value.Version.Type == "version_ref" && value.Version.Version == g.cfg.Image.Target.AgentVersion
}

func (g *hostedGateway) openChannel(ctx context.Context, role string) (*websocket.Conn, hostedChallenge, error) {
	var challenge hostedChallenge
	token, err := g.accessToken(ctx)
	if err != nil {
		return nil, challenge, err
	}
	endpoint := strings.Replace(g.remoteURL("/endpoint/protocols/invocations_ws"), "https://", "wss://", 1) +
		"&agent_session_id=" + url.QueryEscape(g.cfg.SessionID)
	headers := http.Header{"Authorization": []string{"Bearer " + token}, "Foundry-Features": []string{"HostedAgents=V1Preview"}}
	ws, response, err := g.dial(ctx, endpoint, headers)
	if err != nil {
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		return nil, challenge, errHostedInvalid
	}
	observation := observeHostedChannel(ws, role, time.Now(), g.channelLog)
	if role == "forward" {
		g.forwardObservation = observation
	} else {
		g.reverseObservation = observation
	}
	ws.SetReadLimit(hostedMaxHandshakeBytes)
	_ = ws.SetReadDeadline(time.Now().Add(30 * time.Second))
	if readHostedWSJSON(ws, &challenge) != nil || challenge.Protocol != hostedProtocol ||
		challenge.DeploymentID != g.cfg.Image.DeploymentID || challenge.ConfigurationDigest != foundry.JSONDigest(g.cfg.Image) ||
		challenge.AgentName != g.cfg.Image.Target.AgentName || challenge.AgentVersion != g.cfg.Image.Target.AgentVersion ||
		challenge.SessionID != g.cfg.SessionID || !hostedUUIDValid(challenge.BootID) {
		_ = ws.Close()
		observation.finish()
		return nil, hostedChallenge{}, errHostedInvalid
	}
	if _, ok := hostedCanonicalBytes(challenge.Nonce, 32); !ok {
		_ = ws.Close()
		observation.finish()
		return nil, hostedChallenge{}, errHostedInvalid
	}
	return ws, challenge, nil
}
