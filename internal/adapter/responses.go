package adapter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/orka-agents/agent-runtime-foundry/internal/foundry"
)

const backendMetadata = "foundry-hosted-responses"

type foundryResponsesClient struct {
	cfg           config
	httpClient    *http.Client
	tokenProvider foundry.TokenProvider
}

func newResponsesClient(cfg config, httpClient *http.Client, provider foundry.TokenProvider) *foundryResponsesClient {
	return &foundryResponsesClient{cfg, httpClient, provider}
}

type providerHTTPError struct {
	StatusCode int
	Operation  string
}

func (e providerHTTPError) Error() string {
	return fmt.Sprintf("Foundry %s failed with HTTP %d", e.Operation, e.StatusCode)
}

func newFoundryHTTPClient(endpoint string) *http.Client {
	base, _ := url.Parse(strings.TrimSpace(endpoint))
	return &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 10 {
				return errors.New("stopped after 10 Foundry redirects")
			}
			if base == nil || !strings.EqualFold(req.URL.Scheme, base.Scheme) ||
				!strings.EqualFold(req.URL.Host, base.Host) {
				return errors.New("refusing Foundry redirect outside the configured origin")
			}
			return nil
		},
	}
}

func (c *foundryResponsesClient) createResponse(
	ctx context.Context,
	request foundry.ResponseRequest,
	callbacks foundry.ResponseCallbacks,
) (foundry.StreamSummary, error) {
	request.Stream = true
	request.Store = true
	body, err := json.Marshal(request)
	if err != nil {
		return foundry.StreamSummary{}, err
	}
	response, err := c.do(ctx, http.MethodPost, c.responsesURL(), bytes.NewReader(body))
	if err != nil {
		return foundry.StreamSummary{}, err
	}
	defer response.Body.Close() //nolint:errcheck
	mediaType := strings.ToLower(response.Header.Get("Content-Type"))
	if strings.Contains(mediaType, "text/event-stream") {
		return foundry.ParseSSE(response.Body, c.cfg.maxStreamBytes, c.cfg.maxEventBytes, c.cfg.maxEvents, callbacks)
	}
	return foundry.ParseJSON(response.Body, c.cfg.maxStreamBytes, callbacks)
}

func (c *foundryResponsesClient) createSession(ctx context.Context) (string, error) {
	body := []byte(`{}`)
	if c.cfg.agentVersion != "" {
		encoded, err := json.Marshal(map[string]any{
			"version_indicator": map[string]string{
				"type":          "version_ref",
				"agent_version": c.cfg.agentVersion,
			},
		})
		if err != nil {
			return "", err
		}
		body = encoded
	}
	response, err := c.do(ctx, http.MethodPost, c.sessionsURL(), bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	defer response.Body.Close() //nolint:errcheck
	data, err := io.ReadAll(io.LimitReader(response.Body, c.cfg.maxEventBytes+1))
	if err != nil {
		return "", err
	}
	if int64(len(data)) > c.cfg.maxEventBytes {
		return "", errors.New("foundry session response exceeded adapter limit")
	}
	var payload struct {
		AgentSessionID string `json:"agent_session_id"`
		SessionID      string `json:"session_id"`
		ID             string `json:"id"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		return "", errors.New("foundry session response was invalid JSON")
	}
	sessionID := foundry.FirstNonBlank(payload.AgentSessionID, payload.SessionID, payload.ID)
	if sessionID == "" {
		return "", errors.New("foundry session response did not include a session id")
	}
	if err := foundry.ValidateIdentifier("agent session id", sessionID); err != nil {
		return "", err
	}
	return sessionID, nil
}

func (c *foundryResponsesClient) validateAgent(ctx context.Context) error {
	agentURL, err := c.agentURL()
	if err != nil {
		return err
	}
	response, err := c.do(ctx, http.MethodGet, agentURL, nil)
	if err != nil {
		return err
	}
	defer response.Body.Close() //nolint:errcheck
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	if c.cfg.agentVersion == "" {
		return nil
	}
	versionURL, err := c.agentVersionURL()
	if err != nil {
		return err
	}
	versionResponse, err := c.do(ctx, http.MethodGet, versionURL, nil)
	if err != nil {
		return err
	}
	defer versionResponse.Body.Close() //nolint:errcheck
	data, err := io.ReadAll(io.LimitReader(versionResponse.Body, c.cfg.maxEventBytes+1))
	if err != nil {
		return err
	}
	if int64(len(data)) > c.cfg.maxEventBytes {
		return errors.New("foundry agent-version response exceeded adapter limit")
	}
	var version struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(data, &version); err != nil {
		return errors.New("foundry agent-version response was invalid JSON")
	}
	if !strings.EqualFold(strings.TrimSpace(version.Status), "active") {
		return errors.New("configured Foundry agent version is not active")
	}
	return nil
}

func (c *foundryResponsesClient) do(ctx context.Context, method, rawURL string, body io.Reader) (*http.Response, error) {
	if c == nil || c.httpClient == nil || c.tokenProvider == nil {
		return nil, errors.New("foundry client is not configured")
	}
	request, err := http.NewRequestWithContext(ctx, method, rawURL, body)
	if err != nil {
		return nil, err
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	request.Header.Set("Accept", "text/event-stream, application/json")
	if c.cfg.foundryFeatures != "" {
		request.Header.Set("Foundry-Features", c.cfg.foundryFeatures)
	}
	if strings.EqualFold(c.cfg.isolationMode, "header") {
		isolationKey, ok := foundryIsolationKeyFromContext(ctx)
		if !ok || isolationKey == "" {
			return nil, errors.New("foundry header isolation requires a scoped isolation key")
		}
		request.Header.Set("x-ms-user-isolation-key", isolationKey)
	}
	token, err := c.tokenProvider.AccessToken(ctx)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := c.httpClient.Do(request)
	if err != nil {
		return nil, err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		defer response.Body.Close() //nolint:errcheck
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		return nil, providerHTTPError{StatusCode: response.StatusCode, Operation: method + " " + request.URL.Path}
	}
	return response, nil
}

func (c *foundryResponsesClient) responsesURL() string {
	if c.cfg.responsesEndpoint != "" {
		return withAPIVersion(c.cfg.responsesEndpoint, c.cfg.apiVersion)
	}
	base := strings.TrimRight(c.cfg.projectEndpoint, "/") + "/agents/" + url.PathEscape(c.cfg.agentName) +
		"/endpoint/protocols/openai/responses"
	return withAPIVersion(base, c.cfg.apiVersion)
}

func (c *foundryResponsesClient) sessionsURL() string {
	base := c.cfg.projectEndpoint
	if base == "" {
		u, _ := url.Parse(c.cfg.responsesEndpoint)
		suffix := "/agents/" + url.PathEscape(c.cfg.agentName) + "/endpoint/protocols/openai/responses"
		base = strings.TrimSuffix(strings.TrimRight(u.Scheme+"://"+u.Host+u.Path, "/"), suffix)
	}
	return withAPIVersion(strings.TrimRight(base, "/")+"/agents/"+url.PathEscape(c.cfg.agentName)+"/endpoint/sessions", c.cfg.apiVersion)
}

func (c *foundryResponsesClient) agentURL() (string, error) {
	if c.cfg.projectEndpoint == "" {
		return "", errors.New("foundry project endpoint is required for readiness validation")
	}
	return withAPIVersion(strings.TrimRight(c.cfg.projectEndpoint, "/")+"/agents/"+url.PathEscape(c.cfg.agentName), c.cfg.apiVersion), nil
}

func (c *foundryResponsesClient) agentVersionURL() (string, error) {
	agentURL, err := c.agentURL()
	if err != nil {
		return "", err
	}
	u, err := url.Parse(agentURL)
	if err != nil {
		return "", err
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/versions/" + url.PathEscape(c.cfg.agentVersion)
	return u.String(), nil
}

func withAPIVersion(rawURL, apiVersion string) string {
	u, err := url.Parse(rawURL)
	if err != nil || apiVersion == "" {
		return rawURL
	}
	query := u.Query()
	query.Set("api-version", apiVersion)
	u.RawQuery = query.Encode()
	return u.String()
}
