package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

const backendMetadata = "foundry-hosted-responses"

type foundryResponsesClient struct {
	cfg           config
	httpClient    *http.Client
	tokenProvider foundryTokenProvider
}

func newResponsesClient(cfg config, httpClient *http.Client, provider foundryTokenProvider) *foundryResponsesClient {
	return &foundryResponsesClient{cfg, httpClient, provider}
}

type foundryResponseRequest struct {
	Input              any                 `json:"input"`
	Stream             bool                `json:"stream"`
	Store              bool                `json:"store"`
	PreviousResponseID string              `json:"previous_response_id,omitempty"`
	AgentSessionID     string              `json:"agent_session_id,omitempty"`
	Tools              []foundryToolSchema `json:"tools,omitempty"`
}

type foundryToolSchema struct {
	Type        string          `json:"type"`
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters"`
}

type foundryFunctionOutput struct {
	Type   string `json:"type"`
	CallID string `json:"call_id"`
	Output string `json:"output"`
}

type foundryResponseEvent struct {
	Type           string             `json:"type"`
	Delta          string             `json:"delta,omitempty"`
	SequenceNumber int64              `json:"sequence_number,omitempty"`
	Response       *foundryResponse   `json:"response,omitempty"`
	Item           *foundryOutputItem `json:"item,omitempty"`
	Error          *foundryError      `json:"error,omitempty"`
}

type foundryResponse struct {
	ID             string              `json:"id"`
	Status         string              `json:"status"`
	AgentSessionID string              `json:"agent_session_id,omitempty"`
	Output         []foundryOutputItem `json:"output,omitempty"`
	Error          *foundryError       `json:"error,omitempty"`
	Incomplete     *foundryIncomplete  `json:"incomplete_details,omitempty"`
}

type foundryOutputItem struct {
	ID        string                 `json:"id,omitempty"`
	Type      string                 `json:"type"`
	CallID    string                 `json:"call_id,omitempty"`
	Name      string                 `json:"name,omitempty"`
	Arguments json.RawMessage        `json:"arguments,omitempty"`
	Content   []foundryOutputContent `json:"content,omitempty"`
}

type foundryOutputContent struct {
	Type string `json:"type,omitempty"`
	Text string `json:"text,omitempty"`
}

type foundryError struct {
	Type    string `json:"type,omitempty"`
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
	Param   string `json:"param,omitempty"`
}

type foundryIncomplete struct {
	Reason string `json:"reason,omitempty"`
}

type foundryStreamSummary struct {
	ResponseID     string
	AgentSessionID string
	Status         string
	Text           string
	FunctionCalls  []foundryOutputItem
	Error          *foundryError
	Incomplete     *foundryIncomplete
}

type responseCallbacks struct {
	OnCreated      func(foundryResponse) error
	OnTextDelta    func(string) error
	OnFunctionCall func(foundryOutputItem) error
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
	request foundryResponseRequest,
	callbacks responseCallbacks,
) (foundryStreamSummary, error) {
	request.Stream = true
	request.Store = true
	body, err := json.Marshal(request)
	if err != nil {
		return foundryStreamSummary{}, err
	}
	response, err := c.do(ctx, http.MethodPost, c.responsesURL(), bytes.NewReader(body))
	if err != nil {
		return foundryStreamSummary{}, err
	}
	defer response.Body.Close() //nolint:errcheck
	mediaType := strings.ToLower(response.Header.Get("Content-Type"))
	if strings.Contains(mediaType, "text/event-stream") {
		return parseFoundrySSE(response.Body, c.cfg.maxStreamBytes, c.cfg.maxEventBytes, c.cfg.maxEvents, callbacks)
	}
	return parseFoundryJSON(response.Body, c.cfg.maxStreamBytes, callbacks)
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
	sessionID := firstNonBlank(payload.AgentSessionID, payload.SessionID, payload.ID)
	if sessionID == "" {
		return "", errors.New("foundry session response did not include a session id")
	}
	if err := validateProviderIdentifier("agent session id", sessionID); err != nil {
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

func parseFoundryJSON(r io.Reader, maxBytes int64, callbacks responseCallbacks) (foundryStreamSummary, error) {
	data, err := io.ReadAll(io.LimitReader(r, maxBytes+1))
	if err != nil {
		return foundryStreamSummary{}, err
	}
	if int64(len(data)) > maxBytes {
		return foundryStreamSummary{}, errors.New("foundry response exceeded adapter stream limit")
	}
	var response foundryResponse
	if err := json.Unmarshal(data, &response); err != nil {
		return foundryStreamSummary{}, errors.New("foundry response was invalid JSON")
	}
	return processCompletedResponse(response, callbacks)
}

func parseFoundrySSE(
	r io.Reader,
	maxBytes int64,
	maxEventBytes int64,
	maxEvents int,
	callbacks responseCallbacks,
) (foundryStreamSummary, error) {
	reader := bufio.NewReader(io.LimitReader(r, maxBytes+1))
	var summary foundryStreamSummary
	var total int64
	var eventData []byte
	var eventCount int
	flushEvent := func() error {
		if len(eventData) == 0 {
			return nil
		}
		eventCount++
		if eventCount > maxEvents {
			return errors.New("foundry response exceeded adapter event limit")
		}
		if int64(len(eventData)) > maxEventBytes {
			return errors.New("foundry response event exceeded adapter limit")
		}
		if bytes.Equal(bytes.TrimSpace(eventData), []byte("[DONE]")) {
			eventData = nil
			return nil
		}
		var event foundryResponseEvent
		if err := json.Unmarshal(eventData, &event); err != nil {
			return errors.New("foundry response stream contained invalid JSON")
		}
		eventData = nil
		return applyFoundryEvent(&summary, event, callbacks)
	}
	for {
		line, err := reader.ReadBytes('\n')
		total += int64(len(line))
		if total > maxBytes {
			return foundryStreamSummary{}, errors.New("foundry response exceeded adapter stream limit")
		}
		trimmed := bytes.TrimRight(line, "\r\n")
		if len(trimmed) == 0 {
			if err := flushEvent(); err != nil {
				return foundryStreamSummary{}, err
			}
		} else if rawData, ok := bytes.CutPrefix(trimmed, []byte("data:")); ok {
			part := bytes.TrimSpace(rawData)
			if len(eventData)+len(part)+1 > int(maxEventBytes) {
				return foundryStreamSummary{}, errors.New("foundry response event exceeded adapter limit")
			}
			if len(eventData) > 0 {
				eventData = append(eventData, '\n')
			}
			eventData = append(eventData, part...)
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return foundryStreamSummary{}, err
			}
			if flushErr := flushEvent(); flushErr != nil {
				return foundryStreamSummary{}, flushErr
			}
			break
		}
	}
	if summary.Status == "" {
		return foundryStreamSummary{}, errors.New("foundry response stream ended without a terminal event")
	}
	return summary, nil
}

func applyFoundryEvent(summary *foundryStreamSummary, event foundryResponseEvent, callbacks responseCallbacks) error {
	switch event.Type {
	case "response.created", "response.in_progress", "response.queued":
		if event.Response != nil {
			mergeFoundryResponse(summary, *event.Response)
			if event.Type == "response.created" && callbacks.OnCreated != nil {
				return callbacks.OnCreated(*event.Response)
			}
		}
	case "response.output_text.delta":
		summary.Text += event.Delta
		if callbacks.OnTextDelta != nil && event.Delta != "" {
			return callbacks.OnTextDelta(event.Delta)
		}
	case "response.output_item.done":
		if event.Item != nil && event.Item.Type == "function_call" {
			summary.FunctionCalls = append(summary.FunctionCalls, *event.Item)
			if callbacks.OnFunctionCall != nil {
				return callbacks.OnFunctionCall(*event.Item)
			}
		}
	case "response.completed", "response.failed", "response.incomplete", "response.cancelled", "response.canceled":
		if event.Response != nil {
			mergeFoundryResponse(summary, *event.Response)
			if event.Type == "response.completed" {
				if err := applyTerminalOutputFallback(summary, *event.Response, callbacks); err != nil {
					return err
				}
			}
		}
		if summary.Status == "" {
			summary.Status = strings.TrimPrefix(event.Type, "response.")
		}
	case "error":
		summary.Status = "failed"
		summary.Error = event.Error
	}
	return nil
}

func applyTerminalOutputFallback(
	summary *foundryStreamSummary,
	response foundryResponse,
	callbacks responseCallbacks,
) error {
	var terminalText strings.Builder
	for _, item := range response.Output {
		if item.Type != "message" {
			continue
		}
		for _, content := range item.Content {
			terminalText.WriteString(content.Text)
		}
	}
	fullText := terminalText.String()
	if fullText != "" {
		if !strings.HasPrefix(fullText, summary.Text) {
			return errors.New("foundry terminal output does not match streamed text")
		}
		remainder := strings.TrimPrefix(fullText, summary.Text)
		if remainder != "" {
			summary.Text += remainder
			if callbacks.OnTextDelta != nil {
				if err := callbacks.OnTextDelta(remainder); err != nil {
					return err
				}
			}
		}
	}
	for _, item := range response.Output {
		if item.Type != "function_call" {
			continue
		}
		seen, conflict := foundryFunctionCallState(summary.FunctionCalls, item)
		if conflict {
			return fmt.Errorf("foundry function call %q changed within one response", item.CallID)
		}
		if seen {
			continue
		}
		summary.FunctionCalls = append(summary.FunctionCalls, item)
		if callbacks.OnFunctionCall != nil {
			if err := callbacks.OnFunctionCall(item); err != nil {
				return err
			}
		}
	}
	return nil
}

func foundryFunctionCallState(calls []foundryOutputItem, item foundryOutputItem) (bool, bool) {
	for _, call := range calls {
		if call.CallID != item.CallID {
			continue
		}
		return true, call.Name != item.Name || !bytes.Equal(call.Arguments, item.Arguments)
	}
	return false, false
}

func processCompletedResponse(response foundryResponse, callbacks responseCallbacks) (foundryStreamSummary, error) {
	summary := foundryStreamSummary{}
	mergeFoundryResponse(&summary, response)
	if callbacks.OnCreated != nil {
		if err := callbacks.OnCreated(response); err != nil {
			return foundryStreamSummary{}, err
		}
	}
	if !strings.EqualFold(response.Status, "completed") {
		return summary, nil
	}
	for _, item := range response.Output {
		switch item.Type {
		case "message":
			for _, content := range item.Content {
				if content.Text == "" {
					continue
				}
				summary.Text += content.Text
				if callbacks.OnTextDelta != nil {
					if err := callbacks.OnTextDelta(content.Text); err != nil {
						return foundryStreamSummary{}, err
					}
				}
			}
		case "function_call":
			summary.FunctionCalls = append(summary.FunctionCalls, item)
			if callbacks.OnFunctionCall != nil {
				if err := callbacks.OnFunctionCall(item); err != nil {
					return foundryStreamSummary{}, err
				}
			}
		}
	}
	return summary, nil
}

func mergeFoundryResponse(summary *foundryStreamSummary, response foundryResponse) {
	if response.ID != "" {
		summary.ResponseID = response.ID
	}
	if response.AgentSessionID != "" {
		summary.AgentSessionID = response.AgentSessionID
	}
	if response.Status != "" {
		summary.Status = response.Status
	}
	if response.Error != nil {
		summary.Error = response.Error
	}
	if response.Incomplete != nil {
		summary.Incomplete = response.Incomplete
	}
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
