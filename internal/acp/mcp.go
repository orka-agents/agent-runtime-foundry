package acp

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/orka-agents/agent-runtime-foundry/internal/foundry"
	"github.com/orka-agents/agent-runtime-foundry/internal/strictjson"
)

const acpMCPVersion = "2025-06-18"

// acpMCPClient speaks only to Orka's per-session loopback proxy. Its route and
// scoped bearer select the session; replies are JSON, without MCP-Session-Id
// negotiation or an SSE stream.
type acpMCPClient struct {
	url    string
	bearer string
	client *http.Client
	nextID atomic.Uint64
}

func newACPMCPClient(server acpMCPServer, client *http.Client) (*acpMCPClient, error) {
	if server.Type != "http" || !foundry.SafeString(server.Name, 128) || len(server.Headers) != 1 {
		return nil, acpInvalidParams
	}
	if _, err := acpLoopbackURL(server.URL); err != nil {
		return nil, acpInvalidParams
	}
	header := server.Headers[0]
	value, ok := strings.CutPrefix(header.Value, "Bearer ")
	if !strings.EqualFold(header.Name, "Authorization") || !ok ||
		!foundry.SafeString(value, 16<<10) || strings.ContainsAny(value, " \t") {
		return nil, acpInvalidParams
	}
	return &acpMCPClient{url: server.URL, bearer: header.Value, client: client}, nil
}

func (m *acpMCPClient) initialize(ctx context.Context) error {
	result, err := m.call(ctx, "initialize", map[string]any{
		"protocolVersion": acpMCPVersion,
		"capabilities":    map[string]any{},
		"clientInfo":      map[string]string{"name": "foundry-acp", "version": "1"},
	})
	if err != nil {
		return err
	}
	var reply struct {
		ProtocolVersion string `json:"protocolVersion"`
		Capabilities    struct {
			Tools json.RawMessage `json:"tools"`
		} `json:"capabilities"`
	}
	if strictjson.Decode(result, &reply, false) != nil || reply.ProtocolVersion != acpMCPVersion ||
		len(reply.Capabilities.Tools) == 0 || reply.Capabilities.Tools[0] != '{' {
		return errACPMCP
	}
	body, _ := json.Marshal(acpRequest{JSONRPC: "2.0", Method: "notifications/initialized"})
	response, err := m.post(ctx, body, acpHTTPTimeout)
	if err != nil {
		return err
	}
	defer response.Body.Close() //nolint:errcheck
	if response.StatusCode != http.StatusAccepted && response.StatusCode != http.StatusNoContent {
		return errACPMCP
	}
	return nil
}

func (m *acpMCPClient) tools(ctx context.Context) ([]foundry.ToolSchema, error) {
	result, err := m.call(ctx, "tools/list", map[string]any{})
	if err != nil {
		return nil, err
	}
	var reply struct {
		Tools []struct {
			Name        string          `json:"name"`
			Description string          `json:"description"`
			InputSchema json.RawMessage `json:"inputSchema"`
		} `json:"tools"`
		NextCursor json.RawMessage `json:"nextCursor"`
	}
	if strictjson.Decode(result, &reply, false) != nil || reply.Tools == nil || len(reply.Tools) > foundry.DefaultMaxBrokeredCalls ||
		(len(reply.NextCursor) != 0 && !bytes.Equal(reply.NextCursor, []byte("null"))) {
		return nil, errACPMCP
	}
	tools := make([]foundry.ToolSchema, 0, len(reply.Tools))
	seen := make(map[string]bool)
	for _, tool := range reply.Tools {
		if foundry.ValidateFunctionName(tool.Name) != nil || seen[tool.Name] || len(tool.InputSchema) > foundry.MaxToolSchemaBytes {
			return nil, errACPMCP
		}
		var schema map[string]any
		if strictjson.Decode(tool.InputSchema, &schema, false) != nil || schema == nil || schema["type"] != "object" {
			return nil, errACPMCP
		}
		seen[tool.Name] = true
		tools = append(tools, foundry.ToolSchema{Type: "function", Name: tool.Name, Description: tool.Description, Parameters: tool.InputSchema})
	}
	encoded, err := json.Marshal(tools)
	if err != nil || len(encoded) > foundry.MaxToolSchemaBytes {
		return nil, errACPMCP
	}
	return tools, nil
}

func (m *acpMCPClient) execute(ctx context.Context, name string, args json.RawMessage) (string, bool, error) {
	result, err := m.call(ctx, "tools/call", map[string]any{"name": name, "arguments": args})
	if err != nil {
		return "", false, err
	}
	var reply struct {
		Content []struct {
			Type string  `json:"type"`
			Text *string `json:"text"`
		} `json:"content"`
		IsError           *bool           `json:"isError,omitempty"`
		StructuredContent json.RawMessage `json:"structuredContent,omitempty"`
	}
	if strictjson.DecodeStruct(result, &reply, false) != nil || reply.Content == nil {
		return "", false, errACPMCP
	}
	for _, content := range reply.Content {
		if content.Type != "text" || content.Text == nil {
			return "", false, errACPMCP
		}
	}
	if len(reply.StructuredContent) != 0 && reply.StructuredContent[0] != '{' {
		return "", false, errACPMCP
	}
	// Only the validated model-visible projection crosses the provider
	// boundary. MCP metadata and extension fields remain local to the client.
	visible, err := json.Marshal(reply)
	if err != nil {
		return "", false, errACPMCP
	}
	return string(visible), reply.IsError != nil && *reply.IsError, nil
}

func (m *acpMCPClient) call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	id, _ := json.Marshal(m.nextID.Add(1))
	encoded, err := json.Marshal(params)
	if err != nil || len(encoded) > foundry.DefaultMaxBrokeredBytes {
		return nil, errACPMCP
	}
	body, _ := json.Marshal(acpRequest{JSONRPC: "2.0", ID: id, Method: method, Params: encoded})
	timeout := acpHTTPTimeout
	if method == "tools/call" {
		timeout = acpToolCallTimeout
	}
	response, err := m.post(ctx, body, timeout)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close() //nolint:errcheck
	mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || response.StatusCode != http.StatusOK || mediaType != "application/json" {
		return nil, errACPMCP
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, foundry.DefaultMaxBrokeredBytes+1))
	if err != nil || len(data) > foundry.DefaultMaxBrokeredBytes {
		return nil, errACPMCP
	}
	var reply struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Result  json.RawMessage `json:"result"`
		Error   json.RawMessage `json:"error"`
	}
	if strictjson.Decode(data, &reply, true) != nil || reply.JSONRPC != "2.0" || !bytes.Equal(reply.ID, id) ||
		len(reply.Error) != 0 || len(reply.Result) == 0 || reply.Result[0] != '{' {
		return nil, errACPMCP
	}
	return reply.Result, nil
}

func (m *acpMCPClient) post(ctx context.Context, body []byte, timeout time.Duration) (*http.Response, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, m.url, bytes.NewReader(body))
	if err != nil {
		return nil, errACPMCP
	}
	request.Header.Set("Authorization", m.bearer)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json, text/event-stream")
	request.Header.Set("MCP-Protocol-Version", acpMCPVersion)
	// A held approval is still this one request. Share the bounded loopback
	// transport, but never lengthen a concurrent model or discovery request.
	client := *m.client
	client.Timeout = timeout
	response, err := client.Do(request)
	if err != nil {
		return nil, errACPMCP
	}
	return response, nil
}
