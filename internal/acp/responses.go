package acp

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime"
	"net/http"

	"github.com/orka-agents/agent-runtime-foundry/internal/foundry"
)

// ACP never constructs a Foundry SDK client: the privileged supervisor owns the
// remote agent session and rewrites that binding outside this child process.
func acpCreateResponse(ctx context.Context, cfg acpConfiguration, client *http.Client, request foundry.ResponseRequest) (foundry.StreamSummary, error) {
	request.Stream, request.Store = true, true
	request.AgentSessionID = ""
	body, err := json.Marshal(foundry.ModelResponseRequest{ResponseRequest: request, Model: cfg.agent.Model})
	if err != nil {
		return foundry.StreamSummary{}, foundry.ErrResponse
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.providerURL, bytes.NewReader(body))
	if err != nil {
		return foundry.StreamSummary{}, foundry.ErrResponse
	}
	httpRequest.Header.Set("Authorization", "Bearer "+cfg.token)
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Accept", "text/event-stream, application/json")
	response, err := client.Do(httpRequest)
	if err != nil {
		return foundry.StreamSummary{}, foundry.ErrResponse
	}
	defer response.Body.Close() //nolint:errcheck
	mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || response.StatusCode != http.StatusOK {
		return foundry.StreamSummary{}, foundry.ErrResponse
	}
	switch mediaType {
	case "text/event-stream":
		return foundry.ParseStrictSSE(response.Body)
	case "application/json":
		data, err := io.ReadAll(io.LimitReader(response.Body, foundry.DefaultMaxStreamBytes+1))
		if err != nil || len(data) > foundry.DefaultMaxStreamBytes {
			return foundry.StreamSummary{}, foundry.ErrResponse
		}
		document, err := foundry.DecodeResponse(data)
		if err != nil || document.Status != "completed" {
			return foundry.StreamSummary{}, foundry.ErrResponse
		}
		summary, err := foundry.CompleteResponse(document, foundry.ResponseCallbacks{})
		if err != nil || foundry.ValidateSummary(summary) != nil {
			return foundry.StreamSummary{}, foundry.ErrResponse
		}
		return summary, nil
	default:
		return foundry.StreamSummary{}, foundry.ErrResponse
	}
}
