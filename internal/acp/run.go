package acp

import (
	"context"
	"strings"
	"sync"

	"github.com/orka-agents/agent-runtime-foundry/internal/foundry"
	"github.com/orka-agents/agent-runtime-foundry/internal/strictjson"
)

const acpMaxResponseRounds = 32

func (s *acpServer) runPrompt(ctx context.Context, session *acpSession, prompt string) (string, string, error) {
	tools, err := session.mcp.tools(ctx)
	if err != nil {
		return "", "", err
	}
	allowed := make(map[string]bool, len(tools))
	for _, tool := range tools {
		allowed[tool.Name] = true
	}
	request := foundry.ResponseRequest{Input: prompt, PreviousResponseID: session.previous}
	if s.cfg.agent.ToolSchemaMode == foundry.ToolSchemaModeRequest {
		request.Tools = tools
	}
	seen := make(map[string]bool)
	var text strings.Builder
	for range acpMaxResponseRounds {
		if err := ctx.Err(); err != nil {
			return "", "", err
		}
		summary, err := acpCreateResponse(ctx, s.cfg, s.client, request)
		if err != nil || text.Len()+len(summary.Text) > foundry.DefaultMaxOutputBytes {
			return "", "", foundry.ErrResponse
		}
		text.WriteString(summary.Text)
		if len(summary.FunctionCalls) == 0 {
			return summary.ResponseID, text.String(), nil
		}
		if len(seen)+len(summary.FunctionCalls) > foundry.DefaultMaxBrokeredCalls {
			return "", "", foundry.ErrResponse
		}
		// Validate the whole batch before admitting its first side effect.
		calls := summary.FunctionCalls
		for i, call := range calls {
			if foundry.ValidateFunctionName(call.Name) != nil || !allowed[call.Name] ||
				foundry.ValidateCallID(call.CallID) != nil || foundry.ValidateIdentifier("call", call.CallID) != nil || seen[call.CallID] || len(call.Arguments) == 0 {
				return "", "", foundry.ErrResponse
			}
			arguments, err := foundry.NormalizeToolArguments(call.Arguments)
			var object map[string]any
			if err != nil || len(arguments) > foundry.DefaultMaxBrokeredBytes || strictjson.Decode(arguments, &object, false) != nil || object == nil {
				return "", "", foundry.ErrResponse
			}
			calls[i].Arguments = arguments
			seen[call.CallID] = true
		}
		outputs, err := s.executeTools(ctx, session, calls)
		if err != nil {
			return "", "", err
		}
		request.Input = outputs
		request.PreviousResponseID = summary.ResponseID
	}
	return "", "", foundry.ErrResponse
}

func (s *acpServer) executeTools(ctx context.Context, session *acpSession, calls []foundry.OutputItem) ([]foundry.FunctionOutput, error) {
	ids := make([]string, len(calls))
	started := 0
	for i, call := range calls {
		if err := ctx.Err(); err != nil {
			for j := range started {
				_ = s.toolEvent(session.id, ids[j], calls[j].Name, "failed")
			}
			return nil, err
		}
		ids[i] = acpOpaqueID("tool-")
		if err := s.toolEvent(session.id, ids[i], call.Name, "in_progress"); err != nil {
			return nil, err
		}
		started++
	}
	group, cancel := context.WithCancel(ctx)
	defer cancel()
	outputs := make([]foundry.FunctionOutput, len(calls))
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstError error
	var total int
	// Orka's loopback MCP session admits two outstanding calls by default.
	slots := make(chan struct{}, 2)
	for i, call := range calls {
		wg.Go(func() {
			var output string
			var isError bool
			var err error
			var acquired bool
			select {
			case slots <- struct{}{}:
				acquired = true
				if group.Err() == nil {
					output, isError, err = session.mcp.execute(group, call.Name, call.Arguments)
				} else {
					err = group.Err()
				}
			case <-group.Done():
				err = group.Err()
			}
			mu.Lock()
			total += len(output)
			if total > foundry.DefaultMaxBrokeredTurnBytes && err == nil {
				err = errACPMCP
			}
			if err != nil {
				if firstError == nil {
					firstError = err
				}
				// Signal before writing events: stdout backpressure must not keep
				// a sibling HTTP request alive after protocol authority is lost.
				cancel()
			}
			mu.Unlock()
			// Revoke the batch before a released slot can admit queued work.
			if acquired {
				<-slots
			}
			status := "completed"
			if err != nil || isError {
				status = "failed"
			}
			if eventErr := s.toolEvent(session.id, ids[i], call.Name, status); eventErr != nil {
				mu.Lock()
				if firstError == nil {
					firstError = eventErr
				}
				cancel()
				mu.Unlock()
			}
			outputs[i] = foundry.FunctionOutput{Type: "function_call_output", CallID: call.CallID, Output: output}
		})
	}
	wg.Wait()
	if firstError != nil {
		return nil, firstError
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return outputs, nil
}

func (s *acpServer) toolEvent(sessionID, id, name, status string) error {
	update := "tool_call_update"
	if status == "in_progress" {
		update = "tool_call"
	}
	return s.update(sessionID, map[string]string{
		"sessionUpdate": update, "toolCallId": id, "title": name, "kind": "other", "status": status,
	})
}
