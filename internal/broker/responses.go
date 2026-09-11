package broker

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/orka-agents/agent-runtime-foundry/internal/brokerapi"
	"github.com/orka-agents/agent-runtime-foundry/internal/foundry"
	"github.com/orka-agents/agent-runtime-foundry/internal/strictjson"
)

func (b *lifecycleBroker) serveResponses(w http.ResponseWriter, r *http.Request, c brokerContext, raw []byte) {
	for name := range r.Header {
		if strings.EqualFold(name, brokerAgentKitProofHeader) {
			brokerWriteError(w, errBrokerInvalid)
			return
		}
	}
	var request foundry.ModelResponseRequest
	if strictjson.Decode(raw, &request, true) != nil || request.Model != b.cfg.agent.Model || !request.Stream || !request.Store || request.Input == nil {
		brokerWriteError(w, errBrokerInvalid)
		return
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		brokerWriteError(w, errBrokerInvalid)
		return
	}
	// Match encoding/json's field-name folding, including presence with null
	// or empty values that would disappear when the decoded request is encoded.
	for name := range fields {
		if strings.EqualFold(name, "agent_session_id") ||
			(b.cfg.agent.ToolSchemaMode == foundry.ToolSchemaModeProviderStatic && strings.EqualFold(name, "tools")) {
			brokerWriteError(w, errBrokerInvalid)
			return
		}
	}
	for _, tool := range request.Tools {
		var schema map[string]any
		if tool.Type != "function" || !foundry.SafeString(tool.Name, 512) || strictjson.Decode(tool.Parameters, &schema, false) != nil {
			brokerWriteError(w, errBrokerInvalid)
			return
		}
	}
	key := foundry.JSONDigest(c.Owner)
	runCtx, cancel := context.WithCancel(b.ctx)
	err := b.reserveInvocation(c, &request, cancel)
	if err != nil {
		cancel()
		brokerWriteError(w, err)
		return
	}
	defer b.wg.Done()
	defer cancel()
	stopDisconnect := context.AfterFunc(r.Context(), func() { b.closePrompt(key, c.promptKey()); cancel() })
	defer stopDisconnect()
	var result foundry.Response
	result, err = b.invoke(runCtx, c, request.ResponseRequest)
	stopDisconnect()
	b.finishInvocation(c, err)
	// Finalization can poison storage after a valid remote completion. The
	// durable failure also takes precedence over earlier transport errors.
	b.mu.Lock()
	if b.storageError != nil {
		err = b.storageError
	}
	b.mu.Unlock()
	if err != nil {
		brokerWriteError(w, err)
		return
	}
	// Buffer only within the advertised response bound. The ACP child also waits
	// for explicit completion before proposing any tool call. Never stream a
	// terminal result that has not been durably linked to this owner.
	_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(5 * time.Second))
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if json.NewEncoder(w).Encode(result) != nil {
		b.closePrompt(key, c.promptKey())
	}
}

func (b *lifecycleBroker) reserveInvocation(c brokerContext, request *foundry.ModelResponseRequest, cancel context.CancelFunc) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.storageError != nil {
		return b.storageError
	}
	if b.ctx.Err() != nil {
		return errBrokerClosed
	}
	key := foundry.JSONDigest(c.Owner)
	if _, active := b.active[key]; active {
		return errBrokerConflict
	}
	err := b.commitCapacityLocked(true, func(next *brokerLedger) error {
		session, err := brokerEnsureSession(next, c)
		if err != nil {
			return err
		}
		if session.Retiring || session.Retired {
			return errBrokerClosed
		}
		duplicate, err := brokerRecordOperation(session, brokerapi.ResponsesPath, c)
		if err != nil {
			return err
		}
		if duplicate {
			return errBrokerConflict
		} // No response replay cache or inference retry.
		prompt, err := brokerEnsurePrompt(session, c)
		if err != nil {
			return err
		}
		if prompt.Closing || prompt.Settled || !prompt.LeaseExpiresAt.After(time.Now()) {
			return errBrokerClosed
		}
		expires, _ := time.Parse(time.RFC3339Nano, c.LeaseExpiresAt)
		if c.LeaseGeneration != prompt.LeaseGeneration || !expires.Equal(prompt.LeaseExpiresAt) || c.InvocationSequence <= prompt.LastSequence {
			return errBrokerConflict
		}
		if prompt.LastSequence != 0 && request.PreviousResponseID != prompt.LastAlias {
			return errBrokerConflict
		}
		if err := brokerTranslatePrevious(session, c, prompt.LastSequence == 0, &request.ResponseRequest); err != nil {
			return err
		}
		if b.cfg.agentKitProof != "" {
			if err := brokerAgentKitOutputs(&request.ResponseRequest); err != nil {
				return err
			}
		}
		prompt.LastSequence = c.InvocationSequence
		prompt.Invocations[c.InvocationSequence] = &brokerInvocation{Sequence: c.InvocationSequence, OperationID: c.OperationID,
			BodyDigest: c.BodySHA256, State: "reserved"}
		return nil
	})
	if err != nil {
		return err
	}
	b.active[key] = brokerActive{prompt: c.promptKey(), cancel: cancel}
	b.wg.Add(1)
	return nil
}

func brokerTranslatePrevious(session *brokerSession, c brokerContext, first bool, request *foundry.ResponseRequest) error {
	previous, hasPrevious := session.Responses[request.PreviousResponseID]
	if request.PreviousResponseID != "" && (!hasPrevious || !previous.Completed ||
		(first && previous.HasFunctions)) {
		return errBrokerConflict
	}
	if !first && (!hasPrevious || previous.PromptKey != c.promptKey()) {
		return errBrokerConflict
	}
	if first {
		if _, ok := request.Input.(string); !ok {
			// This bridge sends one user text on the first round. Native item
			// references and child-chosen provider identities are not accepted.
			return errBrokerInvalid
		}
		if hasPrevious {
			request.PreviousResponseID = previous.RemoteID
		}
		return nil
	}
	inputs, list := request.Input.([]any)
	if !list || !previous.HasFunctions || len(inputs) != len(previous.CallIDs) {
		return errBrokerConflict
	}
	seen := map[string]bool{}
	for _, input := range inputs {
		item, object := input.(map[string]any)
		if !object || item["type"] != "function_call_output" {
			return errBrokerConflict
		}
		call, ok := item["call_id"].(string)
		output, outputOK := item["output"].(string)
		remote, owned := previous.CallIDs[call]
		if !ok || !outputOK || !owned || seen[call] || len(item) != 3 || len(output) > foundry.DefaultMaxBrokeredBytes {
			return errBrokerConflict
		}
		seen[call] = true
		item["call_id"] = remote
	}
	request.PreviousResponseID = previous.RemoteID
	return nil
}

func (b *lifecycleBroker) invoke(ctx context.Context, c brokerContext, request foundry.ResponseRequest) (foundry.Response, error) {
	key, promptKey := foundry.JSONDigest(c.Owner), c.promptKey()
	b.mu.Lock()
	session := b.ledger.Sessions[key]
	createState, remoteID := session.CreateState, session.RemoteID
	b.mu.Unlock()
	if createState == "none" {
		if err := b.validateRemoteTarget(ctx); err != nil {
			return foundry.Response{}, err
		}
		remoteID = uuid.NewString()
		prepareCtx, cancelPrepare := context.WithTimeout(ctx, b.cfg.operationTimeout)
		prepared, err := b.prepareRemoteSessionCreate(prepareCtx, remoteID)
		deadline, _ := prepareCtx.Deadline()
		cancelPrepare()
		if err != nil {
			return foundry.Response{}, err
		}
		// Before durable intent, caller cancellation must leave creation unsent.
		// After intent, keep this one bounded attempt alive to retain its ack.
		createCtx, cancelCreate := context.WithDeadline(b.ctx, deadline)
		prepared = prepared.WithContext(createCtx)
		b.mu.Lock()
		err = b.commitLocked(func(next *brokerLedger) error {
			session := next.Sessions[key]
			prompt := session.Prompts[promptKey]
			if session.Retiring || prompt.Closing || ctx.Err() != nil || createCtx.Err() != nil || !prompt.LeaseExpiresAt.After(time.Now()) {
				return errBrokerClosed
			}
			session.RemoteID, session.CreateState = remoteID, "intent"
			return nil
		})
		b.mu.Unlock()
		if err != nil {
			cancelCreate()
			return foundry.Response{}, err
		}
		// Once the intent is durable, finish this one creation attempt even if
		// the prompt closes. Losing its acknowledgement would leave an owner
		// that cannot be retired from a later 404. Broker shutdown and the
		// operation deadline still bound the attempt; it is never replayed.
		created, createErr := b.remoteSessionCreate(prepared, remoteID)
		cancelCreate()
		if createErr != nil && !errors.Is(createErr, errBrokerRequestUnsent) {
			return foundry.Response{}, createErr
		}
		b.mu.Lock()
		err = b.commitLocked(func(next *brokerLedger) error {
			session := next.Sessions[key]
			if created {
				session.CreateState = "known"
			} else {
				// Deliberately skipped dispatch or complete admission rejection
				// proves no session was created. Ambiguity never enters here.
				session.RemoteID, session.CreateState = "", "none"
			}
			return nil
		})
		b.mu.Unlock()
		if err != nil {
			return foundry.Response{}, err
		}
		if createErr != nil {
			return foundry.Response{}, createErr
		}
		if !created {
			return foundry.Response{}, errBrokerRemote
		}
	} else if createState != "known" {
		return foundry.Response{}, errBrokerPending
	}
	if ctx.Err() != nil {
		return foundry.Response{}, errBrokerClosed
	}
	current, status, err := b.remoteSessionGet(ctx, remoteID)
	if err != nil || status != http.StatusOK || (current.Status != "active" && current.Status != "idle") {
		return foundry.Response{}, errBrokerRemote
	}
	request.AgentSessionID = remoteID
	body, err := b.marshalResponseRequest(request)
	if err != nil {
		return foundry.Response{}, errBrokerInvalid
	}
	prepared, err := b.prepareRemoteRequest(ctx, http.MethodPost, "/endpoint/protocols/openai/responses", body)
	if err != nil {
		return foundry.Response{}, err
	}
	b.mu.Lock()
	err = b.commitLocked(func(next *brokerLedger) error {
		session := next.Sessions[key]
		prompt := session.Prompts[promptKey]
		if session.Retiring || prompt.Closing || ctx.Err() != nil || !prompt.LeaseExpiresAt.After(time.Now()) {
			return errBrokerClosed
		}
		prompt.Invocations[c.InvocationSequence].State = "intent"
		return nil
	})
	b.mu.Unlock()
	if err != nil {
		return foundry.Response{}, err
	}
	response, err := b.sendRemoteRequest(prepared)
	if err != nil {
		return foundry.Response{}, err
	}
	defer response.Body.Close() //nolint:errcheck
	if response.StatusCode != http.StatusOK {
		count, readErr := io.Copy(io.Discard, io.LimitReader(response.Body, foundry.MaxAgentConfigBytes+1))
		if readErr != nil || count > foundry.MaxAgentConfigBytes || !foundry.DefiniteRejection(response.StatusCode) {
			return foundry.Response{}, errBrokerAmbiguous
		}
		// An explicit complete HTTP rejection is different from a lost response.
		// It never authorizes a retry, but has no delayed unacknowledged request.
		b.mu.Lock()
		err = b.commitLocked(func(next *brokerLedger) error {
			next.Sessions[key].Prompts[promptKey].Invocations[c.InvocationSequence].State = "rejected"
			return nil
		})
		b.mu.Unlock()
		if err != nil {
			return foundry.Response{}, err
		}
		return foundry.Response{}, errBrokerRemote
	}
	mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil {
		return foundry.Response{}, errBrokerAmbiguous
	}
	var summary foundry.StreamSummary
	switch mediaType {
	case "text/event-stream":
		var data []byte
		data, err = b.readTrackedStream(response.Body, c, remoteID)
		if err == nil {
			summary, err = foundry.ParseStrictSSE(bytes.NewReader(data))
		}
	case "application/json":
		var data []byte
		data, err = io.ReadAll(io.LimitReader(response.Body, foundry.DefaultMaxStreamBytes+1))
		if err == nil && len(data) <= foundry.DefaultMaxStreamBytes {
			var document foundry.Response
			document, err = brokerDecodeResponseEvidence(data)
			if err == nil {
				err = b.recordResponseIdentity(c, document, remoteID)
			}
			if err == nil && document.Status == "completed" {
				document, err = foundry.DecodeResponse(data)
				if err == nil {
					summary, err = foundry.CompleteResponse(document, foundry.ResponseCallbacks{})
				}
			} else if err == nil {
				err = errBrokerRemote
			}
		} else {
			err = errBrokerAmbiguous
		}
	default:
		err = errBrokerAmbiguous
	}
	if err != nil || foundry.ValidateSummary(summary) != nil {
		return foundry.Response{}, errBrokerAmbiguous
	}
	return b.commitCompletedResponse(c, summary)
}

// Acceptance evidence is distinct from usable output. A coherent failed or
// incomplete response still acknowledges this invocation and permits stop
// containment, while only the stricter ACP decoder may admit successful output.
func brokerDecodeResponseEvidence(data []byte) (foundry.Response, error) {
	var evidence struct {
		ID             string                 `json:"id"`
		Status         string                 `json:"status"`
		AgentSessionID string                 `json:"agent_session_id"`
		Output         json.RawMessage        `json:"output"`
		Error          *foundry.ResponseError `json:"error"`
		Incomplete     *foundry.Incomplete    `json:"incomplete_details"`
	}
	if strictjson.DecodeStruct(data, &evidence, false) != nil || evidence.ID == "" || foundry.ValidateIdentifier("response", evidence.ID) != nil {
		return foundry.Response{}, errBrokerRemote
	}
	response := foundry.Response{ID: evidence.ID, Status: evidence.Status, AgentSessionID: evidence.AgentSessionID,
		Error: evidence.Error, Incomplete: evidence.Incomplete}
	switch response.Status {
	case "queued", "in_progress", "completed", "cancelled":
		if response.Error != nil || response.Incomplete != nil {
			return foundry.Response{}, errBrokerRemote
		}
	case "failed":
		if response.Incomplete != nil {
			return foundry.Response{}, errBrokerRemote
		}
	case "incomplete":
		if response.Error != nil {
			return foundry.Response{}, errBrokerRemote
		}
	default:
		return foundry.Response{}, errBrokerRemote
	}
	return response, nil
}

func (b *lifecycleBroker) readTrackedStream(reader io.Reader, c brokerContext, remoteID string) ([]byte, error) {
	limited := &io.LimitedReader{R: reader, N: foundry.DefaultMaxStreamBytes + 1}
	scanner := bufio.NewScanner(limited)
	scanner.Buffer(make([]byte, 32<<10), foundry.DefaultMaxEventBytes)
	var raw, event []byte
	flush := func() error {
		if len(event) == 0 || bytes.Equal(bytes.TrimSpace(event), []byte("[DONE]")) {
			return nil
		}
		var frame struct {
			Type     string                 `json:"type"`
			Response json.RawMessage        `json:"response"`
			Error    *foundry.ResponseError `json:"error"`
		}
		if strictjson.DecodeStruct(event, &frame, false) != nil {
			return errBrokerRemote
		}
		if frame.Response == nil {
			return nil
		}
		response, err := brokerDecodeResponseEvidence(frame.Response)
		if err != nil || frame.Error != nil {
			return errBrokerRemote
		}
		switch frame.Type {
		case "response.created":
			if response.Status != "queued" && response.Status != "in_progress" {
				return errBrokerRemote
			}
		case "response.queued", "response.in_progress", "response.completed", "response.failed", "response.incomplete", "response.cancelled":
			if frame.Type != "response."+response.Status {
				return errBrokerRemote
			}
		default:
			return errBrokerRemote
		}
		return b.recordResponseIdentity(c, response, remoteID)
	}
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(raw)+len(line)+1 > foundry.DefaultMaxStreamBytes {
			return nil, errBrokerRemote
		}
		raw = append(raw, line...)
		raw = append(raw, '\n')
		if len(line) == 0 {
			if err := flush(); err != nil {
				return nil, err
			}
			event = nil
		} else if part, ok := bytes.CutPrefix(line, []byte("data:")); ok {
			part = bytes.TrimPrefix(part, []byte(" "))
			if len(event)+len(part)+1 > foundry.DefaultMaxEventBytes {
				return nil, errBrokerRemote
			}
			if len(event) > 0 {
				event = append(event, '\n')
			}
			event = append(event, part...)
		}
	}
	if scanner.Err() != nil || limited.N <= 0 || len(event) != 0 {
		return nil, errBrokerAmbiguous
	}
	return raw, nil
}

func (b *lifecycleBroker) recordResponseIdentity(c brokerContext, response foundry.Response, remoteID string) error {
	if response.ID == "" || foundry.ValidateIdentifier("response", response.ID) != nil ||
		(response.AgentSessionID != "" && response.AgentSessionID != remoteID) {
		return errBrokerConflict
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.storageError != nil {
		return b.storageError
	}
	key := foundry.JSONDigest(c.Owner)
	// Lifecycle events repeat the same identity. Only its first acceptance
	// changes durable ownership, but duplicates must still fail on storage loss.
	if previous := b.ledger.Sessions[key].Prompts[c.promptKey()].Invocations[c.InvocationSequence].ResponseID; previous != "" {
		if previous != response.ID {
			return errBrokerConflict
		}
		return nil
	}
	return b.commitLocked(func(next *brokerLedger) error {
		session := next.Sessions[key]
		invocation := session.Prompts[c.promptKey()].Invocations[c.InvocationSequence]
		for _, previous := range session.Responses {
			if previous.RemoteID == response.ID {
				return errBrokerConflict
			}
		}
		alias := "fr_" + uuid.NewString()
		invocation.ResponseID, invocation.ResponseAlias, invocation.State = response.ID, alias, "accepted"
		session.Responses[alias] = brokerResponseID{RemoteID: response.ID, PromptKey: c.promptKey()}
		return nil
	})
}

func (b *lifecycleBroker) commitCompletedResponse(c brokerContext, summary foundry.StreamSummary) (foundry.Response, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var output foundry.Response
	err := b.commitCapacityLocked(true, func(next *brokerLedger) error {
		session := next.Sessions[foundry.JSONDigest(c.Owner)]
		prompt := session.Prompts[c.promptKey()]
		invocation := prompt.Invocations[c.InvocationSequence]
		if invocation.ResponseID != summary.ResponseID || invocation.ResponseAlias == "" {
			return errBrokerConflict
		}
		if prompt.Closing || !prompt.LeaseExpiresAt.After(time.Now()) {
			return errBrokerClosed
		}
		output = foundry.Response{ID: invocation.ResponseAlias, Status: "completed", Output: []foundry.OutputItem{}}
		if summary.Text != "" {
			output.Output = append(output.Output, foundry.OutputItem{ID: "fi_" + uuid.NewString(), Type: "message",
				Content: []foundry.OutputContent{{Type: "output_text", Text: summary.Text}}})
		}
		link := session.Responses[invocation.ResponseAlias]
		link.CallIDs = map[string]string{}
		seen := map[string]bool{}
		for _, item := range summary.FunctionCalls {
			if !foundry.SafeString(item.CallID, foundry.MaxIdentifierBytes) || seen[item.CallID] {
				return errBrokerConflict
			}
			seen[item.CallID] = true
			alias := "fc_" + uuid.NewString()
			link.CallIDs[alias] = item.CallID
			item.CallID, item.ID = alias, "fi_"+uuid.NewString()
			output.Output = append(output.Output, item)
		}
		link.Completed, link.HasFunctions = true, len(link.CallIDs) != 0
		session.Responses[invocation.ResponseAlias] = link
		invocation.State, prompt.LastAlias = "completed", invocation.ResponseAlias
		return nil
	})
	return output, err
}

func (b *lifecycleBroker) finishInvocation(c brokerContext, invocationError error) {
	key := foundry.JSONDigest(c.Owner)
	b.mu.Lock()
	_ = b.commitLocked(func(next *brokerLedger) error {
		prompt := next.Sessions[key].Prompts[c.promptKey()]
		invocation := prompt.Invocations[c.InvocationSequence]
		if invocationError != nil {
			prompt.Closing = true
			if invocation.State == "reserved" {
				invocation.State = "rejected"
			}
			if invocation.State == "intent" {
				if errors.Is(invocationError, errBrokerAmbiguous) {
					invocation.State = "uncertain"
				} else {
					invocation.State = "rejected"
				}
			}
		}
		return nil
	})
	delete(b.active, key)
	b.mu.Unlock()
	if invocationError != nil {
		b.startReconcile(key, true)
	}
}

func (b *lifecycleBroker) closePrompt(key, promptKey string) {
	b.mu.Lock()
	if b.ctx.Err() == nil {
		_ = b.commitLocked(func(next *brokerLedger) error {
			if session := next.Sessions[key]; session != nil {
				if prompt := session.Prompts[promptKey]; prompt != nil {
					prompt.Closing = true
				}
			}
			return nil
		})
		if active, ok := b.active[key]; ok && active.prompt == promptKey {
			active.cancel()
		}
	}
	b.mu.Unlock()
	b.startReconcile(key, true)
}
