package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/orka-agents/agent-runtime-foundry/internal/harness"
)

const (
	maxTurnTombstones = 10_000
)

var foundryToolNameRE = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

type responsesBackend interface {
	CreateResponse(context.Context, foundryResponseRequest, responseCallbacks) (foundryStreamSummary, error)
	CreateSession(context.Context) (string, error)
	ValidateAgent(context.Context) error
}

type foundryBackend struct {
	client *foundryResponsesClient
}

func (b foundryBackend) CreateResponse(ctx context.Context, request foundryResponseRequest, callbacks responseCallbacks) (foundryStreamSummary, error) {
	return b.client.createResponse(ctx, request, callbacks)
}

func (b foundryBackend) CreateSession(ctx context.Context) (string, error) {
	return b.client.createSession(ctx)
}

func (b foundryBackend) ValidateAgent(ctx context.Context) error {
	return b.client.validateAgent(ctx)
}

type providerStartError struct {
	err error
}

func (e providerStartError) Error() string { return e.err.Error() }
func (e providerStartError) Unwrap() error { return e.err }

type adapterValidationError struct {
	err error
}

func (e adapterValidationError) Error() string { return e.err.Error() }
func (e adapterValidationError) Unwrap() error { return e.err }

type adapterLimitError struct {
	err error
}

func (e adapterLimitError) Error() string { return e.err.Error() }
func (e adapterLimitError) Unwrap() error { return e.err }

type runtimeSessionState struct {
	OwnerFingerprint string
	AgentSessionID   string
	PreviousResponse string
	LastSeen         time.Time
}

type turnTombstone struct {
	fingerprint string
	expiresAt   time.Time
}

type turnState struct {
	request                    harness.StartTurnRequest
	requestFingerprint         string
	ownerFingerprint           string
	frames                     []harness.HarnessEventFrame
	retainedBytes              int64
	responseID                 string
	agentSessionID             string
	previousResponse           string
	text                       strings.Builder
	pendingTools               map[string]string
	pendingCallDigests         map[string]string
	providerCallIDs            map[string]string
	bufferedResults            map[string]harness.ToolCallResult
	bufferedResultDigests      map[string]string
	submittedResultDigests     map[string]string
	emittedResults             map[string]struct{}
	brokeredStateBytes         int64
	brokeredCallCount          int
	exposedBrokeredWrite       bool
	completed                  bool
	providerInFlight           int
	currentInvocationUncertain bool
	pendingOutput              strings.Builder
	outputFlushTimer           *time.Timer
	cleanupAt                  time.Time
	waitingForTools            bool
	continuationInFlight       bool
	started                    bool
	initializing               bool
	initDone                   chan struct{}
	initErr                    error
	ctx                        context.Context
	cancel                     context.CancelFunc
	continueMu                 sync.Mutex
	done                       chan struct{}
	cleanupTimer               *time.Timer
}

type adapter struct {
	cfg             config
	backend         responsesBackend
	mu              sync.Mutex
	turns           map[harness.HarnessTurnID]*turnState
	runtimeSessions map[harness.RuntimeSessionID]*runtimeSessionState
	turnTombstones  map[harness.HarnessTurnID]turnTombstone
	notify          *sync.Cond
	now             func() time.Time
}

func newAdapter(cfg config, backend responsesBackend) *adapter {
	a := &adapter{
		cfg:             cfg,
		backend:         backend,
		turns:           map[harness.HarnessTurnID]*turnState{},
		runtimeSessions: map[harness.RuntimeSessionID]*runtimeSessionState{},
		turnTombstones:  map[harness.HarnessTurnID]turnTombstone{},
		now:             time.Now,
	}
	a.notify = sync.NewCond(&a.mu)
	return a
}

func (a *adapter) startTurn(request harness.StartTurnRequest) (*turnState, string, error) {
	request = prepareFoundryStartRequest(request)
	if err := validateAdapterStartRequest(request); err != nil {
		return nil, "", err
	}
	requestFingerprint := startTurnFingerprint(request)
	ownerFingerprint := runtimeSessionOwnerFingerprint(request)
	if request.ToolExecutionMode == harness.ToolExecutionModeBrokered {
		if len(a.cfg.brokeredToolClasses) == 0 {
			return nil, "", errors.New("foundry brokered tool mode is not enabled")
		}
		if err := validateFoundryToolDefinitions(request.Input.Tools); err != nil {
			return nil, "", err
		}
		if err := a.validateConfiguredToolDefinitions(request.Input.Tools); err != nil {
			return nil, "", err
		}
	}
	eventsPath, err := harness.EventStreamPath(request.TurnID)
	if err != nil {
		return nil, "", err
	}
	now := a.now().UTC()
	a.mu.Lock()
	if tombstone, ok := a.turnTombstones[request.TurnID]; ok {
		if tombstone.expiresAt.After(now) {
			a.mu.Unlock()
			if tombstone.fingerprint == requestFingerprint {
				return nil, "", errTurnCompleted
			}
			return nil, "", errTurnIDConsumed
		}
		delete(a.turnTombstones, request.TurnID)
	}
	a.pruneLocked(now)
	if existing := a.turns[request.TurnID]; existing != nil {
		if existing.requestFingerprint != requestFingerprint {
			a.mu.Unlock()
			return nil, "", errTurnAlreadyExists
		}
		initDone := existing.initDone
		a.mu.Unlock()
		<-initDone
		if existing.initErr != nil {
			return nil, "", existing.initErr
		}
		return existing, eventsPath, nil
	}
	activeTurns := 0
	for _, active := range a.turns {
		if active.completed && !active.initializing && active.providerInFlight == 0 {
			continue
		}
		activeTurns++
		if active.request.RuntimeSessionID == request.RuntimeSessionID {
			a.mu.Unlock()
			return nil, "", errRuntimeSessionBusy
		}
	}
	if a.cfg.maxConcurrent > 0 && activeTurns >= a.cfg.maxConcurrent {
		a.mu.Unlock()
		return nil, "", errAdapterAtCapacity
	}
	deadline := foundryTurnDeadline(now, request.Deadline, a.cfg.turnTimeout)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	if strings.EqualFold(a.cfg.isolationMode, "header") {
		ctx = withFoundryIsolationKey(ctx, isolationKeyForRuntimeSession(string(request.RuntimeSessionID)))
	}
	compactRequest := compactStartTurnRequest(request)
	compactRequestBytes, compactErr := json.Marshal(compactRequest)
	if compactErr != nil {
		cancel()
		a.mu.Unlock()
		return nil, "", compactErr
	}
	turn := &turnState{
		request:                compactRequest,
		requestFingerprint:     requestFingerprint,
		ownerFingerprint:       ownerFingerprint,
		retainedBytes:          int64(len(compactRequestBytes)),
		pendingTools:           map[string]string{},
		pendingCallDigests:     map[string]string{},
		providerCallIDs:        map[string]string{},
		bufferedResults:        map[string]harness.ToolCallResult{},
		bufferedResultDigests:  map[string]string{},
		submittedResultDigests: map[string]string{},
		emittedResults:         map[string]struct{}{},
		initializing:           true,
		initDone:               make(chan struct{}),
		ctx:                    ctx,
		cancel:                 cancel,
		done:                   make(chan struct{}),
	}
	if session := a.runtimeSessions[request.RuntimeSessionID]; session != nil {
		if session.OwnerFingerprint != ownerFingerprint {
			cancel()
			a.mu.Unlock()
			return nil, "", errRuntimeSessionOwnerMismatch
		}
		turn.agentSessionID = session.AgentSessionID
		turn.previousResponse = session.PreviousResponse
	}
	a.turns[request.TurnID] = turn
	a.mu.Unlock()

	if a.cfg.agentVersion != "" && turn.agentSessionID == "" {
		sessionID, createErr := a.backend.CreateSession(ctx)
		if createErr != nil {
			wrapped := providerStartError{err: createErr}
			if a.failInitialization(turn, wrapped) {
				return turn, eventsPath, nil
			}
			return nil, "", wrapped
		}
		a.mu.Lock()
		if turn.completed {
			turn.initializing = false
			close(turn.initDone)
			a.mu.Unlock()
			return turn, eventsPath, nil
		}
		turn.agentSessionID = sessionID
		a.mu.Unlock()
	}
	a.mu.Lock()
	if turn.completed {
		turn.initializing = false
		close(turn.initDone)
		a.mu.Unlock()
		return turn, eventsPath, nil
	}
	turn.initializing = false
	close(turn.initDone)
	a.mu.Unlock()
	go a.watchDeadline(turn)
	go a.runResponse(turn, foundryResponseRequest{
		Input:              request.Input.Prompt,
		PreviousResponseID: turn.previousResponse,
		AgentSessionID:     turn.agentSessionID,
		Tools:              a.providerToolSchemas(request),
	})
	return turn, eventsPath, nil
}

func (a *adapter) failInitialization(turn *turnState, err error) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	turn.initializing = false
	if turn.completed {
		turn.initErr = nil
		close(turn.initDone)
		return true
	}
	turn.initErr = err
	delete(a.turns, turn.request.TurnID)
	close(turn.initDone)
	turn.cancel()
	return false
}

func (a *adapter) watchDeadline(turn *turnState) {
	<-turn.ctx.Done()
	if !errors.Is(context.Cause(turn.ctx), context.DeadlineExceeded) {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if !turn.completed {
		a.finishFailedLocked(turn, "foundry_timeout", "Foundry response timed out", true)
	}
}

func (a *adapter) runResponse(turn *turnState, request foundryResponseRequest) {
	var invocationMu sync.Mutex
	invocationResponseID := ""
	invocationSessionID := request.AgentSessionID
	recordInvocationIDs := func(responseID, sessionID string) error {
		if err := validateProviderIdentifier("response id", responseID); err != nil {
			return err
		}
		if err := validateProviderIdentifier("agent session id", sessionID); err != nil {
			return err
		}
		invocationMu.Lock()
		defer invocationMu.Unlock()
		if responseID != "" {
			if invocationResponseID != "" && invocationResponseID != responseID {
				return errors.New("foundry response id changed within one invocation")
			}
			invocationResponseID = responseID
		}
		if sessionID != "" {
			if invocationSessionID != "" && invocationSessionID != sessionID {
				return errors.New("foundry agent session id changed within one invocation")
			}
			invocationSessionID = sessionID
		}
		return nil
	}
	a.mu.Lock()
	if turn.completed {
		a.mu.Unlock()
		return
	}
	turn.providerInFlight++
	turn.currentInvocationUncertain = true
	a.mu.Unlock()
	providerReleased := false
	releaseProviderLocked := func() {
		if providerReleased {
			return
		}
		providerReleased = true
		if turn.providerInFlight > 0 {
			turn.providerInFlight--
		}
	}
	defer func() {
		a.mu.Lock()
		releaseProviderLocked()
		a.maybeDeleteExpiredTurnLocked(turn)
		a.mu.Unlock()
	}()
	summary, err := a.backend.CreateResponse(turn.ctx, request, responseCallbacks{
		OnCreated: func(response foundryResponse) error {
			if err := recordInvocationIDs(response.ID, response.AgentSessionID); err != nil {
				return err
			}
			a.mu.Lock()
			defer a.mu.Unlock()
			if turn.completed {
				return nil
			}
			if response.AgentSessionID != "" {
				turn.agentSessionID = response.AgentSessionID
			}
			return a.ensureTurnStartedLocked(turn)
		},
		OnTextDelta: func(delta string) error {
			a.mu.Lock()
			defer a.mu.Unlock()
			if turn.completed {
				return nil
			}
			if err := a.ensureTurnStartedLocked(turn); err != nil {
				return err
			}
			if int64(turn.text.Len()+len(delta)) > a.cfg.maxOutputBytes {
				return errors.New("foundry output exceeded adapter limit")
			}
			turn.text.WriteString(delta)
			turn.pendingOutput.WriteString(delta)
			if turn.pendingOutput.Len() >= 4<<10 {
				return a.flushOutputLocked(turn)
			}
			a.scheduleOutputFlushLocked(turn)
			return nil
		},
		OnFunctionCall: func(call foundryOutputItem) error {
			return a.recordFunctionCall(turn, call)
		},
	})
	if err == nil {
		err = validateFoundryStreamSummary(summary)
	}
	if err == nil {
		err = recordInvocationIDs(summary.ResponseID, summary.AgentSessionID)
	}
	invocationMu.Lock()
	finalResponseID := invocationResponseID
	finalSessionID := invocationSessionID
	invocationMu.Unlock()
	terminalStatus := strings.ToLower(strings.TrimSpace(summary.Status))
	if err == nil && terminalStatus == "completed" {
		switch {
		case finalResponseID == "":
			err = errors.New("foundry completed response omitted its response id")
		case finalSessionID == "":
			err = errors.New("foundry completed response omitted its agent session id")
		case request.PreviousResponseID != "" && finalResponseID == request.PreviousResponseID:
			err = errors.New("foundry completed response did not advance previous_response_id")
		}
	}
	if err != nil {
		a.mu.Lock()
		releaseProviderLocked()
		turn.continuationInFlight = false
		if !turn.completed {
			if providerSessionCheckpointInvalid(err, request) {
				delete(a.runtimeSessions, turn.request.RuntimeSessionID)
				turn.agentSessionID = ""
				turn.previousResponse = ""
			}
			switch {
			case errors.Is(context.Cause(turn.ctx), context.DeadlineExceeded):
				a.finishFailedLocked(turn, "foundry_timeout", "Foundry response timed out", true)
			case errors.Is(err, context.DeadlineExceeded):
				a.finishFailedLocked(turn, "foundry_timeout", "Foundry response timed out", true)
			case errors.Is(err, context.Canceled):
				a.finishCancelledLocked(turn)
			default:
				a.finishFailedLocked(
					turn,
					"foundry_response_failed",
					providerSafeMessage(err),
					retryableProviderError(err),
				)
			}
		}
		a.mu.Unlock()
		return
	}

	a.mu.Lock()
	releaseProviderLocked()
	turn.continuationInFlight = false
	if turn.completed {
		a.mu.Unlock()
		return
	}
	if errors.Is(context.Cause(turn.ctx), context.DeadlineExceeded) {
		turn.currentInvocationUncertain = false
		a.finishFailedLocked(turn, "foundry_timeout", "Foundry response timed out", true)
		a.mu.Unlock()
		return
	}
	turn.currentInvocationUncertain = false
	if finalResponseID != "" {
		turn.responseID = finalResponseID
	}
	if finalSessionID != "" {
		turn.agentSessionID = finalSessionID
	}
	if err := a.ensureTurnStartedLocked(turn); err != nil {
		a.finishFailedLocked(turn, "foundry_event_limit", err.Error(), false)
		a.mu.Unlock()
		return
	}
	if err := a.flushOutputLocked(turn); err != nil {
		turn.pendingOutput.Reset()
		a.finishFailedLocked(turn, "foundry_event_limit", err.Error(), false)
		a.mu.Unlock()
		return
	}
	switch terminalStatus {
	case "completed":
		if len(turn.pendingTools) > 0 {
			turn.waitingForTools = true
			a.recordRuntimeSessionLocked(turn, false)
			a.notify.Broadcast()
			if len(turn.bufferedResults) == len(turn.pendingTools) {
				continuation, dispatchErr := a.prepareContinuationLocked(turn)
				if dispatchErr != nil {
					a.finishFailedLocked(turn, "foundry_continuation_failed", dispatchErr.Error(), false)
					a.mu.Unlock()
					return
				}
				a.mu.Unlock()
				go a.runResponse(turn, continuation)
				return
			}
			a.mu.Unlock()
			return
		}
		a.finishCompletedLocked(turn)
	case "failed":
		a.finishFailedLocked(
			turn,
			"foundry_response_failed",
			providerFailureMessage(summary.Error),
			retryableResponseFailure(summary.Error),
		)
	case "incomplete":
		a.finishFailedLocked(turn, "foundry_response_incomplete", incompleteFailureMessage(summary.Incomplete), false)
	case "cancelled", "canceled":
		a.finishCancelledLocked(turn)
	default:
		a.finishFailedLocked(turn, "foundry_response_invalid_terminal", "Foundry response ended without a supported terminal status", false)
	}
	a.mu.Unlock()
}

func (a *adapter) recordFunctionCall(turn *turnState, call foundryOutputItem) error {
	providerCallID := call.CallID
	name := call.Name
	if err := validateFoundryCallID(providerCallID); err != nil {
		return err
	}
	if err := validateFoundryFunctionName(name); err != nil {
		return err
	}
	callID := orkaToolCallID(turn.request.RuntimeSessionID, turn.request.TurnID, providerCallID)
	arguments, err := normalizeFoundryToolArguments(call.Arguments)
	if err != nil {
		return err
	}
	callDigest := foundryFunctionCallFingerprint(name, arguments)
	a.mu.Lock()
	defer a.mu.Unlock()
	if turn.completed {
		return nil
	}
	if turn.request.ToolExecutionMode != harness.ToolExecutionModeBrokered || !foundryToolAllowed(turn.request.Input.Tools, name) {
		return fmt.Errorf("foundry requested tool %q that was not supplied by Orka", name)
	}
	if _, submitted := turn.submittedResultDigests[callID]; submitted {
		return fmt.Errorf("foundry reused already submitted function call id %q", callID)
	}
	if existing, ok := turn.pendingTools[callID]; ok {
		if existing != name || turn.pendingCallDigests[callID] != callDigest ||
			turn.providerCallIDs[callID] != providerCallID {
			return fmt.Errorf("foundry reused function call id %q with conflicting content", providerCallID)
		}
		return nil
	}
	if err := a.ensureTurnStartedLocked(turn); err != nil {
		return err
	}
	if err := a.flushOutputLocked(turn); err != nil {
		return err
	}
	if err := a.ensureNonterminalFrameCapacityLocked(turn, 1); err != nil {
		return err
	}
	if foundryToolClass(turn.request.Input.Tools, name) == harness.BrokeredToolClassWrite {
		turn.exposedBrokeredWrite = true
	}
	if a.cfg.maxBrokeredCalls > 0 && turn.brokeredCallCount >= a.cfg.maxBrokeredCalls {
		return errors.New("foundry brokered tool call count exceeds adapter limit")
	}
	callStateBytes := int64(len(callID) + len(providerCallID) + len(name) + len(arguments))
	if a.cfg.maxBrokeredTurnBytes > 0 && turn.brokeredStateBytes+callStateBytes > a.cfg.maxBrokeredTurnBytes {
		return errors.New("foundry brokered tool state exceeds adapter limit")
	}
	turn.brokeredCallCount++
	turn.brokeredStateBytes += callStateBytes
	turn.pendingTools[callID] = name
	turn.pendingCallDigests[callID] = callDigest
	turn.providerCallIDs[callID] = providerCallID
	return a.appendFrameLocked(turn, harness.FrameToolCallRequested, "Foundry requested brokered tool", func(frame *harness.HarnessEventFrame) {
		frame.ToolName = name
		frame.ToolCallID = callID
		frame.Content = arguments
		frame.Metadata = foundryFrameMetadata(turn)
	})
}

func (a *adapter) continueTurn(request harness.ContinueTurnRequest) error {
	a.mu.Lock()
	turn := a.turns[request.TurnID]
	a.mu.Unlock()
	if turn == nil {
		return errTurnNotFound
	}
	turn.continueMu.Lock()
	defer turn.continueMu.Unlock()
	if err := request.Validate(); err != nil {
		return adapterValidationError{err: err}
	}
	if request.RuntimeSessionID != turn.request.RuntimeSessionID || request.TurnID != turn.request.TurnID || request.CorrelationID != turn.request.CorrelationID ||
		request.Namespace != turn.request.Namespace || request.TaskName != turn.request.TaskName || request.SessionName != turn.request.SessionName {
		return adapterValidationError{err: errors.New("continue request does not match started turn")}
	}
	if err := validateBrokeredResultFrames(request.ToolResults, a.cfg.maxBrokeredBytes); err != nil {
		return err
	}
	a.mu.Lock()
	type stagedToolResult struct {
		callID string
		digest string
		size   int64
		result harness.ToolCallResult
	}
	staged := make([]stagedToolResult, 0, len(request.ToolResults))
	var additionalBytes int64
	for _, result := range request.ToolResults {
		callID := strings.TrimSpace(result.ToolCallID)
		digest, size, digestErr := toolResultFingerprint(result)
		if digestErr != nil {
			a.mu.Unlock()
			return digestErr
		}
		if submittedDigest, ok := turn.submittedResultDigests[callID]; ok {
			if submittedDigest != digest {
				a.mu.Unlock()
				return fmt.Errorf("tool result %q conflicts with an already submitted result", callID)
			}
			continue
		}
		if turn.completed {
			a.mu.Unlock()
			return fmt.Errorf("tool result %q was not submitted before the turn became terminal", callID)
		}
		if _, pending := turn.pendingTools[callID]; !pending {
			a.mu.Unlock()
			return fmt.Errorf("tool result %q is not pending for this turn", callID)
		}
		if bufferedDigest, exists := turn.bufferedResultDigests[callID]; exists {
			if bufferedDigest != digest {
				a.mu.Unlock()
				return fmt.Errorf("tool result %q conflicts with a buffered result", callID)
			}
			continue
		}
		additionalBytes += size
		staged = append(staged, stagedToolResult{callID: callID, digest: digest, size: size, result: result})
	}
	if a.cfg.maxBrokeredTurnBytes > 0 && turn.brokeredStateBytes+additionalBytes > a.cfg.maxBrokeredTurnBytes {
		a.mu.Unlock()
		return adapterLimitError{err: errors.New("brokered tool results cause turn state to exceed adapter limit")}
	}
	for _, item := range staged {
		turn.brokeredStateBytes += item.size
		turn.bufferedResults[item.callID] = item.result
		turn.bufferedResultDigests[item.callID] = item.digest
	}
	newResults := len(staged)
	if turn.completed {
		a.mu.Unlock()
		return nil
	}
	if newResults == 0 {
		a.mu.Unlock()
		return nil
	}
	if len(turn.pendingTools) == 0 || len(turn.bufferedResults) < len(turn.pendingTools) || !turn.waitingForTools {
		a.mu.Unlock()
		return nil
	}
	continuation, err := a.prepareContinuationLocked(turn)
	if err != nil {
		a.finishFailedLocked(turn, "foundry_continuation_failed", providerSafeMessage(err), false)
		a.mu.Unlock()
		return err
	}
	a.mu.Unlock()
	go a.runResponse(turn, continuation)
	return nil
}

func (a *adapter) prepareContinuationLocked(turn *turnState) (foundryResponseRequest, error) {
	if turn.continuationInFlight {
		return foundryResponseRequest{}, errors.New("foundry continuation is already in flight")
	}
	callIDs := make([]string, 0, len(turn.pendingTools))
	for callID := range turn.pendingTools {
		callIDs = append(callIDs, callID)
	}
	slices.Sort(callIDs)
	if err := a.ensureNonterminalFrameCapacityLocked(turn, len(callIDs)); err != nil {
		return foundryResponseRequest{}, err
	}
	outputs := make([]foundryFunctionOutput, 0, len(callIDs))
	for _, callID := range callIDs {
		result := turn.bufferedResults[callID]
		output := string(result.Output)
		if result.Error != nil {
			encoded, _ := json.Marshal(result.Error)
			output = string(encoded)
		}
		providerCallID := turn.providerCallIDs[callID]
		outputs = append(outputs, foundryFunctionOutput{
			Type:   "function_call_output",
			CallID: providerCallID,
			Output: output,
		})
		if _, emitted := turn.emittedResults[callID]; !emitted {
			name := turn.pendingTools[callID]
			if err := a.appendFrameLocked(turn, harness.FrameToolResultReceived, "brokered tool result received", func(frame *harness.HarnessEventFrame) {
				frame.ToolName = name
				frame.ToolCallID = callID
				frame.Content = result.Output
				frame.Error = result.Error
				frame.Metadata = foundryFrameMetadata(turn)
			}); err != nil {
				return foundryResponseRequest{}, err
			}
			turn.emittedResults[callID] = struct{}{}
		}
	}
	previousResponseID := firstNonBlank(turn.responseID, turn.previousResponse)
	if previousResponseID == "" {
		return foundryResponseRequest{}, errors.New("foundry continuation is missing previous_response_id")
	}
	for _, callID := range callIDs {
		turn.submittedResultDigests[callID] = turn.bufferedResultDigests[callID]
		delete(turn.pendingTools, callID)
		delete(turn.pendingCallDigests, callID)
		delete(turn.providerCallIDs, callID)
		delete(turn.bufferedResults, callID)
		delete(turn.bufferedResultDigests, callID)
	}
	turn.waitingForTools = false
	turn.continuationInFlight = true
	return foundryResponseRequest{
		Input:              outputs,
		PreviousResponseID: previousResponseID,
		AgentSessionID:     turn.agentSessionID,
		Tools:              a.providerToolSchemas(turn.request),
	}, nil
}

func (a *adapter) cancelTurn(request harness.CancelTurnRequest) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	turn := a.turns[request.TurnID]
	if turn == nil {
		return errTurnNotFound
	}
	if !cancelRequestMatchesTurn(request, turn.request) {
		return errors.New("cancel request does not match started turn")
	}
	if !turn.completed {
		a.finishCancelledLocked(turn)
	}
	return nil
}

func (a *adapter) ensureTerminalTurnStartedLocked(turn *turnState) {
	if turn.started {
		return
	}
	if err := a.appendFrameLocked(turn, harness.FrameTurnStarted, "Foundry response started", func(frame *harness.HarnessEventFrame) {
		frame.Metadata = foundryFrameMetadata(turn)
	}); err == nil {
		turn.started = true
	}
}

func (a *adapter) finishCompletedLocked(turn *turnState) {
	a.ensureTerminalTurnStartedLocked(turn)
	a.flushOutputBestEffortLocked(turn)
	result := turn.text.String()
	a.recordRuntimeSessionLocked(turn, true)
	if err := a.appendFrameLocked(turn, harness.FrameTurnCompleted, "Foundry response completed", func(frame *harness.HarnessEventFrame) {
		frame.Completed = &harness.TurnCompleted{
			Result:        result,
			RetainSession: turn.agentSessionID != "" || turn.responseID != "",
		}
		frame.Metadata = foundryFrameMetadata(turn)
	}); err != nil {
		a.appendMinimalFailureLocked(turn, "terminal_frame_too_large", "terminal response exceeded harness frame limit", false)
	}
	a.finishTerminalLocked(turn)
}

func (a *adapter) finishFailedLocked(turn *turnState, reason, message string, retryable bool) {
	a.ensureTerminalTurnStartedLocked(turn)
	a.flushOutputBestEffortLocked(turn)
	if turn.currentInvocationUncertain || turn.exposedBrokeredWrite ||
		turn.request.ToolExecutionMode == harness.ToolExecutionModeObserved {
		retryable = false
	}
	if err := a.appendFrameLocked(turn, harness.FrameTurnFailed, "Foundry response failed", func(frame *harness.HarnessEventFrame) {
		frame.Failed = &harness.TurnFailed{Reason: reason, Message: message, Retryable: retryable}
		frame.Metadata = foundryFrameMetadata(turn)
	}); err != nil {
		a.appendMinimalFailureLocked(turn, "terminal_frame_too_large", "terminal failure exceeded harness frame limit", false)
	}
	a.finishTerminalLocked(turn)
}

func (a *adapter) finishCancelledLocked(turn *turnState) {
	a.ensureTerminalTurnStartedLocked(turn)
	a.flushOutputBestEffortLocked(turn)
	if err := a.appendFrameLocked(turn, harness.FrameTurnCancelled, "turn cancelled", func(frame *harness.HarnessEventFrame) {
		frame.Metadata = foundryFrameMetadata(turn)
	}); err != nil {
		a.appendMinimalFailureLocked(turn, "terminal_frame_too_large", "cancellation frame exceeded harness frame limit", false)
	}
	a.finishTerminalLocked(turn)
}

func (a *adapter) finishTerminalLocked(turn *turnState) {
	if turn.completed {
		return
	}
	turn.completed = true
	turn.cleanupAt = a.now().UTC().Add(defaultStateRetention)
	turn.cancel()
	close(turn.done)
	a.turnTombstones[turn.request.TurnID] = turnTombstone{
		fingerprint: turn.requestFingerprint,
		expiresAt:   a.now().UTC().Add(defaultTombstoneRetention),
	}
	if turn.outputFlushTimer != nil {
		turn.outputFlushTimer.Stop()
		turn.outputFlushTimer = nil
	}
	turn.pendingOutput.Reset()
	turn.text.Reset()
	turn.pendingTools = nil
	turn.pendingCallDigests = nil
	turn.providerCallIDs = nil
	turn.bufferedResults = nil
	turn.bufferedResultDigests = nil
	turn.emittedResults = nil
	oldRequestBytes, _ := json.Marshal(turn.request)
	turn.request.Input.Tools = nil
	turn.request.ToolExecutionMode = ""
	newRequestBytes, _ := json.Marshal(turn.request)
	turn.retainedBytes -= int64(len(oldRequestBytes) - len(newRequestBytes))
	if turn.retainedBytes < 0 {
		turn.retainedBytes = 0
	}
	a.scheduleTurnCleanupLocked(turn)
	a.pruneRetainedTurnsLocked(turn.request.TurnID)
	a.notify.Broadcast()
}

func (a *adapter) recordRuntimeSessionLocked(turn *turnState, commitResponse bool) {
	if commitResponse && turn.responseID != "" {
		turn.previousResponse = turn.responseID
	}
	if turn.agentSessionID == "" && turn.previousResponse == "" {
		return
	}
	now := a.now().UTC()
	a.runtimeSessions[turn.request.RuntimeSessionID] = &runtimeSessionState{
		OwnerFingerprint: turn.ownerFingerprint,
		AgentSessionID:   turn.agentSessionID,
		PreviousResponse: turn.previousResponse,
		LastSeen:         now,
	}
	a.pruneRuntimeSessionsLocked(now)
}

func (a *adapter) scheduleOutputFlushLocked(turn *turnState) {
	if turn.outputFlushTimer != nil {
		return
	}
	turn.outputFlushTimer = time.AfterFunc(100*time.Millisecond, func() {
		a.mu.Lock()
		defer a.mu.Unlock()
		turn.outputFlushTimer = nil
		if turn.completed {
			return
		}
		if err := a.flushOutputLocked(turn); err != nil {
			turn.pendingOutput.Reset()
			a.finishFailedLocked(turn, "foundry_event_limit", err.Error(), false)
		}
	})
}

func (a *adapter) flushOutputLocked(turn *turnState) error {
	if turn.outputFlushTimer != nil {
		turn.outputFlushTimer.Stop()
		turn.outputFlushTimer = nil
	}
	if turn.pendingOutput.Len() == 0 {
		return nil
	}
	if err := a.ensureNonterminalFrameCapacityLocked(turn, 1); err != nil {
		return err
	}
	chunk := turn.pendingOutput.String()
	turn.pendingOutput.Reset()
	return a.appendFrameLocked(turn, harness.FrameRuntimeOutput, "Foundry output", func(frame *harness.HarnessEventFrame) {
		frame.ContentText = chunk
		frame.Metadata = foundryFrameMetadata(turn)
	})
}

func (a *adapter) flushOutputBestEffortLocked(turn *turnState) {
	if err := a.flushOutputLocked(turn); err != nil {
		turn.pendingOutput.Reset()
	}
}

func (a *adapter) ensureTurnStartedLocked(turn *turnState) error {
	if turn.started {
		return nil
	}
	if err := a.ensureNonterminalFrameCapacityLocked(turn, 1); err != nil {
		return err
	}
	if err := a.appendFrameLocked(turn, harness.FrameTurnStarted, "Foundry response started", func(frame *harness.HarnessEventFrame) {
		frame.Metadata = foundryFrameMetadata(turn)
	}); err != nil {
		return err
	}
	turn.started = true
	return nil
}

func (a *adapter) ensureNonterminalFrameCapacityLocked(turn *turnState, additional int) error {
	if additional < 0 {
		return errors.New("invalid additional frame count")
	}
	if a.cfg.maxEvents > 0 && len(turn.frames)+additional >= a.cfg.maxEvents {
		return errors.New("foundry event count exceeds adapter limit")
	}
	return nil
}

func (a *adapter) appendFrameLocked(
	turn *turnState,
	typ harness.FrameType,
	summary string,
	mutate func(*harness.HarnessEventFrame),
) error {
	frame := harness.HarnessEventFrame{
		Version:          harness.ProtocolVersion,
		Type:             typ,
		RuntimeSessionID: turn.request.RuntimeSessionID,
		TurnID:           turn.request.TurnID,
		CorrelationID:    turn.request.CorrelationID,
		Seq:              int64(len(turn.frames) + 1),
		CreatedAt:        a.now().UTC(),
		Summary:          summary,
		Metadata:         map[string]string{"backend": backendMetadata},
	}
	if mutate != nil {
		mutate(&frame)
	}
	encoded, err := json.Marshal(frame)
	if err != nil {
		return err
	}
	if len(encoded)+len("data: \n\n") > maxHarnessFrameBytes {
		return adapterLimitError{err: errors.New("harness frame exceeds adapter serialization limit")}
	}
	turn.frames = append(turn.frames, frame)
	turn.retainedBytes += int64(len(encoded))
	a.notify.Broadcast()
	return nil
}

func (a *adapter) appendMinimalFailureLocked(
	turn *turnState,
	reason string,
	message string,
	retryable bool,
) {
	_ = a.appendFrameLocked(turn, harness.FrameTurnFailed, "Foundry response failed", func(frame *harness.HarnessEventFrame) {
		frame.Failed = &harness.TurnFailed{Reason: reason, Message: message, Retryable: retryable}
		frame.Metadata = foundryFrameMetadata(turn)
	})
}

func (a *adapter) turnStreamIdle(turn *turnState) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return turn.completed || turn.waitingForTools
}

func (a *adapter) framesAfter(turn *turnState, afterSeq int64) ([]harness.HarnessEventFrame, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	start := 0
	if afterSeq > 0 {
		start = int(min(afterSeq, int64(len(turn.frames))))
	}
	frames := slices.Clone(turn.frames[start:])
	return frames, turn.completed || turn.waitingForTools
}

func (a *adapter) waitForFrames(ctx context.Context, turn *turnState, afterSeq int64) ([]harness.HarnessEventFrame, bool) {
	for {
		frames, idle := a.framesAfter(turn, afterSeq)
		if len(frames) > 0 || idle {
			return frames, idle
		}
		select {
		case <-ctx.Done():
			return nil, false
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (a *adapter) pruneRetainedTurnsLocked(preferKeep harness.HarnessTurnID) {
	for {
		count := 0
		var totalBytes int64
		var oldestID harness.HarnessTurnID
		var oldestAt time.Time
		for turnID, turn := range a.turns {
			if !turn.completed || turn.providerInFlight > 0 {
				continue
			}
			count++
			totalBytes += turn.retainedBytes
			if turnID != preferKeep && (oldestID == "" || turn.cleanupAt.Before(oldestAt)) {
				oldestID = turnID
				oldestAt = turn.cleanupAt
			}
		}
		if count <= maxRetainedTurns && totalBytes <= maxRetainedTurnStateBytes {
			return
		}
		if oldestID == "" {
			return
		}
		a.deleteTurnLocked(oldestID)
	}
}

func (a *adapter) maybeDeleteExpiredTurnLocked(turn *turnState) {
	if turn == nil || !turn.completed || turn.providerInFlight > 0 ||
		turn.cleanupAt.After(a.now().UTC()) {
		return
	}
	a.deleteTurnLocked(turn.request.TurnID)
}

func (a *adapter) scheduleTurnCleanupLocked(turn *turnState) {
	delay := max(turn.cleanupAt.Sub(a.now().UTC()), time.Duration(0))
	turnID := turn.request.TurnID
	turn.cleanupTimer = time.AfterFunc(delay, func() {
		a.mu.Lock()
		defer a.mu.Unlock()
		current := a.turns[turnID]
		if current != nil {
			current.cleanupTimer = nil
			a.maybeDeleteExpiredTurnLocked(current)
		}
	})
}

func (a *adapter) deleteTurnLocked(turnID harness.HarnessTurnID) {
	turn := a.turns[turnID]
	if turn != nil && turn.cleanupTimer != nil {
		turn.cleanupTimer.Stop()
		turn.cleanupTimer = nil
	}
	delete(a.turns, turnID)
}

func (a *adapter) getTurn(turnID harness.HarnessTurnID) *turnState {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.pruneLocked(a.now().UTC())
	return a.turns[turnID]
}

func (a *adapter) pruneRuntimeSessionsLocked(now time.Time) {
	for runtimeSessionID, session := range a.runtimeSessions {
		if !session.LastSeen.Add(defaultRuntimeSessionRetention).After(now) {
			delete(a.runtimeSessions, runtimeSessionID)
		}
	}
	for len(a.runtimeSessions) > maxRuntimeSessions {
		var oldestID harness.RuntimeSessionID
		var oldestSeen time.Time
		for runtimeSessionID, session := range a.runtimeSessions {
			if oldestID == "" || session.LastSeen.Before(oldestSeen) {
				oldestID = runtimeSessionID
				oldestSeen = session.LastSeen
			}
		}
		delete(a.runtimeSessions, oldestID)
	}
}

func (a *adapter) pruneLocked(now time.Time) {
	a.pruneRuntimeSessionsLocked(now)
	a.pruneRetainedTurnsLocked("")
	for _, turn := range a.turns {
		if turn.completed && !turn.cleanupAt.After(now) {
			a.maybeDeleteExpiredTurnLocked(turn)
		}
	}
	for turnID, tombstone := range a.turnTombstones {
		if !tombstone.expiresAt.After(now) {
			delete(a.turnTombstones, turnID)
		}
	}
	for len(a.turnTombstones) > maxTurnTombstones {
		var oldestID harness.HarnessTurnID
		var oldestExpiry time.Time
		for turnID, tombstone := range a.turnTombstones {
			if oldestID == "" || tombstone.expiresAt.Before(oldestExpiry) {
				oldestID = turnID
				oldestExpiry = tombstone.expiresAt
			}
		}
		delete(a.turnTombstones, oldestID)
	}
}

func foundryFrameMetadata(_ *turnState) map[string]string {
	return map[string]string{"backend": backendMetadata}
}

func prepareFoundryStartRequest(request harness.StartTurnRequest) harness.StartTurnRequest {
	if request.ToolExecutionMode == "" {
		request.ToolExecutionMode = harness.ToolExecutionModeObserved
	}
	for i := range request.Input.Env {
		request.Input.Env[i].Value = ""
	}
	request.Input.Env = nil
	return request
}

func validateFoundryFunctionName(name string) error {
	if len(name) == 0 || len(name) > 128 || !foundryToolNameRE.MatchString(name) {
		return errors.New("foundry function call name is invalid")
	}
	return nil
}

func validateFoundryCallID(callID string) error {
	if callID == "" {
		return errors.New("foundry function call omitted call_id")
	}
	if len([]rune(callID)) > 64 {
		return errors.New("foundry function call id exceeds 64 characters")
	}
	for _, char := range callID {
		if unicode.IsControl(char) {
			return errors.New("foundry function call id contains control characters")
		}
	}
	return nil
}

func orkaToolCallID(
	runtimeSessionID harness.RuntimeSessionID,
	turnID harness.HarnessTurnID,
	providerCallID string,
) string {
	payload, _ := json.Marshal([]string{
		string(runtimeSessionID),
		string(turnID),
		providerCallID,
	})
	digest := sha256.Sum256(payload)
	return "foundry-" + hex.EncodeToString(digest[:16])
}

func foundryFunctionCallFingerprint(name string, arguments json.RawMessage) string {
	digest := sha256.New()
	_, _ = digest.Write([]byte(name))
	_, _ = digest.Write([]byte{0})
	_, _ = digest.Write(arguments)
	return hex.EncodeToString(digest.Sum(nil))
}

func toolResultFingerprint(result harness.ToolCallResult) (string, int64, error) {
	encoded, err := json.Marshal(result)
	if err != nil {
		return "", 0, err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), int64(len(encoded)), nil
}

func runtimeSessionOwnerFingerprint(request harness.StartTurnRequest) string {
	owner := struct {
		Namespace   string `json:"namespace"`
		SessionName string `json:"sessionName"`
		Principal   string `json:"principal"`
		Issuer      string `json:"issuer,omitempty"`
	}{
		Namespace:   request.Namespace,
		SessionName: request.SessionName,
		Principal:   runtimeSessionPrincipal(request),
		Issuer:      request.AuthIdentity.Issuer,
	}
	encoded, err := json.Marshal(owner)
	if err != nil {
		return ""
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}

func runtimeSessionPrincipal(request harness.StartTurnRequest) string {
	subject := strings.TrimSpace(request.AuthIdentity.Subject)
	taskPrefix := "task:" + strings.TrimSpace(request.Namespace) + "/"
	if strings.HasPrefix(subject, taskPrefix) {
		// Orka task subjects are turn-scoped. The namespace and SessionName above
		// define the stable owner for task-to-task session continuation.
		return "task-namespace:" + strings.TrimSpace(request.Namespace)
	}
	if subject != "" {
		return "subject:" + subject
	}
	return "user:" + strings.TrimSpace(request.AuthIdentity.Username)
}

func startTurnFingerprint(request harness.StartTurnRequest) string {
	request.Deadline = time.Time{}
	request.EventCursor = 0
	encoded, err := json.Marshal(request)
	if err != nil {
		return ""
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}

func validateAdapterStartRequest(request harness.StartTurnRequest) error {
	for field, value := range map[string]string{
		"namespace":          string(request.Namespace),
		"task name":          request.TaskName,
		"session name":       request.SessionName,
		"runtime session id": string(request.RuntimeSessionID),
		"turn id":            string(request.TurnID),
		"correlation id":     request.CorrelationID,
	} {
		if len(value) > maxHarnessIdentityBytes {
			return fmt.Errorf("%s exceeds adapter identity limit", field)
		}
	}
	if len(request.Input.Prompt) > maxFoundryPromptBytes {
		return errors.New("prompt exceeds adapter limit")
	}
	if len(request.Input.Tools) > defaultMaxBrokeredCalls {
		return errors.New("tool schema count exceeds adapter limit")
	}
	toolBytes := 0
	for _, tool := range request.Input.Tools {
		toolBytes += len(tool.Name) + len(tool.Description) + len(tool.Parameters)
	}
	if toolBytes > maxFoundryToolSchemaBytes {
		return errors.New("tool schemas exceed adapter limit")
	}
	return nil
}

func compactStartTurnRequest(request harness.StartTurnRequest) harness.StartTurnRequest {
	request.AuthIdentity = harness.AuthIdentity{}
	request.ToolPolicyRef = nil
	request.ApprovalPolicyRef = nil
	request.Input.Prompt = ""
	request.Input.ContextRefs = nil
	request.Input.Env = nil
	request.Metadata = nil
	return request
}

func foundryTurnDeadline(now, requested time.Time, timeout time.Duration) time.Time {
	deadline := now.Add(timeout)
	if !requested.IsZero() && requested.Before(deadline) {
		deadline = requested
	}
	return deadline
}

func cancelRequestMatchesTurn(cancel harness.CancelTurnRequest, started harness.StartTurnRequest) bool {
	return cancel.Namespace == started.Namespace &&
		cancel.TaskName == started.TaskName &&
		cancel.SessionName == started.SessionName &&
		cancel.RuntimeSessionID == started.RuntimeSessionID &&
		cancel.TurnID == started.TurnID &&
		cancel.CorrelationID == started.CorrelationID
}

func (a *adapter) validateConfiguredToolDefinitions(definitions []harness.ToolDefinition) error {
	allowed := make(map[harness.BrokeredToolClass]struct{}, len(a.cfg.brokeredToolClasses))
	for _, class := range a.cfg.brokeredToolClasses {
		allowed[class] = struct{}{}
	}
	for _, definition := range definitions {
		if _, ok := allowed[definition.BrokeredClass]; !ok {
			return fmt.Errorf("foundry brokered tool class %q is not enabled", definition.BrokeredClass)
		}
	}
	return nil
}

func validateFoundryToolDefinitions(definitions []harness.ToolDefinition) error {
	seen := make(map[string]struct{}, len(definitions))
	for _, definition := range definitions {
		name := strings.TrimSpace(definition.Name)
		if len(name) > 128 || !foundryToolNameRE.MatchString(name) {
			return fmt.Errorf("foundry tool name %q must contain 1-128 letters, numbers, underscores, or hyphens", name)
		}
		if _, duplicate := seen[name]; duplicate {
			return fmt.Errorf("duplicate Foundry tool name %q", name)
		}
		seen[name] = struct{}{}
		if !foundryToolClassSupported(definition.BrokeredClass) {
			return fmt.Errorf("unsupported Foundry brokered tool class %q for tool %q", definition.BrokeredClass, definition.Name)
		}
		if len(definition.Parameters) > 0 {
			var schema map[string]any
			if err := json.Unmarshal(definition.Parameters, &schema); err != nil || schema == nil {
				return fmt.Errorf("foundry tool %q parameters must be a JSON object", name)
			}
		}
	}
	return nil
}

func foundryToolClassSupported(class harness.BrokeredToolClass) bool {
	return class == harness.BrokeredToolClassRead || class == harness.BrokeredToolClassWrite
}

func foundryToolAllowed(definitions []harness.ToolDefinition, name string) bool {
	for _, definition := range definitions {
		if foundryToolClassSupported(definition.BrokeredClass) && strings.TrimSpace(definition.Name) == strings.TrimSpace(name) {
			return true
		}
	}
	return false
}

func foundryToolClass(definitions []harness.ToolDefinition, name string) harness.BrokeredToolClass {
	for _, definition := range definitions {
		if strings.TrimSpace(definition.Name) == strings.TrimSpace(name) {
			return definition.BrokeredClass
		}
	}
	return ""
}

func foundryToolSchemas(request harness.StartTurnRequest) []foundryToolSchema {
	if request.ToolExecutionMode != harness.ToolExecutionModeBrokered {
		return nil
	}
	tools := make([]foundryToolSchema, 0, len(request.Input.Tools))
	for _, definition := range request.Input.Tools {
		if !foundryToolClassSupported(definition.BrokeredClass) || strings.TrimSpace(definition.Name) == "" {
			continue
		}
		parameters := json.RawMessage(`{"type":"object","additionalProperties":true}`)
		if len(definition.Parameters) > 0 {
			parameters = slices.Clone(definition.Parameters)
		}
		tools = append(tools, foundryToolSchema{
			Type:        "function",
			Name:        strings.TrimSpace(definition.Name),
			Description: strings.TrimSpace(definition.Description),
			Parameters:  parameters,
		})
	}
	return tools
}

func (a *adapter) providerToolSchemas(request harness.StartTurnRequest) []foundryToolSchema {
	if strings.EqualFold(strings.TrimSpace(a.cfg.toolSchemaMode), toolSchemaModeProviderStatic) {
		return nil
	}
	return foundryToolSchemas(request)
}

func normalizeFoundryToolArguments(raw json.RawMessage) (json.RawMessage, error) {
	if len(raw) == 0 {
		return json.RawMessage(`{}`), nil
	}
	var encoded string
	if err := json.Unmarshal(raw, &encoded); err == nil {
		encoded = strings.TrimSpace(encoded)
		if encoded != "" && json.Valid([]byte(encoded)) {
			return json.RawMessage(encoded), nil
		}
		return nil, errors.New("foundry tool arguments are not valid JSON")
	}
	if json.Valid(raw) {
		return slices.Clone(raw), nil
	}
	return nil, errors.New("foundry tool arguments are not valid JSON")
}

func validateBrokeredResultFrames(results []harness.ToolCallResult, maxBytes int64) error {
	for _, result := range results {
		encoded, err := json.Marshal(result)
		if err != nil {
			return adapterValidationError{err: err}
		}
		if int64(len(encoded)) > maxBytes {
			return adapterLimitError{err: fmt.Errorf(
				"brokered tool result %q frame exceeds adapter limit of %d bytes",
				result.ToolCallID,
				maxBytes,
			)}
		}
	}
	return nil
}

func validateFoundryStreamSummary(summary foundryStreamSummary) error {
	if err := validateProviderIdentifier("response id", summary.ResponseID); err != nil {
		return err
	}
	return validateProviderIdentifier("agent session id", summary.AgentSessionID)
}

func validateProviderIdentifier(label, value string) error {
	if value == "" {
		return nil
	}
	if len(value) > maxProviderIdentifierBytes {
		return fmt.Errorf("foundry %s exceeds adapter limit", label)
	}
	for _, char := range value {
		if unicode.IsSpace(char) || unicode.IsControl(char) {
			return fmt.Errorf("foundry %s contains unsafe characters", label)
		}
	}
	return nil
}

func providerSafeMessage(err error) string {
	var providerErr providerHTTPError
	if errors.As(err, &providerErr) {
		return providerErr.Error()
	}
	if strings.Contains(strings.ToLower(err.Error()), "adapter limit") || strings.Contains(strings.ToLower(err.Error()), "invalid json") ||
		strings.Contains(strings.ToLower(err.Error()), "without a terminal") || strings.Contains(strings.ToLower(err.Error()), "not supplied by orka") ||
		strings.Contains(strings.ToLower(err.Error()), "requested tool") {
		message := err.Error()
		if remainder, ok := strings.CutPrefix(message, "foundry "); ok {
			message = "Foundry " + remainder
		}
		return message
	}
	return "Foundry request failed"
}

func providerFailureMessage(providerError *foundryError) string {
	if providerError == nil {
		return "Foundry response failed"
	}
	code := firstNonBlank(providerError.Code, providerError.Type)
	if code == "" {
		return "Foundry response failed"
	}
	return "Foundry response failed (" + code + ")"
}

func incompleteFailureMessage(incomplete *foundryIncomplete) string {
	if incomplete == nil || strings.TrimSpace(incomplete.Reason) == "" {
		return "Foundry response was incomplete"
	}
	return "Foundry response was incomplete (" + strings.TrimSpace(incomplete.Reason) + ")"
}

func retryableResponseFailure(providerError *foundryError) bool {
	if providerError == nil {
		return false
	}
	code := strings.ToLower(firstNonBlank(providerError.Code, providerError.Type))
	return code == "server_error" || code == "too_many_requests" || code == "rate_limit_exceeded" ||
		code == "no_capacity" || code == "timeout" || code == "temporarily_unavailable"
}

func providerSessionCheckpointInvalid(err error, request foundryResponseRequest) bool {
	if request.AgentSessionID == "" && request.PreviousResponseID == "" {
		return false
	}
	var providerErr providerHTTPError
	if !errors.As(err, &providerErr) {
		return false
	}
	return providerErr.StatusCode == http.StatusNotFound || providerErr.StatusCode == http.StatusGone
}

func retryableProviderError(err error) bool {
	var providerErr providerHTTPError
	if errors.As(err, &providerErr) {
		return providerErr.StatusCode == 408 || providerErr.StatusCode == 429 || providerErr.StatusCode >= 500
	}
	return false
}

var (
	errTurnCompleted               = errors.New("turn already completed and is no longer replayable")
	errTurnIDConsumed              = errors.New("turn id was already consumed")
	errTurnAlreadyExists           = errors.New("turn already exists")
	errTurnNotFound                = errors.New("turn not found")
	errRuntimeSessionBusy          = errors.New("runtime session already has an active turn")
	errRuntimeSessionOwnerMismatch = errors.New("runtime session owner does not match the cached Foundry session")
	errAdapterAtCapacity           = errors.New("adapter is at turn capacity")
)
