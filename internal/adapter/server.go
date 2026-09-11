package adapter

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/orka-agents/agent-runtime-foundry/internal/harness"
)

const (
	healthValidationTTL     = 30 * time.Second
	maxStartRequestBytes    = 32 << 20
	maxContinueRequestBytes = 32 << 20
	maxCancelRequestBytes   = 1 << 20
)

var errRequestTooLarge = errors.New("request body exceeds adapter limit")

type server struct {
	cfg     config
	adapter *adapter

	healthMu        sync.Mutex
	healthCheckedAt time.Time
	healthErr       error
}

func (s *server) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(harness.HealthPath, s.health)
	mux.HandleFunc(harness.CapabilitiesPath, s.capabilities)
	mux.HandleFunc(harness.TurnsPath, s.startTurn)
	mux.HandleFunc(harness.TurnsPath+"/", s.turn)
	return mux
}

func (s *server) health(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		harness.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	status := harness.HealthStatusOK
	ready := true
	message := "ready"
	if err := s.cfg.validate(); err != nil {
		status = harness.HealthStatusDegraded
		ready = false
		message = err.Error()
	} else if s.adapter == nil || s.adapter.backend == nil {
		status = harness.HealthStatusDegraded
		ready = false
		message = "Foundry backend is not configured"
	} else if err := s.validateProviderHealth(); err != nil {
		status = harness.HealthStatusDegraded
		ready = false
		message = providerSafeMessage(err)
	}
	harness.WriteJSON(w, http.StatusOK, harness.HealthResponse{
		Version:   harness.ProtocolVersion,
		Status:    status,
		Ready:     ready,
		CheckedAt: time.Now().UTC(),
		Message:   message,
		Metadata:  map[string]string{"backend": backendMetadata},
	})
}

func (s *server) validateProviderHealth() error {
	s.healthMu.Lock()
	defer s.healthMu.Unlock()
	now := time.Now().UTC()
	if !s.healthCheckedAt.IsZero() && now.Sub(s.healthCheckedAt) < healthValidationTTL {
		return s.healthErr
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if strings.EqualFold(s.cfg.isolationMode, "header") {
		ctx = withFoundryIsolationKey(ctx, isolationKeyForRuntimeSession("readiness"))
	}
	err := s.adapter.backend.ValidateAgent(ctx)
	cancel()
	s.healthCheckedAt = now
	s.healthErr = err
	return err
}

func (s *server) capabilities(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		harness.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	toolModes := []harness.ToolExecutionMode{harness.ToolExecutionModeObserved}
	if len(s.cfg.brokeredToolClasses) > 0 {
		toolModes = append(toolModes, harness.ToolExecutionModeBrokered)
	}
	harness.WriteJSON(w, http.StatusOK, harness.CapabilitiesResponse{
		Version:                 harness.ProtocolVersion,
		ProtocolVersion:         harness.ProtocolVersion,
		Transport:               harness.HTTPTransport,
		RuntimeName:             s.cfg.runtimeName,
		RuntimeVersion:          "foundry-hosted-responses-adapter",
		ProviderKind:            harness.ProviderKindRemote,
		ToolExecutionModes:      toolModes,
		BrokeredToolClasses:     slices.Clone(s.cfg.brokeredToolClasses),
		SupportsCancel:          true,
		SupportsRuntimeSessions: true,
		SupportsContinuation:    len(s.cfg.brokeredToolClasses) > 0,
		SupportsArtifacts:       false,
		MaxConcurrentTurns:      s.cfg.maxConcurrent,
		MaxTurnSeconds:          int(math.Ceil(s.cfg.turnTimeout.Seconds())),
		MaxOutputBytes:          s.cfg.maxOutputBytes,
		Metadata:                map[string]string{"backend": backendMetadata},
	})
}

func (s *server) startTurn(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(w, r) {
		return
	}
	if r.Method != http.MethodPost {
		harness.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if err := s.cfg.validate(); err != nil {
		harness.WriteError(w, http.StatusServiceUnavailable, "adapter is not ready")
		return
	}
	var request harness.StartTurnRequest
	if err := decodeJSONRequest(r, &request, maxStartRequestBytes); err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, errRequestTooLarge) {
			status = http.StatusRequestEntityTooLarge
		}
		harness.WriteError(w, status, err.Error())
		return
	}
	if err := request.Validate(); err != nil {
		harness.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	_, eventsPath, err := s.adapter.startTurn(request)
	if err != nil {
		var providerErr providerStartError
		switch {
		case errors.Is(err, errTurnCompleted), errors.Is(err, errTurnIDConsumed),
			errors.Is(err, errTurnAlreadyExists), errors.Is(err, errRuntimeSessionBusy),
			errors.Is(err, errRuntimeSessionOwnerMismatch):
			harness.WriteError(w, http.StatusConflict, err.Error())
		case errors.Is(err, errAdapterAtCapacity):
			harness.WriteError(w, http.StatusTooManyRequests, err.Error())
		case errors.As(err, &providerErr):
			harness.WriteError(w, http.StatusBadGateway, providerSafeMessage(err))
		case startErrorIsTooLarge(err):
			harness.WriteError(w, http.StatusRequestEntityTooLarge, err.Error())
		default:
			harness.WriteError(w, http.StatusBadRequest, err.Error())
		}
		return
	}
	harness.WriteJSON(w, http.StatusAccepted, harness.StartTurnResponse{
		Version:          harness.ProtocolVersion,
		Accepted:         true,
		RuntimeSessionID: request.RuntimeSessionID,
		TurnID:           request.TurnID,
		CorrelationID:    request.CorrelationID,
		EventStreamPath:  eventsPath,
	})
}

func (s *server) turn(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(w, r) {
		return
	}
	if err := s.cfg.validate(); err != nil {
		harness.WriteError(w, http.StatusServiceUnavailable, "adapter is not ready")
		return
	}
	turnID, resource, err := harness.ParseTurnResourcePath(r.URL.EscapedPath())
	if err != nil {
		harness.WriteError(w, http.StatusNotFound, "not found")
		return
	}
	turn := s.adapter.getTurn(turnID)
	if turn == nil {
		harness.WriteError(w, http.StatusNotFound, "turn not found")
		return
	}
	if resource == harness.TurnResourceCancel {
		if r.Method != http.MethodPost {
			harness.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		s.cancelTurn(w, r, turn)
		return
	}
	select {
	case <-turn.initDone:
		if turn.initErr != nil {
			harness.WriteError(w, http.StatusBadGateway, providerSafeMessage(turn.initErr))
			return
		}
	case <-r.Context().Done():
		return
	}
	switch resource {
	case harness.TurnResourceEvents:
		if r.Method != http.MethodGet {
			harness.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		s.streamEvents(w, r, turn)
	case harness.TurnResourceContinue:
		if r.Method != http.MethodPost {
			harness.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		s.continueTurn(w, r, turn)
	default:
		harness.WriteError(w, http.StatusNotFound, "not found")
	}
}

func (s *server) streamEvents(w http.ResponseWriter, r *http.Request, turn *turnState) {
	afterSeq := parseAfterSeq(r.URL.Query().Get("afterSeq"))
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)
	if flusher != nil {
		flusher.Flush()
	}
	lastWritten := afterSeq
	for {
		frames, idle := s.adapter.waitForFrames(r.Context(), turn, lastWritten)
		for _, frame := range frames {
			if err := harness.WriteSSEFrame(w, frame); err != nil {
				return
			}
			lastWritten = frame.Seq
		}
		if flusher != nil {
			flusher.Flush()
		}
		if idle && s.adapter.turnStreamIdle(turn) {
			_ = harness.WriteSSEDone(w)
			return
		}
		if r.Context().Err() != nil {
			return
		}
	}
}

func (s *server) continueTurn(w http.ResponseWriter, r *http.Request, turn *turnState) {
	var request harness.ContinueTurnRequest
	if err := decodeJSONRequest(r, &request, maxContinueRequestBytes); err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, errRequestTooLarge) {
			status = http.StatusRequestEntityTooLarge
		}
		harness.WriteError(w, status, err.Error())
		return
	}
	if request.TurnID != turn.request.TurnID {
		harness.WriteError(w, http.StatusBadRequest, "continue turn id does not match URL")
		return
	}
	if err := s.adapter.continueTurn(request); err != nil {
		status := http.StatusConflict
		var validationErr adapterValidationError
		var limitErr adapterLimitError
		switch {
		case errors.Is(err, errTurnNotFound):
			status = http.StatusNotFound
		case errors.As(err, &validationErr):
			status = http.StatusBadRequest
		case errors.As(err, &limitErr):
			status = http.StatusRequestEntityTooLarge
		}
		harness.WriteError(w, status, err.Error())
		return
	}
	harness.WriteJSON(w, http.StatusAccepted, harness.ContinueTurnResponse{
		Version:          harness.ProtocolVersion,
		Accepted:         true,
		RuntimeSessionID: turn.request.RuntimeSessionID,
		TurnID:           turn.request.TurnID,
		CorrelationID:    turn.request.CorrelationID,
		Message:          "continue accepted",
	})
}

func (s *server) cancelTurn(w http.ResponseWriter, r *http.Request, turn *turnState) {
	var request harness.CancelTurnRequest
	if err := decodeJSONRequest(r, &request, maxCancelRequestBytes); err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, errRequestTooLarge) {
			status = http.StatusRequestEntityTooLarge
		}
		harness.WriteError(w, status, err.Error())
		return
	}
	if err := request.Validate(); err != nil {
		harness.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if request.TurnID != turn.request.TurnID {
		harness.WriteError(w, http.StatusBadRequest, "cancel turn id does not match URL")
		return
	}
	if err := s.adapter.cancelTurn(request); err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, errTurnNotFound) {
			status = http.StatusNotFound
		}
		harness.WriteError(w, status, err.Error())
		return
	}
	harness.WriteJSON(w, http.StatusAccepted, harness.CancelTurnResponse{
		Version:          harness.ProtocolVersion,
		Accepted:         true,
		RuntimeSessionID: turn.request.RuntimeSessionID,
		TurnID:           turn.request.TurnID,
		CorrelationID:    turn.request.CorrelationID,
	})
}

func (s *server) authorized(w http.ResponseWriter, r *http.Request) bool {
	if s.cfg.adapterBearer == "" {
		harness.WriteError(w, http.StatusUnauthorized, "adapter bearer token is required")
		return false
	}
	value := strings.TrimSpace(r.Header.Get("Authorization"))
	parts := strings.SplitN(value, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") ||
		subtle.ConstantTimeCompare([]byte(strings.TrimSpace(parts[1])), []byte(s.cfg.adapterBearer)) != 1 {
		harness.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return false
	}
	return true
}

func startErrorIsTooLarge(err error) bool {
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "exceeds adapter limit") ||
		strings.Contains(message, "tool schema count exceeds") ||
		strings.Contains(message, "tool schemas exceed") ||
		strings.Contains(message, "prompt exceeds")
}

func decodeJSONRequest(r *http.Request, out any, maxBytes int64) error {
	reader := http.MaxBytesReader(nil, r.Body, maxBytes)
	defer reader.Close() //nolint:errcheck
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			return errRequestTooLarge
		}
		return errors.New("invalid JSON request")
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			return errRequestTooLarge
		}
		return errors.New("invalid JSON request")
	}
	return nil
}
