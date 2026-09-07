package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"unicode/utf8"
)

type acpSession struct {
	id       string
	mcp      *acpMCPClient
	previous string
	poisoned bool
}

type acpOperation struct {
	key    string
	id     json.RawMessage
	ctx    context.Context
	cancel context.CancelFunc
}

type acpWrite struct {
	frame []byte
	done  chan error
}

type acpServer struct {
	ctx    context.Context
	cancel context.CancelFunc
	cfg    acpConfiguration
	client *http.Client
	writes chan acpWrite
	wg     sync.WaitGroup

	mu          sync.Mutex
	initialized bool
	session     *acpSession
	active      *acpOperation
}

func serveACP(parent context.Context, cfg acpConfiguration, input io.ReadCloser, output io.WriteCloser) error {
	ctx, cancel := context.WithCancel(parent)
	s := &acpServer{ctx: ctx, cancel: cancel, cfg: cfg, client: newACPHTTPClient(), writes: make(chan acpWrite, 32)}
	type readResult struct {
		line []byte
		err  error
	}
	reads := make(chan readResult, 1)
	var transport sync.WaitGroup
	transport.Add(2)
	go func() {
		defer transport.Done()
		reader := bufio.NewReaderSize(input, 32<<10)
		for {
			line, err := acpReadLine(reader)
			select {
			case reads <- readResult{line, err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	go func() {
		defer transport.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case write := <-s.writes:
				n, err := output.Write(write.frame)
				if err == nil && n != len(write.frame) {
					err = io.ErrShortWrite
				}
				write.done <- err
				if err != nil {
					cancel()
					return
				}
			}
		}
	}()
	defer func() {
		cancel()
		_ = input.Close()
		_ = output.Close()
		s.wg.Wait()
		transport.Wait()
		s.client.CloseIdleConnections()
	}()
	for {
		select {
		case <-ctx.Done():
			return errACPTransport
		case read := <-reads:
			if errors.Is(read.err, io.EOF) {
				return nil
			}
			if read.err != nil {
				return errACPTransport
			}
			if len(read.line) != 0 {
				s.accept(read.line)
			}
		}
	}
}

func (s *acpServer) enqueue(value any) (<-chan error, error) {
	frame, err := json.Marshal(value)
	if err != nil || len(frame) >= acpMaxMessageBytes {
		return nil, errACPTransport
	}
	write := acpWrite{frame: append(frame, '\n'), done: make(chan error, 1)}
	select {
	case s.writes <- write:
		return write.done, nil
	case <-s.ctx.Done():
		return nil, errACPTransport
	default:
		// A bounded queue lets cancellation continue to read stdin even when
		// the parent stops consuming stdout. Floods close the child channel.
		s.cancel()
		return nil, errACPTransport
	}
}

func (s *acpServer) send(value any) error {
	done, err := s.enqueue(value)
	if err != nil {
		return err
	}
	select {
	case err := <-done:
		return err
	case <-s.ctx.Done():
		return errACPTransport
	}
}

func (s *acpServer) respond(id json.RawMessage, result any, failure *acpRPCError) {
	if len(id) == 0 {
		id = json.RawMessage("null")
	}
	if _, err := s.enqueue(acpResponse{JSONRPC: "2.0", ID: id, Result: result, Error: failure}); err != nil {
		s.cancel()
	}
}

func (s *acpServer) accept(line []byte) {
	var request acpRequest
	if !json.Valid(line) {
		s.respond(nil, nil, &acpRPCError{-32700, "invalid ACP JSON"})
		return
	}
	if acpDecode(line, &request, true) != nil || request.JSONRPC != "2.0" || !acpSafeString(request.Method, 128) {
		s.respond(nil, nil, acpInvalidRequest)
		return
	}
	if len(request.ID) == 0 {
		s.notification(request)
		return
	}
	key, err := acpRequestKey(request.ID)
	if err != nil {
		s.respond(nil, nil, acpInvalidRequest)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active != nil && s.active.key == key {
		// Two live requests cannot share one response identity.
		s.active.cancel()
		s.cancel()
		return
	}
	switch request.Method {
	case "initialize":
		var params struct {
			ProtocolVersion    int             `json:"protocolVersion"`
			ClientCapabilities json.RawMessage `json:"clientCapabilities"`
			ClientInfo         json.RawMessage `json:"clientInfo"`
			Meta               json.RawMessage `json:"_meta,omitempty"`
		}
		if s.initialized || acpDecode(request.Params, &params, true) != nil || params.ProtocolVersion != 1 ||
			(len(params.ClientCapabilities) != 0 && params.ClientCapabilities[0] != '{') {
			s.respond(request.ID, nil, acpInvalidParams)
			return
		}
		s.initialized = true
		s.respond(request.ID, map[string]any{
			"protocolVersion": 1,
			"agentCapabilities": map[string]any{
				"loadSession":         false,
				"promptCapabilities":  map[string]bool{"image": false, "audio": false, "embeddedContext": false},
				"mcpCapabilities":     map[string]bool{"http": true},
				"sessionCapabilities": map[string]any{}, "auth": map[string]any{},
			},
			"agentInfo": map[string]string{"name": "foundry-acp", "version": "1"},
		}, nil)
	case "session/new":
		s.newSessionLocked(request, key)
	case "session/prompt":
		s.promptLocked(request, key)
	default:
		s.respond(request.ID, nil, &acpRPCError{-32601, "ACP method not supported"})
	}
}

func (s *acpServer) notification(request acpRequest) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active == nil {
		return
	}
	switch request.Method {
	case "session/cancel":
		var params struct {
			SessionID string          `json:"sessionId"`
			Meta      json.RawMessage `json:"_meta,omitempty"`
		}
		if acpDecode(request.Params, &params, true) == nil && s.session != nil && params.SessionID == s.session.id {
			s.active.cancel()
		}
	case "$/cancel_request":
		var params struct {
			RequestID json.RawMessage `json:"requestId"`
		}
		key, err := "", error(nil)
		if acpDecode(request.Params, &params, true) == nil {
			key, err = acpRequestKey(params.RequestID)
		}
		if err == nil && key == s.active.key {
			s.active.cancel()
		}
	}
}

func (s *acpServer) newSessionLocked(request acpRequest, key string) {
	if !s.initialized || s.session != nil || s.active != nil {
		s.respond(request.ID, nil, acpInvalidRequest)
		return
	}
	var params acpNewSession
	if acpDecode(request.Params, &params, true) != nil || len(params.AdditionalDirectories) != 0 || len(params.MCPServers) != 1 || !filepath.IsAbs(params.CWD) {
		s.respond(request.ID, nil, acpInvalidParams)
		return
	}
	cwd, err := os.Getwd()
	requested, requestedErr := filepath.EvalSymlinks(params.CWD)
	actual, actualErr := filepath.EvalSymlinks(cwd)
	if err != nil || requestedErr != nil || actualErr != nil || requested != actual {
		s.respond(request.ID, nil, acpInvalidParams)
		return
	}
	mcp, err := newACPMCPClient(params.MCPServers[0], s.client)
	if err != nil {
		s.respond(request.ID, nil, acpInvalidParams)
		return
	}
	op := s.newOperationLocked(request, key)
	s.wg.Go(func() {
		defer op.cancel()
		err := mcp.initialize(op.ctx)
		s.mu.Lock()
		defer s.mu.Unlock()
		if err != nil || op.ctx.Err() != nil {
			s.respond(op.id, nil, acpInternalError)
		} else {
			s.session = &acpSession{id: acpOpaqueID("foundry-"), mcp: mcp}
			s.respond(op.id, map[string]string{"sessionId": s.session.id}, nil)
		}
		s.active = nil
	})
}

func (s *acpServer) newOperationLocked(request acpRequest, key string) *acpOperation {
	ctx, cancel := context.WithCancel(s.ctx)
	op := &acpOperation{key: key, id: request.ID, ctx: ctx, cancel: cancel}
	s.active = op
	return op
}

func (s *acpServer) promptLocked(request acpRequest, key string) {
	var params acpPrompt
	if acpDecode(request.Params, &params, true) != nil {
		s.respond(request.ID, nil, acpInvalidParams)
		return
	}
	text, err := params.text()
	if err != nil || s.session == nil || params.SessionID != s.session.id || s.session.poisoned {
		s.respond(request.ID, nil, acpInvalidParams)
		return
	}
	if s.active != nil {
		s.respond(request.ID, nil, &acpRPCError{-32800, "ACP session already has an active prompt"})
		return
	}
	op := s.newOperationLocked(request, key)
	session := s.session
	s.wg.Go(func() {
		defer op.cancel()
		previous, output, err := s.runPrompt(op.ctx, session, text)
		if err == nil {
			err = s.outputText(op.ctx, session.id, output)
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		switch {
		case op.ctx.Err() != nil:
			session.poisoned, session.previous = true, ""
			s.respond(op.id, map[string]string{"stopReason": "cancelled"}, nil)
		case err != nil:
			session.poisoned, session.previous = true, ""
			s.respond(op.id, nil, acpInternalError)
		default:
			session.previous = previous
			s.respond(op.id, map[string]string{"stopReason": "end_turn"}, nil)
		}
		s.active = nil
	})
}

func (s *acpServer) update(sessionID string, update any) error {
	return s.send(map[string]any{
		"jsonrpc": "2.0", "method": "session/update",
		"params": map[string]any{"sessionId": sessionID, "update": update},
	})
}

func (s *acpServer) outputText(ctx context.Context, sessionID, text string) error {
	for len(text) != 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		n := min(len(text), acpTextChunkBytes)
		for n < len(text) && !utf8.RuneStart(text[n]) {
			n--
		}
		if err := s.update(sessionID, map[string]any{
			"sessionUpdate": "agent_message_chunk",
			"content":       map[string]string{"type": "text", "text": text[:n]},
		}); err != nil {
			return err
		}
		text = text[n:]
	}
	return ctx.Err()
}

func acpOpaqueID(prefix string) string {
	var data [16]byte
	_, _ = rand.Read(data[:])
	return prefix + hex.EncodeToString(data[:])
}
