package main

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"golang.org/x/net/http2"
)

const (
	hostedWSMaxMessageBytes   = 1 << 20
	hostedWSWriteChunkBytes   = 256 << 10
	hostedTransportWriteLimit = 30 * time.Second
	hostedHTTP2MaxStreams     = 64
	hostedHTTP2MaxHeaderBytes = 32 << 10
	hostedHTTP2MaxFrameBytes  = 256 << 10
)

var errHostedWSMessage = errors.New("hosted transport requires nonempty binary messages of at most 1 MiB")

// A terminal summary contains only locally assigned role/time and a numeric
// WebSocket close code. Peer close text and transport errors never enter logs.
type hostedChannelObservation struct {
	role      string
	openedAt  time.Time
	logger    *log.Logger
	closeCode atomic.Int32
	once      sync.Once
}

func observeHostedChannel(ws *websocket.Conn, role string, openedAt time.Time, logger *log.Logger) *hostedChannelObservation {
	if role != "forward" && role != "reverse" {
		return nil
	}
	if logger == nil {
		logger = log.Default()
	}
	o := &hostedChannelObservation{role: role, openedAt: openedAt, logger: logger}
	previous := ws.CloseHandler()
	ws.SetCloseHandler(func(code int, text string) error {
		// Record before the existing reply handler or a lifetime watcher can
		// close the pair. Preserve Gorilla's control-frame behavior unchanged.
		o.closeCode.CompareAndSwap(0, int32(code))
		return previous(code, text)
	})
	return o
}

func (o *hostedChannelObservation) observeError(err error) {
	if o == nil {
		return
	}
	var closed *websocket.CloseError
	if errors.As(err, &closed) {
		o.closeCode.CompareAndSwap(0, int32(closed.Code))
	}
}

func (o *hostedChannelObservation) finish() {
	if o == nil {
		return
	}
	o.once.Do(func() {
		o.logger.Printf("Foundry hosted channel closed role=%s opened_at=%s duration_ms=%d close_code=%d",
			o.role, o.openedAt.UTC().Format(time.RFC3339Nano), time.Since(o.openedAt).Milliseconds(), o.closeCode.Load())
	})
}

// hostedWSConn carries a byte stream over an already authenticated WebSocket.
// Construction transfers exclusive ownership of the WebSocket to this adapter.
// Whole messages are validated before exposing any bytes to HTTP/2.
type hostedWSConn struct {
	ws          *websocket.Conn
	observation *hostedChannelObservation
	done        chan struct{}
	closeOnce   sync.Once
	closeErr    error
	readMu      sync.Mutex
	readBuf     []byte
	writeMu     sync.Mutex

	deadlineMu    sync.Mutex
	writeDeadline time.Time
	activeWrite   *hostedWSWrite
}

type hostedWSWrite struct {
	limit      time.Time
	timer      *time.Timer
	generation uint64
	timedOut   bool
}

var _ net.Conn = (*hostedWSConn)(nil)

func newHostedWSConn(ws *websocket.Conn) *hostedWSConn {
	return newHostedObservedWSConn(ws, nil)
}

func newHostedObservedWSConn(ws *websocket.Conn, observation *hostedChannelObservation) *hostedWSConn {
	c := &hostedWSConn{ws: ws, observation: observation, done: make(chan struct{})}
	ws.SetReadLimit(hostedWSMaxMessageBytes)
	// Remove handshake deadlines. No timer runs while this connection is idle.
	// Gorilla's write deadline is only set before handing ownership to callers;
	// its setter is not safe concurrently with a writer, nor can it interrupt one.
	_ = ws.SetWriteDeadline(time.Time{})
	if err := ws.UnderlyingConn().SetDeadline(time.Time{}); err != nil {
		_ = c.Close()
	}
	return c
}

func (c *hostedWSConn) Done() <-chan struct{} { return c.done }

func (c *hostedWSConn) closed() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

func (c *hostedWSConn) Close() error {
	c.closeOnce.Do(func() {
		close(c.done)
		c.deadlineMu.Lock()
		if c.activeWrite != nil && c.activeWrite.timer != nil {
			c.activeWrite.timer.Stop()
		}
		c.deadlineMu.Unlock()
		c.closeErr = c.ws.Close()
		c.observation.finish()
	})
	return c.closeErr
}

func (c *hostedWSConn) Read(p []byte) (int, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	if c.closed() {
		return 0, net.ErrClosed
	}
	if len(p) == 0 {
		return 0, nil
	}
	if len(c.readBuf) == 0 {
		kind, reader, err := c.ws.NextReader()
		if err != nil {
			c.observation.observeError(err)
			_ = c.Close()
			return 0, hostedTransportError(err)
		}
		if kind != websocket.BinaryMessage {
			_ = c.Close()
			return 0, errHostedWSMessage
		}
		// The extra byte also bounds expanded data if the handshake negotiated
		// compression; Gorilla's read limit counts the on-wire message length.
		message, err := io.ReadAll(io.LimitReader(reader, hostedWSMaxMessageBytes+1))
		if err != nil {
			c.observation.observeError(err)
			_ = c.Close()
			return 0, hostedTransportError(err)
		}
		if len(message) == 0 || len(message) > hostedWSMaxMessageBytes {
			_ = c.Close()
			return 0, errHostedWSMessage
		}
		c.readBuf = message
	}
	n := copy(p, c.readBuf)
	c.readBuf = c.readBuf[n:]
	if len(c.readBuf) == 0 {
		c.readBuf = nil
	}
	return n, nil
}

func (c *hostedWSConn) Write(p []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.closed() {
		return 0, net.ErrClosed
	}
	if len(p) == 0 {
		return 0, nil
	}
	write, err := c.beginWrite()
	if err != nil {
		_ = c.Close()
		return 0, err
	}
	n := 0
	for n < len(p) {
		end := n + min(len(p)-n, hostedWSWriteChunkBytes)
		if err = c.ws.WriteMessage(websocket.BinaryMessage, p[n:end]); err != nil {
			break
		}
		// A failed message may have an incomplete frame on the wire. Only
		// completed messages count toward the accepted prefix; never resend it.
		n = end
	}
	if c.finishWrite(write) {
		err = os.ErrDeadlineExceeded
	}
	if err != nil {
		c.observation.observeError(err)
		_ = c.Close()
	}
	return n, hostedTransportError(err)
}

func hostedTransportError(err error) error {
	var networkError net.Error
	if errors.As(err, &networkError) && networkError.Timeout() {
		// Gorilla deliberately removes the wrapped syscall error. Restore
		// net.Conn's deadline sentinel while keeping this connection terminal.
		return os.ErrDeadlineExceeded
	}
	return err
}

func (c *hostedWSConn) beginWrite() (*hostedWSWrite, error) {
	c.deadlineMu.Lock()
	defer c.deadlineMu.Unlock()
	if c.closed() {
		return nil, net.ErrClosed
	}
	now := time.Now()
	if !c.writeDeadline.IsZero() && !c.writeDeadline.After(now) {
		return nil, os.ErrDeadlineExceeded
	}
	write := &hostedWSWrite{limit: now.Add(hostedTransportWriteLimit)}
	c.activeWrite = write
	c.armWriteTimer(write)
	return write, nil
}

// armWriteTimer runs under deadlineMu. A generation prevents an already queued
// timer callback from applying a deadline that a caller extended or cleared.
func (c *hostedWSConn) armWriteTimer(write *hostedWSWrite) {
	if write.timer != nil {
		write.timer.Stop()
	}
	deadline := write.limit
	if !c.writeDeadline.IsZero() && c.writeDeadline.Before(deadline) {
		deadline = c.writeDeadline
	}
	write.generation++
	generation := write.generation
	write.timer = time.AfterFunc(time.Until(deadline), func() {
		c.deadlineMu.Lock()
		if c.activeWrite != write || write.generation != generation || c.closed() {
			c.deadlineMu.Unlock()
			return
		}
		write.timedOut = true
		c.deadlineMu.Unlock()
		_ = c.Close()
	})
}

func (c *hostedWSConn) finishWrite(write *hostedWSWrite) bool {
	c.deadlineMu.Lock()
	defer c.deadlineMu.Unlock()
	write.timer.Stop()
	c.activeWrite = nil
	return write.timedOut
}

func (c *hostedWSConn) LocalAddr() net.Addr  { return c.ws.LocalAddr() }
func (c *hostedWSConn) RemoteAddr() net.Addr { return c.ws.RemoteAddr() }

func (c *hostedWSConn) SetDeadline(deadline time.Time) error {
	if err := c.SetReadDeadline(deadline); err != nil {
		return err
	}
	return c.SetWriteDeadline(deadline)
}

func (c *hostedWSConn) SetReadDeadline(deadline time.Time) error {
	return c.ws.UnderlyingConn().SetReadDeadline(deadline)
}

func (c *hostedWSConn) SetWriteDeadline(deadline time.Time) error {
	c.deadlineMu.Lock()
	defer c.deadlineMu.Unlock()
	if c.closed() {
		return net.ErrClosed
	}
	c.writeDeadline = deadline
	if c.activeWrite != nil && !c.activeWrite.timedOut {
		c.armWriteTimer(c.activeWrite)
	}
	return nil
}

// Call ClientConn.RoundTrip directly. A Transport.RoundTrip would add connection
// selection and transparent retries, which cannot preserve mutation ownership.
func newHostedHTTP2ClientConn(conn net.Conn) (*http2.ClientConn, error) {
	// ConfigureTransports initializes x/net's underlying implementation on
	// both Go 1.26 and 1.27. Neither transport's RoundTrip is ever used.
	transport, err := http2.ConfigureTransports(&http.Transport{})
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	transport.AllowHTTP = true
	transport.DisableCompression = true
	transport.StrictMaxConcurrentStreams = true
	transport.MaxHeaderListSize = hostedHTTP2MaxHeaderBytes
	transport.MaxReadFrameSize = hostedHTTP2MaxFrameBytes
	transport.WriteByteTimeout = hostedTransportWriteLimit
	client, err := transport.NewClientConn(conn)
	if err != nil {
		_ = conn.Close()
	}
	return client, err
}

func serveHostedHTTP2(ctx context.Context, conn net.Conn, handler http.Handler) {
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	defer conn.Close()
	server := &http2.Server{
		MaxConcurrentStreams: hostedHTTP2MaxStreams,
		MaxReadFrameSize:     hostedHTTP2MaxFrameBytes,
		WriteByteTimeout:     hostedTransportWriteLimit,
	}
	server.ServeConn(conn, &http2.ServeConnOpts{
		Context:    ctx,
		BaseConfig: &http.Server{MaxHeaderBytes: hostedHTTP2MaxHeaderBytes},
		Handler:    handler,
	})
}
