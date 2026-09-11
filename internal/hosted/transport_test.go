package hosted

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"golang.org/x/net/http2"
)

func hostedTestWSPair(t *testing.T, compression bool, gate *hostedTestWriteGate) (*websocket.Conn, *websocket.Conn) {
	t.Helper()
	accepted := make(chan *websocket.Conn, 1)
	upgradeError := make(chan error, 1)
	upgrader := websocket.Upgrader{WriteBufferSize: 1024, EnableCompression: compression}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			upgradeError <- err
			return
		}
		accepted <- conn
	}))
	t.Cleanup(server.Close)
	dialer := websocket.Dialer{
		WriteBufferSize:   hostedWSWriteChunkBytes,
		EnableCompression: compression,
		HandshakeTimeout:  5 * time.Second,
	}
	if gate != nil {
		dialer.NetDialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
			conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
			if err != nil {
				return nil, err
			}
			gate.Conn = conn
			return gate, nil
		}
	}
	client, response, err := dialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if response != nil && response.Body != nil {
		_ = response.Body.Close()
	}
	if err != nil {
		t.Fatal("localhost WebSocket handshake failed")
	}
	var peer *websocket.Conn
	select {
	case peer = <-accepted:
	case <-upgradeError:
		t.Fatal("localhost WebSocket upgrade failed")
	case <-time.After(5 * time.Second):
		t.Fatal("localhost WebSocket upgrade did not complete")
	}
	t.Cleanup(func() {
		_ = client.Close()
		_ = peer.Close()
	})
	return client, peer
}

func hostedTestDone(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("transport operation did not finish")
	}
}

func TestHostedWSConnReadsValidatedMessages(t *testing.T) {
	client, peer := hostedTestWSPair(t, false, nil)
	conn := newHostedWSConn(client)
	defer conn.Close()
	if conn.LocalAddr() == nil || conn.RemoteAddr() == nil {
		t.Fatal("transport lost its endpoint addresses")
	}
	payload := bytes.Repeat([]byte{0x41}, hostedWSMaxMessageBytes)
	sent := make(chan error, 1)
	go func() {
		writer, err := peer.NextWriter(websocket.BinaryMessage)
		if err == nil {
			// Several writes and a small peer buffer force WebSocket continuation
			// frames; the receiver still sees a single bounded message.
			for offset := 0; offset < len(payload) && err == nil; {
				end := min(offset+719, len(payload))
				_, err = writer.Write(payload[offset:end])
				offset = end
			}
			if err == nil {
				err = writer.Close()
			}
		}
		if err == nil {
			err = peer.WriteMessage(websocket.BinaryMessage, []byte("tail"))
		}
		sent <- err
	}()
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal("could not bound test read")
	}
	got := make([]byte, len(payload)+4)
	for offset := 0; offset < len(got); {
		end := min(offset+113, len(got))
		n, err := conn.Read(got[offset:end])
		if err != nil || n == 0 {
			t.Fatal("valid fragmented stream was truncated")
		}
		offset += n
	}
	if !bytes.Equal(got[:len(payload)], payload) || string(got[len(payload):]) != "tail" || <-sent != nil {
		t.Fatal("message boundaries changed the byte stream")
	}
}

func TestHostedWSConnRejectsInvalidMessagesWithoutExposingBytes(t *testing.T) {
	for _, test := range []struct {
		name        string
		kind        int
		length      int
		compression bool
		incomplete  bool
	}{
		{name: "empty", kind: websocket.BinaryMessage},
		{name: "text", kind: websocket.TextMessage, length: 4},
		{name: "oversized fragmented", kind: websocket.BinaryMessage, length: hostedWSMaxMessageBytes + 1},
		{name: "oversized expanded", kind: websocket.BinaryMessage, length: hostedWSMaxMessageBytes + 1, compression: true},
		{name: "incomplete fragmented", kind: websocket.BinaryMessage, length: 4096, incomplete: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, peer := hostedTestWSPair(t, test.compression, nil)
			conn := newHostedWSConn(client)
			defer conn.Close()
			_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			sent := make(chan struct{})
			go func() {
				defer close(sent)
				writer, err := peer.NextWriter(test.kind)
				if err == nil {
					_, err = writer.Write(bytes.Repeat([]byte{'x'}, test.length))
					if err == nil && !test.incomplete {
						_ = writer.Close()
					}
				}
				if test.incomplete {
					_ = peer.Close()
				}
			}()
			if n, err := conn.Read(make([]byte, 31)); n != 0 || err == nil {
				t.Fatal("invalid or incomplete message exposed stream bytes")
			}
			hostedTestDone(t, conn.Done())
			hostedTestDone(t, sent)
			// A caller that continues after failure must not re-enter Gorilla's
			// failed-read path, which eventually panics on repeated reads.
			for range 1100 {
				if n, err := conn.Read(make([]byte, 1)); n != 0 || !errors.Is(err, net.ErrClosed) {
					t.Fatal("failed connection became readable again")
				}
			}
		})
	}
}

func TestHostedWSConnWritesBoundedMessages(t *testing.T) {
	client, peer := hostedTestWSPair(t, false, nil)
	conn := newHostedWSConn(client)
	defer conn.Close()
	payload := bytes.Repeat([]byte{0x42}, 2*hostedWSWriteChunkBytes+17)
	if n, err := conn.Write(nil); n != 0 || err != nil {
		t.Fatal("zero-length write failed")
	}
	writeResult := make(chan error, 1)
	go func() {
		n, err := conn.Write(payload)
		if err == nil && n != len(payload) {
			err = io.ErrShortWrite
		}
		writeResult <- err
	}()
	_ = peer.SetReadDeadline(time.Now().Add(5 * time.Second))
	var got []byte
	for _, size := range []int{hostedWSWriteChunkBytes, hostedWSWriteChunkBytes, 17} {
		kind, part, err := peer.ReadMessage()
		if err != nil || kind != websocket.BinaryMessage || len(part) != size {
			t.Fatal("write emitted an empty, non-binary or incorrectly sized message")
		}
		got = append(got, part...)
	}
	if <-writeResult != nil || !bytes.Equal(got, payload) {
		t.Fatal("chunked write changed the stream")
	}
}

func TestHostedWSConnConcurrentWritesPreserveCompletePrefixes(t *testing.T) {
	client, peer := hostedTestWSPair(t, false, nil)
	conn := newHostedWSConn(client)
	defer conn.Close()
	const count = 4
	written := make(chan error, count)
	for id := range count {
		go func() {
			payload := bytes.Repeat([]byte{byte(id + 1)}, hostedWSWriteChunkBytes+17)
			n, err := conn.Write(payload)
			if err == nil && n != len(payload) {
				err = io.ErrShortWrite
			}
			written <- err
		}()
	}
	_ = peer.SetReadDeadline(time.Now().Add(5 * time.Second))
	seen := make(map[byte]bool)
	for range count {
		kind, first, err := peer.ReadMessage()
		if err != nil || kind != websocket.BinaryMessage || len(first) != hostedWSWriteChunkBytes {
			t.Fatal("concurrent write lost its first chunk")
		}
		kind, last, err := peer.ReadMessage()
		id := first[0]
		if err != nil || kind != websocket.BinaryMessage || len(last) != 17 || seen[id] ||
			!bytes.Equal(first, bytes.Repeat([]byte{id}, len(first))) ||
			!bytes.Equal(last, bytes.Repeat([]byte{id}, len(last))) {
			t.Fatal("concurrent writes interleaved or duplicated stream bytes")
		}
		seen[id] = true
	}
	for range count {
		if <-written != nil {
			t.Fatal("concurrent write failed")
		}
	}
}

var errHostedTestWrite = errors.New("injected transport write failure")

// hostedTestWriteGate controls an actual TCP connection after its WS handshake.
// It makes write failure and blocked-write cancellation independent of TCP buffer
// capacity and scheduling. No transport implementation is replaced.
type hostedTestWriteGate struct {
	net.Conn
	mu        sync.Mutex
	enabled   bool
	block     bool
	failAfter int
	calls     int
	entered   chan struct{}
	closed    chan struct{}
	enterOnce sync.Once
	closeOnce sync.Once
}

func newHostedTestWriteGate() *hostedTestWriteGate {
	return &hostedTestWriteGate{entered: make(chan struct{}), closed: make(chan struct{})}
}

func (g *hostedTestWriteGate) arm(block bool, failAfter int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.enabled, g.block, g.failAfter = true, block, failAfter
}

func (g *hostedTestWriteGate) Write(p []byte) (int, error) {
	g.mu.Lock()
	enabled, block := g.enabled, g.block
	if enabled {
		g.calls++
	}
	fail := enabled && !block && g.calls > g.failAfter
	g.mu.Unlock()
	if enabled {
		g.enterOnce.Do(func() { close(g.entered) })
	}
	if block {
		<-g.closed
		return 0, net.ErrClosed
	}
	if fail {
		n, _ := g.Conn.Write(p[:min(17, len(p))])
		return n, errHostedTestWrite
	}
	return g.Conn.Write(p)
}

func (g *hostedTestWriteGate) Close() error {
	g.closeOnce.Do(func() { close(g.closed) })
	return g.Conn.Close()
}

func TestHostedWSConnPartialWriteCannotReplay(t *testing.T) {
	gate := newHostedTestWriteGate()
	client, peer := hostedTestWSPair(t, false, gate)
	conn := newHostedWSConn(client)
	reader := newHostedWSConn(peer)
	defer conn.Close()
	defer reader.Close()
	gate.arm(false, 1)
	payload := bytes.Repeat([]byte{0x43}, 2*hostedWSWriteChunkBytes)
	type result struct {
		n   int
		err error
	}
	written := make(chan result, 1)
	go func() {
		n, err := conn.Write(payload)
		written <- result{n, err}
	}()
	_ = reader.SetReadDeadline(time.Now().Add(5 * time.Second))
	got := make([]byte, hostedWSWriteChunkBytes)
	if _, err := io.ReadFull(reader, got); err != nil || !bytes.Equal(got, payload[:len(got)]) {
		t.Fatal("completed write prefix was lost")
	}
	if n, err := reader.Read(make([]byte, 31)); n != 0 || err == nil {
		t.Fatal("partial failed message exposed bytes")
	}
	resultValue := <-written
	if resultValue.n != hostedWSWriteChunkBytes || !errors.Is(resultValue.err, errHostedTestWrite) {
		t.Fatal("partial write did not report its completed prefix and error")
	}
	hostedTestDone(t, conn.Done())
	if n, err := conn.Write(payload); n != 0 || !errors.Is(err, net.ErrClosed) {
		t.Fatal("failed connection allowed another write")
	}
	gate.mu.Lock()
	calls := gate.calls
	gate.mu.Unlock()
	if calls != 2 {
		t.Fatal("failed write was retried")
	}
}

func TestHostedWSConnActiveWriteDeadlineCanChange(t *testing.T) {
	gate := newHostedTestWriteGate()
	client, _ := hostedTestWSPair(t, false, gate)
	conn := newHostedWSConn(client)
	defer conn.Close()
	gate.arm(true, 0)
	written := make(chan error, 1)
	started := time.Now()
	go func() {
		n, err := conn.Write([]byte("blocked"))
		if n != 0 {
			err = errors.New("blocked write reported accepted bytes")
		}
		written <- err
	}()
	hostedTestDone(t, gate.entered)
	conn.deadlineMu.Lock()
	limit := conn.activeWrite.limit
	conn.deadlineMu.Unlock()
	if limit.Before(started.Add(hostedTransportWriteLimit)) || limit.After(time.Now().Add(hostedTransportWriteLimit)) {
		t.Fatal("active write lacks its default finite limit")
	}
	if err := conn.SetWriteDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatal("could not shorten active deadline")
	}
	if err := conn.SetWriteDeadline(time.Time{}); err != nil {
		t.Fatal("could not clear caller deadline")
	}
	select {
	case <-conn.Done():
		t.Fatal("superseded deadline closed the active write")
	case <-time.After(100 * time.Millisecond):
	}
	if err := conn.SetWriteDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatal("could not expire pending write")
	}
	select {
	case err := <-written:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatal("blocked write did not report deadline expiry")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("changing the deadline did not interrupt the writer")
	}
	hostedTestDone(t, conn.Done())
}

func TestHostedWSConnExpiredIdleDeadlineDoesNotCloseUntilIO(t *testing.T) {
	client, _ := hostedTestWSPair(t, false, nil)
	conn := newHostedWSConn(client)
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatal("could not set idle deadline")
	}
	conn.deadlineMu.Lock()
	active := conn.activeWrite
	conn.deadlineMu.Unlock()
	if active != nil || conn.closed() {
		t.Fatal("idle connection acquired a write timer or closed without I/O")
	}
	if n, err := conn.Write([]byte("expired")); n != 0 || !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatal("past write deadline allowed bytes")
	}
	hostedTestDone(t, conn.Done())
}

func TestHostedWSConnReadDeadlineAndConcurrentClose(t *testing.T) {
	client, _ := hostedTestWSPair(t, false, nil)
	conn := newHostedWSConn(client)
	read := make(chan error, 1)
	go func() {
		_, err := conn.Read(make([]byte, 1))
		read <- err
	}()
	if err := conn.SetReadDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatal("could not expire pending read")
	}
	select {
	case err := <-read:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatal("blocked read did not report deadline expiry")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("read deadline did not unblock the connection")
	}
	var group sync.WaitGroup
	for range 16 {
		group.Go(func() { _ = conn.Close() })
	}
	group.Wait()
	hostedTestDone(t, conn.Done())
	if !errors.Is(conn.SetWriteDeadline(time.Time{}), net.ErrClosed) {
		t.Fatal("closed connection accepted a deadline")
	}
}

type hostedTestHTTP2 struct {
	client     *http2.ClientConn
	clientConn *hostedWSConn
	serverConn *hostedWSConn
	cancel     context.CancelFunc
	done       <-chan struct{}
}

func hostedTestHTTP2Pair(t *testing.T, handler http.Handler) *hostedTestHTTP2 {
	t.Helper()
	clientWS, serverWS := hostedTestWSPair(t, false, nil)
	clientConn, serverConn := newHostedWSConn(clientWS), newHostedWSConn(serverWS)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		serveHostedHTTP2(ctx, serverConn, handler)
	}()
	client, err := newHostedHTTP2ClientConn(clientConn)
	if err != nil {
		cancel()
		t.Fatal("HTTP/2 client construction failed")
	}
	t.Cleanup(func() {
		cancel()
		_ = client.Close()
		_ = clientConn.Close()
		_ = serverConn.Close()
		hostedTestDone(t, done)
	})
	return &hostedTestHTTP2{client, clientConn, serverConn, cancel, done}
}

func TestHostedHTTP2NDJSONStreamingAndCancellation(t *testing.T) {
	cancelled := make(chan struct{})
	pair := hostedTestHTTP2Pair(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = io.WriteString(w, "{\"sequence\":1}\n")
		_ = http.NewResponseController(w).Flush()
		<-r.Context().Done()
		close(cancelled)
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://hosted/stream", nil)
	response, err := pair.client.RoundTrip(request)
	if err != nil {
		t.Fatal("stream request failed")
	}
	defer response.Body.Close()
	line, err := bufio.NewReader(response.Body).ReadString('\n')
	if err != nil || line != "{\"sequence\":1}\n" {
		t.Fatal("NDJSON was not available while the handler was active")
	}
	select {
	case <-cancelled:
		t.Fatal("stream ended before client cancellation")
	default:
	}
	cancel()
	hostedTestDone(t, cancelled)
	freshContext, freshCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer freshCancel()
	request, _ = http.NewRequestWithContext(freshContext, http.MethodGet, "http://hosted/health", nil)
	response, err = pair.client.RoundTrip(request)
	if err != nil {
		t.Fatal("cancelling a stream destroyed the shared HTTP/2 connection")
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusNoContent || pair.clientConn.closed() || pair.serverConn.closed() {
		t.Fatal("connection was not reusable after stream cancellation")
	}
}

func TestHostedHTTP2ConcurrentRequests(t *testing.T) {
	const count = 12
	entered := make(chan struct{}, count)
	release := make(chan struct{})
	pair := hostedTestHTTP2Pair(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		select {
		case <-release:
			_, _ = io.Copy(w, r.Body)
		case <-r.Context().Done():
		}
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	results := make(chan error, count)
	for id := range count {
		go func() {
			payload := bytes.Repeat([]byte{byte(id + 1)}, 64<<10)
			request, _ := http.NewRequestWithContext(ctx, http.MethodPost, "http://hosted/echo", bytes.NewReader(payload))
			response, err := pair.client.RoundTrip(request)
			if err == nil {
				var got []byte
				got, err = io.ReadAll(response.Body)
				_ = response.Body.Close()
				if err == nil && !bytes.Equal(got, payload) {
					err = errors.New("multiplexed request content changed")
				}
			}
			results <- err
		}()
	}
	for range count {
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("requests were serialized instead of multiplexed")
		}
	}
	close(release)
	for range count {
		if <-results != nil {
			t.Fatal("concurrent HTTP/2 request failed")
		}
	}
}

func TestHostedHTTP2HeaderAndStreamBounds(t *testing.T) {
	var called atomic.Int32
	pair := hostedTestHTTP2Pair(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called.Add(1)
		if r.URL.Path == "/large-response" {
			w.Header().Set("X-Large", strings.Repeat("x", 2*hostedHTTP2MaxHeaderBytes))
		}
		w.WriteHeader(http.StatusOK)
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://hosted/ready", nil)
	response, err := pair.client.RoundTrip(request)
	if err != nil {
		t.Fatal("initial settings exchange failed")
	}
	_ = response.Body.Close()
	if pair.client.State().MaxConcurrentStreams != hostedHTTP2MaxStreams {
		t.Fatal("server did not advertise the stream bound")
	}
	request, _ = http.NewRequestWithContext(ctx, http.MethodGet, "http://hosted/large-request", nil)
	request.Header.Set("X-Large", strings.Repeat("x", 2*hostedHTTP2MaxHeaderBytes))
	if response, err = pair.client.RoundTrip(request); err == nil {
		_ = response.Body.Close()
		t.Fatal("oversized request headers were accepted")
	}
	if called.Load() != 1 {
		t.Fatal("oversized headers reached the handler")
	}
	request, _ = http.NewRequestWithContext(ctx, http.MethodGet, "http://hosted/large-response", nil)
	if response, err = pair.client.RoundTrip(request); err == nil {
		_ = response.Body.Close()
		t.Fatal("oversized response headers were accepted")
	}
}

func TestHostedHTTP2SaturatedStreamCanBeCancelledWithoutDispatch(t *testing.T) {
	entered := make(chan struct{}, hostedHTTP2MaxStreams+1)
	release := make(chan struct{})
	pair := hostedTestHTTP2Pair(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		select {
		case <-release:
			w.WriteHeader(http.StatusNoContent)
		case <-r.Context().Done():
		}
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	results := make(chan error, hostedHTTP2MaxStreams)
	for range hostedHTTP2MaxStreams {
		go func() {
			request, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://hosted/hold", nil)
			response, err := pair.client.RoundTrip(request)
			if response != nil {
				_ = response.Body.Close()
			}
			results <- err
		}()
	}
	for range hostedHTTP2MaxStreams {
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("stream capacity could not be filled")
		}
	}
	queued, cancelQueued := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancelQueued()
	request, _ := http.NewRequestWithContext(queued, http.MethodGet, "http://hosted/queued", nil)
	response, err := pair.client.RoundTrip(request)
	if response != nil {
		_ = response.Body.Close()
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("queued stream did not observe cancellation")
	}
	select {
	case <-entered:
		t.Fatal("request exceeded the concurrent stream bound")
	default:
	}
	close(release)
	for range hostedHTTP2MaxStreams {
		if <-results != nil {
			t.Fatal("queued cancellation disturbed an admitted stream")
		}
	}
}

func TestHostedHTTP2AcceptedRequestIsNotReplayedAfterDisconnect(t *testing.T) {
	var calls, replays atomic.Int32
	sever := make(chan *hostedWSConn, 1)
	pair := hostedTestHTTP2Pair(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		calls.Add(1)
		_ = (<-sever).Close()
	}))
	sever <- pair.serverConn
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	request, _ := http.NewRequestWithContext(ctx, http.MethodPost, "http://hosted/mutation", strings.NewReader("once"))
	request.Header.Set("Idempotency-Key", "fixture")
	request.GetBody = func() (io.ReadCloser, error) {
		replays.Add(1)
		return io.NopCloser(strings.NewReader("once")), nil
	}
	if response, err := pair.client.RoundTrip(request); err == nil {
		_ = response.Body.Close()
		t.Fatal("severed accepted request unexpectedly succeeded")
	}
	hostedTestDone(t, pair.clientConn.Done())
	if calls.Load() != 1 || replays.Load() != 0 {
		t.Fatal("accepted mutation was replayed after connection loss")
	}
	request, _ = http.NewRequestWithContext(ctx, http.MethodPost, "http://hosted/after-loss", nil)
	if response, err := pair.client.RoundTrip(request); err == nil {
		_ = response.Body.Close()
		t.Fatal("closed ClientConn acquired a replacement connection")
	}
	if calls.Load() != 1 {
		t.Fatal("closed connection dispatched another mutation")
	}
}

func TestHostedHTTP2ServerContextClosesIdleConnection(t *testing.T) {
	pair := hostedTestHTTP2Pair(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	pair.cancel()
	hostedTestDone(t, pair.done)
	hostedTestDone(t, pair.serverConn.Done())
	hostedTestDone(t, pair.clientConn.Done())
}
