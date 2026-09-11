package hosted

import (
	"bytes"
	"errors"
	"log"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

const hostedTestCloseText = "synthetic-peer-text https://fixture.invalid/private?token=synthetic-secret"

type hostedTestChannelLog struct {
	mu     sync.Mutex
	data   bytes.Buffer
	logger *log.Logger
}

func newHostedTestChannelLog() *hostedTestChannelLog {
	l := &hostedTestChannelLog{}
	l.logger = log.New(l, "", 0)
	return l
}

func (l *hostedTestChannelLog) Write(data []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.data.Write(data)
}

func (l *hostedTestChannelLog) text() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.data.String()
}

type hostedTestChannelRecord struct {
	role     string
	openedAt time.Time
	duration time.Duration
	code     int
}

func (l *hostedTestChannelLog) records(t *testing.T) []hostedTestChannelRecord {
	t.Helper()
	data := l.text()
	if strings.Contains(data, hostedTestCloseText) || strings.Contains(data, "synthetic-secret") {
		t.Fatal("channel lifecycle log exposed peer text")
	}
	if data == "" {
		return nil
	}
	pattern := regexp.MustCompile(`^Foundry hosted channel closed role=(forward|reverse) opened_at=([^ ]+) duration_ms=([0-9]+) close_code=([0-9]+)$`)
	var records []hostedTestChannelRecord
	for _, line := range strings.Split(strings.TrimSuffix(data, "\n"), "\n") {
		fields := pattern.FindStringSubmatch(line)
		if fields == nil {
			t.Fatal("channel lifecycle log contains fields outside the safe schema")
		}
		openedAt, timeErr := time.Parse(time.RFC3339Nano, fields[2])
		duration, durationErr := strconv.ParseInt(fields[3], 10, 64)
		code, codeErr := strconv.Atoi(fields[4])
		if timeErr != nil || durationErr != nil || codeErr != nil {
			t.Fatal("channel lifecycle log contains invalid safe metadata")
		}
		records = append(records, hostedTestChannelRecord{fields[1], openedAt, time.Duration(duration) * time.Millisecond, code})
	}
	return records
}

func hostedTestGoingAway(t *testing.T, ws *websocket.Conn) {
	t.Helper()
	if ws.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseGoingAway, hostedTestCloseText), time.Now().Add(time.Second)) != nil {
		t.Fatal("could not send synthetic platform close code 1001")
	}
}

func hostedTestClosedChannelLogs(t *testing.T, logs *hostedTestChannelLog, affectedRole string, earliest, latest time.Time) {
	t.Helper()
	hostedServerWait(t, func() bool { return len(logs.records(t)) == 2 })
	seen := map[string]bool{}
	for _, record := range logs.records(t) {
		if seen[record.role] || record.openedAt.Before(earliest) || record.openedAt.After(latest) ||
			record.duration > time.Since(record.openedAt) {
			t.Fatal("channel summary duplicated a role or lost its actual upgrade time")
		}
		seen[record.role] = true
		if record.role == affectedRole && record.code != websocket.CloseGoingAway {
			t.Fatal("affected channel did not retain observed close code 1001")
		}
	}
	if !seen["forward"] || !seen["reverse"] {
		t.Fatal("closed lifetime did not summarize both roles")
	}
}

func TestHostedWSConnCloseSummaryPreservesHandlerAndExcludesPeerText(t *testing.T) {
	client, peer := hostedTestWSPair(t, false, nil)
	logs := newHostedTestChannelLog()
	openedAt := time.Now().Add(-time.Second)
	previousCalled := false
	previous := client.CloseHandler()
	client.SetCloseHandler(func(code int, text string) error {
		previousCalled = code == websocket.CloseGoingAway && text == hostedTestCloseText
		return previous(code, text)
	})
	observation := observeHostedChannel(client, "forward", openedAt, logs.logger)
	conn := newHostedObservedWSConn(client, observation)
	defer conn.Close()
	hostedTestGoingAway(t, peer)
	_, err := conn.Read(make([]byte, 1))
	var closed *websocket.CloseError
	if !errors.As(err, &closed) || closed.Code != websocket.CloseGoingAway || closed.Text != hostedTestCloseText || !previousCalled {
		t.Fatal("observation changed the original close handler or returned error")
	}
	_ = peer.SetReadDeadline(time.Now().Add(time.Second))
	_, _, err = peer.ReadMessage()
	if !errors.As(err, &closed) || closed.Code != websocket.CloseGoingAway {
		t.Fatal("observation changed Gorilla's close reply")
	}
	_ = conn.Close()
	observation.finish()
	records := logs.records(t)
	if len(records) != 1 || records[0].role != "forward" || records[0].code != websocket.CloseGoingAway ||
		!records[0].openedAt.Equal(openedAt) || records[0].duration < time.Second {
		t.Fatal("channel summary lost its original time/code or was emitted more than once")
	}
}

func TestHostedWSConnCloseSummaryDistinguishesUnobservedAndAbnormal(t *testing.T) {
	for _, abrupt := range []bool{false, true} {
		name := "local close without observed code"
		if abrupt {
			name = "peer EOF without close frame"
		}
		t.Run(name, func(t *testing.T) {
			client, peer := hostedTestWSPair(t, false, nil)
			logs := newHostedTestChannelLog()
			observation := observeHostedChannel(client, "reverse", time.Now(), logs.logger)
			conn := newHostedObservedWSConn(client, observation)
			want := 0
			if abrupt {
				_ = peer.Close()
				_, _ = conn.Read(make([]byte, 1))
				want = websocket.CloseAbnormalClosure
			} else {
				observation.observeError(errors.New(hostedTestCloseText))
			}
			var closed sync.WaitGroup
			for range 8 {
				closed.Go(func() { _ = conn.Close() })
			}
			closed.Wait()
			records := logs.records(t)
			if len(records) != 1 || records[0].code != want {
				t.Fatal("channel summary fabricated a code or duplicated concurrent closure")
			}
		})
	}
}
