package main

import (
	"bufio"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
)

func TestACPReadLineRequiresBoundedNewlineFrames(t *testing.T) {
	for _, test := range []struct {
		name  string
		input string
		want  string
		err   error
	}{
		{name: "LF", input: " {\"id\":1}\n", want: `{"id":1}`},
		{name: "CRLF", input: "\t{\"id\":1}\r\n", want: `{"id":1}`},
		{name: "blank", input: " \t\r\n"},
		{name: "empty EOF", err: io.EOF},
		{name: "unterminated object", input: `{"id":1}`, err: acpInvalidRequest},
		{name: "unterminated whitespace", input: " ", err: acpInvalidRequest},
		{name: "exact bound", input: strings.Repeat("x", acpMaxMessageBytes-1) + "\n", want: strings.Repeat("x", acpMaxMessageBytes-1)},
		{name: "over bound", input: strings.Repeat("x", acpMaxMessageBytes) + "\n", err: acpInvalidRequest},
	} {
		t.Run(test.name, func(t *testing.T) {
			// A small reader forces the multi-fragment path independently of
			// the operating system's pipe scheduling.
			reader := bufio.NewReaderSize(strings.NewReader(test.input), 31)
			line, err := acpReadLine(reader)
			if !errors.Is(err, test.err) || string(line) != test.want {
				t.Fatal("newline framing or its byte limit changed")
			}
		})
	}
	reader := bufio.NewReaderSize(strings.NewReader("first\nsecond\n"), 31)
	for _, want := range []string{"first", "second"} {
		if line, err := acpReadLine(reader); err != nil || string(line) != want {
			t.Fatal("adjacent newline frames were combined or lost")
		}
	}
	if _, err := acpReadLine(reader); !errors.Is(err, io.EOF) {
		t.Fatal("complete frames did not terminate with EOF")
	}
}

func TestACPInvalidEnvelopesCannotReachProvider(t *testing.T) {
	var requests atomic.Int32
	peer := newACPTestPeer(t, toolSchemaModeRequest, func(w http.ResponseWriter, r *http.Request) {
		acpTestReadProvider(t, r)
		requests.Add(1)
		acpTestCompleted(w, "valid", "ok")
	}, &acpTestMCP{})
	for _, test := range []struct {
		input string
		code  float64
	}{
		{`{`, -32700},
		{`[]`, -32600},
		{`null`, -32600},
		{`{"jsonrpc":"1.0","id":100,"method":"initialize"}`, -32600},
		{`{"jsonrpc":"2.0","id":null,"method":"initialize"}`, -32600},
		{`{"jsonrpc":"2.0","id":true,"method":"initialize"}`, -32600},
		{`{"jsonrpc":"2.0","id":1.5,"method":"initialize"}`, -32600},
		{`{"jsonrpc":"2.0","id":100,"method":"initialize","result":{}}`, -32600},
		{`{"jsonrpc":"2.0","id":100,"method":"initialize","params":{"a":1,"a":2}}`, -32600},
		{`{"jsonrpc":"2.0","id":100,"id":101,"method":"initialize"}`, -32600},
	} {
		if _, err := io.WriteString(peer.in, test.input+"\n"); err != nil {
			t.Fatal("could not send invalid fixture envelope")
		}
		reply := peer.read()
		failure, ok := reply["error"].(map[string]any)
		if !ok || failure["code"] != test.code || reply["id"] != nil || reply["result"] != nil {
			t.Fatal("malformed envelope did not produce an uncorrelated protocol error")
		}
	}
	if requests.Load() != 0 {
		t.Fatal("invalid envelope invoked provider")
	}
	acpAssertStop(t, peer.reply(peer.prompt("valid after malformed envelopes")), "end_turn")
	if requests.Load() != 1 {
		t.Fatal("malformed envelope changed a valid session")
	}
}
