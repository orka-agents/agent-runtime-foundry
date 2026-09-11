package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
)

func hostedBoundaryLedgerFixture(t *testing.T) (hostedGatewayConfig, hostedGatewayLedger) {
	t.Helper()
	f := newHostedProtocolFixture(t)
	cfg := hostedGatewayConfig{
		Protocol: hostedProtocol, Image: f.config,
		ContainerImage: "example.invalid/hosted@" + brokerSHA([]byte("fixture image")),
		SessionID:      f.hello.Challenge.SessionID, RuntimeProfileDigest: brokerSHA([]byte("fixture profile")),
		RuntimeEnvironment: maps.Clone(f.bootstrap.Environment),
		OrkaBaseURL:        "http://orka.test:8080", BrokerBaseURL: "http://127.0.0.1:8091",
	}
	ledger := hostedGatewayLedger{
		Version: 1, ConfigDigest: brokerJSONDigest(cfg), SessionID: cfg.SessionID,
		PrincipalDigest: brokerSHA([]byte("fixture principal")), CreateAttempted: true, SessionCreated: true,
		ExposurePossible: true, Challenge: f.hello.Challenge, PairID: f.hello.PairID,
		BootstrapDigest: f.hello.BootstrapDigest,
	}
	if validateHostedGatewayConfig(cfg) != nil || !hostedGatewayLedgerValid(ledger, cfg) {
		t.Fatal("invalid hosted boundary fixture")
	}
	return cfg, ledger
}

func hostedBoundaryWriteLedger(t *testing.T, data []byte) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "gateway")
	if os.Mkdir(dir, 0o700) != nil || os.WriteFile(filepath.Join(dir, "state.json"), data, 0o600) != nil {
		t.Fatal("could not prepare private ledger fixture")
	}
	return dir
}

func TestHostedBoundaryLedgerRejectsMixedAndMalformedExposure(t *testing.T) {
	cfg, baseline := hostedBoundaryLedgerFixture(t)
	for name, mutate := range map[string]func(*hostedGatewayLedger){
		"version":              func(v *hostedGatewayLedger) { v.Version++ },
		"config digest":        func(v *hostedGatewayLedger) { v.ConfigDigest = brokerSHA([]byte("other config")) },
		"logical session":      func(v *hostedGatewayLedger) { v.SessionID = uuid.NewString() },
		"missing principal":    func(v *hostedGatewayLedger) { v.PrincipalDigest = "" },
		"malformed principal":  func(v *hostedGatewayLedger) { v.PrincipalDigest = "invalid" },
		"unattempted creation": func(v *hostedGatewayLedger) { v.CreateAttempted = false },
		"unexposed creation without principal": func(v *hostedGatewayLedger) {
			*v = hostedGatewayLedger{Version: v.Version, ConfigDigest: v.ConfigDigest, SessionID: v.SessionID, CreateAttempted: true}
		},
		"uncreated exposure":   func(v *hostedGatewayLedger) { v.SessionCreated = false },
		"unrecorded exposure":  func(v *hostedGatewayLedger) { v.ExposurePossible = false },
		"missing pair":         func(v *hostedGatewayLedger) { v.PairID = "" },
		"nil pair":             func(v *hostedGatewayLedger) { v.PairID = uuid.Nil.String() },
		"noncanonical pair":    func(v *hostedGatewayLedger) { v.PairID = "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA" },
		"missing bootstrap":    func(v *hostedGatewayLedger) { v.BootstrapDigest = "" },
		"malformed bootstrap":  func(v *hostedGatewayLedger) { v.BootstrapDigest = "invalid" },
		"challenge protocol":   func(v *hostedGatewayLedger) { v.Challenge.Protocol = "other" },
		"challenge deployment": func(v *hostedGatewayLedger) { v.Challenge.DeploymentID = uuid.NewString() },
		"challenge config": func(v *hostedGatewayLedger) {
			v.Challenge.ConfigurationDigest = brokerSHA([]byte("other image config"))
		},
		"challenge agent":   func(v *hostedGatewayLedger) { v.Challenge.AgentName = "other-agent" },
		"challenge version": func(v *hostedGatewayLedger) { v.Challenge.AgentVersion = "9" },
		"challenge session": func(v *hostedGatewayLedger) { v.Challenge.SessionID = uuid.NewString() },
		"missing boot":      func(v *hostedGatewayLedger) { v.Challenge.BootID = "" },
		"nil boot":          func(v *hostedGatewayLedger) { v.Challenge.BootID = uuid.Nil.String() },
		"noncanonical boot": func(v *hostedGatewayLedger) { v.Challenge.BootID = "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA" },
		"missing nonce":     func(v *hostedGatewayLedger) { v.Challenge.Nonce = "" },
		"padded nonce":      func(v *hostedGatewayLedger) { v.Challenge.Nonce += "=" },
		"zero nonce": func(v *hostedGatewayLedger) {
			v.Challenge.Nonce = base64.RawURLEncoding.EncodeToString(make([]byte, 32))
		},
	} {
		t.Run(name, func(t *testing.T) {
			ledger := baseline
			mutate(&ledger)
			data, err := json.Marshal(ledger)
			if err != nil {
				t.Fatal("could not serialize invalid-state fixture")
			}
			dir := hostedBoundaryWriteLedger(t, data)
			store, _, err := openHostedGatewayStore(dir, cfg)
			if err == nil {
				store.close()
				t.Fatal("malformed exposure was accepted for recovery")
			}
			after, readErr := os.ReadFile(filepath.Join(dir, "state.json"))
			if readErr != nil || !bytes.Equal(after, data) {
				t.Fatal("rejected ledger was changed or discarded")
			}
		})
	}
}

func TestHostedBoundaryLedgerRejectsMalformedEncodingAndConfigChanges(t *testing.T) {
	cfg, ledger := hostedBoundaryLedgerFixture(t)
	baseline, err := json.Marshal(ledger)
	if err != nil {
		t.Fatal("could not serialize fixture")
	}
	malformed := map[string][]byte{
		"empty object":    []byte("{}"),
		"null":            []byte("null"),
		"truncated":       baseline[:len(baseline)-1],
		"duplicate field": append([]byte(`{"version":1,`), baseline[1:]...),
		"unknown field":   append([]byte(`{"invented":true,`), baseline[1:]...),
		"trailing object": append(bytes.Clone(baseline), []byte("{}")...),
		"too large":       bytes.Repeat([]byte(" "), hostedMaxHandshakeBytes+1),
	}
	for _, field := range []string{"version", "configDigest", "sessionID"} {
		var fields map[string]json.RawMessage
		if json.Unmarshal(baseline, &fields) != nil {
			t.Fatal("could not decode baseline fixture")
		}
		delete(fields, field)
		malformed["missing "+field], err = json.Marshal(fields)
		if err != nil {
			t.Fatal("could not encode missing identity fixture")
		}
		fields[field] = json.RawMessage("null")
		malformed["null "+field], err = json.Marshal(fields)
		if err != nil {
			t.Fatal("could not encode null identity fixture")
		}
	}
	for name, data := range malformed {
		t.Run(name, func(t *testing.T) {
			store, _, err := openHostedGatewayStore(hostedBoundaryWriteLedger(t, data), cfg)
			if err == nil {
				store.close()
				t.Fatal("malformed ledger encoding was accepted")
			}
		})
	}
	for name, mutate := range map[string]func(*hostedGatewayConfig){
		"session": func(c *hostedGatewayConfig) { c.SessionID = uuid.NewString() },
		"profile": func(c *hostedGatewayConfig) { c.RuntimeProfileDigest = brokerSHA([]byte("other profile")) },
		"image": func(c *hostedGatewayConfig) {
			c.ContainerImage = "example.invalid/hosted@" + brokerSHA([]byte("other image"))
		},
		"target":      func(c *hostedGatewayConfig) { c.Image.Target.AgentVersion = "9" },
		"destination": func(c *hostedGatewayConfig) { c.OrkaBaseURL = "http://other.test:8080" },
		"epoch":       func(c *hostedGatewayConfig) { c.RuntimeEnvironment["ORKA_ACP_CONTROLLER_EPOCH"] = "2" },
	} {
		t.Run(name, func(t *testing.T) {
			changed := cfg
			changed.RuntimeEnvironment = maps.Clone(cfg.RuntimeEnvironment)
			mutate(&changed)
			store, _, err := openHostedGatewayStore(hostedBoundaryWriteLedger(t, baseline), changed)
			if err == nil {
				store.close()
				t.Fatal("existing gateway ownership was rebound to different configuration")
			}
		})
	}
}

func TestHostedBoundaryLedgerPersistsExposureWithExclusivePrivateOwnership(t *testing.T) {
	cfg, exposed := hostedBoundaryLedgerFixture(t)
	dir := filepath.Join(t.TempDir(), "gateway")
	store, initial, err := openHostedGatewayStore(dir, cfg)
	if err != nil || initial.ExposurePossible || initial.CreateAttempted || initial.SessionCreated || initial.Ready || initial.Closed {
		t.Fatal("new ledger was not empty")
	}
	defer store.close()
	other, _, err := openHostedGatewayStore(dir, cfg)
	if err == nil {
		other.close()
		t.Fatal("two gateway writers acquired the same ledger")
	}
	oldFile, err := os.Open(filepath.Join(dir, "state.json"))
	if err != nil {
		t.Fatal("could not pin prior state inode")
	}
	defer oldFile.Close()
	initialBytes, err := io.ReadAll(oldFile)
	if err != nil {
		t.Fatal("could not read prior state")
	}
	if store.save(exposed) != nil {
		t.Fatal("exposure could not be persisted")
	}
	if _, err := oldFile.Seek(0, io.SeekStart); err != nil {
		t.Fatal("could not reread prior inode")
	}
	oldBytes, err := io.ReadAll(oldFile)
	if err != nil || !bytes.Equal(oldBytes, initialBytes) {
		t.Fatal("save overwrote the existing ledger inode instead of replacing it")
	}
	for _, name := range []string{"state.json", "gateway.lock"} {
		info, err := os.Lstat(filepath.Join(dir, name))
		if err != nil || !brokerPrivateFile(info) {
			t.Fatal("gateway ownership file is not private, singly linked and locally owned")
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 2 {
		t.Fatal("save left temporary ownership files")
	}
	store.close()
	store, restored, err := openHostedGatewayStore(dir, cfg)
	if err != nil || !reflect.DeepEqual(restored, exposed) {
		t.Fatal("restart lost or changed possible exposure")
	}
	defer store.close()
	restored.Ready, restored.Closed = true, true
	if store.save(restored) != nil {
		t.Fatal("closed ownership could not be persisted")
	}
	store.close()
	store, terminal, err := openHostedGatewayStore(dir, cfg)
	if err != nil || !reflect.DeepEqual(terminal, restored) {
		t.Fatal("closed ledger was reset or rebound on restart")
	}
	store.close()
}

func TestHostedBoundaryLedgerRejectsNonprivateAndAliasedFiles(t *testing.T) {
	cfg, baseline := hostedBoundaryLedgerFixture(t)
	data, _ := json.Marshal(baseline)
	for _, name := range []string{"directory permissions", "state permissions", "lock permissions", "state symlink", "lock symlink", "state hardlink", "lock hardlink"} {
		t.Run(name, func(t *testing.T) {
			dir := hostedBoundaryWriteLedger(t, data)
			lock := filepath.Join(dir, "gateway.lock")
			if os.WriteFile(lock, nil, 0o600) != nil {
				t.Fatal("could not create lock fixture")
			}
			target := filepath.Join(dir, "state.json")
			if strings.HasPrefix(name, "lock") {
				target = lock
			}
			var err error
			switch {
			case name == "directory permissions":
				err = os.Chmod(dir, 0o755)
			case strings.HasSuffix(name, "permissions"):
				err = os.Chmod(target, 0o644)
			case strings.HasSuffix(name, "symlink"):
				original := target + "-original"
				if err = os.Rename(target, original); err == nil {
					err = os.Symlink(original, target)
				}
			case strings.HasSuffix(name, "hardlink"):
				err = os.Link(target, target+"-alias")
			}
			if err != nil {
				t.Fatal("could not prepare unsafe file fixture")
			}
			store, _, err := openHostedGatewayStore(dir, cfg)
			if err == nil {
				store.close()
				t.Fatal("nonprivate or aliased ownership file was accepted")
			}
		})
	}
	for _, dir := range []string{"", ".", "relative/gateway", "/"} {
		store, _, err := openHostedGatewayStore(dir, cfg)
		if err == nil {
			store.close()
			t.Fatal("unsafe state directory was accepted")
		}
	}
}

func TestHostedBoundaryLedgerSaveFailurePreservesPriorExposure(t *testing.T) {
	cfg, exposed := hostedBoundaryLedgerFixture(t)
	data, _ := json.Marshal(exposed)
	dir := hostedBoundaryWriteLedger(t, data)
	store, _, err := openHostedGatewayStore(dir, cfg)
	if err != nil {
		t.Fatal("could not open exposure fixture")
	}
	defer store.close()
	moved := dir + "-moved"
	if os.Rename(dir, moved) != nil {
		t.Fatal("could not inject persistence failure")
	}
	exposed.Ready = true
	if store.save(exposed) == nil {
		t.Fatal("save succeeded after state directory disappeared")
	}
	retained, err := os.ReadFile(filepath.Join(moved, "state.json"))
	if err != nil || !bytes.Equal(retained, data) {
		t.Fatal("failed save changed the prior exposure record")
	}
}

type hostedBoundaryRoundTripper func(*http.Request) (*http.Response, error)

func (f hostedBoundaryRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestHostedBoundaryProxyRoutes(t *testing.T) {
	digest := strings.Repeat("a", 64)
	for _, test := range []struct {
		name, method, path string
		allow              func(*http.Request) bool
		accepted           bool
	}{
		{"v2 health", http.MethodGet, "/v2/health", hostedV2Route, true},
		{"v2 prompt", http.MethodPost, "/v2/sessions/session/prompts", hostedV2Route, true},
		{"v2 lease", http.MethodPatch, "/v2/sessions/session", hostedV2Route, true},
		{"v2 delete", http.MethodDelete, "/v2/sessions/session", hostedV2Route, true},
		{"v2 replay query", http.MethodGet, "/v2/prompts/prompt/events?cursor=7", hostedV2Route, true},
		{"v1 denied", http.MethodPost, "/v1/sessions", hostedV2Route, false},
		{"v2 lookalike", http.MethodPost, "/v20/sessions", hostedV2Route, false},
		{"v2 unsupported method", http.MethodTrace, "/v2/health", hostedV2Route, false},
		{"broker responses", http.MethodPost, brokerResponsesPath, hostedBrokerRoute, true},
		{"broker renew", http.MethodPost, brokerRenewPath, hostedBrokerRoute, true},
		{"broker settle", http.MethodPost, brokerSettlePath, hostedBrokerRoute, true},
		{"broker retire", http.MethodPost, brokerRetirePath, hostedBrokerRoute, true},
		{"broker status GET", http.MethodGet, brokerStatusPath, hostedBrokerRoute, true},
		{"broker status POST denied", http.MethodPost, brokerStatusPath, hostedBrokerRoute, false},
		{"broker response GET denied", http.MethodGet, brokerResponsesPath, hostedBrokerRoute, false},
		{"broker query denied", http.MethodPost, brokerResponsesPath + "?target=other", hostedBrokerRoute, false},
		{"broker arbitrary path denied", http.MethodPost, "/v1/other", hostedBrokerRoute, false},
		{"Orka tool", http.MethodPost, "/internal/v2/acp/mcp/tools/call", hostedOrkaRoute, true},
		{"Orka artifact authorization", http.MethodPost, "/internal/v2/acp/artifact-authorizations", hostedOrkaRoute, true},
		{"Orka artifact read", http.MethodGet, "/internal/v2/acp/artifacts/sha256/" + digest, hostedOrkaRoute, true},
		{"Orka artifact write", http.MethodPut, "/internal/v2/acp/artifacts/sha256/" + digest, hostedOrkaRoute, true},
		{"Orka artifact head", http.MethodHead, "/internal/v2/acp/artifacts/sha256/" + digest, hostedOrkaRoute, true},
		{"Orka artifact delete denied", http.MethodDelete, "/internal/v2/acp/artifacts/sha256/" + digest, hostedOrkaRoute, false},
		{"Orka artifact bad digest", http.MethodGet, "/internal/v2/acp/artifacts/sha256/" + digest[:63], hostedOrkaRoute, false},
		{"Orka arbitrary API denied", http.MethodPost, "/api/tasks", hostedOrkaRoute, false},
		{"Orka query denied", http.MethodPost, "/internal/v2/acp/mcp/tools/call?target=other", hostedOrkaRoute, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			proxy, err := newHostedProxy("http://fixed.invalid", hostedBoundaryRoundTripper(func(*http.Request) (*http.Response, error) {
				calls++
				return &http.Response{StatusCode: http.StatusNoContent, Header: make(http.Header), Body: http.NoBody}, nil
			}), test.allow)
			if err != nil {
				t.Fatal("could not construct proxy fixture")
			}
			response := httptest.NewRecorder()
			proxy.ServeHTTP(response, httptest.NewRequest(test.method, test.path, nil))
			if test.accepted && (calls != 1 || response.Code != http.StatusNoContent) ||
				!test.accepted && (calls != 0 || response.Code != http.StatusForbidden) {
				t.Fatal("route did not enforce the downstream method/path contract")
			}
		})
	}
}

func TestHostedBoundaryProxyRejectsAmbiguousPathsBeforeTransport(t *testing.T) {
	for name, mutate := range map[string]func(*http.Request){
		"relative":        func(r *http.Request) { r.URL.Path = "v2/health" },
		"dot segment":     func(r *http.Request) { r.URL.Path = "/v2/../health" },
		"duplicate slash": func(r *http.Request) { r.URL.Path = "/v2//health" },
		"backslash":       func(r *http.Request) { r.URL.Path = "/v2/health\\extra" },
		"NUL":             func(r *http.Request) { r.URL.Path = "/v2/health\x00" },
		"escaped path":    func(r *http.Request) { r.URL.RawPath = "/v2/%68ealth" },
		"fragment":        func(r *http.Request) { r.URL.Fragment = "fragment" },
		"userinfo":        func(r *http.Request) { r.URL.User = url.User("fixture") },
		"opaque":          func(r *http.Request) { r.URL.Opaque = "//other.invalid/v2/health" },
		"oversized path":  func(r *http.Request) { r.URL.Path = "/v2/" + strings.Repeat("x", 4096) },
		"oversized query": func(r *http.Request) { r.URL.RawQuery = strings.Repeat("x", 8193) },
		"upgrade":         func(r *http.Request) { r.Header.Set("Upgrade", "websocket") },
		"CONNECT":         func(r *http.Request) { r.Method = http.MethodConnect },
		"nil URL":         func(r *http.Request) { r.URL = nil },
	} {
		t.Run(name, func(t *testing.T) {
			calls := 0
			proxy, err := newHostedProxy("http://fixed.invalid", hostedBoundaryRoundTripper(func(*http.Request) (*http.Response, error) {
				calls++
				return nil, errors.New("disallowed request reached transport")
			}), hostedV2Route)
			if err != nil {
				t.Fatal("could not construct proxy fixture")
			}
			request := httptest.NewRequest(http.MethodGet, "/v2/health", nil)
			mutate(request)
			response := httptest.NewRecorder()
			proxy.ServeHTTP(response, request)
			if calls != 0 || response.Code != http.StatusForbidden {
				t.Fatal("ambiguous request reached the fixed destination")
			}
		})
	}
}

func TestHostedBoundaryProxyPinsDestinationAndPreservesOnlySuppliedAuthorization(t *testing.T) {
	for _, test := range []struct {
		name, path string
		allow      func(*http.Request) bool
	}{
		{"broker", brokerRenewPath, hostedBrokerRoute},
		{"Orka", "/internal/v2/acp/mcp/tools/call", hostedOrkaRoute},
		{"supervisor", "/v2/sessions", hostedV2Route},
	} {
		for _, authorization := range [][]string{nil, {"Bearer fixture-incoming"}, {"Bearer fixture-first", "Bearer fixture-second"}} {
			t.Run(test.name+"-"+strconv.Itoa(len(authorization)), func(t *testing.T) {
				calls := 0
				payload := []byte("fixture request")
				proxy, err := newHostedProxy("http://fixed.invalid:8091", hostedBoundaryRoundTripper(func(r *http.Request) (*http.Response, error) {
					calls++
					body, readErr := io.ReadAll(r.Body)
					if readErr != nil || !bytes.Equal(body, payload) || r.URL.Scheme != "http" ||
						r.URL.Host != "fixed.invalid:8091" || r.Host != "fixed.invalid:8091" || r.URL.Path != test.path ||
						!reflect.DeepEqual(r.Header.Values("Authorization"), authorization) || r.GetBody != nil {
						t.Error("proxy changed authority/body or allowed caller destination selection")
					}
					if r.Header.Get("Forwarded") != "" || r.Header.Get("X-Forwarded-Host") != "" {
						t.Error("proxy retained untrusted forwarded routing headers")
					}
					return &http.Response{StatusCode: http.StatusAccepted, Header: make(http.Header), Body: http.NoBody}, nil
				}), test.allow)
				if err != nil {
					t.Fatal("could not construct proxy fixture")
				}
				request := httptest.NewRequest(http.MethodPost, "https://untrusted.invalid"+test.path, bytes.NewReader(payload))
				request.Host = "also-untrusted.invalid"
				request.Header["Authorization"] = authorization
				request.Header.Set("X-Foundry-Context", "unsigned-fixture-context")
				request.Header.Set("Forwarded", "host=untrusted.invalid;proto=https")
				request.Header.Set("X-Forwarded-Host", "untrusted.invalid")
				request.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(payload)), nil }
				response := httptest.NewRecorder()
				proxy.ServeHTTP(response, request)
				if response.Code != http.StatusAccepted || calls != 1 {
					t.Fatal("authorized fixed-route fixture failed or retried")
				}
			})
		}
	}
	for _, target := range []string{"", "file:///tmp/socket", "unix:///tmp/socket", "http:///missing-host", "http://fixed.invalid/base"} {
		if _, err := newHostedProxy(target, nil, hostedV2Route); err == nil {
			t.Fatal("invalid proxy destination was accepted")
		}
	}
	for _, target := range []string{"http://remote.invalid", "http://localhost:8091", "http://10.0.0.1:8091", "http://127.0.0.1:8091/base", "http://127.0.0.1:8091?target=other", "http://user@127.0.0.1:8091"} {
		if hostedRelayTargetValid(target, true) {
			t.Fatal("broker destination escaped its configured loopback boundary")
		}
	}
}

func TestHostedBoundaryProxyPreservesStreamingStatusHeadersAndTrailers(t *testing.T) {
	release := make(chan struct{})
	captured := make(chan bool, 1)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		captured <- err == nil && string(body) == "fixture request" && r.Header.Get("Authorization") == "Bearer fixture-incoming"
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.Header().Add("X-Orka-Fence", "first")
		w.Header().Add("X-Orka-Fence", "second")
		w.Header().Set("Trailer", "X-Orka-Complete")
		w.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(w, "{\"sequence\":1}\n")
		_ = http.NewResponseController(w).Flush()
		select {
		case <-release:
			_, _ = io.WriteString(w, "{\"sequence\":2}\n")
			w.Header().Set("X-Orka-Complete", "yes")
		case <-r.Context().Done():
		}
	}))
	defer backend.Close()
	transport := newHostedLocalTransport()
	defer transport.CloseIdleConnections()
	proxy, err := newHostedProxy(backend.URL, transport, hostedV2Route)
	if err != nil {
		t.Fatal("could not construct streaming proxy")
	}
	frontend := httptest.NewServer(proxy)
	defer frontend.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	request, _ := http.NewRequestWithContext(ctx, http.MethodPost, frontend.URL+"/v2/stream", strings.NewReader("fixture request"))
	request.Header.Set("Authorization", "Bearer fixture-incoming")
	response, err := frontend.Client().Do(request)
	if err != nil {
		t.Fatal("streaming request failed")
	}
	defer response.Body.Close()
	reader := bufio.NewReader(response.Body)
	line, err := reader.ReadString('\n')
	if err != nil || line != "{\"sequence\":1}\n" || response.StatusCode != http.StatusAccepted ||
		response.Header.Get("Content-Type") != "application/x-ndjson" ||
		!reflect.DeepEqual(response.Header.Values("X-Orka-Fence"), []string{"first", "second"}) || !<-captured {
		t.Fatal("streaming prefix, status, headers or request authority changed")
	}
	close(release)
	rest, err := io.ReadAll(reader)
	if err != nil || string(rest) != "{\"sequence\":2}\n" || response.Trailer.Get("X-Orka-Complete") != "yes" {
		t.Fatal("streaming suffix or terminal trailer changed")
	}
}

func TestHostedBoundaryProxyPreservesEncodedResponseBytes(t *testing.T) {
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	_, _ = writer.Write([]byte("fixture encoded body"))
	if writer.Close() != nil {
		t.Fatal("could not prepare encoded response")
	}
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("Content-Length", strconv.Itoa(compressed.Len()))
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = w.Write(compressed.Bytes())
	}))
	defer backend.Close()
	transport := newHostedLocalTransport()
	defer transport.CloseIdleConnections()
	proxy, err := newHostedProxy(backend.URL, transport, hostedV2Route)
	if err != nil {
		t.Fatal("could not construct encoded-response proxy")
	}
	response := httptest.NewRecorder()
	proxy.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v2/status", nil))
	if response.Code != http.StatusUnprocessableEntity || response.Header().Get("Content-Encoding") != "gzip" ||
		response.Header().Get("Content-Length") != strconv.Itoa(compressed.Len()) || !bytes.Equal(response.Body.Bytes(), compressed.Bytes()) {
		t.Fatal("local hop transparently altered response encoding or bytes")
	}
}

func TestHostedBoundaryProxyDoesNotFollowRedirectsOrRetryAcceptedLocalRequests(t *testing.T) {
	var calls atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path == "/v2/redirect" {
			w.Header().Set("Location", "http://must-not-be-contacted.invalid/v2/other")
			w.WriteHeader(http.StatusTemporaryRedirect)
			_, _ = io.WriteString(w, "fixture redirect")
			return
		}
		_, _ = io.Copy(io.Discard, r.Body)
		conn, _, err := http.NewResponseController(w).Hijack()
		if err == nil {
			_ = conn.Close()
		}
	}))
	defer backend.Close()
	transport := newHostedLocalTransport()
	defer transport.CloseIdleConnections()
	proxy, err := newHostedProxy(backend.URL, transport, hostedV2Route)
	if err != nil {
		t.Fatal("could not construct local-failure proxy")
	}
	redirect := httptest.NewRecorder()
	proxy.ServeHTTP(redirect, httptest.NewRequest(http.MethodGet, "/v2/redirect", nil))
	if redirect.Code != http.StatusTemporaryRedirect || redirect.Header().Get("Location") != "http://must-not-be-contacted.invalid/v2/other" ||
		redirect.Body.String() != "fixture redirect" || calls.Load() != 1 {
		t.Fatal("proxy changed or followed a downstream redirect")
	}
	request := httptest.NewRequest(http.MethodPost, "/v2/mutation", strings.NewReader("fixture mutation"))
	request.Header.Set("Idempotency-Key", "fixture")
	var replays atomic.Int32
	request.GetBody = func() (io.ReadCloser, error) {
		replays.Add(1)
		return io.NopCloser(strings.NewReader("fixture mutation")), nil
	}
	failed := httptest.NewRecorder()
	proxy.ServeHTTP(failed, request)
	if failed.Code != http.StatusBadGateway || calls.Load() != 2 || replays.Load() != 0 ||
		!strings.Contains(failed.Body.String(), "acceptance may be unknown") {
		t.Fatal("accepted local request was replayed or misclassified after disconnection")
	}
}
