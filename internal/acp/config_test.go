package acp

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/orka-agents/agent-runtime-foundry/internal/foundry"
	"github.com/orka-agents/agent-runtime-foundry/internal/strictjson"
)

func acpTestConfigBytes(mode string) []byte {
	data, _ := json.Marshal(foundry.AgentConfig{
		Model: "test-model", ToolSchemaMode: mode,
		HostedTarget: foundry.HostedTarget{ProjectEndpoint: "https://foundry.example/api/projects/test", AgentName: "test-agent", AgentVersion: "7"},
	})
	return append(data, '\n')
}

func acpTestDigest(data []byte) string {
	digest := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func acpTestEnvironment(data []byte) map[string]string {
	return map[string]string{
		foundry.AgentConfigDigestEnv: acpTestDigest(data), foundry.ModelEnv: "test-model",
		acpProviderBaseEnv: "http://127.0.0.1:1234/local/v1", acpProviderTokenEnv: "test-only-proxy-token",
	}
}

func TestACPConfigurationPinsExactBytesAndHostedTarget(t *testing.T) {
	data := acpTestConfigBytes(foundry.ToolSchemaModeProviderStatic)
	env := acpTestEnvironment(data)
	cfg, err := verifyACPConfiguration(data, func(key string) string { return env[key] })
	if err != nil || cfg.providerURL != "http://127.0.0.1:1234/local/v1/responses" || cfg.agent.HostedTarget.AgentVersion != "7" {
		t.Fatal("valid pinned configuration rejected")
	}
	for name, mutate := range map[string]func(map[string]string){
		"wrong digest": func(e map[string]string) { e[foundry.AgentConfigDigestEnv] = "sha256:" + strings.Repeat("0", 64) },
		"uppercase digest": func(e map[string]string) {
			e[foundry.AgentConfigDigestEnv] = strings.ToUpper(e[foundry.AgentConfigDigestEnv])
		},
		"wrong model":           func(e map[string]string) { e[foundry.ModelEnv] = "other" },
		"missing token":         func(e map[string]string) { delete(e, acpProviderTokenEnv) },
		"newline token":         func(e map[string]string) { e[acpProviderTokenEnv] = "test\nvalue" },
		"public provider":       func(e map[string]string) { e[acpProviderBaseEnv] = "https://foundry.example/v1" },
		"query provider":        func(e map[string]string) { e[acpProviderBaseEnv] += "?credential=test" },
		"fragment provider":     func(e map[string]string) { e[acpProviderBaseEnv] += "#" },
		"userinfo provider":     func(e map[string]string) { e[acpProviderBaseEnv] = "http://user@127.0.0.1/v1" },
		"invalid provider port": func(e map[string]string) { e[acpProviderBaseEnv] = "http://127.0.0.1:65536/v1" },
	} {
		t.Run(name, func(t *testing.T) {
			e := acpTestEnvironment(data)
			mutate(e)
			if _, err := verifyACPConfiguration(data, func(key string) string { return e[key] }); err == nil {
				t.Fatal("unsafe configuration accepted")
			}
		})
	}
	if _, err := verifyACPConfiguration(append(data, '\n'), func(key string) string { return env[key] }); err == nil {
		t.Fatal("different raw configuration bytes accepted")
	}
}

func TestACPMCPServerRejectsUnauthenticatedAndNonlocalTargets(t *testing.T) {
	valid := `{"type":"http","name":"broker","url":"http://127.0.0.1:1234/mcp","headers":[{"name":"Authorization","value":"Bearer test-only"}]}`
	for name, data := range map[string]string{
		"nonlocal":                strings.Replace(valid, "127.0.0.1", "10.0.0.1", 1),
		"remote hostname":         strings.Replace(valid, "127.0.0.1", "remote.invalid", 1),
		"no header":               strings.Replace(valid, `[{"name":"Authorization","value":"Bearer test-only"}]`, `[]`, 1),
		"empty bearer":            strings.Replace(valid, "Bearer test-only", "Bearer ", 1),
		"invalid bearer":          strings.Replace(valid, "Bearer test-only", "Basic test-only", 1),
		"SSE transport":           strings.Replace(valid, `"http"`, `"sse"`, 1),
		"process command":         strings.Replace(valid, `"type":`, `"command":"forbidden","type":`, 1),
		"extra header":            strings.Replace(valid, `"headers":[`, `"headers":[{"name":"X-Override","value":"test"},`, 1),
		"duplicate authorization": strings.Replace(valid, `"headers":[`, `"headers":[{"name":"authorization","value":"Bearer other"},`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			var server acpMCPServer
			if err := strictjson.Decode([]byte(data), &server, true); err != nil {
				return
			}
			if _, err := newACPMCPClient(server, newACPHTTPClient()); err == nil {
				t.Fatal("unsafe MCP endpoint accepted")
			}
		})
	}
}

func TestACPConfigurationRejectsBakedToolsAndUnpinnedTargets(t *testing.T) {
	base := string(acpTestConfigBytes(foundry.ToolSchemaModeRequest))
	cases := map[string]string{
		"tools":                strings.Replace(base, `"model":`, `"tools":[],"model":`, 1),
		"brokeredTools":        strings.Replace(base, `"model":`, `"brokeredTools":[],"model":`, 1),
		"context":              strings.Replace(base, `"model":`, `"context":{},"model":`, 1),
		"unknown mode":         strings.Replace(base, `"request"`, `"native"`, 1),
		"missing target":       `{"model":"test-model","toolSchemaMode":"request"}`,
		"missing version":      strings.Replace(base, `"agentVersion":"7"`, `"agentVersion":""`, 1),
		"floating version":     strings.Replace(base, `"agentVersion":"7"`, `"agentVersion":"latest"`, 1),
		"alias version":        strings.Replace(base, `"agentVersion":"7"`, `"agentVersion":"@latest"`, 1),
		"unknown target field": strings.Replace(base, `"agentVersion":"7"`, `"agentVersion":"7","sessionId":"untrusted"`, 1),
		"duplicate key":        strings.Replace(base, `"model":`, `"model":"other","model":`, 1),
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			data := []byte(raw)
			if _, err := foundry.DecodeAgentConfig(data, acpTestDigest(data), "test-model"); err == nil {
				t.Fatal("unsupported configuration accepted")
			}
		})
	}
}

func TestACPHTTPTransportRejectsRedirectsAndEnvironmentProxy(t *testing.T) {
	var redirected, proxied atomic.Int32
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		redirected.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer destination.Close()
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		proxied.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer proxy.Close()
	t.Setenv("HTTP_PROXY", proxy.URL)
	t.Setenv("HTTPS_PROXY", proxy.URL)
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, destination.URL, http.StatusTemporaryRedirect)
	}))
	defer source.Close()
	client := newACPHTTPClient()
	defer client.CloseIdleConnections()
	response, err := client.Get(source.URL)
	if response != nil {
		_ = response.Body.Close()
	}
	if err == nil || redirected.Load() != 0 || proxied.Load() != 0 {
		t.Fatal("local transport followed redirect or consulted environment proxy")
	}
	response, err = client.Get("http://not-loopback.invalid/")
	if response != nil {
		_ = response.Body.Close()
	}
	if err == nil || proxied.Load() != 0 {
		t.Fatal("local transport attempted non-loopback access")
	}
}

func TestACPStrictJSONAndFraming(t *testing.T) {
	for _, data := range []string{
		`{"a":1,"a":2}`, `{"a":{"b":1,"b":2}}`, `{"a":1} {"b":2}`,
		strings.Repeat("[", 66) + "0" + strings.Repeat("]", 66), string([]byte{'"', 0xff, '"'}),
		`"\ud800"`, `"\udfff"`, `"\ud800\u0000"`,
	} {
		var value any
		if strictjson.Decode([]byte(data), &value, false) == nil {
			t.Fatal("ambiguous or unbounded JSON accepted")
		}
	}
	for _, raw := range []string{`"\ud83c\udf0d"`, `"\\ud800"`, `"\ufffd"`, `"héllo 🌍"`} {
		var value string
		if strictjson.Decode([]byte(raw), &value, false) != nil {
			t.Fatal("valid Unicode rejected")
		}
	}
	for _, raw := range []string{`null`, `true`, `1.5`, `1e0`, `[]`, `{}`, `""`} {
		if _, err := acpRequestKey(json.RawMessage(raw)); err == nil {
			t.Fatal("unsupported request identity accepted")
		}
	}
	for _, raw := range []string{`1`, `-2`, `"request-1"`} {
		if _, err := acpRequestKey(json.RawMessage(raw)); err != nil {
			t.Fatal("supported request identity rejected")
		}
	}
}

func TestACPProtocolSelectionDoesNotInitializeAzure(t *testing.T) {
	in := io.NopCloser(strings.NewReader(""))
	if handled, err := MaybeServe(nil, in, nil); handled || err != nil {
		t.Fatal("legacy entry point was claimed")
	}
	if handled, err := MaybeServe([]string{"--protocol", "unsupported"}, in, nil); !handled || err == nil {
		t.Fatal("unsupported protocol was not rejected")
	}
}
