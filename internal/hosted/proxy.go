package hosted

import (
	"crypto/tls"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/orka-agents/agent-runtime-foundry/internal/brokerapi"
	"github.com/orka-agents/agent-runtime-foundry/internal/foundry"
)

// A fresh HTTP/1 connection for each local hop prevents the standard
// transport's stale-connection retry. The WebSocket hop uses ClientConn
// directly and likewise never retries an accepted request.
func newHostedLocalTransport() *http.Transport {
	return &http.Transport{
		Proxy: nil, DialContext: (&net.Dialer{Timeout: 5 * time.Second}).DialContext,
		DisableKeepAlives: true, DisableCompression: true, ForceAttemptHTTP2: false,
		TLSHandshakeTimeout: 5 * time.Second, MaxResponseHeaderBytes: 64 << 10,
		TLSNextProto: make(map[string]func(string, *tls.Conn) http.RoundTripper),
	}
}

func newHostedProxy(target string, transport http.RoundTripper, allow func(*http.Request) bool) (http.Handler, error) {
	base, err := url.Parse(target)
	if err != nil || base.Host == "" || base.Path != "" || (base.Scheme != "http" && base.Scheme != "https") {
		return nil, errHostedInvalid
	}
	proxy := &httputil.ReverseProxy{
		Rewrite: func(request *httputil.ProxyRequest) {
			request.Out.URL.Scheme = base.Scheme
			request.Out.URL.Host = base.Host
			request.Out.Host = base.Host
			request.Out.GetBody = nil
			// Preserve authorization exactly. In particular, the unsigned
			// Foundry context is not authority to inject a broker credential.
		},
		Transport: transport, FlushInterval: -1,
		ErrorLog: log.New(io.Discard, "", 0),
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, _ error) {
			http.Error(w, "hosted transport unavailable; acceptance may be unknown", http.StatusBadGateway)
		},
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !hostedRequestPathValid(r) || !allow(r) || r.Header.Get("Upgrade") != "" || r.Method == http.MethodConnect {
			http.Error(w, "hosted route denied", http.StatusForbidden)
			return
		}
		proxy.ServeHTTP(w, r)
	}), nil
}

func hostedRequestPathValid(r *http.Request) bool {
	return r.URL != nil && r.URL.User == nil && r.URL.Opaque == "" &&
		r.URL.RawPath == "" && r.URL.Fragment == "" && len(r.URL.Path) <= 4096 &&
		len(r.URL.RawQuery) <= 8192 && path.Clean(r.URL.Path) == r.URL.Path &&
		strings.HasPrefix(r.URL.Path, "/") && !strings.ContainsAny(r.URL.Path, "\\\x00")
}

func hostedV2Route(r *http.Request) bool {
	if !strings.HasPrefix(r.URL.Path, "/v2/") {
		return false
	}
	switch r.Method {
	case http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	default:
		return false
	}
}

func hostedBrokerRoute(r *http.Request) bool {
	if r.URL.RawQuery != "" {
		return false
	}
	if r.URL.Path == brokerapi.StatusPath {
		return r.Method == http.MethodGet
	}
	if r.Method != http.MethodPost {
		return false
	}
	switch r.URL.Path {
	case brokerapi.ResponsesPath, brokerapi.RenewPath, brokerapi.SettlePath, brokerapi.RetirePath:
		return true
	default:
		return false
	}
}

func hostedOrkaRoute(r *http.Request) bool {
	if r.URL.RawQuery != "" {
		return false
	}
	if r.Method == http.MethodPost && (r.URL.Path == "/internal/v2/acp/mcp/tools/call" ||
		r.URL.Path == "/internal/v2/acp/artifact-authorizations") {
		return true
	}
	const prefix = "/internal/v2/acp/artifacts/sha256/"
	if (r.Method == http.MethodGet || r.Method == http.MethodPut || r.Method == http.MethodHead) && strings.HasPrefix(r.URL.Path, prefix) {
		return foundry.DigestValid("sha256:" + strings.TrimPrefix(r.URL.Path, prefix))
	}
	return false
}
