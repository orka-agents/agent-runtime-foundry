package adapter

import (
	"log"
	"net/http"
	"time"

	"github.com/orka-agents/agent-runtime-foundry/internal/foundry"
)

// Serve runs the harness v1 HTTP adapter using its environment configuration.
func Serve() error {
	cfg := loadConfig()
	if err := cfg.validate(); err != nil {
		return err
	}
	credentialProvider, err := foundry.NewTokenProvider()
	if err != nil {
		return err
	}
	endpointForRedirects := cfg.responsesEndpoint
	if endpointForRedirects == "" {
		endpointForRedirects = cfg.projectEndpoint
	}
	foundryClient := newResponsesClient(cfg, newFoundryHTTPClient(endpointForRedirects), credentialProvider)
	adapter := newAdapter(cfg, foundryBackend{client: foundryClient})

	harnessServer := &server{cfg: cfg, adapter: adapter}
	log.Printf("Foundry Hosted Agents adapter listening on %s (runtime=%s agent=%s)", cfg.addr, cfg.runtimeName, cfg.agentName)
	httpServer := &http.Server{
		Addr:              cfg.addr,
		Handler:           harnessServer.handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	return httpServer.ListenAndServe()
}
