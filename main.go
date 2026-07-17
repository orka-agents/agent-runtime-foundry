package main

import (
	"log"
	"net/http"
	"time"
)

func main() {
	cfg := loadConfig()
	if err := cfg.validate(); err != nil {
		log.Fatal(err)
	}
	credentialProvider, err := newAzureFoundryTokenProvider()
	if err != nil {
		log.Fatal(err)
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
	log.Fatal(httpServer.ListenAndServe())
}
