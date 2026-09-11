package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

func maybeServeHosted(args []string) (bool, error) {
	mode := ""
	for index, arg := range args {
		for _, value := range []string{"hosted", "hosted-gateway"} {
			if arg == "--protocol="+value || (arg == "--protocol" && index+1 < len(args) && args[index+1] == value) {
				mode = value
			}
		}
	}
	if mode == "" {
		return false, nil
	}
	flags := flag.NewFlagSet("foundry-hosted", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	protocol := flags.String("protocol", "", "")
	defaultPath := hostedImageConfigPath
	if mode == "hosted-gateway" {
		defaultPath = hostedGatewayConfigPath
	}
	path := flags.String("config", defaultPath, "")
	if flags.Parse(args) != nil || flags.NArg() != 0 || *protocol != mode {
		return true, errHostedInvalid
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if mode == "hosted-gateway" {
		settings, err := loadHostedGatewaySettings(*path, os.Getenv)
		if err != nil {
			return true, err
		}
		provider, err := newAzureFoundryTokenProvider()
		if err != nil {
			clear(settings.signingKey)
			return true, errHostedInvalid
		}
		return true, serveHostedGateway(ctx, settings, provider)
	}
	cfg, err := loadHostedImageConfig(*path)
	if err != nil {
		return true, err
	}
	port := firstNonBlank(os.Getenv("PORT"), "8088")
	n, err := strconv.Atoi(port)
	// The supervisor and reverse relays bind these loopback ports after bootstrap.
	if err != nil || n < 1 || n > 65535 || n == 8080 || n == 8091 || n == 8092 {
		return true, errHostedInvalid
	}
	server, err := newHostedServer(ctx, cfg, os.Getenv, launchHostedSupervisor)
	if err != nil {
		return true, err
	}
	defer server.cancel()
	return true, serveHostedHTTP(server.ctx, ":"+port, server)
}

func serveHostedGateway(ctx context.Context, settings hostedGatewaySettings, provider foundryTokenProvider) error {
	defer clear(settings.signingKey)
	// Retain the listener before creating a one-shot remote lifetime. A local
	// bind failure must not reserve a session or expose bootstrap credentials.
	listener, err := net.Listen("tcp", settings.address)
	if err != nil {
		return errHostedInvalid
	}
	defer listener.Close() //nolint:errcheck
	gateway, err := newHostedGateway(ctx, settings, provider)
	if err != nil {
		return err
	}
	defer gateway.close()
	return serveHostedHTTPListener(gateway.ctx, listener, gateway)
}

func serveHostedHTTP(ctx context.Context, address string, handler http.Handler) (serveErr error) {
	if hosted, ok := handler.(*hostedServer); ok {
		defer func() { serveErr = errors.Join(serveErr, hosted.close()) }()
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return errHostedInvalid
	}
	defer listener.Close() //nolint:errcheck
	return serveHostedHTTPListener(ctx, listener, handler)
}

func serveHostedHTTPListener(ctx context.Context, listener net.Listener, handler http.Handler) error {
	server := newHostedHTTPServer(handler)
	defer server.Close() //nolint:errcheck
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	select {
	case err := <-done:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return errHostedInvalid
	case <-ctx.Done():
		// Close active requests too. Their acceptance is unknown to callers;
		// this process never reconnects or creates another hosted lifetime.
		_ = server.Close()
		return nil
	}
}

func newHostedHTTPServer(handler http.Handler) *http.Server {
	// Operation owners enforce byte limits and authorization/deadlines. A
	// whole-body read timeout here truncates legitimate artifact uploads;
	// header and idle timeouts bound inactive HTTP connections instead.
	return &http.Server{Handler: handler, ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout: 2 * time.Minute, MaxHeaderBytes: 32 << 10}
}
