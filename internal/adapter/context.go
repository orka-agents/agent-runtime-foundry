package adapter

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

type foundryIsolationContextKey struct{}

func withFoundryIsolationKey(ctx context.Context, key string) context.Context {
	return context.WithValue(ctx, foundryIsolationContextKey{}, strings.TrimSpace(key))
}

func foundryIsolationKeyFromContext(ctx context.Context) (string, bool) {
	value, ok := ctx.Value(foundryIsolationContextKey{}).(string)
	return strings.TrimSpace(value), ok
}

func isolationKeyForRuntimeSession(runtimeSessionID string) string {
	digest := sha256.Sum256([]byte(runtimeSessionID))
	return "orka-" + hex.EncodeToString(digest[:16])
}
