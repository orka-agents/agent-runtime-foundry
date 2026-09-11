package foundry

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
)

func Digest(data []byte) string {
	digest := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func JSONDigest(value any) string {
	data, _ := json.Marshal(value) // Only concrete, JSON-safe broker structs are used.
	return Digest(data)
}

func DigestValid(value string) bool {
	if len(value) != len("sha256:")+64 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	for _, char := range value[len("sha256:"):] {
		if !(char >= '0' && char <= '9') && !(char >= 'a' && char <= 'f') {
			return false
		}
	}
	return true
}
