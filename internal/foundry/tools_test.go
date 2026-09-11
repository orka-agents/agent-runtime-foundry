package foundry

import (
	"strings"
	"testing"
)

func TestValidateProviderIdentifierBoundsAndRejectsWhitespace(t *testing.T) {
	if err := ValidateIdentifier("response id", strings.Repeat("x", MaxIdentifierBytes+1)); err == nil {
		t.Fatal("oversized provider identifier was accepted")
	}
	if err := ValidateIdentifier("response id", "response id"); err == nil {
		t.Fatal("provider identifier with whitespace was accepted")
	}
}
