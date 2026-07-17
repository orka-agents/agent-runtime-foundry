package events

import (
	"regexp"
	"strings"

	"github.com/orka-agents/agent-runtime-foundry/internal/redact"
)

const executionEventRedactedValue = "[REDACTED]"

var (
	authorizationHeaderRe = regexp.MustCompile(`(?i)\b(authorization\s*:\s*)[^\r\n]+`)
	transactionHeaderRe   = regexp.MustCompile(`(?i)\b((?:txn-token|transaction-token)\s*:\s*)[A-Za-z0-9._~+/=-]+`)
	cookieHeaderRe        = regexp.MustCompile(`(?i)\b((?:cookie|set-cookie)\s*:\s*)[^\r\n]+`)
)

// RedactExecutionEventText removes credential-shaped text from conformance diagnostics.
func RedactExecutionEventText(value string) string {
	value = redact.SensitiveText(strings.TrimSpace(value))
	value = authorizationHeaderRe.ReplaceAllString(value, `${1}`+executionEventRedactedValue)
	value = transactionHeaderRe.ReplaceAllString(value, `${1}`+executionEventRedactedValue)
	value = cookieHeaderRe.ReplaceAllString(value, `${1}`+executionEventRedactedValue)
	return value
}
