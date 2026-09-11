package foundry

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"unicode"
)

var toolNameRE = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

func ValidateFunctionName(name string) error {
	if len(name) == 0 || len(name) > 128 || !toolNameRE.MatchString(name) {
		return errors.New("foundry function call name is invalid")
	}
	return nil
}

func ValidateCallID(callID string) error {
	if callID == "" {
		return errors.New("foundry function call omitted call_id")
	}
	if len([]rune(callID)) > 64 {
		return errors.New("foundry function call id exceeds 64 characters")
	}
	for _, char := range callID {
		if unicode.IsControl(char) {
			return errors.New("foundry function call id contains control characters")
		}
	}
	return nil
}

func NormalizeToolArguments(raw json.RawMessage) (json.RawMessage, error) {
	if len(raw) == 0 {
		return json.RawMessage(`{}`), nil
	}
	var encoded string
	if err := json.Unmarshal(raw, &encoded); err == nil {
		encoded = strings.TrimSpace(encoded)
		if encoded != "" && json.Valid([]byte(encoded)) {
			return json.RawMessage(encoded), nil
		}
		return nil, errors.New("foundry tool arguments are not valid JSON")
	}
	if json.Valid(raw) {
		return slices.Clone(raw), nil
	}
	return nil, errors.New("foundry tool arguments are not valid JSON")
}

func ValidateIdentifier(label, value string) error {
	if value == "" {
		return nil
	}
	if len(value) > MaxIdentifierBytes {
		return fmt.Errorf("foundry %s exceeds adapter limit", label)
	}
	for _, char := range value {
		if unicode.IsSpace(char) || unicode.IsControl(char) {
			return fmt.Errorf("foundry %s contains unsafe characters", label)
		}
	}
	return nil
}
