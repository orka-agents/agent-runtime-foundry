package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strconv"
	"strings"
	"unicode/utf8"
)

type acpRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *acpRPCError) Error() string { return e.Message }

var (
	acpInvalidRequest = &acpRPCError{-32600, "invalid ACP request"}
	acpInvalidParams  = &acpRPCError{-32602, "invalid ACP parameters"}
	acpInternalError  = &acpRPCError{-32603, "Foundry ACP prompt failed"}
)

type acpRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type acpResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *acpRPCError    `json:"error,omitempty"`
}

type acpMCPServer struct {
	Type    string `json:"type"`
	Name    string `json:"name"`
	URL     string `json:"url"`
	Headers []struct {
		Name  string `json:"name"`
		Value string `json:"value"`
	} `json:"headers"`
	Meta json.RawMessage `json:"_meta,omitempty"`
}

type acpNewSession struct {
	CWD                   string          `json:"cwd"`
	AdditionalDirectories []string        `json:"additionalDirectories,omitempty"`
	MCPServers            []acpMCPServer  `json:"mcpServers"`
	Meta                  json.RawMessage `json:"_meta,omitempty"`
}

type acpPrompt struct {
	SessionID string `json:"sessionId"`
	Prompt    []struct {
		Type     string          `json:"type"`
		Text     *string         `json:"text,omitempty"`
		Name     string          `json:"name,omitempty"`
		URI      string          `json:"uri,omitempty"`
		MIMEType string          `json:"mimeType,omitempty"`
		Meta     json.RawMessage `json:"_meta,omitempty"`
	} `json:"prompt"`
	Meta json.RawMessage `json:"_meta,omitempty"`
}

func (p acpPrompt) text() (string, error) {
	if !acpSafeString(p.SessionID, 512) || len(p.Prompt) == 0 {
		return "", acpInvalidParams
	}
	var blocks []string
	for _, block := range p.Prompt {
		switch block.Type {
		case "text":
			if block.Text == nil || block.Name != "" || block.URI != "" || block.MIMEType != "" {
				return "", acpInvalidParams
			}
			blocks = append(blocks, *block.Text)
		case "resource_link":
			if block.Text != nil || !acpSafeString(block.Name, 1024) || !acpSafeString(block.URI, 8<<10) {
				return "", acpInvalidParams
			}
			text := "Resource link: " + block.Name + "\nURI: " + block.URI
			if block.MIMEType != "" {
				if !acpSafeString(block.MIMEType, 256) {
					return "", acpInvalidParams
				}
				text += "\nMIME type: " + block.MIMEType
			}
			blocks = append(blocks, text)
		default:
			return "", acpInvalidParams
		}
	}
	text := strings.Join(blocks, "\n")
	if len(text) > maxFoundryPromptBytes {
		return "", acpInvalidParams
	}
	return text, nil
}

func acpRequestKey(raw json.RawMessage) (string, error) {
	if len(raw) == 0 || len(raw) > 1024 {
		return "", acpInvalidRequest
	}
	if raw[0] == '"' {
		var value string
		if json.Unmarshal(raw, &value) != nil || !acpSafeString(value, 512) {
			return "", acpInvalidRequest
		}
		return "s:" + value, nil
	}
	value, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil {
		return "", acpInvalidRequest
	}
	return "n:" + strconv.FormatInt(value, 10), nil
}

func acpReadLine(reader *bufio.Reader) ([]byte, error) {
	var line []byte
	for {
		part, err := reader.ReadSlice('\n')
		if len(line)+len(part) > acpMaxMessageBytes {
			return nil, acpInvalidRequest
		}
		line = append(line, part...)
		if err == nil {
			return bytes.TrimSpace(line), nil
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if len(line) != 0 {
			return nil, acpInvalidRequest
		}
		return nil, err
	}
}

// Go's ordinary JSON decoder accepts duplicate object members. Reject them at
// every depth before interpreting authority-bearing envelopes or tool arguments.
func acpDecode(data []byte, value any, strictFields bool) error {
	return acpDecodeJSON(data, value, strictFields, nil)
}

// Remote authority and Responses structs accept single case-folded field names,
// as encoding/json does, but two names must never replace or merge one field.
// Maps, interfaces and custom JSON values remain opaque to field folding.
func acpDecodeStruct(data []byte, value any, strictFields bool) error {
	return acpDecodeJSON(data, value, strictFields, reflect.TypeOf(value))
}

func acpDecodeJSON(data []byte, value any, strictFields bool, shape reflect.Type) error {
	if !utf8.Valid(data) || !json.Valid(data) || !acpValidStringEscapes(data) {
		return acpInvalidParams
	}
	check := json.NewDecoder(bytes.NewReader(data))
	check.UseNumber()
	if acpJSONValue(check, 0, shape) != nil {
		return acpInvalidParams
	}
	if _, err := check.Token(); !errors.Is(err, io.EOF) {
		return acpInvalidParams
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if strictFields {
		decoder.DisallowUnknownFields()
	}
	if err := decoder.Decode(value); err != nil {
		return acpInvalidParams
	}
	return nil
}

// encoding/json replaces unpaired UTF-16 escapes with U+FFFD. Tool arguments
// must retain their exact Unicode value, so reject malformed surrogate pairs.
func acpValidStringEscapes(data []byte) bool {
	quoted := false
	for i := 0; i < len(data); i++ {
		if data[i] == '"' {
			quoted = !quoted
			continue
		}
		if !quoted || data[i] != '\\' {
			continue
		}
		i++
		if data[i] != 'u' {
			continue
		}
		value, err := strconv.ParseUint(string(data[i+1:i+5]), 16, 16)
		if err != nil {
			return false
		}
		i += 4
		if value >= 0xdc00 && value <= 0xdfff {
			return false
		}
		if value < 0xd800 || value > 0xdbff {
			continue
		}
		if i+6 >= len(data) || data[i+1] != '\\' || data[i+2] != 'u' {
			return false
		}
		low, err := strconv.ParseUint(string(data[i+3:i+7]), 16, 16)
		if err != nil || low < 0xdc00 || low > 0xdfff {
			return false
		}
		i += 6
	}
	return true
}

func acpJSONValue(decoder *json.Decoder, depth int, shape reflect.Type) error {
	if depth > 64 {
		return acpInvalidParams
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		fields, err := acpJSONStructFields(shape, 0)
		if err != nil {
			return err
		}
		seen := make(map[string]bool)
		for decoder.More() {
			key, err := decoder.Token()
			if err != nil {
				return err
			}
			name, ok := key.(string)
			if !ok {
				return acpInvalidParams
			}
			name, fieldType := acpJSONMatchField(fields, name)
			if seen[name] {
				return acpInvalidParams
			}
			seen[name] = true
			if err := acpJSONValue(decoder, depth+1, fieldType); err != nil {
				return err
			}
		}
	case '[':
		var element reflect.Type
		if shape := acpJSONStructType(shape); shape != nil && (shape.Kind() == reflect.Slice || shape.Kind() == reflect.Array) {
			element = shape.Elem()
		}
		for decoder.More() {
			if err := acpJSONValue(decoder, depth+1, element); err != nil {
				return err
			}
		}
	default:
		return acpInvalidParams
	}
	_, err = decoder.Token()
	return err
}
