package acp

import "net/http"

type acpTestTransport func(*http.Request) (*http.Response, error)

func (f acpTestTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
