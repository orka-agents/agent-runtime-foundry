package foundry

import (
	"net/http"
	"net/url"
)

type RemoteSession struct {
	ID      string `json:"agent_session_id"`
	Version struct {
		Type    string `json:"type"`
		Version string `json:"agent_version"`
	} `json:"version_indicator"`
	Status string `json:"status"`
}

func SessionSuffix(id string) string { return "/endpoint/sessions/" + url.PathEscape(id) }

func DefiniteRejection(status int) bool {
	// A gateway timeout or server error may follow a forwarded request whose
	// response was lost. Only explicit admission rejections close this ambiguity.
	switch status {
	case http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden,
		http.StatusNotFound, http.StatusMethodNotAllowed, http.StatusRequestEntityTooLarge,
		http.StatusUnsupportedMediaType, http.StatusUnprocessableEntity, http.StatusTooManyRequests:
		return true
	default:
		return false
	}
}
