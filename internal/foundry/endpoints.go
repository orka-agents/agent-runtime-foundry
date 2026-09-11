package foundry

import (
	"errors"
	"net"
	"net/url"
	"regexp"
	"strings"
)

var (
	agentNameRE    = regexp.MustCompile(`^[A-Za-z0-9]+(?:-[A-Za-z0-9]+)*$`)
	agentVersionRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
)

func ValidateAgentName(name string) error {
	if name == "" {
		return errors.New("foundry agent name is required")
	}
	if len(name) > 63 || !agentNameRE.MatchString(name) {
		return errors.New("foundry agent name must be 1-63 alphanumeric or hyphen characters without leading, trailing, or repeated hyphens")
	}
	return nil
}

func ValidateAgentVersion(version string) error {
	if version == "" {
		return nil
	}
	if strings.HasPrefix(version, "@") || !agentVersionRE.MatchString(version) {
		return errors.New("foundry agent version must be a concrete version identifier")
	}
	return nil
}

func EndpointIsSafe(raw string) bool {
	trimmed := strings.TrimSpace(raw)
	parsed, err := url.Parse(trimmed)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return false
	}
	if parsed.User != nil || parsed.ForceQuery || parsed.RawQuery != "" || strings.Contains(trimmed, "#") {
		return false
	}
	if strings.EqualFold(parsed.Scheme, "https") {
		return true
	}
	if !strings.EqualFold(parsed.Scheme, "http") {
		return false
	}
	host := strings.Trim(strings.ToLower(parsed.Hostname()), "[]")
	ip := net.ParseIP(host)
	return host == "localhost" || (ip != nil && ip.IsLoopback())
}
