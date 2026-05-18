package externalagentprotocol

import (
	"fmt"
	"net/url"
	"strings"
)

const (
	// ExternalAgentWebsocketPath is the canonical websocket path for Connector sessions.
	ExternalAgentWebsocketPath = "/v1/external-agent/ws"
	// ExternalAgentPairingExchangePath is the canonical pairing exchange path.
	ExternalAgentPairingExchangePath = "/v1/external-agent/pairing/exchange"
	// DefaultGatewayBaseURL is the default public PersonaStack gateway URL for Connector sessions.
	DefaultGatewayBaseURL = "https://cluster-agent.personastack.ai"
)

// ResolveWebsocketURL resolves a canonical Connector websocket URL from a base URL or host.
func ResolveWebsocketURL(raw string) (string, error) {
	base := strings.TrimSpace(raw)
	if base == "" {
		base = DefaultGatewayBaseURL
	}

	parsed, err := url.Parse(base)
	if err != nil {
		return "", fmt.Errorf("invalid gateway url")
	}
	if parsed.Scheme == "" {
		parsed.Scheme = "https"
	}
	if parsed.Host == "" {
		parsed.Host = parsed.Path
		parsed.Path = ""
	}
	if parsed.Host == "" {
		return "", fmt.Errorf("gateway host required")
	}
	parsed.Path = ExternalAgentWebsocketPath
	parsed.RawQuery = ""
	parsed.Fragment = ""

	return normalizeWebsocketURL(parsed)
}

// ResolvePairingExchangeURL resolves the public Connector pairing exchange URL.
func ResolvePairingExchangeURL(raw string) (string, error) {
	base := strings.TrimSpace(raw)
	if base == "" {
		base = DefaultGatewayBaseURL
	}
	parsed, err := url.Parse(base)
	if err != nil {
		return "", fmt.Errorf("invalid gateway url")
	}
	if parsed.Scheme == "" {
		parsed.Scheme = "https"
	}
	if parsed.Host == "" {
		parsed.Host = parsed.Path
		parsed.Path = ""
	}
	if parsed.Host == "" {
		return "", fmt.Errorf("gateway host required")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", fmt.Errorf("unsupported gateway scheme")
	}
	parsed.Path = ExternalAgentPairingExchangePath
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String(), nil
}

// NormalizeWebsocketURL normalizes an absolute websocket URL and pins the Connector path.
func NormalizeWebsocketURL(raw string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", fmt.Errorf("invalid websocket url")
	}
	if !strings.EqualFold(parsed.Scheme, "ws") && !strings.EqualFold(parsed.Scheme, "wss") {
		return "", fmt.Errorf("unsupported websocket scheme")
	}
	if parsed.Host == "" {
		return "", fmt.Errorf("websocket host required")
	}
	parsed.Path = ExternalAgentWebsocketPath
	parsed.RawQuery = ""
	parsed.Fragment = ""

	return normalizeWebsocketURL(parsed)
}

func normalizeWebsocketURL(parsed *url.URL) (string, error) {
	scheme := strings.ToLower(parsed.Scheme)
	if scheme == "http" {
		scheme = "ws"
	}
	if scheme == "https" {
		scheme = "wss"
	}
	if scheme != "ws" && scheme != "wss" {
		return "", fmt.Errorf("unsupported websocket scheme")
	}

	return (&url.URL{
		Scheme: scheme,
		Host:   parsed.Host,
		Path:   ExternalAgentWebsocketPath,
	}).String(), nil
}
