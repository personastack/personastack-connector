package pairing

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/personastack/agent-gateway/pkg/externalagentprotocol"
	"github.com/personastack/personastack-connector/internal/config"
	"github.com/personastack/personastack-connector/internal/runtime"
)

const connectorVersion = "0.1.0-dev"

type Client struct {
	GatewayBaseURL string
	HTTPClient     *http.Client
}

type Request struct {
	Code         string
	RuntimeKind  runtime.AdapterKind
	ConfigureMCP bool
}

type Result struct {
	Binding config.Binding
}

func (c Client) Exchange(ctx context.Context, request Request) (Result, error) {
	code := strings.TrimSpace(request.Code)
	if code == "" {
		return Result{}, fmt.Errorf("pairing code required")
	}
	runtimeKind := runtimeKindForAdapter(request.RuntimeKind)
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return Result{}, fmt.Errorf("generate bridge key: %w", err)
	}
	websocketURL, err := externalagentprotocol.ResolveWebsocketURL(c.GatewayBaseURL)
	if err != nil {
		return Result{}, err
	}
	payload := externalagentprotocol.PairingExchangeRequest{
		Code:                code,
		RuntimeKind:         runtimeKind,
		ConnectorVersion:    connectorVersion,
		DevicePublicKey:     base64.StdEncoding.EncodeToString(publicKey),
		HostnameHash:        hostnameHash(),
		GatewayWebsocketURL: websocketURL,
		ConfigureMCP:        request.ConfigureMCP,
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return Result{}, fmt.Errorf("encode pairing exchange: %w", err)
	}
	endpoint, err := externalagentprotocol.ResolvePairingExchangeURL(c.GatewayBaseURL)
	if err != nil {
		return Result{}, err
	}
	httpClient := c.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 15 * time.Second}
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(raw))
	if err != nil {
		return Result{}, err
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	response, err := httpClient.Do(httpRequest)
	if err != nil {
		return Result{}, fmt.Errorf("pairing exchange: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		return Result{}, fmt.Errorf("pairing exchange failed: status=%d", response.StatusCode)
	}
	var decoded externalagentprotocol.PairingExchangeResponse
	if err := json.NewDecoder(response.Body).Decode(&decoded); err != nil {
		return Result{}, fmt.Errorf("decode pairing response: %w", err)
	}
	return Result{Binding: config.Binding{
		ConnectionID:         config.ConnectionID(strings.TrimSpace(decoded.ConnectionID)),
		PersonaID:            config.PersonaID(strings.TrimSpace(decoded.PersonaID)),
		ExternalAgentKind:    externalKindForRuntime(runtimeKind),
		ConnectionGeneration: decoded.ConnectionGeneration,
		GatewayWebsocketURL:  strings.TrimSpace(decoded.GatewayWebsocketURL),
		BridgeCredentialID:   strings.TrimSpace(decoded.CredentialID),
		BridgePrivateKey:     base64.StdEncoding.EncodeToString(privateKey),
		BridgePublicKey:      base64.StdEncoding.EncodeToString(publicKey),
		NativeMCPServer:      strings.TrimSpace(decoded.NativeMCPServerName),
		NativeMCPNamespace:   strings.TrimSpace(decoded.NativeMCPToolNamespace),
		RuntimeKind:          request.RuntimeKind,
		ReadinessState:       runtime.AdapterStateRuntimeMissing,
		HasBridgeSecret:      true,
		HasPersonaMCPToken:   false,
	}}, nil
}

func runtimeKindForAdapter(kind runtime.AdapterKind) externalagentprotocol.RuntimeKind {
	switch kind {
	case runtime.AdapterKindOpenClaw:
		return externalagentprotocol.RuntimeKindOpenClaw
	default:
		return externalagentprotocol.RuntimeKindHermes
	}
}

func externalKindForRuntime(kind externalagentprotocol.RuntimeKind) config.ExternalAgentKind {
	if kind == externalagentprotocol.RuntimeKindOpenClaw {
		return config.ExternalAgentKindOpenClaw
	}
	return config.ExternalAgentKindHermes
}

func hostnameHash() string {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	sum := sha256.Sum256([]byte(strings.ToLower(strings.TrimSpace(hostname))))
	return hex.EncodeToString(sum[:])
}
