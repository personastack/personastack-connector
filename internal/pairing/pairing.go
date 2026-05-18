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
	goruntime "runtime"
	"strings"
	"time"

	"github.com/personastack/personastack-connector/internal/externalagentprotocol"
	"github.com/personastack/personastack-connector/internal/buildinfo"
	"github.com/personastack/personastack-connector/internal/config"
	"github.com/personastack/personastack-connector/internal/runtime"
)

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

type ExchangeError struct {
	StatusCode              int
	ErrorCode               externalagentprotocol.PairingExchangeErrorCode
	Message                 string
	MinimumConnectorVersion string
	UpdateCommand           string
}

func (e ExchangeError) Error() string {
	message := strings.TrimSpace(e.Message)
	if message == "" {
		message = "pairing exchange failed"
	}
	if e.ErrorCode == "" {
		return fmt.Sprintf("%s: status=%d", message, e.StatusCode)
	}
	if strings.TrimSpace(e.UpdateCommand) != "" {
		return fmt.Sprintf("%s: %s (update: %s)", e.ErrorCode, message, strings.TrimSpace(e.UpdateCommand))
	}
	return fmt.Sprintf("%s: %s", e.ErrorCode, message)
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
		ConnectorVersion:    buildinfo.VersionString(),
		OS:                  goruntime.GOOS,
		Arch:                goruntime.GOARCH,
		DevicePublicKey:     base64.StdEncoding.EncodeToString(publicKey),
		HostnameHash:        hostnameHash(),
		GatewayWebsocketURL: websocketURL,
		ConfigureMCP:        request.ConfigureMCP,
	}
	payload.DeviceKeyProof = base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, []byte(deviceProofMessage(payload))))
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
		var errorResponse externalagentprotocol.PairingExchangeErrorResponse
		if err := json.NewDecoder(response.Body).Decode(&errorResponse); err != nil {
			return Result{}, fmt.Errorf("pairing exchange failed: status=%d", response.StatusCode)
		}
		return Result{}, ExchangeError{
			StatusCode:              response.StatusCode,
			ErrorCode:               errorResponse.ErrorCode,
			Message:                 errorResponse.Message,
			MinimumConnectorVersion: errorResponse.MinimumConnectorVersion,
			UpdateCommand:           errorResponse.UpdateCommand,
		}
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
		PersonaMCPURL:        strings.TrimSpace(decoded.PersonaMCPURL),
		PersonaMCPToken:      strings.TrimSpace(decoded.PersonaMCPToken),
		RuntimeKind:          request.RuntimeKind,
		ReadinessState:       runtime.AdapterStateRuntimeMissing,
		HasBridgeSecret:      true,
		HasPersonaMCPToken:   strings.TrimSpace(decoded.PersonaMCPToken) != "",
	}}, nil
}

func deviceProofMessage(request externalagentprotocol.PairingExchangeRequest) string {
	return strings.Join([]string{
		pairingCodeHash(request.Code),
		string(request.RuntimeKind),
		strings.TrimSpace(request.DevicePublicKey),
	}, "\n")
}

func pairingCodeHash(code string) string {
	normalized := strings.NewReplacer(" ", "", "-", "").Replace(strings.ToUpper(strings.TrimSpace(code)))
	sum := sha256.Sum256([]byte(normalized))
	return hex.EncodeToString(sum[:])
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
