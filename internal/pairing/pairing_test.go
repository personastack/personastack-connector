package pairing

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	goruntime "runtime"
	"strings"
	"testing"

	"github.com/personastack/agent-gateway/pkg/externalagentprotocol"
	"github.com/personastack/personastack-connector/internal/runtime"
)

func TestClientExchangeBuildsBinding(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != externalagentprotocol.ExternalAgentPairingExchangePath {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		var request externalagentprotocol.PairingExchangeRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if request.Code != "PAIR-1234" || request.RuntimeKind != externalagentprotocol.RuntimeKindOpenClaw || request.DevicePublicKey == "" || request.DeviceKeyProof == "" {
			t.Fatalf("unexpected request: %+v", request)
		}
		if request.OS != goruntime.GOOS || request.Arch != goruntime.GOARCH {
			t.Fatalf("missing platform metadata: %+v", request)
		}
		_ = json.NewEncoder(w).Encode(externalagentprotocol.PairingExchangeResponse{
			PersonaID:              "persona-1",
			ConnectionID:           "conn-1",
			CredentialID:           "cred-1",
			RuntimeKind:            externalagentprotocol.RuntimeKindOpenClaw,
			ConnectionGeneration:   2,
			GatewayWebsocketURL:    "ws://example/v1/external-agent/ws",
			NativeMCPServerName:    "personastack-conn-1",
			NativeMCPToolNamespace: "personastack",
			PersonaMCPURL:          "https://mcp.personastack.ai/mcp",
			PersonaMCPToken:        "mcp-token-1",
		})
	}))
	defer server.Close()

	result, err := Client{GatewayBaseURL: server.URL}.Exchange(t.Context(), Request{
		Code:         "PAIR-1234",
		RuntimeKind:  runtime.AdapterKindOpenClaw,
		ConfigureMCP: true,
	})
	if err != nil {
		t.Fatalf("exchange: %v", err)
	}
	if result.Binding.ConnectionID != "conn-1" || result.Binding.BridgeCredentialID != "cred-1" || !result.Binding.HasBridgeSecret {
		t.Fatalf("unexpected binding: %+v", result.Binding)
	}
	if result.Binding.BridgePrivateKey == "" || result.Binding.BridgePublicKey == "" {
		t.Fatalf("expected bridge key material")
	}
	if !result.Binding.HasPersonaMCPToken || result.Binding.PersonaMCPToken != "mcp-token-1" {
		t.Fatalf("expected persona mcp token")
	}
}

func TestClientExchangeSurfacesUnsupportedConnectorVersion(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUpgradeRequired)
		_ = json.NewEncoder(w).Encode(externalagentprotocol.PairingExchangeErrorResponse{
			ErrorCode:               externalagentprotocol.PairingExchangeErrorUnsupportedConnectorVersion,
			Message:                 "Connector version is unsupported. Update Connector and rerun the pairing command.",
			MinimumConnectorVersion: "0.1.0",
			UpdateCommand:           "personastack-connector update",
		})
	}))
	defer server.Close()

	_, err := Client{GatewayBaseURL: server.URL}.Exchange(t.Context(), Request{
		Code:        "PAIR-1234",
		RuntimeKind: runtime.AdapterKindHermes,
	})
	if err == nil {
		t.Fatalf("expected error")
	}
	var exchangeError ExchangeError
	if !errors.As(err, &exchangeError) {
		t.Fatalf("expected exchange error, got %T: %v", err, err)
	}
	if exchangeError.StatusCode != http.StatusUpgradeRequired || exchangeError.ErrorCode != externalagentprotocol.PairingExchangeErrorUnsupportedConnectorVersion {
		t.Fatalf("unexpected exchange error: %+v", exchangeError)
	}
	if !strings.Contains(err.Error(), "personastack-connector update") {
		t.Fatalf("missing update command: %v", err)
	}
}
