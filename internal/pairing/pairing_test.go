package pairing

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
		_ = json.NewEncoder(w).Encode(externalagentprotocol.PairingExchangeResponse{
			PersonaID:              "persona-1",
			ConnectionID:           "conn-1",
			CredentialID:           "cred-1",
			RuntimeKind:            externalagentprotocol.RuntimeKindOpenClaw,
			ConnectionGeneration:   2,
			GatewayWebsocketURL:    "ws://example/v1/external-agent/ws",
			NativeMCPServerName:    "personastack-conn-1",
			NativeMCPToolNamespace: "personastack",
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
}
