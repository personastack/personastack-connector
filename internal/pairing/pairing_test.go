package pairing

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	goruntime "runtime"
	"strings"
	"testing"

	"github.com/personastack/personastack-connector/internal/externalagentprotocol"
	"github.com/personastack/personastack-connector/internal/runtime"
)

type pairingRoundTripper func(*http.Request) (*http.Response, error)

func (roundTripper pairingRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return roundTripper(request)
}

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

func TestClientExchangeAutoNegotiatesWithoutLocalReadiness(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request externalagentprotocol.PairingExchangeRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if request.RuntimeKind != externalagentprotocol.RuntimeKindAuto {
			t.Fatalf("runtime kind = %q, want auto", request.RuntimeKind)
		}
		if len(request.SupportedRuntimeKinds) != 2 || request.SupportedRuntimeKinds[0] != externalagentprotocol.RuntimeKindHermes || request.SupportedRuntimeKinds[1] != externalagentprotocol.RuntimeKindOpenClaw {
			t.Fatalf("supported runtime kinds = %+v", request.SupportedRuntimeKinds)
		}
		_ = json.NewEncoder(w).Encode(externalagentprotocol.PairingExchangeResponse{
			PersonaID:            "persona-auto",
			ConnectionID:         "conn-auto",
			CredentialID:         "cred-auto",
			RuntimeKind:          externalagentprotocol.RuntimeKindHermes,
			ConnectionGeneration: 1,
			GatewayWebsocketURL:  "ws://example/v1/external-agent/ws",
		})
	}))
	defer server.Close()

	result, err := Client{GatewayBaseURL: server.URL}.Exchange(t.Context(), Request{Code: "PAIR-AUTO", RuntimeKind: runtime.AdapterKindAuto})
	if err != nil {
		t.Fatalf("exchange: %v", err)
	}
	if result.Binding.RuntimeKind != runtime.AdapterKindHermes {
		t.Fatalf("negotiated binding runtime = %s, want hermes", result.Binding.RuntimeKind)
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

func TestClientExchangeContract(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name         string
		request      Request
		responseKind externalagentprotocol.RuntimeKind
		wantCalls    int
		wantError    bool
	}
	testCases := []testCase{
		{
			name: "builds explicit hermes binding",
			request: Request{
				Code:         " PAIR-1234 ",
				RuntimeKind:  runtime.AdapterKindHermes,
				ConfigureMCP: true,
			},
			responseKind: externalagentprotocol.RuntimeKindHermes,
			wantCalls:    1,
		},
		{
			name: "rejects empty pairing code before http",
			request: Request{
				Code:        "   ",
				RuntimeKind: runtime.AdapterKindHermes,
			},
			wantError: true,
		},
		{
			name: "rejects response that changes explicit runtime",
			request: Request{
				Code:        "PAIR-1234",
				RuntimeKind: runtime.AdapterKindHermes,
			},
			responseKind: externalagentprotocol.RuntimeKindOpenClaw,
			wantCalls:    1,
			wantError:    true,
		},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			calls := 0
			client := Client{
				GatewayBaseURL: "https://gateway.example",
				HTTPClient: &http.Client{Transport: pairingRoundTripper(func(request *http.Request) (*http.Response, error) {
					calls++
					if request.Method != http.MethodPost || request.URL.Path != externalagentprotocol.ExternalAgentPairingExchangePath || request.Header.Get("Content-Type") != "application/json" {
						t.Fatalf("exchange request = method:%q path:%q content-type:%q", request.Method, request.URL.Path, request.Header.Get("Content-Type"))
					}
					var payload externalagentprotocol.PairingExchangeRequest
					err := json.NewDecoder(request.Body).Decode(&payload)
					if err != nil {
						t.Fatalf("decode exchange request: %v", err)
					}
					if payload.Code != "PAIR-1234" || payload.RuntimeKind != externalagentprotocol.RuntimeKindHermes || payload.DevicePublicKey == "" || payload.DeviceKeyProof == "" || payload.GatewayWebsocketURL != "wss://gateway.example/v1/external-agent/ws" || payload.ConfigureMCP != testCase.request.ConfigureMCP {
						t.Fatalf("exchange payload = %+v", payload)
					}
					responseBody, err := json.Marshal(externalagentprotocol.PairingExchangeResponse{
						PersonaID:            "persona-1",
						ConnectionID:         "conn-1",
						CredentialID:         "credential-1",
						RuntimeKind:          testCase.responseKind,
						ConnectionGeneration: 3,
						GatewayWebsocketURL:  "wss://gateway.example/v1/external-agent/ws",
						PersonaMCPURL:        "https://mcp.example/mcp",
						PersonaMCPToken:      "mcp-token",
					})
					if err != nil {
						t.Fatalf("marshal exchange response: %v", err)
					}
					headers := make(http.Header)
					headers.Set("Content-Type", "application/json")
					return &http.Response{StatusCode: http.StatusOK, Header: headers, Body: io.NopCloser(strings.NewReader(string(responseBody)))}, nil
				})},
			}

			result, err := client.Exchange(t.Context(), testCase.request)

			if calls != testCase.wantCalls {
				t.Fatalf("http calls = %d, want %d", calls, testCase.wantCalls)
			}
			if testCase.wantError {
				if err == nil {
					t.Fatal("expected exchange error")
				}
				if result.Binding.ConnectionID != "" || result.Binding.BridgePrivateKey != "" || result.Binding.PersonaMCPToken != "" {
					t.Fatalf("rejected exchange returned binding: %+v", result.Binding)
				}
				return
			}
			if err != nil {
				t.Fatalf("exchange: %v", err)
			}
			if result.Binding.ConnectionID != "conn-1" || result.Binding.PersonaID != "persona-1" || result.Binding.RuntimeKind != runtime.AdapterKindHermes || result.Binding.ConnectionGeneration != 3 || !result.Binding.HasBridgeSecret || !result.Binding.HasPersonaMCPToken {
				t.Fatalf("binding = %+v", result.Binding)
			}
		})
	}
}

func TestClientExchangeCancelledContextMakesNoHTTPRequest(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	client := Client{
		GatewayBaseURL: "https://gateway.example",
		HTTPClient: &http.Client{Transport: pairingRoundTripper(func(request *http.Request) (*http.Response, error) {
			t.Fatalf("unexpected request: %s %s", request.Method, request.URL)
			return nil, nil
		})},
	}

	_, err := client.Exchange(ctx, Request{Code: "PAIR-1234", RuntimeKind: runtime.AdapterKindHermes})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Exchange() error = %v, want context canceled", err)
	}
}
