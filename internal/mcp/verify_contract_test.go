package mcp

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"

	"github.com/personastack/personastack-connector/internal/config"
)

type verifyContractRoundTripper func(*http.Request) (*http.Response, error)

func (roundTripper verifyContractRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return roundTripper(request)
}

func TestVerifyBindingLiveContractMapsMCPHandshake(t *testing.T) {
	t.Parallel()

	requests := []struct {
		method       string
		body         string
		sessionID    string
		protocol     string
		responseCode int
		responseBody string
	}{
		{
			method:       http.MethodPost,
			body:         `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"personastack-connector","version":"verify"}}}`,
			responseCode: http.StatusOK,
			responseBody: `{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-11-25","capabilities":{}}}`,
		},
		{
			method:       http.MethodPost,
			body:         `{"jsonrpc":"2.0","method":"notifications/initialized"}`,
			sessionID:    "session-1",
			protocol:     defaultMCPProtocolVersion,
			responseCode: http.StatusAccepted,
		},
		{
			method:       http.MethodPost,
			body:         `{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}`,
			sessionID:    "session-1",
			protocol:     defaultMCPProtocolVersion,
			responseCode: http.StatusOK,
			responseBody: `{"jsonrpc":"2.0","id":2,"result":{"tools":[]}}`,
		},
	}
	call := 0
	client := &http.Client{Transport: verifyContractRoundTripper(func(request *http.Request) (*http.Response, error) {
		if call >= len(requests) {
			t.Fatalf("unexpected request %s %s", request.Method, request.URL)
		}
		want := requests[call]
		call++
		if request.URL.String() != "https://mcp.example.test/mcp" {
			t.Fatalf("URL = %q", request.URL)
		}
		if request.Method != want.method {
			t.Fatalf("method = %q, want %q", request.Method, want.method)
		}
		if request.Header.Get("Authorization") != "Bearer mcp-token" {
			t.Fatalf("authorization = %q", request.Header.Get("Authorization"))
		}
		if request.Header.Get("Content-Type") != "application/json" || request.Header.Get("Accept") != "application/json, text/event-stream" {
			t.Fatalf("content negotiation headers = %+v", request.Header)
		}
		if request.Header.Get("MCP-Session-Id") != want.sessionID || request.Header.Get("MCP-Protocol-Version") != want.protocol {
			t.Fatalf("MCP session headers = %+v", request.Header)
		}
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Fatalf("read request body: %v", err)
		}
		if !jsonRawMessagesEqual(body, []byte(want.body)) {
			t.Fatalf("request body = %s, want %s", body, want.body)
		}
		headers := make(http.Header)
		headers.Set("Content-Type", "application/json")
		if call == 1 {
			headers.Set("MCP-Session-Id", "session-1")
		}
		return &http.Response{
			StatusCode: want.responseCode,
			Header:     headers,
			Body:       io.NopCloser(bytes.NewBufferString(want.responseBody)),
			Request:    request,
		}, nil
	})}

	result := VerifyBindingLive(context.Background(), config.Binding{
		PersonaMCPURL:   "https://mcp.example.test/mcp",
		PersonaMCPToken: "mcp-token",
	}, client)
	want := LiveVerifyResult{OK: true, Note: "PersonaStack MCP endpoint verified"}
	if result != want {
		t.Fatalf("VerifyBindingLive() = %+v, want %+v", result, want)
	}
	if call != len(requests) {
		t.Fatalf("request count = %d, want %d", call, len(requests))
	}
}

func TestVerifyBindingLiveContractRejectsMissingCredentialWithoutRequest(t *testing.T) {
	t.Parallel()

	client := &http.Client{Transport: verifyContractRoundTripper(func(request *http.Request) (*http.Response, error) {
		t.Fatalf("unexpected request %s %s", request.Method, request.URL)
		return nil, nil
	})}
	result := VerifyBindingLive(context.Background(), config.Binding{PersonaMCPURL: "https://mcp.example.test/mcp"}, client)
	want := LiveVerifyResult{Note: "PersonaStack MCP credential missing", DiagnosticCode: "mcp_token_missing"}
	if result != want {
		t.Fatalf("VerifyBindingLive() = %+v, want %+v", result, want)
	}
}

func TestVerifyBindingLiveContractStopsAfterAuthenticationRejection(t *testing.T) {
	t.Parallel()

	call := 0
	client := &http.Client{Transport: verifyContractRoundTripper(func(request *http.Request) (*http.Response, error) {
		call++
		if request.Method != http.MethodPost || request.URL.String() != "https://mcp.example.test/mcp" {
			t.Fatalf("request = %s %s", request.Method, request.URL)
		}
		if request.Header.Get("Authorization") != "Bearer rejected-token" {
			t.Fatalf("authorization = %q", request.Header.Get("Authorization"))
		}
		return &http.Response{
			StatusCode: http.StatusUnauthorized,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(bytes.NewBufferString(`{"error":"invalid token"}`)),
			Request:    request,
		}, nil
	})}

	result := VerifyBindingLive(context.Background(), config.Binding{
		PersonaMCPURL:   "https://mcp.example.test/mcp",
		PersonaMCPToken: "rejected-token",
	}, client)
	if result.OK || result.DiagnosticCode != "mcp_token_rejected" {
		t.Fatalf("VerifyBindingLive() = %+v", result)
	}
	if call != 1 {
		t.Fatalf("request count = %d, want 1", call)
	}
}
