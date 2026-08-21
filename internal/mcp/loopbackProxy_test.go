package mcp

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/personastack/personastack-connector/internal/config"
)

type loopbackProxyRoundTripper func(*http.Request) (*http.Response, error)

func (roundTripper loopbackProxyRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return roundTripper(request)
}

func TestLoopbackHTTPProxyHandlerIngressFence(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name          string
		method        string
		path          string
		authorization string
		remoteURL     string
		remoteToken   string
		wantStatus    int
		wantCalls     int
	}
	testCases := []testCase{
		{
			name:          "wrong path",
			method:        http.MethodPost,
			path:          "/mcp/other",
			authorization: "Bearer local-token",
			remoteURL:     "https://mcp.example/mcp",
			remoteToken:   "remote-token",
			wantStatus:    http.StatusNotFound,
		},
		{
			name:        "wrong bearer token",
			method:      http.MethodPost,
			path:        "/mcp/conn-1",
			remoteURL:   "https://mcp.example/mcp",
			remoteToken: "remote-token",
			wantStatus:  http.StatusUnauthorized,
		},
		{
			name:          "unsupported method",
			method:        http.MethodPut,
			path:          "/mcp/conn-1",
			authorization: "Bearer local-token",
			remoteURL:     "https://mcp.example/mcp",
			remoteToken:   "remote-token",
			wantStatus:    http.StatusMethodNotAllowed,
		},
		{
			name:          "missing remote credential",
			method:        http.MethodPost,
			path:          "/mcp/conn-1",
			authorization: "Bearer local-token",
			wantStatus:    http.StatusBadGateway,
		},
		{
			name:          "authenticated request forwards mcp contract",
			method:        http.MethodPost,
			path:          "/mcp/conn-1?ignored=local",
			authorization: "Bearer local-token",
			remoteURL:     "https://mcp.example/mcp?server=owned",
			remoteToken:   "remote-token",
			wantStatus:    http.StatusAccepted,
			wantCalls:     1,
		},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			calls := 0
			client := &http.Client{Transport: loopbackProxyRoundTripper(func(request *http.Request) (*http.Response, error) {
				calls++
				if request.URL.String() != "https://mcp.example/mcp?server=owned" {
					t.Fatalf("upstream URL = %q", request.URL)
				}
				if request.Method != http.MethodPost {
					t.Fatalf("upstream method = %q", request.Method)
				}
				if request.Header.Get("Authorization") != "Bearer remote-token" {
					t.Fatalf("upstream authorization = %q", request.Header.Get("Authorization"))
				}
				if request.Header.Get("MCP-Protocol-Version") != "2025-11-25" || request.Header.Get("MCP-Session-Id") != "session-1" {
					t.Fatalf("upstream MCP headers = %+v", request.Header)
				}
				if request.Header.Get("X-Not-Forwarded") != "" {
					t.Fatalf("unexpected local header forwarding: %+v", request.Header)
				}
				body, err := io.ReadAll(request.Body)
				if err != nil {
					t.Fatalf("read upstream body: %v", err)
				}
				if string(body) != `{"jsonrpc":"2.0","method":"tools/list"}` {
					t.Fatalf("upstream body = %q", body)
				}
				responseHeaders := make(http.Header)
				responseHeaders.Set("MCP-Session-Id", "session-2")
				return &http.Response{
					StatusCode: http.StatusAccepted,
					Header:     responseHeaders,
					Body:       io.NopCloser(strings.NewReader(`{"ok":true}`)),
				}, nil
			})}
			handler := loopbackHTTPProxyHandler{
				binding: config.Binding{
					PersonaMCPURL:   testCase.remoteURL,
					PersonaMCPToken: testCase.remoteToken,
				},
				localPath:  "/mcp/conn-1",
				localToken: "local-token",
				client:     client,
			}
			request := httptest.NewRequest(testCase.method, "http://127.0.0.1"+testCase.path, bytes.NewBufferString(`{"jsonrpc":"2.0","method":"tools/list"}`))
			request.Header.Set("Authorization", testCase.authorization)
			request.Header.Set("MCP-Protocol-Version", "2025-11-25")
			request.Header.Set("MCP-Session-Id", "session-1")
			request.Header.Set("X-Not-Forwarded", "local-only")
			recorder := httptest.NewRecorder()

			handler.ServeHTTP(recorder, request)

			if recorder.Code != testCase.wantStatus {
				t.Fatalf("status = %d, want %d; body=%q", recorder.Code, testCase.wantStatus, recorder.Body.String())
			}
			if calls != testCase.wantCalls {
				t.Fatalf("upstream calls = %d, want %d", calls, testCase.wantCalls)
			}
			if testCase.wantCalls == 1 {
				if recorder.Header().Get("MCP-Session-Id") != "session-2" {
					t.Fatalf("response MCP session = %q", recorder.Header().Get("MCP-Session-Id"))
				}
				if recorder.Body.String() != `{"ok":true}` {
					t.Fatalf("response body = %q", recorder.Body.String())
				}
			}
		})
	}
}
