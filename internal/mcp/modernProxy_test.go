package mcp

import (
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/personastack/personastack-connector/internal/config"
)

func TestLoopbackProxyPreservesModernCatalogHeaders(t *testing.T) {
	t.Parallel()
	for _, status := range []int{http.StatusOK, http.StatusUnauthorized, http.StatusBadRequest} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			t.Parallel()
			calls := 0
			headers := http.Header{}
			headers.Set("MCP-Protocol-Version", "2026-07-28")
			headers.Set("Mcp-Method", "tools/call")
			headers.Set("Mcp-Name", "personastack_call_tool")
			headers.Set("PersonaStack-Tool-Catalog", "compact")
			headers.Add("Mcp-Name", "ambiguous-name")
			headers.Add("Mcp-Param-Region", "us-west1")
			headers.Add("Mcp-Param-Region", "us-east1")
			headers.Set("Mcp-Param-Message", "=?base64?SGVsbG8sIOS4lueVjA==?=")
			client := &http.Client{Transport: loopbackProxyRoundTripper(func(request *http.Request) (*http.Response, error) {
				calls++
				for key, expected := range headers {
					if !reflect.DeepEqual(request.Header.Values(key), expected) {
						t.Fatalf("header %s = %v, want %v", key, request.Header.Values(key), expected)
					}
				}
				if request.Header.Get("Authorization") != "Bearer remote-token" || request.Header.Get("X-Untrusted") != "" {
					t.Fatal("credential or header boundary changed")
				}
				return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(`{"result":{}}`)), Header: http.Header{
					"Cache-Control": {"private, no-store"}, "Vary": {"Authorization", "PersonaStack-Tool-Catalog"},
					"Www-Authenticate":  {`Bearer resource_metadata="https://mcp.example/.well-known/oauth-protected-resource"`},
					"X-Accel-Buffering": {"no"}, "Set-Cookie": {"must-not-forward"},
				}}, nil
			})}
			handler := loopbackHTTPProxyHandler{binding: config.Binding{PersonaMCPURL: "https://mcp.example/v1/mcp", PersonaMCPToken: "remote-token"}, localPath: "/mcp/local", localToken: "local-token", client: client}
			request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/mcp/local", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/call"}`))
			request.Header = headers.Clone()
			request.Header.Set("Authorization", "Bearer local-token")
			request.Header.Set("X-Untrusted", "private")
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			if calls != 1 || recorder.Code != status || recorder.Header().Get("Cache-Control") != "private, no-store" || len(recorder.Header().Values("Vary")) != 2 || recorder.Header().Get("WWW-Authenticate") == "" || recorder.Header().Get("X-Accel-Buffering") != "no" || recorder.Header().Get("Set-Cookie") != "" {
				t.Fatalf("response contract changed: status=%d calls=%d headers=%v", recorder.Code, calls, recorder.Header())
			}
		})
	}
}
