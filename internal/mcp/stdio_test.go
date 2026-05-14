package mcp

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/personastack/personastack-connector/internal/config"
)

func TestStdioProxyForwardsJSONLines(t *testing.T) {
	var authHeader string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{}}`))
	}))
	defer server.Close()
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{{
		ConnectionID:    "conn-1",
		PersonaMCPURL:   server.URL,
		PersonaMCPToken: "token-1",
	}}})
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := NewStdioProxy(store).Serve(context.Background(), "conn-1", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"initialize"}`+"\n"), &stdout, &stderr)
	if err != nil {
		t.Fatalf("Serve() error = %v stderr=%s", err, stderr.String())
	}
	if authHeader != "Bearer token-1" {
		t.Fatalf("auth header = %q", authHeader)
	}
	if !strings.Contains(stdout.String(), `"result"`) {
		t.Fatalf("unexpected stdout: %s", stdout.String())
	}
}

func TestStdioProxyMissingToken(t *testing.T) {
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{{ConnectionID: "conn-1"}}})
	err := NewStdioProxy(store).Serve(context.Background(), "conn-1", strings.NewReader(""), &bytes.Buffer{}, &bytes.Buffer{})
	if !errors.Is(err, ErrMissingMCPToken) {
		t.Fatalf("Serve() error = %v, want ErrMissingMCPToken", err)
	}
}
