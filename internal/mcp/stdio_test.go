package mcp

import (
	"bytes"
	"context"
	"encoding/json"
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

func TestStdioProxyCarriesMCPSessionHeaders(t *testing.T) {
	var initializedSessionHeader string
	var initializedProtocolHeader string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var message rpcMessage
		if err := json.NewDecoder(r.Body).Decode(&message); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		switch message.Method {
		case "initialize":
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("MCP-Session-Id", "session-1")
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-11-25","capabilities":{},"serverInfo":{"name":"personastack","version":"test"}}}`))
		case "notifications/initialized":
			initializedSessionHeader = r.Header.Get("MCP-Session-Id")
			initializedProtocolHeader = r.Header.Get("MCP-Protocol-Version")
			w.WriteHeader(http.StatusAccepted)
		default:
			t.Fatalf("unexpected method: %s", message.Method)
		}
	}))
	defer server.Close()
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{{
		ConnectionID:    "conn-1",
		PersonaMCPURL:   server.URL,
		PersonaMCPToken: "token-1",
	}}})
	input := strings.Join([]string{
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`,
		`{"jsonrpc":"2.0","method":"notifications/initialized"}`,
		"",
	}, "\n")
	var stdout bytes.Buffer
	err := NewStdioProxy(store).Serve(context.Background(), "conn-1", strings.NewReader(input), &stdout, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("Serve() error = %v", err)
	}
	if initializedSessionHeader != "session-1" {
		t.Fatalf("MCP-Session-Id = %q", initializedSessionHeader)
	}
	if initializedProtocolHeader != "2025-11-25" {
		t.Fatalf("MCP-Protocol-Version = %q", initializedProtocolHeader)
	}
	if strings.Count(stdout.String(), "\n") != 1 {
		t.Fatalf("notification should not emit an empty stdio response, stdout=%q", stdout.String())
	}
}

func TestStdioProxyDecodesSSEJSONPayload(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: message\n"))
		_, _ = w.Write([]byte(`data: {"jsonrpc":"2.0","id":1,"result":{"ok":true}}` + "\n\n"))
	}))
	defer server.Close()
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{{
		ConnectionID:    "conn-1",
		PersonaMCPURL:   server.URL,
		PersonaMCPToken: "token-1",
	}}})
	var stdout bytes.Buffer
	err := NewStdioProxy(store).Serve(context.Background(), "conn-1", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`+"\n"), &stdout, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("Serve() error = %v", err)
	}
	if !strings.Contains(stdout.String(), `"ok":true`) {
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
