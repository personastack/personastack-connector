package mcp

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/personastack/personastack-connector/internal/config"
	"github.com/personastack/personastack-connector/internal/runtime"
	"gopkg.in/yaml.v3"
)

func TestInstallerWritesHermesStdioServer(t *testing.T) {
	homeDir := t.TempDir()
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{{
		ConnectionID:    "conn-1",
		PersonaID:       "persona-1",
		RuntimeKind:     runtime.AdapterKindHermes,
		NativeMCPServer: "personastack-conn-1",
		PersonaMCPToken: "secret-mcp-token",
	}}})
	results, err := (Installer{Store: store, HomeDir: homeDir, ExecutablePath: "/usr/local/bin/personastack-connector"}).InstallAll()
	if err != nil {
		t.Fatalf("InstallAll() error = %v", err)
	}
	if len(results) != 1 || results[0].ServerName != "personastack-conn-1" {
		t.Fatalf("unexpected results: %+v", results)
	}
	raw, err := os.ReadFile(filepath.Join(homeDir, ".hermes", "config.yaml"))
	if err != nil {
		t.Fatalf("read Hermes config: %v", err)
	}
	var root map[string]any
	if err := yaml.Unmarshal(raw, &root); err != nil {
		t.Fatalf("parse Hermes config: %v", err)
	}
	server := root["mcp"].(map[string]any)["servers"].(map[string]any)["personastack-conn-1"].(map[string]any)
	if server["command"] != "/usr/local/bin/personastack-connector" {
		t.Fatalf("unexpected server: %+v", server)
	}
	args, ok := server["args"].([]any)
	if !ok || len(args) != 4 || args[0] != "mcp" || args[1] != "stdio" || args[2] != "--binding" || args[3] != "conn-1" {
		t.Fatalf("unexpected args: %+v", server["args"])
	}
	if strings.Contains(string(raw), "secret-mcp-token") {
		t.Fatalf("Hermes config contains MCP bearer token: %s", string(raw))
	}
}

func TestInstallerWritesOpenClawStdioServer(t *testing.T) {
	homeDir := t.TempDir()
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{{
		ConnectionID:    "conn-2",
		PersonaID:       "persona-2",
		RuntimeKind:     runtime.AdapterKindOpenClaw,
		NativeMCPServer: "personastack-conn-2",
		PersonaMCPToken: "secret-mcp-token",
	}}})
	_, err := (Installer{Store: store, HomeDir: homeDir, ExecutablePath: "/opt/personastack-connector"}).InstallAll()
	if err != nil {
		t.Fatalf("InstallAll() error = %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(homeDir, ".openclaw", "config.json"))
	if err != nil {
		t.Fatalf("read OpenClaw config: %v", err)
	}
	var root map[string]any
	if err := json.Unmarshal(raw, &root); err != nil {
		t.Fatalf("parse OpenClaw config: %v", err)
	}
	server := root["mcp"].(map[string]any)["servers"].(map[string]any)["personastack-conn-2"].(map[string]any)
	if server["command"] != "/opt/personastack-connector" {
		t.Fatalf("unexpected server: %+v", server)
	}
	args, ok := server["args"].([]any)
	if !ok || len(args) != 4 || args[0] != "mcp" || args[1] != "stdio" || args[2] != "--binding" || args[3] != "conn-2" {
		t.Fatalf("unexpected args: %+v", server["args"])
	}
	if strings.Contains(string(raw), "secret-mcp-token") {
		t.Fatalf("OpenClaw config contains MCP bearer token: %s", string(raw))
	}
}

func TestVerifyBindingRequiresCredentialAndInstalledServer(t *testing.T) {
	homeDir := t.TempDir()
	binding := config.Binding{
		ConnectionID:       "conn-1",
		PersonaID:          "persona-1",
		RuntimeKind:        runtime.AdapterKindHermes,
		NativeMCPServer:    "personastack-conn-1",
		PersonaMCPURL:      "https://mcp.personastack.ai/mcp",
		PersonaMCPToken:    "token-1",
		HasPersonaMCPToken: true,
	}
	missing := VerifyBinding(homeDir, binding)
	if missing.State != runtime.AdapterStateMCPConfigMissing {
		t.Fatalf("missing.State = %s", missing.State)
	}
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{binding}})
	if _, err := (Installer{Store: store, HomeDir: homeDir, ExecutablePath: "/usr/local/bin/personastack-connector"}).InstallAll(); err != nil {
		t.Fatalf("InstallAll() error = %v", err)
	}
	verified := VerifyBinding(homeDir, binding)
	if verified.State != runtime.AdapterStateMCPVerified {
		t.Fatalf("verified.State = %s note=%s", verified.State, verified.Note)
	}
}

func TestVerifyBindingRejectsBrokenStdioServerConfig(t *testing.T) {
	tests := []struct {
		name   string
		server map[string]any
	}{
		{
			name:   "missing command",
			server: map[string]any{"args": []any{"mcp", "stdio", "--binding", "conn-1"}},
		},
		{
			name:   "wrong command shape",
			server: map[string]any{"command": "/bin/personastack-connector", "args": []any{"not-a-binding", "conn-1"}},
		},
		{
			name:   "wrong binding",
			server: map[string]any{"command": "/bin/personastack-connector", "args": []any{"mcp", "stdio", "--binding", "conn-2"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state, note := verifyServerMap(map[string]any{
				"mcp": map[string]any{
					"servers": map[string]any{
						"personastack-conn-1": tt.server,
					},
				},
			}, "personastack-conn-1", "conn-1")
			if state != runtime.AdapterStateMCPConfigMissing {
				t.Fatalf("state = %s note=%s", state, note)
			}
		})
	}
}
