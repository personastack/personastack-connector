package mcp

import (
	"encoding/json"
	"os"
	"path/filepath"
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
}

func TestInstallerWritesOpenClawStdioServer(t *testing.T) {
	homeDir := t.TempDir()
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{{
		ConnectionID:    "conn-2",
		PersonaID:       "persona-2",
		RuntimeKind:     runtime.AdapterKindOpenClaw,
		NativeMCPServer: "personastack-conn-2",
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
