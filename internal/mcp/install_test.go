package mcp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/personastack/personastack-connector/internal/config"
	"github.com/personastack/personastack-connector/internal/runtime"
	"gopkg.in/yaml.v3"
)

func TestInstallerWritesHermesStdioServer(t *testing.T) {
	t.Setenv("PERSONASTACK_CONNECTOR_DISABLE_HERMES_GATEWAY_START", "1")
	homeDir := t.TempDir()
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{{
		ConnectionID:    "conn-1",
		PersonaID:       "persona-1",
		RuntimeKind:     runtime.AdapterKindHermes,
		NativeMCPServer: "personastack-conn-1",
		PersonaMCPToken: "secret-mcp-token",
	}}})
	results, err := (Installer{Store: store, HomeDir: homeDir, ExecutablePath: "/usr/local/bin/personastack-connector", GOOS: "linux"}).InstallAll()
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
	server := root["mcp_servers"].(map[string]any)["personastack-conn-1"].(map[string]any)
	shimPath := filepath.Join(homeDir, ".local", "bin", "personastack-connector")
	if server["command"] != shimPath {
		t.Fatalf("unexpected server: %+v", server)
	}
	args, ok := server["args"].([]any)
	if !ok || len(args) != 4 || args[0] != "mcp" || args[1] != "stdio" || args[2] != "--binding" || args[3] != "conn-1" {
		t.Fatalf("unexpected args: %+v", server["args"])
	}
	if server["timeout"] != 120 || server["connect_timeout"] != 60 || server["enabled"] != true {
		t.Fatalf("unexpected Hermes server policy: %+v", server)
	}
	if strings.Contains(string(raw), "secret-mcp-token") {
		t.Fatalf("Hermes config contains MCP bearer token: %s", string(raw))
	}
	envRaw, err := os.ReadFile(filepath.Join(homeDir, ".hermes", ".env"))
	if err != nil {
		t.Fatalf("read Hermes env: %v", err)
	}
	for _, want := range []string{"API_SERVER_ENABLED=true", "API_SERVER_HOST=127.0.0.1", "API_SERVER_PORT=8642", "API_SERVER_KEY="} {
		if !strings.Contains(string(envRaw), want) {
			t.Fatalf("Hermes env missing %q:\n%s", want, string(envRaw))
		}
	}
	shim, err := os.ReadFile(shimPath)
	if err != nil {
		t.Fatalf("read shim: %v", err)
	}
	if !strings.Contains(string(shim), "exec '/usr/local/bin/personastack-connector'") {
		t.Fatalf("unexpected shim:\n%s", shim)
	}
}

func TestInstallerWritesDistinctHermesServersPerBinding(t *testing.T) {
	homeDir := t.TempDir()
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{
		{
			ConnectionID:    "conn-1",
			PersonaID:       "persona-1",
			RuntimeKind:     runtime.AdapterKindHermes,
			PersonaMCPToken: "token-1",
		},
		{
			ConnectionID:    "conn-2",
			PersonaID:       "persona-2",
			RuntimeKind:     runtime.AdapterKindHermes,
			PersonaMCPToken: "token-2",
		},
	}})
	results, err := (Installer{Store: store, HomeDir: homeDir, ExecutablePath: "/usr/local/bin/personastack-connector", GOOS: "linux"}).InstallAll()
	if err != nil {
		t.Fatalf("InstallAll() error = %v", err)
	}
	if len(results) != 2 {
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
	servers := root["mcp_servers"].(map[string]any)
	if _, ok := servers["personastack-conn-1"]; !ok {
		t.Fatalf("first Hermes server missing: %+v", servers)
	}
	if _, ok := servers["personastack-conn-2"]; !ok {
		t.Fatalf("second Hermes server missing: %+v", servers)
	}
}

func TestInstallerWritesDistinctNativeMCPServersForMultipleBindings(t *testing.T) {
	homeDir := t.TempDir()
	t.Setenv("PERSONASTACK_CONNECTOR_DISABLE_HERMES_GATEWAY_START", "1")
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{
		{
			ConnectionID:    "conn-1",
			PersonaID:       "persona-1",
			RuntimeKind:     runtime.AdapterKindHermes,
			NativeMCPServer: "personastack-conn-1",
		},
		{
			ConnectionID:    "conn-2",
			PersonaID:       "persona-2",
			RuntimeKind:     runtime.AdapterKindOpenClaw,
			NativeMCPServer: "personastack-conn-2",
		},
	}})
	results, err := (Installer{Store: store, HomeDir: homeDir, ExecutablePath: "/usr/local/bin/personastack-connector", GOOS: "linux"}).InstallAll()
	if err != nil {
		t.Fatalf("InstallAll() error = %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("unexpected results: %+v", results)
	}
	if results[0].ServerName != "personastack-conn-1" || results[1].ServerName != "personastack-conn-2" {
		t.Fatalf("unexpected server names: %+v", results)
	}
	hermesRaw, err := os.ReadFile(filepath.Join(homeDir, ".hermes", "config.yaml"))
	if err != nil {
		t.Fatalf("read Hermes config: %v", err)
	}
	var hermesRoot map[string]any
	if err := yaml.Unmarshal(hermesRaw, &hermesRoot); err != nil {
		t.Fatalf("parse Hermes config: %v", err)
	}
	hermesServer := hermesRoot["mcp_servers"].(map[string]any)["personastack-conn-1"].(map[string]any)
	if args, ok := hermesServer["args"].([]any); !ok || len(args) != 4 || args[3] != "conn-1" {
		t.Fatalf("unexpected Hermes args: %+v", hermesServer["args"])
	}
	openClawRaw, err := os.ReadFile(filepath.Join(homeDir, ".openclaw", "config.json"))
	if err != nil {
		t.Fatalf("read OpenClaw config: %v", err)
	}
	var openClawRoot map[string]any
	if err := json.Unmarshal(openClawRaw, &openClawRoot); err != nil {
		t.Fatalf("parse OpenClaw config: %v", err)
	}
	openClawServer := openClawRoot["mcp"].(map[string]any)["servers"].(map[string]any)["personastack-conn-2"].(map[string]any)
	if args, ok := openClawServer["args"].([]any); !ok || len(args) != 4 || args[3] != "conn-2" {
		t.Fatalf("unexpected OpenClaw args: %+v", openClawServer["args"])
	}
}

func TestVerifyBindingWithLivePromotesVerifiedState(t *testing.T) {
	homeDir := t.TempDir()
	binding := config.Binding{
		ConnectionID:       "conn-1",
		PersonaID:          "persona-1",
		RuntimeKind:        runtime.AdapterKindHermes,
		NativeMCPServer:    "personastack-conn-1",
		PersonaMCPURL:      "",
		PersonaMCPToken:    "token-1",
		HasPersonaMCPToken: true,
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodPost:
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	binding.PersonaMCPURL = server.URL
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{binding}})
	if _, err := (Installer{Store: store, HomeDir: homeDir, ExecutablePath: "/usr/local/bin/personastack-connector", GOOS: "linux"}).InstallAll(); err != nil {
		t.Fatalf("InstallAll() error = %v", err)
	}
	verified := VerifyBindingWithLive(context.Background(), homeDir, binding, server.Client())
	if verified.State != runtime.AdapterStateMCPVerified {
		t.Fatalf("verified.State = %s note=%s", verified.State, verified.Note)
	}
}

func TestHermesInstallCreatesBackupAndRemovesLegacyPersonaStackEntry(t *testing.T) {
	homeDir := t.TempDir()
	path := filepath.Join(homeDir, ".hermes", "config.yaml")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	original := []byte(strings.Join([]string{
		"mcp:",
		"  servers:",
		"    personastack-conn-1:",
		"      command: old",
		"      args: [old]",
		"unrelated: true",
		"",
	}, "\n"))
	if err := os.WriteFile(path, original, 0o600); err != nil {
		t.Fatalf("write original: %v", err)
	}
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{{
		ConnectionID:    "conn-1",
		PersonaID:       "persona-1",
		RuntimeKind:     runtime.AdapterKindHermes,
		NativeMCPServer: "personastack-conn-1",
		PersonaMCPToken: "secret-mcp-token",
	}}})
	if _, err := (Installer{Store: store, HomeDir: homeDir, ExecutablePath: "/usr/local/bin/personastack-connector", GOOS: "linux"}).InstallAll(); err != nil {
		t.Fatalf("InstallAll() error = %v", err)
	}
	backup, err := os.ReadFile(path + ".personastack.bak")
	if err != nil {
		t.Fatalf("read backup: %v", err)
	}
	if string(backup) != string(original) {
		t.Fatalf("backup mismatch:\n%s", string(backup))
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	var root map[string]any
	if err := yaml.Unmarshal(raw, &root); err != nil {
		t.Fatalf("parse config: %v", err)
	}
	if _, ok := root["mcp"].(map[string]any); ok {
		t.Fatalf("legacy mcp entry remains: %+v", root["mcp"])
	}
	if _, ok := root["mcp_servers"].(map[string]any)["personastack-conn-1"]; !ok {
		t.Fatalf("mcp_servers entry missing: %+v", root)
	}
}

func TestHermesInstallPreservesOriginalBackupAcrossMultipleBindings(t *testing.T) {
	homeDir := t.TempDir()
	path := filepath.Join(homeDir, ".hermes", "config.yaml")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	original := []byte("unrelated: true\n")
	if err := os.WriteFile(path, original, 0o600); err != nil {
		t.Fatalf("write original: %v", err)
	}
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{
		{
			ConnectionID:    "conn-1",
			PersonaID:       "persona-1",
			RuntimeKind:     runtime.AdapterKindHermes,
			NativeMCPServer: "personastack-conn-1",
			PersonaMCPToken: "secret-mcp-token",
		},
		{
			ConnectionID:    "conn-2",
			PersonaID:       "persona-2",
			RuntimeKind:     runtime.AdapterKindHermes,
			NativeMCPServer: "personastack-conn-2",
			PersonaMCPToken: "secret-mcp-token",
		},
	}})
	if _, err := (Installer{Store: store, HomeDir: homeDir, ExecutablePath: "/usr/local/bin/personastack-connector", GOOS: "linux"}).InstallAll(); err != nil {
		t.Fatalf("InstallAll() error = %v", err)
	}
	backup, err := os.ReadFile(path + ".personastack.bak")
	if err != nil {
		t.Fatalf("read backup: %v", err)
	}
	if string(backup) != string(original) {
		t.Fatalf("backup mismatch:\n%s", string(backup))
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	var root map[string]any
	if err := yaml.Unmarshal(raw, &root); err != nil {
		t.Fatalf("parse config: %v", err)
	}
	servers := root["mcp_servers"].(map[string]any)
	if _, ok := servers["personastack-conn-1"]; !ok {
		t.Fatalf("first server missing: %+v", servers)
	}
	if _, ok := servers["personastack-conn-2"]; !ok {
		t.Fatalf("second server missing: %+v", servers)
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
	_, err := (Installer{Store: store, HomeDir: homeDir, ExecutablePath: "/opt/personastack-connector", GOOS: "linux"}).InstallAll()
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
	shimPath := filepath.Join(homeDir, ".local", "bin", "personastack-connector")
	if server["command"] != shimPath {
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

func TestInstallerUsesOpenClawCLIWhenAvailable(t *testing.T) {
	homeDir := t.TempDir()
	binDir := t.TempDir()
	logPath := filepath.Join(t.TempDir(), "openclaw.log")
	scriptPath := filepath.Join(binDir, "openclaw")
	script := []byte("#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$PERSONASTACK_OPENCLAW_LOG\"\n")
	if err := os.WriteFile(scriptPath, script, 0o755); err != nil {
		t.Fatalf("write script: %v", err)
	}
	t.Setenv("PERSONASTACK_OPENCLAW_LOG", logPath)
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{{
		ConnectionID:    "conn-2",
		PersonaID:       "persona-2",
		RuntimeKind:     runtime.AdapterKindOpenClaw,
		NativeMCPServer: "personastack-conn-2",
		PersonaMCPToken: "secret-mcp-token",
	}}})
	results, err := (Installer{Store: store, HomeDir: homeDir, ExecutablePath: "/opt/personastack-connector", GOOS: "linux"}).InstallAll()
	if err != nil {
		t.Fatalf("InstallAll() error = %v", err)
	}
	if len(results) != 1 || results[0].Path != filepath.Join(homeDir, ".openclaw", "config.json") {
		t.Fatalf("unexpected results: %+v", results)
	}
	if _, err := os.Stat(filepath.Join(homeDir, ".openclaw", "config.json")); !os.IsNotExist(err) {
		t.Fatalf("unexpected OpenClaw config write: %v", err)
	}
	raw, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) != 4 || lines[0] != "mcp" || lines[1] != "set" || lines[2] != "personastack-conn-2" {
		t.Fatalf("unexpected CLI args: %q", string(raw))
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(lines[3]), &payload); err != nil {
		t.Fatalf("parse CLI payload: %v", err)
	}
	if payload["command"] != filepath.Join(homeDir, ".local", "bin", "personastack-connector") {
		t.Fatalf("unexpected command: %+v", payload)
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
	if _, err := (Installer{Store: store, HomeDir: homeDir, ExecutablePath: "/usr/local/bin/personastack-connector", GOOS: "linux"}).InstallAll(); err != nil {
		t.Fatalf("InstallAll() error = %v", err)
	}
	verified := VerifyBinding(homeDir, binding)
	if verified.State != runtime.AdapterStateMCPRestartRequired {
		t.Fatalf("verified.State = %s note=%s", verified.State, verified.Note)
	}
}

func TestVerifyBindingRequiresRestartForOpenClawServer(t *testing.T) {
	homeDir := t.TempDir()
	binding := config.Binding{
		ConnectionID:       "conn-2",
		PersonaID:          "persona-2",
		RuntimeKind:        runtime.AdapterKindOpenClaw,
		NativeMCPServer:    "personastack-conn-2",
		PersonaMCPURL:      "https://mcp.personastack.ai/mcp",
		PersonaMCPToken:    "token-2",
		HasPersonaMCPToken: true,
	}
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{binding}})
	if _, err := (Installer{Store: store, HomeDir: homeDir, ExecutablePath: "/usr/local/bin/personastack-connector", GOOS: "linux"}).InstallAll(); err != nil {
		t.Fatalf("InstallAll() error = %v", err)
	}
	verified := VerifyBinding(homeDir, binding)
	if verified.State != runtime.AdapterStateMCPRestartRequired {
		t.Fatalf("verified.State = %s note=%s", verified.State, verified.Note)
	}
}

func TestVerifyBindingWithLiveRequiresLiveVerificationForHermesAndOpenClaw(t *testing.T) {
	live := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		var message struct {
			Method string `json:"method"`
		}
		if err := json.NewDecoder(r.Body).Decode(&message); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		switch message.Method {
		case "initialize":
			w.Header().Set("MCP-Session-Id", "session-1")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-11-25","capabilities":{}}}`))
		case "notifications/initialized":
			w.WriteHeader(http.StatusAccepted)
		case "tools/list":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":2}`))
		default:
			t.Fatalf("unexpected method: %s", message.Method)
		}
	}))
	defer live.Close()

	for _, tt := range []struct {
		name        string
		runtimeKind runtime.AdapterKind
		configPath  string
	}{
		{name: "hermes", runtimeKind: runtime.AdapterKindHermes, configPath: ".hermes/config.yaml"},
		{name: "openclaw", runtimeKind: runtime.AdapterKindOpenClaw, configPath: ".openclaw/config.json"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			homeDir := t.TempDir()
			store := config.NewMemoryStore(config.State{Bindings: []config.Binding{{
				ConnectionID:    "conn-1",
				PersonaID:       "persona-1",
				RuntimeKind:     tt.runtimeKind,
				NativeMCPServer: "personastack-conn-1",
				PersonaMCPURL:   live.URL,
				PersonaMCPToken: "token-1",
			}}})
			if _, err := (Installer{Store: store, HomeDir: homeDir, ExecutablePath: "/usr/local/bin/personastack-connector", GOOS: "linux"}).InstallAll(); err != nil {
				t.Fatalf("InstallAll() error = %v", err)
			}

			binding := config.Binding{
				ConnectionID:       "conn-1",
				PersonaID:          "persona-1",
				RuntimeKind:        tt.runtimeKind,
				NativeMCPServer:    "personastack-conn-1",
				PersonaMCPURL:      live.URL,
				PersonaMCPToken:    "token-1",
				HasPersonaMCPToken: true,
			}
			verified := VerifyBinding(homeDir, binding)
			if verified.State != runtime.AdapterStateMCPRestartRequired {
				t.Fatalf("VerifyBinding() = %+v", verified)
			}
			liveVerified := VerifyBindingWithLive(context.Background(), homeDir, binding, live.Client())
			if liveVerified.State != runtime.AdapterStateMCPRestartRequired {
				t.Fatalf("VerifyBindingWithLive() = %+v", liveVerified)
			}
			if !strings.Contains(liveVerified.Note, "tools/list invalid") {
				t.Fatalf("unexpected live note: %q", liveVerified.Note)
			}
			if _, err := os.Stat(filepath.Join(homeDir, tt.configPath)); err != nil {
				t.Fatalf("expected config path %s: %v", tt.configPath, err)
			}
		})
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
