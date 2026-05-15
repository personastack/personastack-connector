package mcp

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
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

func TestInstallerFallsBackToHermesLoopbackHTTPWhenStdioConfigIsInvalid(t *testing.T) {
	homeDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(homeDir, ".hermes"), 0o700); err != nil {
		t.Fatalf("mkdir hermes: %v", err)
	}
	if err := os.WriteFile(filepath.Join(homeDir, ".hermes", "config.yaml"), []byte("!!! invalid yaml\n"), 0o600); err != nil {
		t.Fatalf("write invalid hermes config: %v", err)
	}
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{{
		ConnectionID:    "conn-1",
		PersonaID:       "persona-1",
		RuntimeKind:     runtime.AdapterKindHermes,
		PersonaMCPToken: "secret-mcp-token",
	}}})
	results, err := (Installer{Store: store, HomeDir: homeDir, ExecutablePath: "/usr/local/bin/personastack-connector", GOOS: "linux"}).InstallAll()
	if err != nil {
		t.Fatalf("InstallAll() error = %v", err)
	}
	if len(results) != 1 || !strings.Contains(results[0].Note, "loopback HTTP MCP proxy configured") || !strings.Contains(results[0].Note, "credential warning") {
		t.Fatalf("unexpected results: %+v", results)
	}
	envRaw, err := os.ReadFile(filepath.Join(homeDir, ".hermes", ".env"))
	if err != nil {
		t.Fatalf("read Hermes env: %v", err)
	}
	if !strings.Contains(string(envRaw), "PERSONASTACK_CONNECTOR_LOCAL_MCP_CONN_1=") {
		t.Fatalf("Hermes env missing local MCP token:\n%s", string(envRaw))
	}
	envInfo, err := os.Stat(filepath.Join(homeDir, ".hermes", ".env"))
	if err != nil {
		t.Fatalf("stat Hermes env: %v", err)
	}
	if envInfo.Mode().Perm() != 0o600 {
		t.Fatalf("unexpected Hermes env mode: %v", envInfo.Mode().Perm())
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
	if got := server["url"]; !strings.HasPrefix(got.(string), "http://127.0.0.1:") || !strings.Contains(got.(string), "/mcp/conn-1") {
		t.Fatalf("unexpected loopback url: %+v", got)
	}
	headers := server["headers"].(map[string]any)
	if got := headers["Authorization"]; !strings.HasPrefix(got.(string), "Bearer ${PERSONASTACK_CONNECTOR_LOCAL_MCP_CONN_1}") {
		t.Fatalf("unexpected Hermes headers: %+v", headers)
	}
	info, err := os.Stat(filepath.Join(homeDir, ".hermes", "config.yaml"))
	if err != nil {
		t.Fatalf("stat Hermes config: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("unexpected Hermes config mode: %v", info.Mode().Perm())
	}
}

func TestInstallerReusesExistingLoopbackHTTPProxy(t *testing.T) {
	homeDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(homeDir, ".openclaw"), 0o700); err != nil {
		t.Fatalf("mkdir openclaw: %v", err)
	}
	if err := os.WriteFile(filepath.Join(homeDir, ".openclaw", "config.json"), []byte("{invalid json"), 0o600); err != nil {
		t.Fatalf("write invalid openclaw config: %v", err)
	}
	binding := config.Binding{
		ConnectionID:          "conn-2",
		PersonaID:             "persona-2",
		RuntimeKind:           runtime.AdapterKindOpenClaw,
		PersonaMCPToken:       "secret-mcp-token",
		LocalMCPProxyURL:      "http://127.0.0.1:23119/mcp/conn-2",
		LocalMCPProxyToken:    "local-token",
		HasLocalMCPProxyToken: true,
	}
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{binding}})
	if _, err := (Installer{Store: &store, HomeDir: homeDir, ExecutablePath: "/usr/local/bin/personastack-connector", GOOS: "linux"}).InstallAll(); err != nil {
		t.Fatalf("InstallAll() error = %v", err)
	}
	stored, ok := store.Binding("conn-2")
	if !ok {
		t.Fatal("binding missing")
	}
	if stored.LocalMCPProxyURL != binding.LocalMCPProxyURL || stored.LocalMCPProxyToken != binding.LocalMCPProxyToken {
		t.Fatalf("loopback proxy changed: %+v", stored)
	}
	raw, err := os.ReadFile(filepath.Join(homeDir, ".openclaw", "config.json"))
	if err != nil {
		t.Fatalf("read OpenClaw config: %v", err)
	}
	if !strings.Contains(string(raw), binding.LocalMCPProxyURL) || !strings.Contains(string(raw), "Bearer "+binding.LocalMCPProxyToken) {
		t.Fatalf("config did not reuse loopback proxy:\n%s", string(raw))
	}
}

func TestInstallerFallsBackToOpenClawLoopbackHTTPWhenStdioConfigIsInvalid(t *testing.T) {
	homeDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(homeDir, ".openclaw"), 0o700); err != nil {
		t.Fatalf("mkdir openclaw: %v", err)
	}
	if err := os.WriteFile(filepath.Join(homeDir, ".openclaw", "config.json"), []byte("{invalid json"), 0o600); err != nil {
		t.Fatalf("write invalid openclaw config: %v", err)
	}
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{{
		ConnectionID:    "conn-2",
		PersonaID:       "persona-2",
		RuntimeKind:     runtime.AdapterKindOpenClaw,
		PersonaMCPToken: "secret-mcp-token",
	}}})
	results, err := (Installer{Store: store, HomeDir: homeDir, ExecutablePath: "/usr/local/bin/personastack-connector", GOOS: "linux"}).InstallAll()
	if err != nil {
		t.Fatalf("InstallAll() error = %v", err)
	}
	if len(results) != 1 || !strings.Contains(results[0].Note, "loopback HTTP MCP proxy configured") || !strings.Contains(results[0].Note, "credential warning") {
		t.Fatalf("unexpected results: %+v", results)
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
	if got := server["url"]; !strings.HasPrefix(got.(string), "http://127.0.0.1:") || !strings.Contains(got.(string), "/mcp/conn-2") {
		t.Fatalf("unexpected loopback url: %+v", got)
	}
	if got := server["transport"]; got != "streamable-http" {
		t.Fatalf("unexpected transport: %+v", got)
	}
	headers := server["headers"].(map[string]any)
	if got := headers["Authorization"]; !strings.HasPrefix(got.(string), "Bearer ") || strings.Contains(got.(string), "secret-mcp-token") {
		t.Fatalf("unexpected OpenClaw headers: %+v", headers)
	}
	info, err := os.Stat(filepath.Join(homeDir, ".openclaw", "config.json"))
	if err != nil {
		t.Fatalf("stat OpenClaw config: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("unexpected OpenClaw config mode: %v", info.Mode().Perm())
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

func TestVerifyBindingAcceptsStreamableHTTPServer(t *testing.T) {
	homeDir := t.TempDir()
	path := filepath.Join(homeDir, ".openclaw", "config.json")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	raw := []byte(`{
  "mcp": {
    "servers": {
      "personastack-conn-2": {
        "transport": "streamable-http",
        "url": "http://127.0.0.1:23119/mcp/conn-2",
        "headers": {
          "Authorization": "Bearer secret"
        }
      }
    }
  }
}
`)
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	binding := config.Binding{
		ConnectionID:       "conn-2",
		PersonaID:          "persona-2",
		RuntimeKind:        runtime.AdapterKindOpenClaw,
		NativeMCPServer:    "personastack-conn-2",
		PersonaMCPURL:      "https://mcp.personastack.ai/mcp",
		PersonaMCPToken:    "token-2",
		HasPersonaMCPToken: true,
	}
	verified := VerifyBinding(homeDir, binding)
	if verified.State != runtime.AdapterStateMCPRestartRequired {
		t.Fatalf("verified.State = %s note=%s", verified.State, verified.Note)
	}
	if strings.Contains(verified.Note, "secret") {
		t.Fatalf("verification leaked header value: %q", verified.Note)
	}
	if !strings.Contains(verified.Note, "credential warning") {
		t.Fatalf("verification note missing credential warning: %q", verified.Note)
	}
}

func TestVerifyBindingWithLiveChecksLoopbackHTTPProxy(t *testing.T) {
	personaMCP := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer stable-token" {
			t.Fatalf("unexpected remote auth: %q", r.Header.Get("Authorization"))
		}
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
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-11-25","capabilities":{}}}`))
		case "notifications/initialized":
			w.WriteHeader(http.StatusAccepted)
		case "tools/list":
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":2,"result":{"tools":[{"name":"calendar"}]}}`))
		default:
			t.Fatalf("unexpected method: %s", message.Method)
		}
	}))
	defer personaMCP.Close()

	homeDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(homeDir, ".hermes"), 0o700); err != nil {
		t.Fatalf("mkdir hermes: %v", err)
	}
	if err := os.WriteFile(filepath.Join(homeDir, ".hermes", "config.yaml"), []byte("!!! invalid yaml\n"), 0o600); err != nil {
		t.Fatalf("write invalid hermes config: %v", err)
	}
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{{
		ConnectionID:    "conn-1",
		PersonaID:       "persona-1",
		RuntimeKind:     runtime.AdapterKindHermes,
		NativeMCPServer: "personastack-conn-1",
		PersonaMCPURL:   personaMCP.URL,
		PersonaMCPToken: "stable-token",
	}}})
	if _, err := (Installer{Store: &store, HomeDir: homeDir, ExecutablePath: "/usr/local/bin/personastack-connector", GOOS: "linux", Transport: MCPProxyTransportLoopbackHTTP}).InstallAll(); err != nil {
		t.Fatalf("InstallAll() error = %v", err)
	}
	binding, ok := store.Binding("conn-1")
	if !ok || binding.LocalMCPProxyURL == "" || binding.LocalMCPProxyToken == "" {
		t.Fatalf("loopback proxy state missing: %+v", binding)
	}
	withoutProxy := VerifyBindingWithLive(context.Background(), homeDir, binding, personaMCP.Client())
	if withoutProxy.State == runtime.AdapterStateMCPVerified {
		t.Fatalf("VerifyBindingWithLive() verified without local proxy: %+v", withoutProxy)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errs := make(chan error, 1)
	go func() {
		errs <- ServeLoopbackHTTPProxy(ctx, binding, personaMCP.Client())
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-errs:
		case <-time.After(2 * time.Second):
			t.Fatal("loopback proxy did not stop")
		}
	})
	requireLoopbackProxyListening(t, binding.LocalMCPProxyURL)

	verified := VerifyBindingWithLive(context.Background(), homeDir, binding, personaMCP.Client())
	if verified.State != runtime.AdapterStateMCPVerified {
		t.Fatalf("VerifyBindingWithLive() = %+v", verified)
	}
}

func TestLoopbackHTTPProxyUsesLatestBindingFromStore(t *testing.T) {
	requestTokens := make(chan string, 4)
	personaMCP := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestTokens <- r.Header.Get("Authorization")
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
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-11-25","capabilities":{}}}`))
		case "notifications/initialized":
			w.WriteHeader(http.StatusAccepted)
		case "tools/list":
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":2,"result":{"tools":[]}}`))
		default:
			t.Fatalf("unexpected method: %s", message.Method)
		}
	}))
	defer personaMCP.Close()

	initial := config.Binding{
		ConnectionID:          "conn-1",
		PersonaID:             "persona-1",
		RuntimeKind:           runtime.AdapterKindHermes,
		PersonaMCPURL:         personaMCP.URL,
		PersonaMCPToken:       "old-token",
		LocalMCPProxyURL:      "http://127.0.0.1:0/mcp/conn-1",
		LocalMCPProxyToken:    "local-token",
		HasLocalMCPProxyToken: true,
	}
	loopback, err := newLoopbackHTTPMCPServer(initial)
	if err != nil {
		t.Fatalf("new loopback server: %v", err)
	}
	initial.LocalMCPProxyURL = loopback.URL
	initial.LocalMCPProxyToken = loopback.Token
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{initial}})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errs, err := StartLoopbackHTTPProxyWithStore(ctx, &store, initial, personaMCP.Client())
	if err != nil {
		t.Fatalf("start loopback proxy: %v", err)
	}
	t.Cleanup(func() {
		cancel()
		<-errs
	})
	requireLoopbackProxyListening(t, initial.LocalMCPProxyURL)

	latest := initial
	latest.PersonaMCPToken = "new-token"
	if err := (&store).SaveBinding(latest); err != nil {
		t.Fatalf("save latest binding: %v", err)
	}
	localBinding := latest
	localBinding.PersonaMCPURL = latest.LocalMCPProxyURL
	localBinding.PersonaMCPToken = latest.LocalMCPProxyToken
	localBinding.ActiveRunMCPToken = ""
	if live := VerifyBindingLive(context.Background(), localBinding, personaMCP.Client()); !live.OK {
		t.Fatalf("VerifyBindingLive() = %+v", live)
	}
	if got := <-requestTokens; got != "Bearer new-token" {
		t.Fatalf("proxy used stale token: %q", got)
	}
}

func requireLoopbackProxyListening(t *testing.T, rawURL string) {
	t.Helper()
	parsed, err := url.Parse(rawURL)
	if err != nil {
		t.Fatalf("parse loopback url: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", parsed.Host, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("loopback proxy did not listen on %s", parsed.Host)
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
			if tt.runtimeKind == runtime.AdapterKindOpenClaw {
				gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
					conn, err := upgrader.Upgrade(w, r, nil)
					if err != nil {
						t.Fatalf("upgrade: %v", err)
					}
					defer conn.Close()
					_ = conn.WriteJSON(map[string]any{"type": "connect.challenge", "event": "connect.challenge"})
					var connect struct {
						Method string         `json:"method"`
						Params map[string]any `json:"params"`
					}
					if err := conn.ReadJSON(&connect); err != nil {
						t.Fatalf("read connect: %v", err)
					}
					_ = conn.WriteJSON(map[string]any{
						"type": "hello-ok",
						"result": json.RawMessage(`{
  "protocol":4,
  "role":"operator",
  "scopes":["operator.read","operator.write"],
  "features":{"methods":["health","status","agents.list","agent","agent.wait","sessions.abort"]}
}`),
					})
					var catalog struct {
						ID     string         `json:"id"`
						Method string         `json:"method"`
						Params map[string]any `json:"params"`
					}
					if err := conn.ReadJSON(&catalog); err != nil {
						t.Fatalf("read tools.catalog: %v", err)
					}
					_ = conn.WriteJSON(map[string]any{
						"id": catalog.ID,
						"result": json.RawMessage(`{
  "agentId":"agent-1",
  "groups":[
    {
      "id":"plugin:personastack-conn-1",
      "label":"personastack-conn-1",
      "source":"plugin",
      "pluginId":"personastack-conn-1",
      "tools":[
        {"id":"calendar","label":"Calendar","source":"plugin","pluginId":"personastack-conn-1"}
      ]
    }
  ]
}`),
					})
				}))
				defer gateway.Close()
				t.Setenv("PERSONASTACK_CONNECTOR_OPENCLAW_GATEWAY_URL", "ws"+gateway.URL[len("http"):])
			}
			store := config.NewMemoryStore(config.State{Bindings: []config.Binding{{
				ConnectionID:         "conn-1",
				PersonaID:            "persona-1",
				RuntimeKind:          tt.runtimeKind,
				NativeMCPServer:      "personastack-conn-1",
				PersonaMCPURL:        live.URL,
				PersonaMCPToken:      "token-1",
				OpenClawAgentID:      "agent-1",
				OpenClawGatewayToken: "token-1",
			}}})
			if _, err := (Installer{Store: store, HomeDir: homeDir, ExecutablePath: "/usr/local/bin/personastack-connector", GOOS: "linux"}).InstallAll(); err != nil {
				t.Fatalf("InstallAll() error = %v", err)
			}

			binding := config.Binding{
				ConnectionID:         "conn-1",
				PersonaID:            "persona-1",
				RuntimeKind:          tt.runtimeKind,
				NativeMCPServer:      "personastack-conn-1",
				PersonaMCPURL:        live.URL,
				PersonaMCPToken:      "token-1",
				HasPersonaMCPToken:   true,
				OpenClawAgentID:      "agent-1",
				OpenClawGatewayToken: "token-1",
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

func TestVerifyBindingWithLiveMarksOpenClawVerifiedWhenCatalogAndMCPAreReachable(t *testing.T) {
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()
		_ = conn.WriteJSON(map[string]any{"type": "connect.challenge", "event": "connect.challenge"})
		var connect struct {
			Method string         `json:"method"`
			Params map[string]any `json:"params"`
		}
		if err := conn.ReadJSON(&connect); err != nil {
			t.Fatalf("read connect: %v", err)
		}
		if connect.Method != "connect" {
			t.Fatalf("expected connect, got %+v", connect)
		}
		_ = conn.WriteJSON(map[string]any{
			"type": "hello-ok",
			"result": json.RawMessage(`{
  "protocol":4,
  "role":"operator",
  "scopes":["operator.read","operator.write"],
  "features":{"methods":["health","status","agents.list","agent","agent.wait","sessions.abort"]}
}`),
		})
		var catalog struct {
			ID     string         `json:"id"`
			Method string         `json:"method"`
			Params map[string]any `json:"params"`
		}
		if err := conn.ReadJSON(&catalog); err != nil {
			t.Fatalf("read tools.catalog: %v", err)
		}
		if catalog.Method != "tools.catalog" {
			t.Fatalf("expected tools.catalog, got %+v", catalog)
		}
		_ = conn.WriteJSON(map[string]any{
			"id": catalog.ID,
			"result": json.RawMessage(`{
  "agentId":"agent-1",
  "groups":[
    {
      "id":"plugin:personastack-conn-1",
      "label":"personastack-conn-1",
      "source":"plugin",
      "pluginId":"personastack-conn-1",
      "tools":[
        {"id":"calendar","label":"Calendar","source":"plugin","pluginId":"personastack-conn-1"}
      ]
    }
  ]
}`),
		})
	}))
	defer gateway.Close()

	personaMCP := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":2,"result":{"tools":[{"name":"calendar"}]}}`))
		default:
			t.Fatalf("unexpected method: %s", message.Method)
		}
	}))
	defer personaMCP.Close()

	homeDir := t.TempDir()
	rawConfig := []byte(`{
  "mcp": {
    "servers": {
      "personastack-conn-1": {
        "command": "/usr/local/bin/personastack-connector",
        "args": ["mcp", "stdio", "--binding", "conn-1"]
      }
    }
  }
}
`)
	if err := os.MkdirAll(filepath.Join(homeDir, ".openclaw"), 0o700); err != nil {
		t.Fatalf("mkdir openclaw config dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(homeDir, ".openclaw", "config.json"), rawConfig, 0o600); err != nil {
		t.Fatalf("write openclaw config: %v", err)
	}
	t.Setenv("PERSONASTACK_CONNECTOR_OPENCLAW_GATEWAY_URL", "ws"+gateway.URL[len("http"):])

	binding := config.Binding{
		ConnectionID:         "conn-1",
		PersonaID:            "persona-1",
		RuntimeKind:          runtime.AdapterKindOpenClaw,
		NativeMCPServer:      "personastack-conn-1",
		OpenClawAgentID:      "agent-1",
		OpenClawGatewayToken: "token-1",
		PersonaMCPURL:        personaMCP.URL,
		PersonaMCPToken:      "mcp-token-1",
		HasPersonaMCPToken:   true,
	}

	verified := VerifyBindingWithLive(context.Background(), homeDir, binding, personaMCP.Client())
	if verified.State != runtime.AdapterStateMCPVerified {
		t.Fatalf("VerifyBindingWithLive() = %+v", verified)
	}
	if !strings.Contains(verified.Note, "effective tool catalog visible") {
		t.Fatalf("unexpected note: %q", verified.Note)
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
