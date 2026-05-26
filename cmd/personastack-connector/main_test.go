package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/personastack/personastack-connector/internal/config"
	"github.com/personastack/personastack-connector/internal/mcp"
	"github.com/personastack/personastack-connector/internal/openclawauth"
	"github.com/personastack/personastack-connector/internal/runtime"
	"github.com/personastack/personastack-connector/internal/service"
	"github.com/zalando/go-keyring"
)

func TestRunHelp(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	err := Run(context.Background(), []string{"--help"}, strings.NewReader(""), &stdout, &stderr)
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	output := stdout.String()
	for _, want := range []string{"pair <code>", "status [--repair]", "diagnostics", "runtime detect", "mcp stdio --binding", "service plan", "service uninstall", "run --foreground", "version"} {
		if !strings.Contains(output, want) {
			t.Fatalf("help output missing %q: %s", want, output)
		}
	}
	if strings.Contains(output, "pair <code> [--runtime auto|hermes|openclaw] [--configure-mcp]") {
		t.Fatalf("help output should not require --configure-mcp in primary pair usage: %s", output)
	}
	for _, want := range []string{"runtime hermes configure", "runtime openclaw configure"} {
		if !strings.Contains(output, want) {
			t.Fatalf("help output missing %q: %s", want, output)
		}
	}
}

func TestRunVersion(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := Run(context.Background(), []string{"version"}, strings.NewReader(""), &stdout, &stderr)
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if !strings.Contains(stdout.String(), "personastack-connector version=") {
		t.Fatalf("unexpected version output: %s", stdout.String())
	}
}

func TestMCPStdioMissingBinding(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	err := Run(context.Background(), []string{"mcp", "stdio", "--binding", "fake"}, strings.NewReader(""), &stdout, &stderr)
	if !errors.Is(err, mcp.ErrMissingBinding) {
		t.Fatalf("Run error = %v, want ErrMissingBinding", err)
	}
}

func TestRunServicePlan(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	err := Run(context.Background(), []string{"service", "plan"}, strings.NewReader(""), &stdout, &stderr)
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if !strings.Contains(stdout.String(), "service plan kind=") {
		t.Fatalf("unexpected service plan output: %s", stdout.String())
	}
}

func TestRunServiceUninstall(t *testing.T) {
	homeDir := t.TempDir()
	path := filepath.Join(homeDir, ".config", "systemd", "user", "personastack-connector.service")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("mkdir service dir: %v", err)
	}
	if err := os.WriteFile(path, []byte("old"), 0o600); err != nil {
		t.Fatalf("write service file: %v", err)
	}
	t.Setenv("HOME", homeDir)
	oldGOOS := currentGOOS
	currentGOOS = "linux"
	t.Cleanup(func() { currentGOOS = oldGOOS })
	oldServiceInstaller := newServiceInstaller
	newServiceInstaller = func(scope service.ServiceScope) service.Installer {
		return service.Installer{
			HomeDir:      homeDir,
			GOOS:         "linux",
			Runner:       &serviceRecordingRunner{},
			ServiceScope: scope,
		}
	}
	t.Cleanup(func() { newServiceInstaller = oldServiceInstaller })
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	err := Run(context.Background(), []string{"service", "uninstall"}, strings.NewReader(""), &stdout, &stderr)
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	output := stdout.String()
	if !strings.Contains(output, "service uninstalled kind=") || !strings.Contains(output, "removed=true") {
		t.Fatalf("unexpected uninstall output: %s", output)
	}
}

type serviceRecordingRunner struct{}

func (serviceRecordingRunner) Run(string, ...string) error {
	return nil
}

func TestRunServiceUninstallSystemScopeRequiresAccess(t *testing.T) {
	oldGOOS := currentGOOS
	currentGOOS = "darwin"
	t.Cleanup(func() { currentGOOS = oldGOOS })
	if os.Geteuid() == 0 {
		t.Setenv("PERSONASTACK_CONNECTOR_SYSTEM_ROOT", t.TempDir())
	}
	oldServiceInstaller := newServiceInstaller
	newServiceInstaller = func(scope service.ServiceScope) service.Installer {
		return service.Installer{
			HomeDir:      t.TempDir(),
			GOOS:         "darwin",
			Runner:       &serviceRecordingRunner{},
			ServiceScope: scope,
			SystemRoot:   os.Getenv("PERSONASTACK_CONNECTOR_SYSTEM_ROOT"),
		}
	}
	t.Cleanup(func() { newServiceInstaller = oldServiceInstaller })
	cmd := command{stdout: io.Discard, stderr: io.Discard, store: config.EmptyStore()}

	err := cmd.runService([]string{"uninstall", "--service-scope", "system"})
	if os.Geteuid() == 0 {
		if err != nil {
			t.Fatalf("runService() error = %v, want root access allowed", err)
		}
		return
	}
	if err == nil || !strings.Contains(err.Error(), "requires sudo before uninstall") {
		t.Fatalf("runService() error = %v, want sudo uninstall rejection", err)
	}
}

func TestRunUnpairDeletesBindings(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	store := config.NewMemoryStore(config.State{
		Bindings: []config.Binding{
			{ConnectionID: "conn-1", PersonaID: "persona-1"},
			{ConnectionID: "conn-2", PersonaID: "persona-2"},
		},
	})
	cmd := command{
		stdout: &stdout,
		stderr: &stderr,
		store:  &store,
	}

	if err := cmd.runUnpair(nil); err != nil {
		t.Fatalf("runUnpair() error = %v", err)
	}
	if len(store.ListBindings()) != 0 {
		t.Fatalf("bindings were not deleted: %+v", store.ListBindings())
	}
	for _, want := range []string{"unpaired connection=conn-1 persona=persona-1", "unpaired connection=conn-2 persona=persona-2"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("unpair output missing %q: %s", want, stdout.String())
		}
	}
}

func TestRunStatusReadsSystemScopeStore(t *testing.T) {
	root := t.TempDir()
	t.Setenv("PERSONASTACK_CONNECTOR_SYSTEM_ROOT", root)
	store := config.SystemFileStore(root)
	err := store.SaveBinding(config.Binding{ConnectionID: "conn-system", PersonaID: "persona-system", RuntimeKind: runtime.AdapterKindOpenClaw})
	if err != nil {
		t.Fatalf("save system binding: %v", err)
	}
	var stdout bytes.Buffer
	cmd := command{stdout: &stdout, stderr: io.Discard, store: config.EmptyStore()}

	if err := cmd.runStatus(context.Background(), []string{"--service-scope", "system"}); err != nil {
		t.Fatalf("runStatus() error = %v", err)
	}
	if !strings.Contains(stdout.String(), "conn-system") {
		t.Fatalf("status did not read system store: %s", stdout.String())
	}
}

func TestRunUnpairDeletesSystemScopeStore(t *testing.T) {
	root := t.TempDir()
	t.Setenv("PERSONASTACK_CONNECTOR_SYSTEM_ROOT", root)
	store := config.SystemFileStore(root)
	err := store.SaveBinding(config.Binding{ConnectionID: "conn-system", PersonaID: "persona-system", RuntimeKind: runtime.AdapterKindOpenClaw})
	if err != nil {
		t.Fatalf("save system binding: %v", err)
	}
	var stdout bytes.Buffer
	cmd := command{stdout: &stdout, stderr: io.Discard, store: config.EmptyStore()}

	if err := cmd.runUnpair([]string{"--service-scope", "system"}); err != nil {
		t.Fatalf("runUnpair() error = %v", err)
	}
	if bindings := store.ListBindings(); len(bindings) != 0 {
		t.Fatalf("system bindings were not deleted: %+v", bindings)
	}
}

func TestRunPairSystemScopeAllowsHermesAfterAccessPreflight(t *testing.T) {
	oldGOOS := currentGOOS
	currentGOOS = "darwin"
	t.Cleanup(func() { currentGOOS = oldGOOS })
	keyring.MockInit()
	store := config.NewMemoryStore(config.State{})
	cmd := command{
		stdout: io.Discard,
		stderr: io.Discard,
		store:  &store,
	}

	err := cmd.runPair([]string{"PAIR-1234", "--runtime", "hermes", "--service-scope", "system"})
	if os.Geteuid() == 0 {
		if err == nil || !strings.Contains(err.Error(), "Post") {
			t.Fatalf("runPair() error = %v, want exchange only after root access", err)
		}
		return
	}
	if err == nil || !strings.Contains(err.Error(), "requires sudo before pairing") {
		t.Fatalf("runPair() error = %v, want sudo preflight rejection", err)
	}
}

func TestRunPairSystemScopeRequiresAccessBeforeExchange(t *testing.T) {
	oldGOOS := currentGOOS
	currentGOOS = "darwin"
	t.Cleanup(func() { currentGOOS = oldGOOS })
	keyring.MockInit()
	store := config.NewMemoryStore(config.State{})
	cmd := command{
		stdout: io.Discard,
		stderr: io.Discard,
		store:  &store,
	}

	err := cmd.runPair([]string{"PAIR-1234", "--runtime", "openclaw", "--service-scope", "system"})
	if os.Geteuid() == 0 {
		if err == nil || !strings.Contains(err.Error(), "Post") {
			t.Fatalf("runPair() error = %v, want exchange only after root access", err)
		}
		return
	}
	if err == nil || !strings.Contains(err.Error(), "requires sudo before pairing") {
		t.Fatalf("runPair() error = %v, want sudo preflight rejection", err)
	}
}

func TestRunDaemonSystemScopeRejectsNonDarwin(t *testing.T) {
	oldGOOS := currentGOOS
	currentGOOS = "linux"
	t.Cleanup(func() { currentGOOS = oldGOOS })
	cmd := command{stdout: io.Discard, stderr: io.Discard, store: config.EmptyStore()}

	err := cmd.runDaemon(context.Background(), []string{"--foreground", "--service-scope", "system_launch_daemon"})
	if err == nil || !strings.Contains(err.Error(), "requires macOS") {
		t.Fatalf("runDaemon() error = %v, want macOS rejection", err)
	}
}

func TestRuntimeRepairSystemScopeAllowsHermesBinding(t *testing.T) {
	oldGOOS := currentGOOS
	currentGOOS = "darwin"
	t.Cleanup(func() { currentGOOS = oldGOOS })
	root := t.TempDir()
	t.Setenv("PERSONASTACK_CONNECTOR_SYSTEM_ROOT", root)
	store := config.SystemFileStore(root)
	err := store.SaveBinding(config.Binding{ConnectionID: "conn-hermes", PersonaID: "persona-1", RuntimeKind: runtime.AdapterKindHermes})
	if err != nil {
		t.Fatalf("save system binding: %v", err)
	}
	cmd := command{stdout: io.Discard, stderr: io.Discard, store: config.EmptyStore()}

	err = cmd.runRuntime([]string{"repair", "--service-scope", "system"})
	if err != nil && strings.Contains(err.Error(), "requires OpenClaw") {
		t.Fatalf("runRuntime() rejected Hermes system scope: %v", err)
	}
}

func TestRunStatusIncludesActiveAssignmentState(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd := command{
		stdout: &stdout,
		stderr: &stderr,
		store: config.NewMemoryStore(config.State{
			Bindings: []config.Binding{
				{
					ConnectionID:       "connection-1",
					PersonaID:          "persona-1",
					RuntimeKind:        runtime.AdapterKindAuto,
					ActiveRunID:        "run-1",
					ActiveAssignmentID: "assignment-1",
					ActiveNativeRunID:  "native-run-1",
				},
			},
		}),
	}

	if err := cmd.runStatus(context.Background(), nil); err != nil {
		t.Fatalf("runStatus() error = %v", err)
	}
	output := stdout.String()
	for _, want := range []string{"active_run=run-1", "active_assignment=assignment-1", "active_native_run=native-run-1"} {
		if !strings.Contains(output, want) {
			t.Fatalf("status output missing %q: %s", want, output)
		}
	}
}

func TestRunStatusIncludesWebsocketAndWakeProbeState(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd := command{
		stdout: &stdout,
		stderr: &stderr,
		store: config.NewMemoryStore(config.State{
			Bindings: []config.Binding{
				{
					ConnectionID:            "connection-1",
					PersonaID:               "persona-1",
					ConnectionGeneration:    7,
					RuntimeKind:             runtime.AdapterKindHermes,
					LastHeartbeatAt:         time.Now().UTC(),
					LastWakeProbeAt:         time.Now().UTC().Add(-time.Minute),
					LastWakeProbeGeneration: 7,
					HasBridgeSecret:         true,
					PersonaMCPURL:           "http://127.0.0.1:8642",
					HasPersonaMCPToken:      true,
				},
			},
		}),
	}

	if err := cmd.runStatus(context.Background(), nil); err != nil {
		t.Fatalf("runStatus() error = %v", err)
	}
	output := stdout.String()
	for _, want := range []string{"summary=", "websocket=connected", "last_wake_probe="} {
		if !strings.Contains(output, want) {
			t.Fatalf("status output missing %q: %s", want, output)
		}
	}
}

func TestConnectorStatusSummaryRequiresCurrentGenerationWakeProbe(t *testing.T) {
	binding := config.Binding{
		ConnectionGeneration:    2,
		LastWakeProbeAt:         time.Now().UTC(),
		LastWakeProbeGeneration: 1,
		HasBridgeSecret:         true,
	}

	summary := strings.Join(connectorStatusSummaryTokens(binding, runtime.AdapterStateReady, runtime.AdapterStateMCPVerified, "connected"), " ")
	if strings.Contains(summary, " wakeable") {
		t.Fatalf("summary should not be wakeable with stale wake probe generation: %s", summary)
	}

	binding.ConnectionGeneration = 0
	binding.LastWakeProbeGeneration = 0
	summary = strings.Join(connectorStatusSummaryTokens(binding, runtime.AdapterStateReady, runtime.AdapterStateMCPVerified, "connected"), " ")
	if strings.Contains(summary, " wakeable") {
		t.Fatalf("summary should not be wakeable with zero wake probe generation: %s", summary)
	}

	binding.ConnectionGeneration = 2
	binding.LastWakeProbeGeneration = 2
	summary = strings.Join(connectorStatusSummaryTokens(binding, runtime.AdapterStateReady, runtime.AdapterStateMCPVerified, "connected"), " ")
	if !strings.Contains(summary, " wakeable") {
		t.Fatalf("summary should be wakeable with current wake probe generation: %s", summary)
	}
}

func TestRunPairReportsSuccessState(t *testing.T) {
	keyring.MockInit()
	t.Setenv("HOME", t.TempDir())
	t.Setenv("PERSONASTACK_CONNECTOR_DISABLE_HERMES_GATEWAY_START", "1")
	oldInstallService := installService
	installService = func(scope service.ServiceScope) (service.InstallResult, error) {
		return service.InstallResult{Kind: "launchagent", Scope: scope, Path: "/tmp/personastack-connector.plist"}, nil
	}
	t.Cleanup(func() {
		installService = oldInstallService
	})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"persona_id":                "persona-1",
			"connection_id":             "connection-1",
			"credential_id":             "cred-1",
			"runtime_kind":              "hermes",
			"connection_generation":     1,
			"gateway_websocket_url":     "ws://example/v1/external-agent/ws",
			"native_mcp_server_name":    "personastack-connection-1",
			"native_mcp_tool_namespace": "personastack",
			"persona_mcp_url":           "https://mcp.example/mcp",
			"persona_mcp_token":         "mcp-token-1",
		})
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	store := config.NewMemoryStore(config.State{})
	cmd := command{
		stdout: &stdout,
		stderr: &stderr,
		store:  &store,
	}
	if err := cmd.runPair([]string{"PAIR-1234", "--gateway", server.URL, "--runtime", "hermes"}); err != nil {
		t.Fatalf("runPair() error = %v", err)
	}
	output := stdout.String()
	for _, want := range []string{
		"Connector paired successfully.",
		"Persona: persona-1",
		"Connection: connection-1",
		"Runtime: hermes",
		"Local link: active",
		"MCP: configured",
		"Status: waiting for bridge wake probe",
		"Details:",
		"installed mcp binding=connection-1 runtime=hermes",
		"service installed kind=launchagent scope=user_launch_agent path=/tmp/personastack-connector.plist",
		"paired persona=persona-1 connection=connection-1 runtime=hermes configure_mcp=true service_scope=user_launch_agent setup_state=pending_bridge_wake_probe",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("pair output missing %q: %s", want, output)
		}
	}
}

func TestRunPairReportsDegradedState(t *testing.T) {
	keyring.MockInit()
	t.Setenv("HOME", t.TempDir())
	t.Setenv("PERSONASTACK_CONNECTOR_DISABLE_HERMES_GATEWAY_START", "1")
	oldInstallService := installService
	installService = func(scope service.ServiceScope) (service.InstallResult, error) {
		return service.InstallResult{Kind: "no_user_service_manager", Scope: scope, Path: "/tmp/personastack-connector.desktop"}, nil
	}
	t.Cleanup(func() {
		installService = oldInstallService
	})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"persona_id":                "persona-2",
			"connection_id":             "connection-2",
			"credential_id":             "cred-2",
			"runtime_kind":              "hermes",
			"connection_generation":     1,
			"gateway_websocket_url":     "ws://example/v1/external-agent/ws",
			"native_mcp_server_name":    "personastack-connection-2",
			"native_mcp_tool_namespace": "personastack",
			"persona_mcp_url":           "https://mcp.example/mcp",
			"persona_mcp_token":         "mcp-token-2",
		})
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	store := config.NewMemoryStore(config.State{})
	cmd := command{
		stdout: &stdout,
		stderr: &stderr,
		store:  &store,
	}
	if err := cmd.runPair([]string{"PAIR-2345", "--gateway", server.URL, "--runtime", "hermes"}); err != nil {
		t.Fatalf("runPair() error = %v", err)
	}
	output := stdout.String()
	for _, want := range []string{
		"Connector paired successfully.",
		"Local link: active",
		"Status: waiting for bridge wake probe",
		"Details:",
		"service installed kind=no_user_service_manager scope=user_launch_agent path=/tmp/personastack-connector.desktop",
		"paired persona=persona-2 connection=connection-2 runtime=hermes configure_mcp=true service_scope=user_launch_agent setup_state=pending_bridge_wake_probe",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("pair output missing %q: %s", want, output)
		}
	}
}

func TestRunPairReplacesPreviousLocalBinding(t *testing.T) {
	keyring.MockInit()
	t.Setenv("HOME", t.TempDir())
	t.Setenv("PERSONASTACK_CONNECTOR_DISABLE_HERMES_GATEWAY_START", "1")
	oldInstallService := installService
	installService = func(scope service.ServiceScope) (service.InstallResult, error) {
		return service.InstallResult{Kind: "launchagent", Scope: scope, Path: "/tmp/personastack-connector.plist"}, nil
	}
	t.Cleanup(func() {
		installService = oldInstallService
	})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"persona_id":                "persona-2",
			"connection_id":             "connection-2",
			"credential_id":             "cred-2",
			"runtime_kind":              "hermes",
			"connection_generation":     1,
			"gateway_websocket_url":     "ws://example/v1/external-agent/ws",
			"native_mcp_server_name":    "personastack-connection-2",
			"native_mcp_tool_namespace": "personastack",
			"persona_mcp_url":           "https://mcp.example/mcp",
			"persona_mcp_token":         "mcp-token-2",
		})
	}))
	defer server.Close()

	var stdout bytes.Buffer
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{{
		ConnectionID: "connection-1",
		PersonaID:    "persona-1",
		RuntimeKind:  runtime.AdapterKindHermes,
	}}})
	cmd := command{
		stdout: &stdout,
		stderr: io.Discard,
		store:  &store,
	}
	if err := cmd.runPair([]string{"PAIR-2345", "--gateway", server.URL, "--runtime", "hermes"}); err != nil {
		t.Fatalf("runPair() error = %v", err)
	}
	bindings := store.ListBindings()
	if len(bindings) != 1 || bindings[0].ConnectionID != "connection-2" {
		t.Fatalf("expected new binding only, got %+v", bindings)
	}
	if !strings.Contains(stdout.String(), "Local link: replaced 1 previous binding") {
		t.Fatalf("expected replacement output, got %s", stdout.String())
	}
}

func TestRunPairOpenClawRequiresCredentialBeforePairingExchange(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("OPENCLAW_CONFIG_PATH", "")
	t.Setenv("OPENCLAW_HOME", "")
	t.Setenv("OPENCLAW_STATE_DIR", "")
	t.Setenv("OPENCLAW_GATEWAY_TOKEN", "")
	t.Setenv("OPENCLAW_GATEWAY_PASSWORD", "")
	t.Setenv("OPENCLAW_GATEWAY_DEVICE_TOKEN", "")
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		http.Error(w, "pairing code consumed", http.StatusInternalServerError)
	}))
	defer server.Close()

	store := config.NewMemoryStore(config.State{})
	cmd := command{
		stdout: io.Discard,
		stderr: io.Discard,
		store:  &store,
	}
	err := cmd.runPair([]string{"PAIR-OPENCLAW", "--gateway", server.URL, "--runtime", "openclaw"})
	if err == nil || !strings.Contains(err.Error(), "OpenClaw operator credential required") {
		t.Fatalf("runPair() error = %v", err)
	}
	if calls != 0 {
		t.Fatalf("pairing exchange should not be called before OpenClaw credential validation; calls=%d", calls)
	}
}

func TestRunPairLinuxSystemScopeRequiresAccessBeforeExchange(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("non-root access preflight test")
	}
	oldGOOS := currentGOOS
	currentGOOS = "linux"
	t.Cleanup(func() { currentGOOS = oldGOOS })
	keyring.MockInit()
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		http.Error(w, "pairing code consumed", http.StatusInternalServerError)
	}))
	defer server.Close()

	store := config.NewMemoryStore(config.State{})
	cmd := command{
		stdout: io.Discard,
		stderr: io.Discard,
		store:  &store,
	}
	err := cmd.runPair([]string{"PAIR-LINUX", "--gateway", server.URL, "--runtime", "hermes", "--service-scope", "linux-system"})
	if err == nil || !strings.Contains(err.Error(), "linux system service scope requires sudo before pairing") {
		t.Fatalf("runPair() error = %v", err)
	}
	if calls != 0 {
		t.Fatalf("pairing exchange should not be called before Linux system access validation; calls=%d", calls)
	}
}

func TestRunPairLinuxSystemScopeRejectsNonLinuxBeforeExchange(t *testing.T) {
	oldGOOS := currentGOOS
	currentGOOS = "darwin"
	t.Cleanup(func() { currentGOOS = oldGOOS })
	keyring.MockInit()
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		http.Error(w, "pairing code consumed", http.StatusInternalServerError)
	}))
	defer server.Close()

	store := config.NewMemoryStore(config.State{})
	cmd := command{
		stdout: io.Discard,
		stderr: io.Discard,
		store:  &store,
	}
	err := cmd.runPair([]string{"PAIR-LINUX", "--gateway", server.URL, "--runtime", "hermes", "--service-scope", "linux-system"})
	if err == nil || !strings.Contains(err.Error(), "requires Linux") {
		t.Fatalf("runPair() error = %v", err)
	}
	if calls != 0 {
		t.Fatalf("pairing exchange should not be called before Linux platform validation; calls=%d", calls)
	}
}

func TestRunPairOpenClawPromptsForTokenBeforePairingExchange(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("OPENCLAW_CONFIG_PATH", "")
	t.Setenv("OPENCLAW_HOME", "")
	t.Setenv("OPENCLAW_STATE_DIR", "")
	t.Setenv("OPENCLAW_GATEWAY_TOKEN", "")
	t.Setenv("OPENCLAW_GATEWAY_PASSWORD", "")
	t.Setenv("OPENCLAW_GATEWAY_DEVICE_TOKEN", "")
	keyring.MockInit()
	t.Setenv("PERSONASTACK_CONNECTOR_DISABLE_OPENCLAW_GATEWAY_START", "1")
	oldInstallService := installService
	installService = func(scope service.ServiceScope) (service.InstallResult, error) {
		return service.InstallResult{Kind: "no_user_service_manager", Scope: scope, Path: "/tmp/personastack-connector.desktop"}, nil
	}
	t.Cleanup(func() {
		installService = oldInstallService
	})

	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		_ = json.NewEncoder(w).Encode(map[string]any{
			"persona_id":                "persona-openclaw",
			"connection_id":             "connection-openclaw",
			"credential_id":             "cred-openclaw",
			"runtime_kind":              "openclaw",
			"connection_generation":     1,
			"gateway_websocket_url":     "ws://example/v1/external-agent/ws",
			"native_mcp_server_name":    "personastack-connection-openclaw",
			"native_mcp_tool_namespace": "personastack",
			"persona_mcp_url":           "https://mcp.example/mcp",
			"persona_mcp_token":         "mcp-token-openclaw",
		})
	}))
	defer server.Close()

	var stderr bytes.Buffer
	store := config.NewMemoryStore(config.State{})
	cmd := command{
		stdin:  strings.NewReader("token-from-prompt\n"),
		stdout: io.Discard,
		stderr: &stderr,
		store:  &store,
	}
	err := cmd.runPair([]string{"PAIR-OPENCLAW", "--gateway", server.URL, "--runtime", "openclaw"})
	if err != nil {
		t.Fatalf("runPair() error = %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected one pairing exchange after OpenClaw token prompt; calls=%d", calls)
	}
	bindings := store.ListBindings()
	if len(bindings) != 1 || bindings[0].OpenClawGatewayToken != "token-from-prompt" {
		t.Fatalf("expected prompted token to be stored as token: %+v", bindings)
	}
	if !strings.Contains(stderr.String(), "OpenClaw operator token:") {
		t.Fatalf("expected token prompt, got %q", stderr.String())
	}
	if !strings.Contains(stderr.String(), "openclaw config get gateway.auth.token") {
		t.Fatalf("expected token source guidance, got %q", stderr.String())
	}
}

func TestRunPairOpenClawDetectsTokenBeforePrompt(t *testing.T) {
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)
	t.Setenv("OPENCLAW_CONFIG_PATH", "")
	t.Setenv("OPENCLAW_HOME", "")
	t.Setenv("OPENCLAW_STATE_DIR", "")
	t.Setenv("OPENCLAW_GATEWAY_TOKEN", "")
	t.Setenv("OPENCLAW_GATEWAY_PASSWORD", "")
	t.Setenv("OPENCLAW_GATEWAY_DEVICE_TOKEN", "")
	t.Setenv("PERSONASTACK_CONNECTOR_OPENCLAW_GATEWAY_URL", "ws://127.0.0.1:1")
	t.Setenv("PERSONASTACK_CONNECTOR_DISABLE_OPENCLAW_GATEWAY_START", "1")
	if err := os.MkdirAll(filepath.Join(homeDir, ".openclaw"), 0o700); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(homeDir, ".openclaw", ".env"), []byte("OPENCLAW_GATEWAY_TOKEN=detected-token\n"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	oldInstallService := installService
	installService = func(scope service.ServiceScope) (service.InstallResult, error) {
		return service.InstallResult{Kind: "no_user_service_manager", Scope: scope, Path: "/tmp/personastack-connector.desktop"}, nil
	}
	t.Cleanup(func() {
		installService = oldInstallService
	})

	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		_ = json.NewEncoder(w).Encode(map[string]any{
			"persona_id":                "persona-openclaw",
			"connection_id":             "connection-openclaw",
			"credential_id":             "cred-openclaw",
			"runtime_kind":              "openclaw",
			"connection_generation":     1,
			"gateway_websocket_url":     "ws://example/v1/external-agent/ws",
			"native_mcp_server_name":    "personastack-connection-openclaw",
			"native_mcp_tool_namespace": "personastack",
			"persona_mcp_url":           "https://mcp.example/mcp",
			"persona_mcp_token":         "mcp-token-openclaw",
		})
	}))
	defer server.Close()

	var stderr bytes.Buffer
	store := config.NewMemoryStore(config.State{})
	cmd := command{
		stdin:  strings.NewReader("should-not-be-read\n"),
		stdout: io.Discard,
		stderr: &stderr,
		store:  &store,
	}
	err := cmd.runPair([]string{"PAIR-OPENCLAW", "--gateway", server.URL, "--runtime", "openclaw"})
	if err != nil {
		t.Fatalf("runPair() error = %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected one pairing exchange after OpenClaw token detection; calls=%d", calls)
	}
	bindings := store.ListBindings()
	if len(bindings) != 1 || bindings[0].OpenClawGatewayToken != "detected-token" {
		t.Fatalf("expected detected token to be stored: %+v", bindings)
	}
	if stderr.String() != "" {
		t.Fatalf("expected no prompt, got %q", stderr.String())
	}
}

func TestRunPairOpenClawRejectsDetectedInvalidTokenWhenGatewayReachable(t *testing.T) {
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)
	t.Setenv("OPENCLAW_CONFIG_PATH", "")
	t.Setenv("OPENCLAW_HOME", "")
	t.Setenv("OPENCLAW_STATE_DIR", "")
	t.Setenv("OPENCLAW_GATEWAY_TOKEN", "")
	t.Setenv("OPENCLAW_GATEWAY_PASSWORD", "")
	t.Setenv("OPENCLAW_GATEWAY_DEVICE_TOKEN", "")
	if err := os.MkdirAll(filepath.Join(homeDir, ".openclaw"), 0o700); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(homeDir, ".openclaw", ".env"), []byte("OPENCLAW_GATEWAY_TOKEN=stale-token\n"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()
		_ = conn.WriteJSON(map[string]any{"type": "connect.challenge"})
		var connect map[string]any
		if err := conn.ReadJSON(&connect); err != nil {
			t.Fatalf("read connect: %v", err)
		}
		_ = conn.WriteJSON(map[string]any{"id": connect["id"], "type": "res", "ok": false, "error": "invalid token"})
	}))
	defer gateway.Close()
	t.Setenv("PERSONASTACK_CONNECTOR_OPENCLAW_GATEWAY_URL", "ws"+gateway.URL[len("http"):])

	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		http.Error(w, "pairing code consumed", http.StatusInternalServerError)
	}))
	defer server.Close()

	store := config.NewMemoryStore(config.State{})
	cmd := command{
		stdin:  strings.NewReader("should-not-be-read\n"),
		stdout: io.Discard,
		stderr: io.Discard,
		store:  &store,
	}
	err := cmd.runPair([]string{"PAIR-OPENCLAW", "--gateway", server.URL, "--runtime", "openclaw"})
	if err == nil || !strings.Contains(err.Error(), "credential rejected") {
		t.Fatalf("runPair() error = %v", err)
	}
	if calls != 0 {
		t.Fatalf("pairing exchange should not be called with rejected auth; calls=%d", calls)
	}
}

func TestRunDiagnosticsRedactsPathsAndListsRepairActions(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	t.Setenv("HOME", t.TempDir())
	cmd := command{
		stdout: &stdout,
		stderr: &stderr,
		store: config.NewMemoryStore(config.State{
			Bindings: []config.Binding{
				{
					ConnectionID:       "connection-1",
					PersonaID:          "persona-1",
					RuntimeKind:        runtime.AdapterKindAuto,
					BridgeCredentialID: "cred-1",
					HasBridgeSecret:    true,
				},
			},
		}),
	}

	if err := cmd.runDiagnostics(nil); err != nil {
		t.Fatalf("runDiagnostics() error = %v", err)
	}
	output := stdout.String()
	for _, want := range []string{"[LOCAL_PATH]", "repair_actions=runtime_detect,mcp_install,reconnect,rotate_local_token,export_diagnostics"} {
		if !strings.Contains(output, want) {
			t.Fatalf("diagnostics output missing %q: %s", want, output)
		}
	}
}

func TestRuntimeRepairActionMapsToUpdateRuntime(t *testing.T) {
	if got := runtimeRepairAction(runtime.AdapterStateRuntimeMissing); got != "update_runtime" {
		t.Fatalf("runtimeRepairAction(runtime_missing) = %q, want update_runtime", got)
	}
	if got := runtimeRepairAction(runtime.AdapterStateMCPRestartRequired); got != "restart_runtime" {
		t.Fatalf("runtimeRepairAction(mcp_restart_required) = %q, want restart_runtime", got)
	}
}

func TestRuntimeDetectionReportSummariesChoices(t *testing.T) {
	cases := []struct {
		name      string
		report    runtimeDetectionReport
		wantLine  string
		wantError string
	}{
		{
			name: "none",
			report: runtimeDetectionReport{
				readyKinds: []runtime.AdapterKind{},
			},
			wantLine:  "choice=repair action=runtime_repair ready=none",
			wantError: "run personastack-connector runtime repair",
		},
		{
			name: "single",
			report: runtimeDetectionReport{
				readyKinds: []runtime.AdapterKind{runtime.AdapterKindHermes},
			},
			wantLine:  "choice=auto runtime=hermes",
			wantError: "",
		},
		{
			name: "multiple",
			report: runtimeDetectionReport{
				readyKinds: []runtime.AdapterKind{runtime.AdapterKindHermes, runtime.AdapterKindOpenClaw},
			},
			wantLine:  "choice=manual action=choose_runtime options=hermes,openclaw",
			wantError: "rerun with --runtime hermes or --runtime openclaw",
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if got := test.report.summaryLine(); got != test.wantLine {
				t.Fatalf("summaryLine() = %q, want %q", got, test.wantLine)
			}
			err := test.report.autoDetectError()
			if test.wantError == "" {
				if err != nil {
					t.Fatalf("autoDetectError() = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("autoDetectError() = %v, want substring %q", err, test.wantError)
			}
		})
	}
}

func TestRunRuntimeHermesConfigure(t *testing.T) {
	keyring.MockInit()
	t.Setenv("HOME", t.TempDir())
	t.Setenv("PERSONASTACK_CONNECTOR_DISABLE_HERMES_GATEWAY_START", "1")

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd := command{
		stdout: &stdout,
		stderr: &stderr,
		store: config.NewMemoryStore(config.State{
			Bindings: []config.Binding{
				{
					ConnectionID:    "connection-1",
					PersonaID:       "persona-1",
					RuntimeKind:     runtime.AdapterKindHermes,
					PersonaMCPURL:   "https://mcp.personastack.ai/mcp",
					PersonaMCPToken: "mcp-token-1",
				},
			},
		}),
	}

	if err := cmd.runRuntime([]string{"hermes", "configure", "--enable-api", "--configure-mcp"}); err != nil {
		t.Fatalf("runRuntime() error = %v", err)
	}
	output := stdout.String()
	for _, want := range []string{"runtime hermes configure state=ready", "installed mcp binding=connection-1 runtime=hermes"} {
		if !strings.Contains(output, want) {
			t.Fatalf("runtime output missing %q: %s", want, output)
		}
	}
}

func TestRunRuntimeOpenClawConfigure(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("OPENCLAW_CONFIG_PATH", "")
	t.Setenv("OPENCLAW_HOME", "")
	t.Setenv("OPENCLAW_STATE_DIR", "")
	t.Setenv("OPENCLAW_GATEWAY_TOKEN", "")
	t.Setenv("OPENCLAW_GATEWAY_PASSWORD", "")
	t.Setenv("OPENCLAW_GATEWAY_DEVICE_TOKEN", "")

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd := command{
		stdout: &stdout,
		stderr: &stderr,
		store: config.NewMemoryStore(config.State{
			Bindings: []config.Binding{
				{
					ConnectionID:    "connection-1",
					PersonaID:       "persona-1",
					RuntimeKind:     runtime.AdapterKindOpenClaw,
					PersonaMCPURL:   "https://mcp.personastack.ai/mcp",
					PersonaMCPToken: "mcp-token-1",
				},
			},
		}),
	}

	if err := cmd.runRuntime([]string{"openclaw", "configure", "--gateway", "ws://127.0.0.1:1", "--configure-mcp"}); err != nil {
		t.Fatalf("runRuntime() error = %v", err)
	}
	output := stdout.String()
	for _, want := range []string{"runtime openclaw configure binding=connection-1", "installed mcp binding=connection-1 runtime=openclaw"} {
		if !strings.Contains(output, want) {
			t.Fatalf("runtime output missing %q: %s", want, output)
		}
	}
}

func TestApplyOpenClawPairOptionsStoresOperatorCredential(t *testing.T) {
	binding := config.Binding{RuntimeKind: runtime.AdapterKindOpenClaw}

	err := applyOpenClawPairOptions(&binding, openClawPairOptions{
		token:   "token-1",
		agentID: "agent-1",
	})
	if err != nil {
		t.Fatalf("applyOpenClawPairOptions() error = %v", err)
	}
	if binding.OpenClawGatewayToken != "token-1" || binding.OpenClawAgentID != "agent-1" {
		t.Fatalf("binding OpenClaw options not stored: %+v", binding)
	}
}

func TestApplyOpenClawPairOptionsStoresEnvironmentCredential(t *testing.T) {
	t.Setenv("OPENCLAW_GATEWAY_TOKEN", "token-from-env")
	t.Setenv("OPENCLAW_GATEWAY_PASSWORD", "")
	t.Setenv("OPENCLAW_GATEWAY_DEVICE_TOKEN", "")
	binding := config.Binding{RuntimeKind: runtime.AdapterKindOpenClaw}

	err := applyOpenClawPairOptions(&binding, openClawPairOptions{})
	if err != nil {
		t.Fatalf("applyOpenClawPairOptions() error = %v", err)
	}
	if binding.OpenClawGatewayToken != "token-from-env" {
		t.Fatalf("binding OpenClaw env token not stored: %+v", binding)
	}
}

func TestApplyOpenClawPairOptionsRequiresOperatorCredential(t *testing.T) {
	t.Setenv("OPENCLAW_GATEWAY_TOKEN", "")
	t.Setenv("OPENCLAW_GATEWAY_PASSWORD", "")
	t.Setenv("OPENCLAW_GATEWAY_DEVICE_TOKEN", "")
	binding := config.Binding{RuntimeKind: runtime.AdapterKindOpenClaw}

	err := applyOpenClawPairOptions(&binding, openClawPairOptions{})
	if err == nil || !strings.Contains(err.Error(), "OpenClaw operator credential required") {
		t.Fatalf("applyOpenClawPairOptions() error = %v", err)
	}
}

func TestOpenClawGatewayLoopbackDetection(t *testing.T) {
	if !openclawauth.GatewayIsLoopback("ws://127.0.0.1:18789") {
		t.Fatal("expected 127.0.0.1 to be loopback")
	}
	if !openclawauth.GatewayIsLoopback("ws://localhost:18789") {
		t.Fatal("expected localhost to be loopback")
	}
	if !openclawauth.GatewayIsLoopback("ws://[::1]:18789") {
		t.Fatal("expected ::1 to be loopback")
	}
	if openclawauth.GatewayIsLoopback("ws://example.com:18789") {
		t.Fatal("expected example.com to be non-loopback")
	}
}
