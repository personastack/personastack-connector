package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/personastack/personastack-connector/internal/config"
	"github.com/personastack/personastack-connector/internal/mcp"
	"github.com/personastack/personastack-connector/internal/runtime"
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
	for _, want := range []string{"pair <code>", "runtime detect", "mcp stdio --binding", "service plan", "run --foreground", "version"} {
		if !strings.Contains(output, want) {
			t.Fatalf("help output missing %q: %s", want, output)
		}
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

func TestRunStatusIncludesActiveAssignmentState(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd := command{
		stdout: &stdout,
		stderr: &stderr,
		store: config.NewMemoryStore(config.State{
			Bindings: []config.Binding{
				{
					ConnectionID:         "connection-1",
					PersonaID:            "persona-1",
					RuntimeKind:          runtime.AdapterKindAuto,
					ActiveRunID:          "run-1",
					ActiveAssignmentID:   "assignment-1",
					ActiveNativeRunID:    "native-run-1",
					HasActiveRunMCPToken: true,
				},
			},
		}),
	}

	if err := cmd.runStatus(context.Background(), nil); err != nil {
		t.Fatalf("runStatus() error = %v", err)
	}
	output := stdout.String()
	for _, want := range []string{"active_run=run-1", "active_assignment=assignment-1", "active_native_run=native-run-1", "active_run_mcp=true"} {
		if !strings.Contains(output, want) {
			t.Fatalf("status output missing %q: %s", want, output)
		}
	}
}

func TestRunRuntimeHermesConfigure(t *testing.T) {
	keyring.MockInit()
	t.Setenv("HOME", t.TempDir())

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd := command{
		stdout: &stdout,
		stderr: &stderr,
		store: config.NewMemoryStore(config.State{
			Bindings: []config.Binding{
				{
					ConnectionID: "connection-1",
					PersonaID:    "persona-1",
					RuntimeKind:  runtime.AdapterKindHermes,
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
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd := command{
		stdout: &stdout,
		stderr: &stderr,
		store: config.NewMemoryStore(config.State{
			Bindings: []config.Binding{
				{
					ConnectionID: "connection-1",
					PersonaID:    "persona-1",
					RuntimeKind:  runtime.AdapterKindOpenClaw,
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
