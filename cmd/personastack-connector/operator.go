package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/personastack/personastack-connector/internal/config"
	"github.com/personastack/personastack-connector/internal/diagnostics"
	"github.com/personastack/personastack-connector/internal/mcp"
	"github.com/personastack/personastack-connector/internal/runtime"
)

const connectorHeartbeatFreshness = 45 * time.Second

func (cmd command) runDiagnostics(args []string) error {
	if len(args) != 0 {
		return errors.New("diagnostics accepts no arguments")
	}
	bindings := cmd.store.ListBindings()
	if len(bindings) == 0 {
		fmt.Fprintln(cmd.stdout, "no bindings")
		return nil
	}
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("resolve home dir: %w", err)
	}
	redactor := diagnostics.NewRedactor()
	for _, binding := range bindings {
		line, err := cmd.bindingStatusLine(context.Background(), homeDir, binding, true)
		if err != nil {
			return err
		}
		fmt.Fprintln(cmd.stdout, redactor.Redact("home_dir="+homeDir+" "+line))
	}
	return nil
}

func (cmd command) bindingStatusLine(ctx context.Context, homeDir string, binding config.Binding, includeRepairActions bool) (string, error) {
	verifyCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	verify := mcp.VerifyBindingWithLive(verifyCtx, homeDir, binding, nil)
	cancel()
	detection := adapterForBinding(binding).Detect()
	websocketState := connectorWebsocketState(binding, time.Now().UTC())
	summary := strings.Join(connectorStatusSummaryTokens(binding, detection.State, verify.State, websocketState), " ")
	parts := []string{
		fmt.Sprintf("%s summary=%s", binding.ConnectionID, summary),
		fmt.Sprintf("account=%s", summaryToken(binding.HasBridgeSecret, "paired", "unpaired")),
		fmt.Sprintf("persona=%s", binding.PersonaID),
		fmt.Sprintf("runtime=%s", binding.RuntimeKind),
		fmt.Sprintf("runtime_state=%s", detection.State),
		fmt.Sprintf("websocket=%s", websocketState),
		fmt.Sprintf("mcp=%s", verify.State),
	}
	if note := strings.TrimSpace(verify.Note); note != "" {
		parts = append(parts, fmt.Sprintf("mcp_note=%q", note))
	}
	parts = append(parts,
		fmt.Sprintf("last_wake_probe=%s", formatConnectorTime(binding.LastWakeProbeAt)),
		fmt.Sprintf("active_run=%s", emptyOrDash(binding.ActiveRunID)),
		fmt.Sprintf("active_assignment=%s", emptyOrDash(binding.ActiveAssignmentID)),
		fmt.Sprintf("active_native_run=%s", emptyOrDash(binding.ActiveNativeRunID)),
		fmt.Sprintf("active_run_mcp=%t", binding.HasActiveRunMCPToken),
	)
	if includeRepairActions {
		parts = append(parts, fmt.Sprintf("repair_actions=%s", strings.Join(connectorRepairActions(detection.State, verify.State), ",")))
	}
	return strings.Join(parts, " "), nil
}

func connectorStatusSummaryTokens(binding config.Binding, runtimeState runtime.AdapterState, mcpState runtime.AdapterState, websocketState string) []string {
	summary := []string{
		summaryToken(binding.HasBridgeSecret, "paired", "unpaired"),
		summaryToken(websocketState == "connected", "connected", "disconnected"),
		summaryToken(runtimeState == runtime.AdapterStateReady, "runtime_healthy", "runtime_needs_setup"),
		summaryToken(mcpState == runtime.AdapterStateMCPVerified || mcpState == runtime.AdapterStateMCPRestartRequired, "mcp_configured", "mcp_needs_setup"),
		summaryToken(websocketState == "connected" && runtimeState == runtime.AdapterStateReady && mcpState == runtime.AdapterStateMCPVerified && bindingHasWakeProbeForCurrentGeneration(binding), "wakeable", "not_wakeable"),
	}
	return summary
}

func bindingHasWakeProbeForCurrentGeneration(binding config.Binding) bool {
	return !binding.LastWakeProbeAt.IsZero() && binding.ConnectionGeneration > 0 && binding.LastWakeProbeGeneration == binding.ConnectionGeneration
}

func connectorRepairActions(runtimeState runtime.AdapterState, mcpState runtime.AdapterState) []string {
	actions := []string{
		"runtime_detect",
		"mcp_install",
		"reconnect",
		"rotate_local_token",
		"export_diagnostics",
	}
	if runtimeState == runtime.AdapterStateMCPRestartRequired || mcpState == runtime.AdapterStateMCPRestartRequired {
		actions = append(actions, "restart_runtime")
	}
	return dedupeStrings(actions)
}

func connectorWebsocketState(binding config.Binding, now time.Time) string {
	if binding.LastHeartbeatAt.IsZero() {
		return "disconnected"
	}
	if now.Sub(binding.LastHeartbeatAt.UTC()) <= connectorHeartbeatFreshness {
		return "connected"
	}
	return "disconnected"
}

func runtimeRepairAction(state runtime.AdapterState) string {
	switch state {
	case runtime.AdapterStateRuntimeMissing, runtime.AdapterStateRuntimeStopped, runtime.AdapterStateCapabilityMissing:
		return "update_runtime"
	case runtime.AdapterStateAuthMissing:
		return "refresh_runtime_auth"
	case runtime.AdapterStateMCPConfigMissing:
		return "reinstall_mcp_config"
	case runtime.AdapterStateMCPRestartRequired:
		return "restart_runtime"
	case runtime.AdapterStateWakeProbeFailed:
		return "retry_wake_probe"
	case runtime.AdapterStateMCPVerified, runtime.AdapterStateReady:
		return "none"
	default:
		return "manual_runtime_setup_required"
	}
}

func formatConnectorTime(value time.Time) string {
	if value.IsZero() {
		return "-"
	}
	return value.UTC().Format(time.RFC3339)
}

func summaryToken(ok bool, yes string, no string) string {
	if ok {
		return yes
	}
	return no
}

func dedupeStrings(values []string) []string {
	seen := map[string]struct{}{}
	result := make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		result = append(result, trimmed)
	}
	return result
}
