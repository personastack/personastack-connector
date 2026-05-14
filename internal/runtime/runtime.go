package runtime

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"
)

type AdapterKind int

const (
	AdapterKindAuto AdapterKind = iota
	AdapterKindHermes
	AdapterKindOpenClaw
)

func ParseAdapterKind(value string) (AdapterKind, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "auto":
		return AdapterKindAuto, nil
	case "hermes":
		return AdapterKindHermes, nil
	case "openclaw":
		return AdapterKindOpenClaw, nil
	default:
		return AdapterKindAuto, fmt.Errorf("unknown runtime adapter %q", value)
	}
}

func (kind AdapterKind) String() string {
	switch kind {
	case AdapterKindAuto:
		return "auto"
	case AdapterKindHermes:
		return "hermes"
	case AdapterKindOpenClaw:
		return "openclaw"
	default:
		return "unknown"
	}
}

type AdapterState int

const (
	AdapterStateRuntimeMissing AdapterState = iota
	AdapterStateRuntimeStopped
	AdapterStateAuthMissing
	AdapterStateCapabilityMissing
	AdapterStateMCPConfigMissing
	AdapterStateMCPRestartRequired
	AdapterStateMCPVerified
	AdapterStateWakeProbeFailed
	AdapterStateReady
)

func (state AdapterState) String() string {
	switch state {
	case AdapterStateRuntimeMissing:
		return "runtime_missing"
	case AdapterStateRuntimeStopped:
		return "runtime_stopped"
	case AdapterStateAuthMissing:
		return "auth_missing"
	case AdapterStateCapabilityMissing:
		return "capability_missing"
	case AdapterStateMCPConfigMissing:
		return "mcp_config_missing"
	case AdapterStateMCPRestartRequired:
		return "mcp_restart_required"
	case AdapterStateMCPVerified:
		return "mcp_verified"
	case AdapterStateWakeProbeFailed:
		return "wake_probe_failed"
	case AdapterStateReady:
		return "ready"
	default:
		return "unknown"
	}
}

type Detection struct {
	Kind  AdapterKind
	State AdapterState
	Note  string
}

type Adapter interface {
	Kind() AdapterKind
	Detect() Detection
	ConfigureMCP(bindingID string) error
	VerifyMCP(bindingID string) (AdapterState, error)
	StartRun(RunRequest) (string, error)
	WaitRun(ctx context.Context, nativeRunID string) (RunResult, error)
	CancelRun(nativeRunID string) error
	Diagnose() Detection
}

type RunRequest struct {
	RunID                  string
	AssignmentID           string
	FullyComposedPrompt    string
	NativeMCPServerName    string
	NativeMCPToolNamespace string
	Metadata               map[string]string
}

func runMetadata(request RunRequest) map[string]string {
	metadata := map[string]string{}
	for key, value := range request.Metadata {
		trimmedKey := strings.TrimSpace(key)
		trimmedValue := strings.TrimSpace(value)
		if trimmedKey == "" || trimmedValue == "" {
			continue
		}
		metadata[trimmedKey] = trimmedValue
	}
	if strings.TrimSpace(request.RunID) != "" {
		metadata["personastack_run_id"] = strings.TrimSpace(request.RunID)
	}
	if strings.TrimSpace(request.AssignmentID) != "" {
		metadata["personastack_assignment_id"] = strings.TrimSpace(request.AssignmentID)
	}
	return metadata
}

type RunResult struct {
	Status RunStatus
	Output string
}

type RunStatus int

const (
	RunStatusSucceeded RunStatus = iota
	RunStatusFailed
	RunStatusCancelled
)

func NewAdapter(kind AdapterKind) Adapter {
	switch kind {
	case AdapterKindHermes:
		return NewHermesAdapter(os.Getenv("PERSONASTACK_CONNECTOR_HERMES_URL"), os.Getenv("HERMES_API_SERVER_KEY"))
	case AdapterKindOpenClaw:
		return NewOpenClawAdapter(os.Getenv("PERSONASTACK_CONNECTOR_OPENCLAW_GATEWAY_URL"), os.Getenv("OPENCLAW_GATEWAY_TOKEN"))
	default:
		return NewPlaceholderAdapter(kind)
	}
}

func defaultHTTPClient() *http.Client {
	return &http.Client{Timeout: 10 * time.Second}
}

type PlaceholderAdapter struct {
	kind AdapterKind
}

func NewPlaceholderAdapter(kind AdapterKind) PlaceholderAdapter {
	return PlaceholderAdapter{kind: kind}
}

func (adapter PlaceholderAdapter) Kind() AdapterKind {
	return adapter.kind
}

func (adapter PlaceholderAdapter) Detect() Detection {
	return Detection{
		Kind:  adapter.kind,
		State: AdapterStateRuntimeMissing,
		Note:  "runtime networking is not implemented in this scaffold",
	}
}

func (adapter PlaceholderAdapter) ConfigureMCP(bindingID string) error {
	return fmt.Errorf("%s MCP configuration is not implemented", adapter.kind)
}

func (adapter PlaceholderAdapter) VerifyMCP(bindingID string) (AdapterState, error) {
	return AdapterStateMCPConfigMissing, fmt.Errorf("%s MCP verification is not implemented", adapter.kind)
}

func (adapter PlaceholderAdapter) StartRun(RunRequest) (string, error) {
	return "", fmt.Errorf("%s run dispatch is not implemented", adapter.kind)
}

func (adapter PlaceholderAdapter) WaitRun(ctx context.Context, nativeRunID string) (RunResult, error) {
	return RunResult{}, fmt.Errorf("%s run wait is not implemented", adapter.kind)
}

func (adapter PlaceholderAdapter) CancelRun(nativeRunID string) error {
	return fmt.Errorf("%s run cancellation is not implemented", adapter.kind)
}

func (adapter PlaceholderAdapter) Diagnose() Detection {
	return adapter.Detect()
}
