package runtime

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/personastack/personastack-connector/internal/hermessetup"
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
	StartRun(RunRequest) (string, error)
	StreamOrPollRun(ctx context.Context, nativeRunID string, handle RunEventHandler) (RunResult, error)
	CancelRun(nativeRunID string) error
	Diagnose() Detection
}

type NativeCapabilitySource string

const (
	NativeCapabilitySourceOpenClawToolsCatalog NativeCapabilitySource = "openclaw_tools_catalog"
	NativeCapabilitySourceHermesRuntimeAPI     NativeCapabilitySource = "hermes_runtime_api"
)

type NativeCapabilityKind string

const (
	NativeCapabilityKindToolGroup      NativeCapabilityKind = "tool_group"
	NativeCapabilityKindRuntimeFeature NativeCapabilityKind = "runtime_feature"
)

type NativeCapability struct {
	Source       NativeCapabilitySource
	Kind         NativeCapabilityKind
	CapabilityID string
	Label        string
	Summary      string
}

type NativeCapabilityDescriber interface {
	DescribeNativeCapabilities(ctx context.Context, nativeMCPServerName string) ([]NativeCapability, error)
}

type RunEventKind int

const (
	RunEventStarted RunEventKind = iota
	RunEventOutputDelta
	RunEventToolEvent
)

func (kind RunEventKind) String() string {
	switch kind {
	case RunEventStarted:
		return "started"
	case RunEventOutputDelta:
		return "output_delta"
	case RunEventToolEvent:
		return "tool_event"
	default:
		return "unknown"
	}
}

type RunEvent struct {
	Kind      RunEventKind
	StartedAt time.Time
	Delta     string
	ToolName  string
	ToolPhase string
	Summary   string
}

type RunEventHandler func(RunEvent) error

type runEventState struct {
	mu      sync.Mutex
	started bool
}

func (state *runEventState) emitStarted(handle RunEventHandler, startedAt time.Time) error {
	if handle == nil {
		return nil
	}
	state.mu.Lock()
	if state.started {
		state.mu.Unlock()
		return nil
	}
	state.started = true
	state.mu.Unlock()
	if startedAt.IsZero() {
		startedAt = time.Now().UTC()
	}
	return handle(RunEvent{Kind: RunEventStarted, StartedAt: startedAt})
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
		return NewHermesAdapter(os.Getenv("PERSONASTACK_CONNECTOR_HERMES_URL"), hermessetup.LoadAPIKey())
	case AdapterKindOpenClaw:
		return NewOpenClawAdapterWithAuth(os.Getenv("PERSONASTACK_CONNECTOR_OPENCLAW_GATEWAY_URL"), OpenClawAuth{}, os.Getenv("OPENCLAW_AGENT_ID"))
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
		Note:  "runtime adapter is unsupported",
	}
}

func (adapter PlaceholderAdapter) StartRun(RunRequest) (string, error) {
	return "", fmt.Errorf("%s run dispatch is not implemented", adapter.kind)
}

func (adapter PlaceholderAdapter) StreamOrPollRun(ctx context.Context, nativeRunID string, handle RunEventHandler) (RunResult, error) {
	return RunResult{}, fmt.Errorf("%s run wait is not implemented", adapter.kind)
}

func (adapter PlaceholderAdapter) CancelRun(nativeRunID string) error {
	return fmt.Errorf("%s run cancellation is not implemented", adapter.kind)
}

func (adapter PlaceholderAdapter) Diagnose() Detection {
	return adapter.Detect()
}

type ErrorAdapter struct {
	kind  AdapterKind
	state AdapterState
	note  string
}

func NewErrorAdapter(kind AdapterKind, state AdapterState, note string) ErrorAdapter {
	return ErrorAdapter{kind: kind, state: state, note: strings.TrimSpace(note)}
}

func (adapter ErrorAdapter) Kind() AdapterKind {
	return adapter.kind
}

func (adapter ErrorAdapter) Detect() Detection {
	note := adapter.note
	if note == "" {
		note = "runtime adapter error"
	}
	return Detection{Kind: adapter.kind, State: adapter.state, Note: note}
}

func (adapter ErrorAdapter) StartRun(RunRequest) (string, error) {
	return "", fmt.Errorf("%s run dispatch unavailable: %s", adapter.kind, adapter.Detect().Note)
}

func (adapter ErrorAdapter) StreamOrPollRun(ctx context.Context, nativeRunID string, handle RunEventHandler) (RunResult, error) {
	return RunResult{}, fmt.Errorf("%s run wait unavailable: %s", adapter.kind, adapter.Detect().Note)
}

func (adapter ErrorAdapter) CancelRun(nativeRunID string) error {
	return fmt.Errorf("%s run cancellation unavailable: %s", adapter.kind, adapter.Detect().Note)
}

func (adapter ErrorAdapter) Diagnose() Detection {
	return adapter.Detect()
}
