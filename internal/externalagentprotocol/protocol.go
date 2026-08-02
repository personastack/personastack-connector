package externalagentprotocol

import (
	"strings"
	"time"
)

const (
	// ProtocolVersionV2 identifies the legacy Connector websocket protocol version.
	ProtocolVersionV2 = "external-agent-v2"
	// ProtocolVersionV4 identifies the Connector websocket protocol version that carries API-selected runtime targets.
	ProtocolVersionV4 = "external-agent-v4"
)

var supportedProtocolVersions = []string{ProtocolVersionV4}

// SupportedProtocolVersions returns the protocol versions this binary can speak, in preference order.
func SupportedProtocolVersions() []string {
	return append([]string(nil), supportedProtocolVersions...)
}

// ProtocolVersionSupported reports whether the version is currently supported.
func ProtocolVersionSupported(version string) bool {
	trimmed := strings.TrimSpace(version)
	if trimmed == "" {
		return false
	}
	for _, supported := range supportedProtocolVersions {
		if supported == trimmed {
			return true
		}
	}
	return false
}

// NegotiateProtocolVersion selects the first mutually supported version.
func NegotiateProtocolVersion(requested string, offered []string, supported []string) (string, bool) {
	requested = strings.TrimSpace(requested)
	if requested == "" {
		return "", false
	}
	normalizedOffered := normalizeProtocolVersions(offered)
	if len(normalizedOffered) == 0 {
		normalizedOffered = []string{requested}
	}
	if !containsProtocolVersion(normalizedOffered, requested) {
		return "", false
	}
	normalizedSupported := normalizeProtocolVersions(supported)
	for _, candidate := range normalizedSupported {
		if containsProtocolVersion(normalizedOffered, candidate) {
			return candidate, true
		}
	}
	return "", false
}

func normalizeProtocolVersions(versions []string) []string {
	normalized := make([]string, 0, len(versions))
	seen := map[string]struct{}{}
	for _, version := range versions {
		trimmed := strings.TrimSpace(version)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		normalized = append(normalized, trimmed)
	}
	return normalized
}

func containsProtocolVersion(versions []string, version string) bool {
	trimmed := strings.TrimSpace(version)
	if trimmed == "" {
		return false
	}
	for _, candidate := range versions {
		if strings.TrimSpace(candidate) == trimmed {
			return true
		}
	}
	return false
}

// FrameType identifies the closed Connector websocket frame family.
type FrameType string

const (
	FrameTypeConnect            FrameType = "connect"
	FrameTypeConnectAccepted    FrameType = "connect.accepted"
	FrameTypeConnectRejected    FrameType = "connect.rejected"
	FrameTypeHeartbeat          FrameType = "heartbeat"
	FrameTypeCapabilitiesReport FrameType = "capabilities.report"
	FrameTypeTargetInventory    FrameType = "target_inventory.report"
	FrameTypePing               FrameType = "ping"
	FrameTypeWakeProbe          FrameType = "wake.probe"
	FrameTypeWakeProbeAccepted  FrameType = "wake.probe.accepted"
	FrameTypeRunStart           FrameType = "run.start"
	FrameTypeRunAccepted        FrameType = "run.accepted"
	FrameTypeRunStarted         FrameType = "run.started"
	FrameTypeRunOutputDelta     FrameType = "run.output_delta"
	FrameTypeRunToolEvent       FrameType = "run.tool_event"
	FrameTypeRunCompleted       FrameType = "run.completed"
	FrameTypeRunFailed          FrameType = "run.failed"
	FrameTypeRunCancelled       FrameType = "run.cancelled"
	FrameTypeRunTerminalAck     FrameType = "run.terminal_ack"
	FrameTypeRunCancel          FrameType = "run.cancel"
	FrameTypeConfigRefresh      FrameType = "config.refresh"
	FrameTypeConfigClear        FrameType = "config.clear"
	FrameTypeTokenRevoked       FrameType = "token.revoked"
	FrameTypeServerDraining     FrameType = "server.draining"
	FrameTypeShutdown           FrameType = "shutdown"
	FrameTypeDiagnostic         FrameType = "diagnostic"
)

type DiagnosticCode string

const (
	DiagnosticCodeRuntimeMissing          DiagnosticCode = "runtime_missing"
	DiagnosticCodeRuntimeStopped          DiagnosticCode = "runtime_stopped"
	DiagnosticCodeAuthMissing             DiagnosticCode = "auth_missing"
	DiagnosticCodeCapabilityMissing       DiagnosticCode = "capability_missing"
	DiagnosticCodeMCPConfigMissing        DiagnosticCode = "mcp_config_missing"
	DiagnosticCodeMCPConfigParseError     DiagnosticCode = "mcp_config_parse_error"
	DiagnosticCodeMCPConfigConflict       DiagnosticCode = "mcp_config_conflict"
	DiagnosticCodeMCPTokenMissing         DiagnosticCode = "mcp_token_missing"
	DiagnosticCodeMCPTokenRejected        DiagnosticCode = "mcp_token_rejected"
	DiagnosticCodeMCPEndpointUnreachable  DiagnosticCode = "mcp_endpoint_unreachable"
	DiagnosticCodeNativeMCPUnreachable    DiagnosticCode = "native_mcp_unreachable"
	DiagnosticCodeMCPRestartRequired      DiagnosticCode = "mcp_restart_required"
	DiagnosticCodeWakeProbeFailed         DiagnosticCode = "wake_probe_failed"
	DiagnosticCodeRuntimeError            DiagnosticCode = "runtime_error"
	DiagnosticCodeTargetSelectionRequired DiagnosticCode = "target_selection_required"
)

// RuntimeKind identifies a supported external local runtime.
type RuntimeKind string

const (
	RuntimeKindAuto     RuntimeKind = "auto"
	RuntimeKindHermes   RuntimeKind = "hermes"
	RuntimeKindOpenClaw RuntimeKind = "openclaw"
)

// SupportedRuntimeKinds is the negotiation order used when a pairing command
// requests --runtime auto. It describes protocol support, not local readiness.
func SupportedRuntimeKinds() []RuntimeKind {
	return []RuntimeKind{RuntimeKindHermes, RuntimeKindOpenClaw}
}

type ServiceScope string

const (
	ServiceScopeUserLaunchAgent    ServiceScope = "user_launch_agent"
	ServiceScopeSystemLaunchDaemon ServiceScope = "system_launch_daemon"
	ServiceScopeLinuxSystemService ServiceScope = "linux_system_service"
)

// ConnectionStatus identifies Connector bridge status reported to Gateway.
type ConnectionStatus string

const (
	ConnectionStatusBridgeConnected ConnectionStatus = "bridge_connected"
	ConnectionStatusOffline         ConnectionStatus = "offline"
	ConnectionStatusError           ConnectionStatus = "error"
)

// ReadinessStatus identifies runtime readiness reported by Connector.
type ReadinessStatus string

const (
	ReadinessStatusRuntimeHealthy ReadinessStatus = "runtime_healthy"
	ReadinessStatusMCPConfigured  ReadinessStatus = "mcp_configured"
	ReadinessStatusWakeable       ReadinessStatus = "wakeable"
	ReadinessStatusRuntimeError   ReadinessStatus = "runtime_error"
)

type NativeToolNamingRule string

const (
	NativeToolNamingRuleUnknown                 NativeToolNamingRule = "unknown"
	NativeToolNamingRuleMCPServerPrefix         NativeToolNamingRule = "mcp_server_prefix"
	NativeToolNamingRuleRuntimeEffectiveCatalog NativeToolNamingRule = "runtime_effective_catalog"
)

// RunStatus identifies run lifecycle states flowing over the Connector protocol.
type RunStatus string

const (
	RunStatusAccepted  RunStatus = "accepted"
	RunStatusStarted   RunStatus = "started"
	RunStatusCompleted RunStatus = "completed"
	RunStatusFailed    RunStatus = "failed"
	RunStatusCancelled RunStatus = "cancelled"
)

// TerminalReason identifies finite terminal reasons for Connector run completion.
type TerminalReason string

const (
	TerminalReasonSucceeded TerminalReason = "succeeded"
	TerminalReasonFailed    TerminalReason = "failed"
	TerminalReasonCancelled TerminalReason = "cancelled"
	TerminalReasonExpired   TerminalReason = "expired"
)

// RejectionReason identifies finite rejection reasons on the Connector boundary.
type RejectionReason string

const (
	RejectionReasonInvalidCredential          RejectionReason = "invalid_credential"
	RejectionReasonUnsupportedProtocolVersion RejectionReason = "unsupported_protocol_version"
	RejectionReasonStaleGeneration            RejectionReason = "stale_generation"
	RejectionReasonDuplicateSession           RejectionReason = "duplicate_session"
	RejectionReasonUnsupportedRuntime         RejectionReason = "unsupported_runtime"
	RejectionReasonInvalidRequest             RejectionReason = "invalid_request"
	RejectionReasonRuntimeUnavailable         RejectionReason = "runtime_unavailable"
	RejectionReasonPersonaBusy                RejectionReason = "persona_busy"
)

// CapabilityKind identifies bounded Connector/native runtime capabilities.
type CapabilityKind string

const (
	CapabilityKindRuntimeHealth CapabilityKind = "runtime_health"
	CapabilityKindMCPConfigure  CapabilityKind = "mcp_configure"
	CapabilityKindMCPVerify     CapabilityKind = "mcp_verify"
	CapabilityKindRunDispatch   CapabilityKind = "run_dispatch"
	CapabilityKindRunStreaming  CapabilityKind = "run_streaming"
	CapabilityKindRunCancel     CapabilityKind = "run_cancel"
	CapabilityKindWakeProbe     CapabilityKind = "wake_probe"
)

// NativeCapabilitySource identifies the runtime authority used for prompt-safe capability summaries.
type NativeCapabilitySource string

const (
	NativeCapabilitySourceOpenClawToolsCatalog NativeCapabilitySource = "openclaw_tools_catalog"
	NativeCapabilitySourceOpenClawReadySkills  NativeCapabilitySource = "openclaw_ready_skills"
	NativeCapabilitySourceHermesRuntimeAPI     NativeCapabilitySource = "hermes_runtime_api"
	NativeCapabilitySourceHermesToolsList      NativeCapabilitySource = "hermes_tools_list"
)

// NativeCapabilityKind identifies a bounded external runtime delegation capability shape.
type NativeCapabilityKind string

const (
	NativeCapabilityKindToolGroup      NativeCapabilityKind = "tool_group"
	NativeCapabilityKindRuntimeFeature NativeCapabilityKind = "runtime_feature"
	NativeCapabilityKindNativeTool     NativeCapabilityKind = "native_tool"
	NativeCapabilityKindSkill          NativeCapabilityKind = "skill"
)

// NativeCapabilityDiscoveryStatus identifies whether native capability sources are complete.
type NativeCapabilityDiscoveryStatus string

const (
	NativeCapabilityDiscoveryUnsupported NativeCapabilityDiscoveryStatus = "unsupported"
	NativeCapabilityDiscoveryFailed      NativeCapabilityDiscoveryStatus = "failed"
	NativeCapabilityDiscoveryComplete    NativeCapabilityDiscoveryStatus = "complete"
	NativeCapabilityDiscoveryPartial     NativeCapabilityDiscoveryStatus = "partial"
)

// Frame is the typed envelope for all Connector websocket messages.
type Frame struct {
	MessageID    string    `json:"message_id"`
	MessageType  FrameType `json:"message_type"`
	PersonaID    string    `json:"persona_id"`
	ConnectionID string    `json:"connection_id"`
	RunID        string    `json:"run_id,omitempty"`
	AssignmentID string    `json:"assignment_id,omitempty"`
	// ConnectionGeneration binds run callbacks to the Connector session that emitted them.
	ConnectionGeneration int64     `json:"connection_generation,omitempty"`
	Sequence             int64     `json:"sequence,omitempty"`
	SentAt               time.Time `json:"sent_at"`

	Connect           *ConnectPayload           `json:"connect,omitempty"`
	ConnectAccepted   *ConnectAcceptedPayload   `json:"connect_accepted,omitempty"`
	ConnectRejected   *ConnectRejectedPayload   `json:"connect_rejected,omitempty"`
	Heartbeat         *HeartbeatPayload         `json:"heartbeat,omitempty"`
	Capabilities      *CapabilitiesPayload      `json:"capabilities,omitempty"`
	TargetInventory   *TargetInventoryPayload   `json:"target_inventory,omitempty"`
	WakeProbe         *WakeProbePayload         `json:"wake_probe,omitempty"`
	WakeProbeAccepted *WakeProbeAcceptedPayload `json:"wake_probe_accepted,omitempty"`
	RunStart          *RunStartPayload          `json:"run_start,omitempty"`
	RunAccepted       *RunAcceptedPayload       `json:"run_accepted,omitempty"`
	RunStarted        *RunStartedPayload        `json:"run_started,omitempty"`
	RunOutputDelta    *RunOutputDeltaPayload    `json:"run_output_delta,omitempty"`
	RunToolEvent      *RunToolEventPayload      `json:"run_tool_event,omitempty"`
	RunTerminal       *RunTerminalPayload       `json:"run_terminal,omitempty"`
	RunTerminalAck    *RunTerminalAckPayload    `json:"run_terminal_ack,omitempty"`
	RunCancel         *RunCancelPayload         `json:"run_cancel,omitempty"`
	ConfigRefresh     *ConfigRefreshPayload     `json:"config_refresh,omitempty"`
	ConfigClear       *ConfigClearPayload       `json:"config_clear,omitempty"`
	TokenRevoked      *TokenRevokedPayload      `json:"token_revoked,omitempty"`
	ServerDraining    *ServerDrainingPayload    `json:"server_draining,omitempty"`
	Diagnostic        *DiagnosticPayload        `json:"diagnostic,omitempty"`
}

type ConnectPayload struct {
	ProtocolVersion           string      `json:"protocol_version"`
	SupportedProtocolVersions []string    `json:"supported_protocol_versions,omitempty"`
	ConnectorVersion          string      `json:"connector_version"`
	RuntimeKind               RuntimeKind `json:"runtime_kind"`
	ConnectionGeneration      int64       `json:"connection_generation"`
	Hostname                  string      `json:"hostname,omitempty"`
	OS                        string      `json:"os,omitempty"`
	Arch                      string      `json:"arch,omitempty"`
	DevicePublicKey           string      `json:"device_public_key"`
	CredentialID              string      `json:"credential_id"`
	CredentialProof           string      `json:"credential_proof"`
	CredentialProofNonce      string      `json:"credential_proof_nonce"`
	CredentialProofUnix       int64       `json:"credential_proof_unix"`
	SupportsTargetClear       bool        `json:"supports_target_clear"`
}

type ConnectAcceptedPayload struct {
	ProtocolVersion      string `json:"protocol_version"`
	ConnectionGeneration int64  `json:"connection_generation"`
	ServerInstanceID     string `json:"server_instance_id"`
	HeartbeatSeconds     int    `json:"heartbeat_seconds"`
}

type ConnectRejectedPayload struct {
	Reason  RejectionReason `json:"reason"`
	Message string          `json:"message"`
}

type HeartbeatPayload struct {
	ConnectionStatus        ConnectionStatus     `json:"connection_status"`
	ReadinessStatus         ReadinessStatus      `json:"readiness_status"`
	RuntimeKind             RuntimeKind          `json:"runtime_kind"`
	ServiceScope            ServiceScope         `json:"service_scope,omitempty"`
	ConnectionGeneration    int64                `json:"connection_generation"`
	RuntimeLabel            string               `json:"runtime_label,omitempty"`
	Hostname                string               `json:"hostname,omitempty"`
	NativeMCPServerName     string               `json:"native_mcp_server_name,omitempty"`
	NativeMCPToolNamespace  string               `json:"native_mcp_tool_namespace,omitempty"`
	NativeMCPToolPrefix     string               `json:"native_mcp_tool_prefix,omitempty"`
	NativeToolNamingRule    NativeToolNamingRule `json:"native_tool_naming_rule,omitempty"`
	ConnectorVersion        string               `json:"connector_version"`
	GitCommit               string               `json:"git_commit,omitempty"`
	OS                      string               `json:"os,omitempty"`
	Arch                    string               `json:"arch,omitempty"`
	ReleaseChannel          string               `json:"release_channel,omitempty"`
	LastWakeProbeAt         *time.Time           `json:"last_wake_probe_at,omitempty"`
	DiagnosticCode          DiagnosticCode       `json:"diagnostic_code,omitempty"`
	TargetSelectionRevision int64                `json:"target_selection_revision,omitempty"`
	TargetEpoch             uint64               `json:"target_epoch,omitempty"`
}

type CapabilitiesPayload struct {
	ConnectionGeneration            int64                           `json:"connection_generation"`
	Capabilities                    []CapabilityReport              `json:"capabilities"`
	NativeCapabilities              []NativeCapabilityReport        `json:"native_capabilities"`
	NativeCapabilityDiscoveryStatus NativeCapabilityDiscoveryStatus `json:"native_capability_discovery_status,omitempty"`
	NativeCapabilityReportedSources []NativeCapabilitySource        `json:"native_capability_reported_sources,omitempty"`
}

type CapabilityReport struct {
	Kind       CapabilityKind  `json:"kind"`
	Status     ReadinessStatus `json:"status"`
	Label      string          `json:"label,omitempty"`
	Reason     RejectionReason `json:"reason,omitempty"`
	ReportedAt time.Time       `json:"reported_at"`
}

type NativeCapabilityReport struct {
	Source       NativeCapabilitySource `json:"source"`
	Kind         NativeCapabilityKind   `json:"kind"`
	CapabilityID string                 `json:"capability_id"`
	Label        string                 `json:"label"`
	Summary      string                 `json:"summary"`
	Status       ReadinessStatus        `json:"status"`
	ReportedAt   time.Time              `json:"reported_at"`
}

type WakeProbePayload struct {
	ProbeID    string    `json:"probe_id"`
	DeadlineAt time.Time `json:"deadline_at"`
}

type WakeProbeAcceptedPayload struct {
	ProbeID                 string      `json:"probe_id"`
	RuntimeKind             RuntimeKind `json:"runtime_kind"`
	AcceptedAt              time.Time   `json:"accepted_at"`
	TargetSelectionRevision int64       `json:"target_selection_revision,omitempty"`
	TargetEpoch             uint64      `json:"target_epoch,omitempty"`
}

type RunStartPayload struct {
	FullyComposedPrompt    string            `json:"fully_composed_prompt"`
	PromptContext          PromptContext     `json:"prompt_context"`
	TriggerKind            string            `json:"trigger_kind"`
	SourcePersonaID        string            `json:"source_persona_id,omitempty"`
	ConversationID         string            `json:"conversation_id,omitempty"`
	StreamID               string            `json:"stream_id,omitempty"`
	StackID                string            `json:"stack_id,omitempty"`
	MCPURL                 string            `json:"mcp_url"`
	NativeMCPServerName    string            `json:"native_mcp_server_name"`
	NativeMCPToolNamespace string            `json:"native_mcp_tool_namespace"`
	DeadlineAt             time.Time         `json:"deadline_at"`
	RuntimeTarget          *RuntimeTarget    `json:"runtime_target,omitempty"`
	Metadata               map[string]string `json:"metadata,omitempty"`
}

// RuntimeTarget is the API-selected local runtime target. It contains no local path or UID.
type RuntimeTarget struct {
	AccountCandidateID string      `json:"account_candidate_id"`
	ProfileCandidateID string      `json:"profile_candidate_id"`
	RuntimeKind        RuntimeKind `json:"runtime_kind"`
	SelectionRevision  int64       `json:"selection_revision"`
}

// TargetInventoryPayload contains browser-safe candidates discovered on the Connector host.
type TargetInventoryPayload struct {
	InventoryGeneration int64                     `json:"inventory_generation"`
	DiscoveryStatus     DiscoveryStatus           `json:"discovery_status,omitempty"`
	Accounts            []RuntimeAccountCandidate `json:"accounts"`
}

// DiscoveryStatus describes whether an inventory is authoritative. Unknown
// remains the compatibility value for older Connectors that omit the field.
type DiscoveryStatus string

const (
	DiscoveryStatusUnknown  DiscoveryStatus = "unknown"
	DiscoveryStatusComplete DiscoveryStatus = "complete"
	DiscoveryStatusDegraded DiscoveryStatus = "degraded"
)

// RuntimeAccountCandidate is one Connector-accessible local account.
type RuntimeAccountCandidate struct {
	CandidateID string                    `json:"candidate_id"`
	Label       string                    `json:"label"`
	Profiles    []RuntimeProfileCandidate `json:"profiles"`
}

// RuntimeProfileCandidate is one Connector-accessible runtime profile.
type RuntimeProfileCandidate struct {
	CandidateID string      `json:"candidate_id"`
	Label       string      `json:"label"`
	RuntimeKind RuntimeKind `json:"runtime_kind"`
	Readiness   string      `json:"readiness,omitempty"`
}

type PromptContext struct {
	PromptVersion string `json:"prompt_version"`
	PromptHash    string `json:"prompt_hash"`
}

type RunAcceptedPayload struct {
	AcceptedAt  time.Time `json:"accepted_at"`
	NativeRunID string    `json:"native_run_id,omitempty"`
}

type RunStartedPayload struct {
	StartedAt   time.Time `json:"started_at"`
	NativeRunID string    `json:"native_run_id,omitempty"`
}

type RunOutputDeltaPayload struct {
	Delta string `json:"delta"`
}

type RunToolEventPayload struct {
	ToolName string `json:"tool_name"`
	Phase    string `json:"phase"`
	Summary  string `json:"summary,omitempty"`
}

type RunTerminalPayload struct {
	Status       RunStatus      `json:"status"`
	Reason       TerminalReason `json:"reason"`
	FinalMessage string         `json:"final_message,omitempty"`
	ErrorCode    string         `json:"error_code,omitempty"`
	ErrorMessage string         `json:"error_message,omitempty"`
	NativeRunID  string         `json:"native_run_id,omitempty"`
	CompletedAt  time.Time      `json:"completed_at"`
}

type RunTerminalAckPayload struct {
	AcknowledgedAt time.Time `json:"acknowledged_at"`
}

type RunCancelPayload struct {
	Reason string `json:"reason"`
}

type ConfigRefreshPayload struct {
	MCPURL                 string         `json:"mcp_url,omitempty"`
	NativeMCPServerName    string         `json:"native_mcp_server_name,omitempty"`
	NativeMCPToolNamespace string         `json:"native_mcp_tool_namespace,omitempty"`
	RuntimeTarget          *RuntimeTarget `json:"runtime_target,omitempty"`
	ClearRuntimeTarget     bool           `json:"clear_runtime_target,omitempty"`
	SelectionRevision      int64          `json:"selection_revision,omitempty"`
	TargetEpoch            uint64         `json:"target_epoch,omitempty"`
}

type ConfigClearPayload struct {
	TargetSelectionRevision int64  `json:"target_selection_revision"`
	TargetEpoch             uint64 `json:"target_epoch"`
}

type TokenRevokedPayload struct {
	TokenKind string `json:"token_kind"`
	Reason    string `json:"reason"`
}

type ServerDrainingPayload struct {
	DeadlineAt time.Time `json:"deadline_at"`
	Reason     string    `json:"reason"`
}

type DiagnosticPayload struct {
	Code    string `json:"code"`
	Summary string `json:"summary"`
	Level   string `json:"level"`
}
