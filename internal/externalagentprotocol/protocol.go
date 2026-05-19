package externalagentprotocol

import (
	"strings"
	"time"
)

const (
	// ProtocolVersionV1 identifies the first Connector websocket protocol version.
	ProtocolVersionV1 = "external-agent-v1"
)

var supportedProtocolVersions = []string{ProtocolVersionV1}

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
	FrameTypeRunCancel          FrameType = "run.cancel"
	FrameTypeConfigRefresh      FrameType = "config.refresh"
	FrameTypeTokenRevoked       FrameType = "token.revoked"
	FrameTypeServerDraining     FrameType = "server.draining"
	FrameTypeShutdown           FrameType = "shutdown"
	FrameTypeDiagnostic         FrameType = "diagnostic"
)

// RuntimeKind identifies a supported external local runtime.
type RuntimeKind string

const (
	RuntimeKindHermes   RuntimeKind = "hermes"
	RuntimeKindOpenClaw RuntimeKind = "openclaw"
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
	NativeCapabilitySourceHermesRuntimeAPI     NativeCapabilitySource = "hermes_runtime_api"
)

// NativeCapabilityKind identifies a bounded external runtime delegation capability shape.
type NativeCapabilityKind string

const (
	NativeCapabilityKindToolGroup      NativeCapabilityKind = "tool_group"
	NativeCapabilityKindRuntimeFeature NativeCapabilityKind = "runtime_feature"
)

// Frame is the typed envelope for all Connector websocket messages.
type Frame struct {
	MessageID    string    `json:"message_id"`
	MessageType  FrameType `json:"message_type"`
	PersonaID    string    `json:"persona_id"`
	ConnectionID string    `json:"connection_id"`
	RunID        string    `json:"run_id,omitempty"`
	AssignmentID string    `json:"assignment_id,omitempty"`
	Sequence     int64     `json:"sequence,omitempty"`
	SentAt       time.Time `json:"sent_at"`

	Connect           *ConnectPayload           `json:"connect,omitempty"`
	ConnectAccepted   *ConnectAcceptedPayload   `json:"connect_accepted,omitempty"`
	ConnectRejected   *ConnectRejectedPayload   `json:"connect_rejected,omitempty"`
	Heartbeat         *HeartbeatPayload         `json:"heartbeat,omitempty"`
	Capabilities      *CapabilitiesPayload      `json:"capabilities,omitempty"`
	WakeProbe         *WakeProbePayload         `json:"wake_probe,omitempty"`
	WakeProbeAccepted *WakeProbeAcceptedPayload `json:"wake_probe_accepted,omitempty"`
	RunStart          *RunStartPayload          `json:"run_start,omitempty"`
	RunAccepted       *RunAcceptedPayload       `json:"run_accepted,omitempty"`
	RunStarted        *RunStartedPayload        `json:"run_started,omitempty"`
	RunOutputDelta    *RunOutputDeltaPayload    `json:"run_output_delta,omitempty"`
	RunToolEvent      *RunToolEventPayload      `json:"run_tool_event,omitempty"`
	RunTerminal       *RunTerminalPayload       `json:"run_terminal,omitempty"`
	RunCancel         *RunCancelPayload         `json:"run_cancel,omitempty"`
	ConfigRefresh     *ConfigRefreshPayload     `json:"config_refresh,omitempty"`
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
	DevicePublicKey           string      `json:"device_public_key"`
	CredentialID              string      `json:"credential_id"`
	CredentialProof           string      `json:"credential_proof"`
	CredentialProofNonce      string      `json:"credential_proof_nonce"`
	CredentialProofUnix       int64       `json:"credential_proof_unix"`
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
	ConnectionStatus       ConnectionStatus     `json:"connection_status"`
	ReadinessStatus        ReadinessStatus      `json:"readiness_status"`
	RuntimeKind            RuntimeKind          `json:"runtime_kind"`
	ConnectionGeneration   int64                `json:"connection_generation"`
	RuntimeLabel           string               `json:"runtime_label,omitempty"`
	Hostname               string               `json:"hostname,omitempty"`
	NativeMCPServerName    string               `json:"native_mcp_server_name,omitempty"`
	NativeMCPToolNamespace string               `json:"native_mcp_tool_namespace,omitempty"`
	NativeMCPToolPrefix    string               `json:"native_mcp_tool_prefix,omitempty"`
	NativeToolNamingRule   NativeToolNamingRule `json:"native_tool_naming_rule,omitempty"`
	ConnectorVersion       string               `json:"connector_version"`
	GitCommit              string               `json:"git_commit,omitempty"`
	OS                     string               `json:"os,omitempty"`
	Arch                   string               `json:"arch,omitempty"`
	ReleaseChannel         string               `json:"release_channel,omitempty"`
	LastWakeProbeAt        *time.Time           `json:"last_wake_probe_at,omitempty"`
	DiagnosticCode         string               `json:"diagnostic_code,omitempty"`
}

type CapabilitiesPayload struct {
	Capabilities       []CapabilityReport       `json:"capabilities"`
	NativeCapabilities []NativeCapabilityReport `json:"native_capabilities"`
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
	ProbeID     string      `json:"probe_id"`
	RuntimeKind RuntimeKind `json:"runtime_kind"`
	AcceptedAt  time.Time   `json:"accepted_at"`
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
	RunScopedMCPToken      string            `json:"run_scoped_mcp_token,omitempty"`
	DeadlineAt             time.Time         `json:"deadline_at"`
	Metadata               map[string]string `json:"metadata,omitempty"`
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
	Output       string         `json:"output,omitempty"`
	ErrorCode    string         `json:"error_code,omitempty"`
	ErrorMessage string         `json:"error_message,omitempty"`
	NativeRunID  string         `json:"native_run_id,omitempty"`
	CompletedAt  time.Time      `json:"completed_at"`
}

type RunCancelPayload struct {
	Reason string `json:"reason"`
}

type ConfigRefreshPayload struct {
	MCPURL                 string `json:"mcp_url,omitempty"`
	NativeMCPServerName    string `json:"native_mcp_server_name,omitempty"`
	NativeMCPToolNamespace string `json:"native_mcp_tool_namespace,omitempty"`
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
