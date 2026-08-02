package bridge

import (
	"crypto/ed25519"
	"encoding/base64"
	"fmt"
	"os"
	stdruntime "runtime"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/personastack/personastack-connector/internal/buildinfo"
	"github.com/personastack/personastack-connector/internal/config"
	"github.com/personastack/personastack-connector/internal/externalagentprotocol"
	"github.com/personastack/personastack-connector/internal/runtime"
)

type Credential struct {
	ID         string
	PrivateKey ed25519.PrivateKey
	PublicKey  ed25519.PublicKey
}

func CredentialFromBinding(binding config.Binding) (Credential, error) {
	privateKeyRaw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(binding.BridgePrivateKey))
	if err != nil {
		return Credential{}, fmt.Errorf("decode bridge private key: %w", err)
	}
	publicKeyRaw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(binding.BridgePublicKey))
	if err != nil {
		return Credential{}, fmt.Errorf("decode bridge public key: %w", err)
	}
	return Credential{
		ID:         strings.TrimSpace(binding.BridgeCredentialID),
		PrivateKey: ed25519.PrivateKey(privateKeyRaw),
		PublicKey:  ed25519.PublicKey(publicKeyRaw),
	}, nil
}

type Session struct {
	Binding      config.Binding
	Credential   Credential
	ServiceScope externalagentprotocol.ServiceScope
	Now          func() time.Time
}

func NewSession(binding config.Binding, credential Credential) (Session, error) {
	if strings.TrimSpace(string(binding.ConnectionID)) == "" {
		return Session{}, fmt.Errorf("connection id required")
	}
	if strings.TrimSpace(string(binding.PersonaID)) == "" {
		return Session{}, fmt.Errorf("persona id required")
	}
	if strings.TrimSpace(credential.ID) == "" {
		return Session{}, fmt.Errorf("credential id required")
	}
	if len(credential.PrivateKey) != ed25519.PrivateKeySize {
		return Session{}, fmt.Errorf("ed25519 private key required")
	}
	if len(credential.PublicKey) != ed25519.PublicKeySize {
		return Session{}, fmt.Errorf("ed25519 public key required")
	}
	return Session{
		Binding:    binding,
		Credential: credential,
		Now: func() time.Time {
			return time.Now().UTC()
		},
	}, nil
}

func (s Session) ConnectFrame(nonce string) (externalagentprotocol.Frame, error) {
	now := s.now()
	protocolVersions := externalagentprotocol.SupportedProtocolVersions()
	connect := externalagentprotocol.ConnectPayload{
		ProtocolVersion:           protocolVersions[0],
		SupportedProtocolVersions: protocolVersions,
		ConnectorVersion:          buildinfo.VersionString(),
		RuntimeKind:               runtimeKindForAdapter(s.Binding.RuntimeKind),
		ConnectionGeneration:      s.Binding.ConnectionGeneration,
		Hostname:                  localHostname(),
		OS:                        stdruntime.GOOS,
		Arch:                      stdruntime.GOARCH,
		DevicePublicKey:           base64.StdEncoding.EncodeToString(s.Credential.PublicKey),
		CredentialID:              strings.TrimSpace(s.Credential.ID),
		CredentialProofNonce:      strings.TrimSpace(nonce),
		CredentialProofUnix:       now.Unix(),
		SupportsTargetClear:       true,
	}
	frame := s.baseFrame(externalagentprotocol.FrameTypeConnect, now)
	frame.Connect = &connect
	frame.Connect.CredentialProof = base64.StdEncoding.EncodeToString(ed25519.Sign(s.Credential.PrivateKey, []byte(CredentialProofMessage(frame))))
	return frame, nil
}

func (s Session) HeartbeatFrame(state runtime.AdapterState, lastWakeProbeAt *time.Time) externalagentprotocol.Frame {
	return s.HeartbeatFrameWithDiagnostic(state, "", lastWakeProbeAt)
}

func (s Session) HeartbeatFrameWithDetection(detection runtime.Detection, lastWakeProbeAt *time.Time) externalagentprotocol.Frame {
	return s.HeartbeatFrameWithDiagnostic(detection.State, detection.DiagnosticCode, lastWakeProbeAt)
}

func (s Session) HeartbeatFrameWithDetectionAndTarget(detection runtime.Detection, lastWakeProbeAt *time.Time, targetRevision int64, targetEpoch uint64) externalagentprotocol.Frame {
	frame := s.HeartbeatFrameWithDiagnostic(detection.State, detection.DiagnosticCode, lastWakeProbeAt)
	frame.Heartbeat.TargetSelectionRevision = targetRevision
	frame.Heartbeat.TargetEpoch = targetEpoch
	return frame
}

func (s Session) HeartbeatFrameWithDiagnostic(state runtime.AdapterState, diagnosticCode string, lastWakeProbeAt *time.Time) externalagentprotocol.Frame {
	frame := s.baseFrame(externalagentprotocol.FrameTypeHeartbeat, s.now())
	frame.Heartbeat = &externalagentprotocol.HeartbeatPayload{
		ConnectionStatus:       externalagentprotocol.ConnectionStatusBridgeConnected,
		ReadinessStatus:        readinessForAdapterState(state, lastWakeProbeAt),
		DiagnosticCode:         externalagentprotocol.DiagnosticCode(diagnosticCodeForAdapterState(state, diagnosticCode)),
		RuntimeKind:            runtimeKindForAdapter(s.Binding.RuntimeKind),
		ServiceScope:           s.serviceScope(),
		ConnectionGeneration:   s.Binding.ConnectionGeneration,
		Hostname:               localHostname(),
		NativeMCPServerName:    strings.TrimSpace(s.Binding.NativeMCPServer),
		NativeMCPToolNamespace: strings.TrimSpace(s.Binding.NativeMCPNamespace),
		NativeMCPToolPrefix:    nativeMCPToolPrefix(s.Binding.RuntimeKind, s.Binding.NativeMCPServer),
		NativeToolNamingRule:   nativeToolNamingRule(s.Binding.RuntimeKind),
		ConnectorVersion:       buildinfo.VersionString(),
		GitCommit:              buildinfo.GitCommitString(),
		OS:                     stdruntime.GOOS,
		Arch:                   stdruntime.GOARCH,
		ReleaseChannel:         buildinfo.ReleaseChannelString(),
		LastWakeProbeAt:        lastWakeProbeAt,
	}
	return frame
}

func (s Session) serviceScope() externalagentprotocol.ServiceScope {
	if s.ServiceScope != "" {
		return s.ServiceScope
	}
	return externalagentprotocol.ServiceScopeUserLaunchAgent
}

func localHostname() string {
	hostname, err := os.Hostname()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(hostname)
}

func (s Session) WakeProbeAcceptedFrame(probeID string) externalagentprotocol.Frame {
	frame := s.baseFrame(externalagentprotocol.FrameTypeWakeProbeAccepted, s.now())
	frame.WakeProbeAccepted = &externalagentprotocol.WakeProbeAcceptedPayload{
		ProbeID:     strings.TrimSpace(probeID),
		RuntimeKind: runtimeKindForAdapter(s.Binding.RuntimeKind),
		AcceptedAt:  frame.SentAt,
	}
	return frame
}

func (s Session) WakeProbeAcceptedFrameForRequest(request externalagentprotocol.Frame) externalagentprotocol.Frame {
	return s.WakeProbeAcceptedFrameForRequestWithTarget(request, 0, 0)
}

func (s Session) WakeProbeAcceptedFrameForRequestWithTarget(request externalagentprotocol.Frame, targetRevision int64, targetEpoch uint64) externalagentprotocol.Frame {
	probeID := ""
	if request.WakeProbe != nil {
		probeID = request.WakeProbe.ProbeID
	}
	frame := s.WakeProbeAcceptedFrame(probeID)
	frame.MessageID = strings.TrimSpace(request.MessageID)
	frame.WakeProbeAccepted.TargetSelectionRevision = targetRevision
	frame.WakeProbeAccepted.TargetEpoch = targetEpoch
	return frame
}

func (s Session) CapabilitiesFrame(
	capabilities []externalagentprotocol.CapabilityReport,
	nativeCapabilities []externalagentprotocol.NativeCapabilityReport,
	nativeDiscoveryStatus externalagentprotocol.NativeCapabilityDiscoveryStatus,
	nativeReportedSources []externalagentprotocol.NativeCapabilitySource,
) externalagentprotocol.Frame {
	frame := s.baseFrame(externalagentprotocol.FrameTypeCapabilitiesReport, s.now())
	frame.Capabilities = &externalagentprotocol.CapabilitiesPayload{
		ConnectionGeneration:            s.Binding.ConnectionGeneration,
		Capabilities:                    capabilities,
		NativeCapabilities:              nativeCapabilities,
		NativeCapabilityDiscoveryStatus: nativeDiscoveryStatus,
		NativeCapabilityReportedSources: nativeReportedSources,
	}
	return frame
}

func (s Session) RunAcceptedFrame(request externalagentprotocol.Frame, nativeRunID string) externalagentprotocol.Frame {
	frame := s.baseFrame(externalagentprotocol.FrameTypeRunAccepted, s.now())
	frame.MessageID = strings.TrimSpace(request.MessageID)
	frame.RunID = strings.TrimSpace(request.RunID)
	frame.AssignmentID = strings.TrimSpace(request.AssignmentID)
	frame.RunAccepted = &externalagentprotocol.RunAcceptedPayload{
		AcceptedAt:  frame.SentAt,
		NativeRunID: strings.TrimSpace(nativeRunID),
	}
	return frame
}

func (s Session) RunStartedFrame(request externalagentprotocol.Frame, nativeRunID string, startedAt time.Time) externalagentprotocol.Frame {
	frame := s.baseFrame(externalagentprotocol.FrameTypeRunStarted, s.now())
	frame.MessageID = strings.TrimSpace(request.MessageID)
	frame.RunID = strings.TrimSpace(request.RunID)
	frame.AssignmentID = strings.TrimSpace(request.AssignmentID)
	if startedAt.IsZero() {
		startedAt = frame.SentAt
	}
	frame.RunStarted = &externalagentprotocol.RunStartedPayload{
		StartedAt:   startedAt.UTC(),
		NativeRunID: strings.TrimSpace(nativeRunID),
	}
	return frame
}

func (s Session) RunOutputDeltaFrame(request externalagentprotocol.Frame, delta string) externalagentprotocol.Frame {
	frame := s.baseFrame(externalagentprotocol.FrameTypeRunOutputDelta, s.now())
	frame.MessageID = strings.TrimSpace(request.MessageID)
	frame.RunID = strings.TrimSpace(request.RunID)
	frame.AssignmentID = strings.TrimSpace(request.AssignmentID)
	frame.RunOutputDelta = &externalagentprotocol.RunOutputDeltaPayload{
		Delta: strings.TrimSpace(delta),
	}
	return frame
}

func (s Session) RunToolEventFrame(request externalagentprotocol.Frame, toolName string, phase string, summary string) externalagentprotocol.Frame {
	frame := s.baseFrame(externalagentprotocol.FrameTypeRunToolEvent, s.now())
	frame.MessageID = strings.TrimSpace(request.MessageID)
	frame.RunID = strings.TrimSpace(request.RunID)
	frame.AssignmentID = strings.TrimSpace(request.AssignmentID)
	frame.RunToolEvent = &externalagentprotocol.RunToolEventPayload{
		ToolName: strings.TrimSpace(toolName),
		Phase:    strings.TrimSpace(phase),
		Summary:  strings.TrimSpace(summary),
	}
	return frame
}

func (s Session) RunTerminalFrame(request externalagentprotocol.Frame, status externalagentprotocol.RunStatus, reason externalagentprotocol.TerminalReason, output string) externalagentprotocol.Frame {
	frame := s.baseFrame(externalagentprotocol.FrameTypeRunCompleted, s.now())
	frame.MessageID = strings.TrimSpace(request.MessageID)
	if status == externalagentprotocol.RunStatusFailed {
		frame.MessageType = externalagentprotocol.FrameTypeRunFailed
	}
	if status == externalagentprotocol.RunStatusCancelled {
		frame.MessageType = externalagentprotocol.FrameTypeRunCancelled
	}
	frame.RunID = strings.TrimSpace(request.RunID)
	frame.AssignmentID = strings.TrimSpace(request.AssignmentID)
	frame.RunTerminal = &externalagentprotocol.RunTerminalPayload{
		Status:       status,
		Reason:       reason,
		FinalMessage: strings.TrimSpace(output),
		CompletedAt:  frame.SentAt,
	}
	return frame
}

func CredentialProofMessage(frame externalagentprotocol.Frame) string {
	connect := frame.Connect
	return strings.Join([]string{
		strings.TrimSpace(connect.CredentialID),
		strings.TrimSpace(frame.ConnectionID),
		strings.TrimSpace(frame.PersonaID),
		strings.TrimSpace(connect.CredentialProofNonce),
		fmt.Sprintf("%d", connect.CredentialProofUnix),
	}, "\n")
}

func (s Session) baseFrame(kind externalagentprotocol.FrameType, now time.Time) externalagentprotocol.Frame {
	return externalagentprotocol.Frame{
		MessageID:            uuid.NewString(),
		MessageType:          kind,
		PersonaID:            string(s.Binding.PersonaID),
		ConnectionID:         string(s.Binding.ConnectionID),
		ConnectionGeneration: s.Binding.ConnectionGeneration,
		SentAt:               now.UTC(),
	}
}

func (s Session) now() time.Time {
	if s.Now == nil {
		return time.Now().UTC()
	}
	return s.Now().UTC()
}

func runtimeKindForAdapter(kind runtime.AdapterKind) externalagentprotocol.RuntimeKind {
	switch kind {
	case runtime.AdapterKindOpenClaw:
		return externalagentprotocol.RuntimeKindOpenClaw
	default:
		return externalagentprotocol.RuntimeKindHermes
	}
}

func nativeToolNamingRule(kind runtime.AdapterKind) externalagentprotocol.NativeToolNamingRule {
	switch kind {
	case runtime.AdapterKindHermes:
		return externalagentprotocol.NativeToolNamingRuleMCPServerPrefix
	case runtime.AdapterKindOpenClaw:
		return externalagentprotocol.NativeToolNamingRuleRuntimeEffectiveCatalog
	default:
		return externalagentprotocol.NativeToolNamingRuleUnknown
	}
}

func nativeMCPToolPrefix(kind runtime.AdapterKind, serverName string) string {
	if kind != runtime.AdapterKindHermes {
		return ""
	}
	trimmed := strings.TrimSpace(serverName)
	if trimmed == "" {
		return ""
	}
	return "mcp_" + trimmed + "_"
}

func readinessForAdapterState(state runtime.AdapterState, lastWakeProbeAt *time.Time) externalagentprotocol.ReadinessStatus {
	switch state {
	case runtime.AdapterStateReady:
		return externalagentprotocol.ReadinessStatusWakeable
	case runtime.AdapterStateMCPVerified:
		if lastWakeProbeAt != nil && !lastWakeProbeAt.IsZero() {
			return externalagentprotocol.ReadinessStatusWakeable
		}
		return externalagentprotocol.ReadinessStatusMCPConfigured
	case runtime.AdapterStateRuntimeStopped, runtime.AdapterStateRuntimeMissing, runtime.AdapterStateAuthMissing, runtime.AdapterStateCapabilityMissing, runtime.AdapterStateWakeProbeFailed:
		return externalagentprotocol.ReadinessStatusRuntimeError
	default:
		return externalagentprotocol.ReadinessStatusRuntimeHealthy
	}
}

func diagnosticCodeForAdapterState(state runtime.AdapterState, diagnosticCode string) string {
	trimmed := strings.TrimSpace(diagnosticCode)
	if trimmed != "" {
		return trimmed
	}
	switch state {
	case runtime.AdapterStateRuntimeMissing:
		return "runtime_missing"
	case runtime.AdapterStateRuntimeStopped:
		return "runtime_stopped"
	case runtime.AdapterStateAuthMissing:
		return "auth_missing"
	case runtime.AdapterStateCapabilityMissing:
		return "capability_missing"
	case runtime.AdapterStateMCPConfigMissing:
		return "mcp_config_missing"
	case runtime.AdapterStateMCPRestartRequired:
		return "mcp_restart_required"
	case runtime.AdapterStateWakeProbeFailed:
		return "wake_probe_failed"
	case runtime.AdapterStateReady, runtime.AdapterStateMCPVerified:
		return ""
	case runtime.AdapterStateTargetSelectionRequired:
		return "target_selection_required"
	default:
		return "runtime_error"
	}
}
