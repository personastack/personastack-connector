package bridge

import (
	"crypto/ed25519"
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/personastack/agent-gateway/pkg/externalagentprotocol"
	"github.com/personastack/personastack-connector/internal/config"
	"github.com/personastack/personastack-connector/internal/runtime"
)

const connectorVersion = "0.1.0-dev"

type Credential struct {
	ID         string
	PrivateKey ed25519.PrivateKey
	PublicKey  ed25519.PublicKey
}

type Session struct {
	Binding    config.Binding
	Credential Credential
	Now        func() time.Time
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
	connect := externalagentprotocol.ConnectPayload{
		ProtocolVersion:      externalagentprotocol.ProtocolVersionV1,
		ConnectorVersion:     connectorVersion,
		RuntimeKind:          runtimeKindForAdapter(s.Binding.RuntimeKind),
		ConnectionGeneration: s.Binding.ConnectionGeneration,
		DevicePublicKey:      base64.StdEncoding.EncodeToString(s.Credential.PublicKey),
		CredentialID:         strings.TrimSpace(s.Credential.ID),
		CredentialProofNonce: strings.TrimSpace(nonce),
		CredentialProofUnix:  now.Unix(),
	}
	frame := s.baseFrame(externalagentprotocol.FrameTypeConnect, now)
	frame.Connect = &connect
	frame.Connect.CredentialProof = base64.StdEncoding.EncodeToString(ed25519.Sign(s.Credential.PrivateKey, []byte(CredentialProofMessage(frame))))
	return frame, nil
}

func (s Session) HeartbeatFrame(state runtime.AdapterState, lastWakeProbeAt *time.Time) externalagentprotocol.Frame {
	frame := s.baseFrame(externalagentprotocol.FrameTypeHeartbeat, s.now())
	frame.Heartbeat = &externalagentprotocol.HeartbeatPayload{
		ConnectionStatus: externalagentprotocol.ConnectionStatusBridgeConnected,
		ReadinessStatus:  readinessForAdapterState(state),
		RuntimeKind:      runtimeKindForAdapter(s.Binding.RuntimeKind),
		ConnectorVersion: connectorVersion,
		LastWakeProbeAt:  lastWakeProbeAt,
	}
	return frame
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

func (s Session) RunTerminalFrame(request externalagentprotocol.Frame, status externalagentprotocol.RunStatus, reason externalagentprotocol.TerminalReason, output string) externalagentprotocol.Frame {
	frame := s.baseFrame(externalagentprotocol.FrameTypeRunCompleted, s.now())
	if status == externalagentprotocol.RunStatusFailed {
		frame.MessageType = externalagentprotocol.FrameTypeRunFailed
	}
	if status == externalagentprotocol.RunStatusCancelled {
		frame.MessageType = externalagentprotocol.FrameTypeRunCancelled
	}
	frame.RunID = strings.TrimSpace(request.RunID)
	frame.AssignmentID = strings.TrimSpace(request.AssignmentID)
	frame.RunTerminal = &externalagentprotocol.RunTerminalPayload{
		Status:      status,
		Reason:      reason,
		Output:      strings.TrimSpace(output),
		CompletedAt: frame.SentAt,
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
		MessageID:    uuid.NewString(),
		MessageType:  kind,
		PersonaID:    string(s.Binding.PersonaID),
		ConnectionID: string(s.Binding.ConnectionID),
		SentAt:       now.UTC(),
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

func readinessForAdapterState(state runtime.AdapterState) externalagentprotocol.ReadinessStatus {
	switch state {
	case runtime.AdapterStateReady:
		return externalagentprotocol.ReadinessStatusWakeable
	case runtime.AdapterStateMCPVerified:
		return externalagentprotocol.ReadinessStatusMCPConfigured
	case runtime.AdapterStateRuntimeStopped, runtime.AdapterStateRuntimeMissing, runtime.AdapterStateAuthMissing, runtime.AdapterStateCapabilityMissing, runtime.AdapterStateWakeProbeFailed:
		return externalagentprotocol.ReadinessStatusRuntimeError
	default:
		return externalagentprotocol.ReadinessStatusRuntimeHealthy
	}
}
