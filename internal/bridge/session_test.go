package bridge

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	stdruntime "runtime"
	"testing"
	"time"

	"github.com/personastack/personastack-connector/internal/buildinfo"
	"github.com/personastack/personastack-connector/internal/config"
	"github.com/personastack/personastack-connector/internal/externalagentprotocol"
	"github.com/personastack/personastack-connector/internal/runtime"
)

func TestConnectFrameSignsAPIVerifiableMessage(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	session := testSession(t, publicKey, privateKey)
	session.Now = func() time.Time {
		return time.Unix(100, 0).UTC()
	}

	frame, err := session.ConnectFrame("nonce-1")
	if err != nil {
		t.Fatalf("connect frame: %v", err)
	}
	signature, err := base64.StdEncoding.DecodeString(frame.Connect.CredentialProof)
	if err != nil {
		t.Fatalf("decode signature: %v", err)
	}
	if !ed25519.Verify(publicKey, []byte(CredentialProofMessage(frame)), signature) {
		t.Fatal("signature did not verify")
	}
	if frame.Connect.ProtocolVersion != externalagentprotocol.ProtocolVersionV2 {
		t.Fatalf("protocol version: got=%s", frame.Connect.ProtocolVersion)
	}
	if len(frame.Connect.SupportedProtocolVersions) != 1 || frame.Connect.SupportedProtocolVersions[0] != externalagentprotocol.ProtocolVersionV2 {
		t.Fatalf("supported protocol versions: %+v", frame.Connect.SupportedProtocolVersions)
	}
	if frame.Connect.ConnectorVersion != buildinfo.VersionString() {
		t.Fatalf("connector version: got=%s want=%s", frame.Connect.ConnectorVersion, buildinfo.VersionString())
	}
	if frame.Connect.OS != stdruntime.GOOS || frame.Connect.Arch != stdruntime.GOARCH {
		t.Fatalf("connect platform metadata: got=%s/%s want=%s/%s", frame.Connect.OS, frame.Connect.Arch, stdruntime.GOOS, stdruntime.GOARCH)
	}
}

func TestRunAcceptedFrameCorrelatesRequestMessageID(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	session := testSession(t, publicKey, privateKey)
	request := externalagentprotocol.Frame{
		MessageID:    "request-1",
		RunID:        "run-1",
		AssignmentID: "assignment-1",
	}

	frame := session.RunAcceptedFrame(request, "native-1")
	if frame.MessageID != "request-1" || frame.RunAccepted.NativeRunID != "native-1" {
		t.Fatalf("unexpected frame: %+v", frame)
	}
}

func TestRunFrameBuildersCarryEventPayloads(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	session := testSession(t, publicKey, privateKey)
	request := externalagentprotocol.Frame{
		MessageID:    "request-1",
		RunID:        "run-1",
		AssignmentID: "assignment-1",
	}

	startedAt := time.Unix(123, 0).UTC()
	started := session.RunStartedFrame(request, "native-1", startedAt)
	if started.RunStarted == nil || !started.RunStarted.StartedAt.Equal(startedAt) || started.RunStarted.NativeRunID != "native-1" {
		t.Fatalf("unexpected started frame: %+v", started)
	}

	startedDefault := session.RunStartedFrame(request, "native-2", time.Time{})
	if startedDefault.RunStarted == nil || startedDefault.RunStarted.StartedAt.IsZero() {
		t.Fatalf("expected default started frame timestamp, got %+v", startedDefault)
	}

	delta := session.RunOutputDeltaFrame(request, " chunk ")
	if delta.RunOutputDelta == nil || delta.RunOutputDelta.Delta != "chunk" {
		t.Fatalf("unexpected delta frame: %+v", delta)
	}

	tool := session.RunToolEventFrame(request, " browser ", " started ", " opening ")
	if tool.RunToolEvent == nil || tool.RunToolEvent.ToolName != "browser" || tool.RunToolEvent.Phase != "started" || tool.RunToolEvent.Summary != "opening" {
		t.Fatalf("unexpected tool event frame: %+v", tool)
	}
}

func TestHeartbeatFrameReportsBuildMetadata(t *testing.T) {
	oldVersion := buildinfo.Version
	oldCommit := buildinfo.GitCommit
	oldChannel := buildinfo.ReleaseChannel
	buildinfo.Version = "v1.2.3"
	buildinfo.GitCommit = "abc123"
	buildinfo.ReleaseChannel = "test"
	t.Cleanup(func() {
		buildinfo.Version = oldVersion
		buildinfo.GitCommit = oldCommit
		buildinfo.ReleaseChannel = oldChannel
	})
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	session := testSession(t, publicKey, privateKey)

	frame := session.HeartbeatFrame(runtime.AdapterStateReady, nil)
	if frame.Heartbeat.ConnectionGeneration != 5 || frame.Heartbeat.ConnectorVersion != "v1.2.3" || frame.Heartbeat.GitCommit != "abc123" || frame.Heartbeat.ReleaseChannel != "test" {
		t.Fatalf("unexpected heartbeat metadata: %+v", frame.Heartbeat)
	}
	if frame.Heartbeat.NativeMCPServerName != "personastack-conn-1" || frame.Heartbeat.NativeMCPToolPrefix != "mcp_personastack-conn-1_" || frame.Heartbeat.NativeToolNamingRule != externalagentprotocol.NativeToolNamingRuleMCPServerPrefix {
		t.Fatalf("unexpected native mcp metadata: %+v", frame.Heartbeat)
	}
	if frame.Heartbeat.OS == "" || frame.Heartbeat.Arch == "" {
		t.Fatalf("expected os/arch metadata: %+v", frame.Heartbeat)
	}
	if frame.Heartbeat.OS != stdruntime.GOOS || frame.Heartbeat.Arch != stdruntime.GOARCH {
		t.Fatalf("heartbeat platform metadata: got=%s/%s want=%s/%s", frame.Heartbeat.OS, frame.Heartbeat.Arch, stdruntime.GOOS, stdruntime.GOARCH)
	}
}

func TestHeartbeatFrameRequiresWakeProbeForMCPVerifiedWakeable(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	session := testSession(t, publicKey, privateKey)

	withoutProbe := session.HeartbeatFrame(runtime.AdapterStateMCPVerified, nil)
	if withoutProbe.Heartbeat.ReadinessStatus == externalagentprotocol.ReadinessStatusWakeable {
		t.Fatalf("heartbeat reported wakeable without wake probe: %+v", withoutProbe.Heartbeat)
	}

	probedAt := time.Now().UTC()
	withProbe := session.HeartbeatFrame(runtime.AdapterStateMCPVerified, &probedAt)
	if withProbe.Heartbeat.ReadinessStatus != externalagentprotocol.ReadinessStatusWakeable {
		t.Fatalf("heartbeat did not report wakeable after wake probe: %+v", withProbe.Heartbeat)
	}
}

func TestHeartbeatFrameReportsDiagnosticCode(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	session := testSession(t, publicKey, privateKey)
	frame := session.HeartbeatFrameWithDetection(runtime.Detection{
		State:          runtime.AdapterStateMCPConfigMissing,
		DiagnosticCode: "mcp_config_parse_error",
	}, nil)
	if frame.Heartbeat.DiagnosticCode != "mcp_config_parse_error" {
		t.Fatalf("diagnostic code = %q", frame.Heartbeat.DiagnosticCode)
	}
}

func TestHeartbeatFrameReportsServiceScope(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	session := testSession(t, publicKey, privateKey)
	session.ServiceScope = externalagentprotocol.ServiceScopeSystemLaunchDaemon
	frame := session.HeartbeatFrame(runtime.AdapterStateReady, nil)
	if frame.Heartbeat.ServiceScope != externalagentprotocol.ServiceScopeSystemLaunchDaemon {
		t.Fatalf("service scope = %q", frame.Heartbeat.ServiceScope)
	}
}

func testSession(t *testing.T, publicKey ed25519.PublicKey, privateKey ed25519.PrivateKey) Session {
	t.Helper()
	session, err := NewSession(config.Binding{
		ConnectionID:         "conn-1",
		PersonaID:            "persona-1",
		ConnectionGeneration: 5,
		RuntimeKind:          runtime.AdapterKindHermes,
		NativeMCPServer:      "personastack-conn-1",
		NativeMCPNamespace:   "personastack",
	}, Credential{
		ID:         "cred-1",
		PrivateKey: privateKey,
		PublicKey:  publicKey,
	})
	if err != nil {
		t.Fatalf("new session: %v", err)
	}
	return session
}
