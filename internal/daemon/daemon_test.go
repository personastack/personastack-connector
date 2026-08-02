package daemon

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/personastack/personastack-connector/internal/bridge"
	"github.com/personastack/personastack-connector/internal/config"
	"github.com/personastack/personastack-connector/internal/externalagentprotocol"
	"github.com/personastack/personastack-connector/internal/mcp"
	"github.com/personastack/personastack-connector/internal/runtime"
	"github.com/zalando/go-keyring"
)

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "personastack-connector-daemon-test-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "create temp dir: %v\n", err)
		os.Exit(1)
	}
	hermesPath := filepath.Join(dir, "hermes")
	if err := os.WriteFile(hermesPath, []byte("#!/bin/sh\nprintf 'enabled computer_use Computer Use\\nMCP servers:\\n  personastack-conn-1  all tools enabled\\n'\n"), 0o700); err != nil {
		fmt.Fprintf(os.Stderr, "write hermes stub: %v\n", err)
		os.Exit(1)
	}
	if err := os.Setenv("HERMES_BIN", hermesPath); err != nil {
		fmt.Fprintf(os.Stderr, "set HERMES_BIN: %v\n", err)
		os.Exit(1)
	}
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

func readFrameOfType(t *testing.T, conn *websocket.Conn, want externalagentprotocol.FrameType) externalagentprotocol.Frame {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if err := conn.SetReadDeadline(deadline); err != nil {
			t.Fatalf("set read deadline: %v", err)
		}
		var frame externalagentprotocol.Frame
		if err := conn.ReadJSON(&frame); err != nil {
			t.Fatalf("read %s frame: %v", want, err)
		}
		if frame.MessageType == want {
			return frame
		}
		if frame.MessageType == externalagentprotocol.FrameTypeHeartbeat || frame.MessageType == externalagentprotocol.FrameTypeCapabilitiesReport {
			continue
		}
		t.Fatalf("expected %s frame, got %+v", want, frame)
	}
}

func TestRunnerAdapterForBindingDoesNotReuseHermesHome(t *testing.T) {
	homeDir := t.TempDir()
	hermesHome := filepath.Join(t.TempDir(), "hermes-profile")
	if err := os.MkdirAll(hermesHome, 0o700); err != nil {
		t.Fatalf("create Hermes home: %v", err)
	}
	if err := os.WriteFile(filepath.Join(hermesHome, ".env"), []byte("API_SERVER_KEY=profile-key\n"), 0o600); err != nil {
		t.Fatalf("write Hermes env: %v", err)
	}
	t.Setenv("HOME", homeDir)
	t.Setenv("PERSONASTACK_CONNECTOR_FORCE_SECRET_FALLBACK", "1")
	t.Setenv("HERMES_API_SERVER_KEY", "")

	runner := Runner{}
	adapter, ok := runner.adapterForBinding(config.Binding{
		RuntimeKind: runtime.AdapterKindHermes,
		HermesHome:  hermesHome,
	}).(runtime.HermesAdapter)
	if !ok {
		t.Fatalf("adapter type = %T, want runtime.HermesAdapter", adapter)
	}
	if adapter.APIKey == "profile-key" {
		t.Fatalf("adapter reused legacy selected profile key")
	}
}

func TestRunnerStaysAliveWhenLoopbackProxyCannotBind(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	binding := config.Binding{
		ConnectionID:          "conn-1",
		PersonaID:             "persona-1",
		LocalMCPProxyURL:      "http://" + listener.Addr().String() + "/mcp/conn-1",
		LocalMCPProxyToken:    "local-token",
		HasLocalMCPProxyToken: true,
		PersonaMCPURL:         "http://127.0.0.1:1/mcp",
		PersonaMCPToken:       "persona-token",
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- (Runner{
			Store:        config.NewMemoryStore(config.State{Bindings: []config.Binding{binding}}),
			ReconnectMin: 5 * time.Millisecond,
			ReconnectMax: 5 * time.Millisecond,
		}).RunForeground(ctx)
	}()
	select {
	case err := <-errCh:
		t.Fatalf("RunForeground() returned before cancellation: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("RunForeground() after cancellation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for runner shutdown")
	}
}

func TestRunnerStaysAliveWithNoBindings(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	errCh := make(chan error, 1)
	go func() {
		errCh <- (Runner{
			Store:        config.NewFileStore(t.TempDir() + "/state.json"),
			ReconnectMin: 5 * time.Millisecond,
			ReconnectMax: 5 * time.Millisecond,
		}).RunForeground(ctx)
	}()

	select {
	case err := <-errCh:
		t.Fatalf("RunForeground() returned before cancellation: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("RunForeground() after cancellation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for runner shutdown")
	}
}

func TestRunnerStartsBindingSavedAfterIdle(t *testing.T) {
	t.Setenv("PERSONASTACK_CONNECTOR_FORCE_SECRET_FALLBACK", "1")
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	seenHeartbeat := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()
		var connectFrame externalagentprotocol.Frame
		if err := conn.ReadJSON(&connectFrame); err != nil {
			t.Fatalf("read connect: %v", err)
		}
		if connectFrame.MessageType != externalagentprotocol.FrameTypeConnect || connectFrame.ConnectionID != "conn-1" {
			t.Fatalf("unexpected connect frame: %+v", connectFrame)
		}
		err = conn.WriteJSON(externalagentprotocol.Frame{
			MessageType:  externalagentprotocol.FrameTypeConnectAccepted,
			PersonaID:    "persona-1",
			ConnectionID: "conn-1",
			SentAt:       time.Now().UTC(),
			ConnectAccepted: &externalagentprotocol.ConnectAcceptedPayload{
				ProtocolVersion:      externalagentprotocol.ProtocolVersionV4,
				ConnectionGeneration: connectFrame.Connect.ConnectionGeneration,
				HeartbeatSeconds:     15,
			},
		})
		if err != nil {
			t.Fatalf("write connect accepted: %v", err)
		}
		var heartbeat externalagentprotocol.Frame
		if err := conn.ReadJSON(&heartbeat); err != nil {
			if ctx.Err() != nil {
				return
			}
			t.Fatalf("read heartbeat: %v", err)
		}
		if heartbeat.MessageType != externalagentprotocol.FrameTypeHeartbeat {
			t.Fatalf("unexpected heartbeat: %+v", heartbeat)
		}
		seenHeartbeat <- struct{}{}
		cancel()
	}))
	defer server.Close()

	store := config.NewFileStore(t.TempDir() + "/state.json")
	errCh := make(chan error, 1)
	go func() {
		errCh <- (Runner{Store: store, ReconnectMin: 5 * time.Millisecond, ReconnectMax: 5 * time.Millisecond}).RunForeground(ctx)
	}()

	select {
	case err := <-errCh:
		t.Fatalf("RunForeground() returned before SaveBinding: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	err = store.SaveBinding(config.Binding{
		ConnectionID:        "conn-1",
		PersonaID:           "persona-1",
		GatewayWebsocketURL: "ws" + server.URL[len("http"):],
		BridgeCredentialID:  "cred-1",
		BridgePrivateKey:    base64.StdEncoding.EncodeToString(privateKey),
		BridgePublicKey:     base64.StdEncoding.EncodeToString(publicKey),
		RuntimeKind:         runtime.AdapterKindHermes,
	})
	if err != nil {
		t.Fatalf("save binding: %v", err)
	}

	select {
	case <-seenHeartbeat:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for saved binding to start")
	}
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("RunForeground() after saved binding: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for runner shutdown")
	}
}

func TestRunnerCancelsRemovedBindingAndStartsReplacement(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	connected := make(chan string, 4)
	releasedConn1 := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()
		var connectFrame externalagentprotocol.Frame
		if err := conn.ReadJSON(&connectFrame); err != nil {
			t.Fatalf("read connect: %v", err)
		}
		connectionID := connectFrame.ConnectionID
		err = conn.WriteJSON(externalagentprotocol.Frame{
			MessageType:  externalagentprotocol.FrameTypeConnectAccepted,
			PersonaID:    connectFrame.PersonaID,
			ConnectionID: connectionID,
			SentAt:       time.Now().UTC(),
			ConnectAccepted: &externalagentprotocol.ConnectAcceptedPayload{
				ProtocolVersion:      externalagentprotocol.ProtocolVersionV4,
				ConnectionGeneration: connectFrame.Connect.ConnectionGeneration,
				HeartbeatSeconds:     15,
			},
		})
		if err != nil {
			t.Fatalf("write connect accepted: %v", err)
		}
		var heartbeat externalagentprotocol.Frame
		if err := conn.ReadJSON(&heartbeat); err != nil {
			if connectionID == "conn-1" {
				releasedConn1 <- struct{}{}
			}
			t.Fatalf("read heartbeat: %v", err)
		}
		connected <- connectionID
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				if connectionID == "conn-1" {
					releasedConn1 <- struct{}{}
				}
				return
			}
		}
	}))
	defer server.Close()

	store := config.NewFileStore(t.TempDir() + "/state.json")
	err = store.SaveBinding(config.Binding{
		ConnectionID:        "conn-1",
		PersonaID:           "persona-1",
		GatewayWebsocketURL: "ws" + server.URL[len("http"):],
		BridgeCredentialID:  "cred-1",
		BridgePrivateKey:    base64.StdEncoding.EncodeToString(privateKey),
		BridgePublicKey:     base64.StdEncoding.EncodeToString(publicKey),
		RuntimeKind:         runtime.AdapterKindHermes,
	})
	if err != nil {
		t.Fatalf("save first binding: %v", err)
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- (Runner{Store: store, ReconnectMin: 5 * time.Millisecond, ReconnectMax: 5 * time.Millisecond}).RunForeground(ctx)
	}()

	select {
	case got := <-connected:
		if got != "conn-1" {
			t.Fatalf("first connection = %s, want conn-1", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first binding")
	}
	err = store.SaveBinding(config.Binding{
		ConnectionID:        "conn-2",
		PersonaID:           "persona-2",
		GatewayWebsocketURL: "ws" + server.URL[len("http"):],
		BridgeCredentialID:  "cred-1",
		BridgePrivateKey:    base64.StdEncoding.EncodeToString(privateKey),
		BridgePublicKey:     base64.StdEncoding.EncodeToString(publicKey),
		RuntimeKind:         runtime.AdapterKindHermes,
	})
	if err != nil {
		t.Fatalf("save replacement binding: %v", err)
	}
	select {
	case <-releasedConn1:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for old binding cancellation")
	}
	select {
	case got := <-connected:
		if got != "conn-2" {
			t.Fatalf("replacement connection = %s, want conn-2", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for replacement binding")
	}
	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("RunForeground() after cancellation: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for runner shutdown")
	}
}

func TestRunnerConfigRefreshRequiresAPITarget(t *testing.T) {
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)
	if err := os.MkdirAll(filepath.Join(homeDir, ".openclaw"), 0o700); err != nil {
		t.Fatalf("mkdir openclaw: %v", err)
	}
	if err := os.WriteFile(filepath.Join(homeDir, ".openclaw", "openclaw.json"), []byte("{invalid json"), 0o600); err != nil {
		t.Fatalf("write invalid openclaw config: %v", err)
	}
	stale := config.Binding{
		ConnectionID: "conn-1",
		PersonaID:    "persona-1",
		RuntimeKind:  runtime.AdapterKindOpenClaw,
	}
	latest := stale
	latest.LocalMCPProxyURL = "http://127.0.0.1:23119/mcp/conn-1"
	latest.LocalMCPProxyToken = "local-token"
	latest.HasLocalMCPProxyToken = true
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{latest}})

	err := (Runner{Store: &store}).refreshMCPConfig(stale)
	if err == nil || !strings.Contains(err.Error(), "runtime target required") {
		t.Fatalf("refreshMCPConfig() error = %v, want target requirement", err)
	}
	stored, ok := store.Binding("conn-1")
	if !ok {
		t.Fatal("binding missing")
	}
	if stored.LocalMCPProxyURL != latest.LocalMCPProxyURL || stored.LocalMCPProxyToken != latest.LocalMCPProxyToken {
		t.Fatalf("loopback proxy changed: %+v", stored)
	}
	raw, err := os.ReadFile(filepath.Join(homeDir, ".openclaw", "openclaw.json"))
	if err != nil {
		t.Fatalf("read openclaw config: %v", err)
	}
	if string(raw) != "{invalid json" {
		t.Fatalf("refresh changed config without a selected target:\n%s", string(raw))
	}
}

func TestRunnerDefersRuntimeTargetRefreshUntilActiveRunAcknowledged(t *testing.T) {
	binding := config.Binding{ConnectionID: "conn-1", ConnectionGeneration: 7, ActiveRunID: "run-1"}
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{binding}})
	runner := Runner{Store: store}
	if !runner.bindingHasActiveRun(binding) {
		t.Fatal("bindingHasActiveRun() = false, want true")
	}
	if got := runner.activeRunID(binding); got != "run-1" {
		t.Fatalf("activeRunID() = %q, want run-1", got)
	}
	if runner.bindingHasActiveRun(config.Binding{ConnectionID: "conn-1", ConnectionGeneration: 8}) {
		t.Fatal("bindingHasActiveRun() read an active run from a different connection generation")
	}
	target := &externalagentprotocol.RuntimeTarget{AccountCandidateID: "account-1", ProfileCandidateID: "profile-1", RuntimeKind: externalagentprotocol.RuntimeKindHermes, SelectionRevision: 1}
	copy := cloneRuntimeTarget(target)
	copy.AccountCandidateID = "account-2"
	if target.AccountCandidateID != "account-1" {
		t.Fatalf("cloneRuntimeTarget() mutated original target: %+v", target)
	}
}

func TestWriteCapabilitiesFrameKeepsNativeCapabilitiesUnknownOnDiscoveryError(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	session, err := bridge.NewSession(config.Binding{
		ConnectionID: "conn-1",
		PersonaID:    "persona-1",
		RuntimeKind:  runtime.AdapterKindHermes,
	}, bridge.Credential{
		ID:         "cred-1",
		PrivateKey: privateKey,
		PublicKey:  publicKey,
	})
	if err != nil {
		t.Fatalf("new session: %v", err)
	}
	var got externalagentprotocol.Frame
	err = (Runner{}).writeCapabilitiesFrame(
		context.Background(),
		session,
		failingCapabilityAdapter{},
		config.Binding{NativeMCPServer: "personastack-conn-1"},
		runtime.Detection{Kind: runtime.AdapterKindHermes, State: runtime.AdapterStateReady},
		func(frame externalagentprotocol.Frame) error {
			got = frame
			return nil
		},
	)
	if err != nil {
		t.Fatalf("write capabilities frame: %v", err)
	}
	if got.Capabilities == nil {
		t.Fatal("missing capabilities payload")
	}
	if len(got.Capabilities.Capabilities) != 1 {
		t.Fatalf("expected runtime capability report, got %#v", got.Capabilities.Capabilities)
	}
	if len(got.Capabilities.NativeCapabilities) != 0 {
		t.Fatalf("expected no native capabilities on discovery error, got %#v", got.Capabilities.NativeCapabilities)
	}
	if got.Capabilities.NativeCapabilityDiscoveryStatus != externalagentprotocol.NativeCapabilityDiscoveryFailed {
		t.Fatalf("expected failed discovery status, got %s", got.Capabilities.NativeCapabilityDiscoveryStatus)
	}
	if len(got.Capabilities.NativeCapabilityReportedSources) != 0 {
		t.Fatalf("expected no reported native sources on discovery error, got %#v", got.Capabilities.NativeCapabilityReportedSources)
	}
}

func TestNativeCapabilityFingerprintIgnoresReportOrderAndTime(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	first := []externalagentprotocol.NativeCapabilityReport{
		{
			Source:       externalagentprotocol.NativeCapabilitySourceHermesToolsList,
			Kind:         externalagentprotocol.NativeCapabilityKindNativeTool,
			CapabilityID: "shell",
			Label:        "Shell",
			Summary:      "Shell",
			Status:       externalagentprotocol.ReadinessStatusWakeable,
			ReportedAt:   now,
		},
		{
			Source:       externalagentprotocol.NativeCapabilitySourceOpenClawReadySkills,
			Kind:         externalagentprotocol.NativeCapabilityKindSkill,
			CapabilityID: "github",
			Label:        "GitHub",
			Summary:      "GitHub",
			Status:       externalagentprotocol.ReadinessStatusWakeable,
			ReportedAt:   now,
		},
	}
	second := []externalagentprotocol.NativeCapabilityReport{first[1], first[0]}
	second[0].ReportedAt = now.Add(time.Minute)
	second[1].ReportedAt = now.Add(time.Minute)
	if nativeCapabilityFingerprint(first) != nativeCapabilityFingerprint(second) {
		t.Fatalf("fingerprint changed for equivalent capability set")
	}
	second[1].CapabilityID = "git"
	if nativeCapabilityFingerprint(first) == nativeCapabilityFingerprint(second) {
		t.Fatalf("fingerprint did not change after capability id changed")
	}
}

func TestCapabilityFrameFingerprintIncludesPartialNativeCapabilities(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	capabilities := []externalagentprotocol.CapabilityReport{{
		Kind:       externalagentprotocol.CapabilityKindRuntimeHealth,
		Status:     externalagentprotocol.ReadinessStatusWakeable,
		ReportedAt: now,
	}}
	first := []externalagentprotocol.NativeCapabilityReport{{
		Source:       externalagentprotocol.NativeCapabilitySourceHermesRuntimeAPI,
		Kind:         externalagentprotocol.NativeCapabilityKindRuntimeFeature,
		CapabilityID: "run_submission",
		Label:        "Task delegation",
		Summary:      "can accept delegated tasks",
		Status:       externalagentprotocol.ReadinessStatusWakeable,
		ReportedAt:   now,
	}}
	second := append([]externalagentprotocol.NativeCapabilityReport(nil), first...)
	second[0].CapabilityID = "run_status"
	sources := []externalagentprotocol.NativeCapabilitySource{externalagentprotocol.NativeCapabilitySourceHermesRuntimeAPI}
	firstFingerprint := capabilityFrameFingerprint(capabilities, first, externalagentprotocol.NativeCapabilityDiscoveryPartial, sources)
	secondFingerprint := capabilityFrameFingerprint(capabilities, second, externalagentprotocol.NativeCapabilityDiscoveryPartial, sources)
	if firstFingerprint == secondFingerprint {
		t.Fatalf("partial native capability content did not affect fingerprint")
	}
}

func TestCapabilityFrameFingerprintIncludesReportedSources(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	capabilities := []externalagentprotocol.CapabilityReport{{
		Kind:       externalagentprotocol.CapabilityKindRuntimeHealth,
		Status:     externalagentprotocol.ReadinessStatusWakeable,
		ReportedAt: now,
	}}
	firstFingerprint := capabilityFrameFingerprint(
		capabilities,
		nil,
		externalagentprotocol.NativeCapabilityDiscoveryComplete,
		[]externalagentprotocol.NativeCapabilitySource{externalagentprotocol.NativeCapabilitySourceHermesRuntimeAPI},
	)
	secondFingerprint := capabilityFrameFingerprint(
		capabilities,
		nil,
		externalagentprotocol.NativeCapabilityDiscoveryComplete,
		[]externalagentprotocol.NativeCapabilitySource{externalagentprotocol.NativeCapabilitySourceHermesToolsList},
	)
	if firstFingerprint == secondFingerprint {
		t.Fatalf("reported source content did not affect fingerprint")
	}
}

func TestNativeCapabilityChangeReporterWritesRuntimeCapabilitiesOnDiscoveryErrors(t *testing.T) {
	session := newCapabilityTestSession(t)
	reporter := nativeCapabilityChangeReporter{seen: true, lastFingerprint: "existing"}
	writes := 0
	err := reporter.writeIfChanged(
		context.Background(),
		Runner{},
		session,
		failingCapabilityAdapter{},
		config.Binding{NativeMCPServer: "personastack-conn-1"},
		runtime.Detection{Kind: runtime.AdapterKindHermes, State: runtime.AdapterStateReady},
		nil,
		func(externalagentprotocol.Frame) error {
			writes++
			return nil
		},
	)
	if err != nil {
		t.Fatalf("writeIfChanged() error = %v", err)
	}
	if writes != 1 {
		t.Fatalf("expected discovery error to preserve runtime capabilities report, wrote %d", writes)
	}
}

func TestNativeCapabilityChangeReporterRetriesUnchangedReportsPeriodically(t *testing.T) {
	session := newCapabilityTestSession(t)
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	runner := Runner{Now: func() time.Time { return now }}
	reporter := nativeCapabilityChangeReporter{}
	writes := 0
	write := func(externalagentprotocol.Frame) error {
		writes++
		return nil
	}
	detection := runtime.Detection{Kind: runtime.AdapterKindHermes, State: runtime.AdapterStateReady}
	err := reporter.writeIfChanged(context.Background(), runner, session, emptyCapabilityAdapter{}, config.Binding{NativeMCPServer: "personastack-conn-1"}, detection, nil, write)
	if err != nil {
		t.Fatalf("first writeIfChanged() error = %v", err)
	}
	err = reporter.writeIfChanged(context.Background(), runner, session, emptyCapabilityAdapter{}, config.Binding{NativeMCPServer: "personastack-conn-1"}, detection, nil, write)
	if err != nil {
		t.Fatalf("second writeIfChanged() error = %v", err)
	}
	now = now.Add(nativeCapabilityReportRetryInterval)
	err = reporter.writeIfChanged(context.Background(), runner, session, emptyCapabilityAdapter{}, config.Binding{NativeMCPServer: "personastack-conn-1"}, detection, nil, write)
	if err != nil {
		t.Fatalf("retry writeIfChanged() error = %v", err)
	}
	if writes != 2 {
		t.Fatalf("writes = %d, want initial plus periodic retry", writes)
	}
}

func TestNativeCapabilityChangeReporterWritesLiveWakeProbeReadinessChange(t *testing.T) {
	session := newCapabilityTestSession(t)
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	runner := Runner{Now: func() time.Time { return now }}
	reporter := nativeCapabilityChangeReporter{}
	detection := runtime.Detection{Kind: runtime.AdapterKindHermes, State: runtime.AdapterStateMCPVerified}
	statuses := []externalagentprotocol.ReadinessStatus{}
	write := func(frame externalagentprotocol.Frame) error {
		if frame.Capabilities == nil || len(frame.Capabilities.Capabilities) != 1 {
			t.Fatalf("unexpected capabilities frame: %+v", frame)
		}
		statuses = append(statuses, frame.Capabilities.Capabilities[0].Status)
		return nil
	}

	if err := reporter.writeIfChanged(context.Background(), runner, session, emptyCapabilityAdapter{}, config.Binding{NativeMCPServer: "personastack-conn-1"}, detection, nil, write); err != nil {
		t.Fatalf("initial writeIfChanged() error = %v", err)
	}
	wakeAt := now.Add(time.Second)
	if err := reporter.writeIfChanged(context.Background(), runner, session, emptyCapabilityAdapter{}, config.Binding{NativeMCPServer: "personastack-conn-1"}, detection, func() *time.Time { return &wakeAt }, write); err != nil {
		t.Fatalf("wakeable writeIfChanged() error = %v", err)
	}
	if len(statuses) != 2 {
		t.Fatalf("writes = %d, want initial plus live wakeability change", len(statuses))
	}
	if statuses[0] != externalagentprotocol.ReadinessStatusMCPConfigured || statuses[1] != externalagentprotocol.ReadinessStatusWakeable {
		t.Fatalf("unexpected capability readiness transition: %#v", statuses)
	}
}

func TestNativeCapabilityChangeReporterSerializesSlowStartupAndWakeableReport(t *testing.T) {
	session := newCapabilityTestSession(t)
	reporter := newNativeCapabilityChangeReporter()
	blockDiscovery := make(chan struct{})
	discoveryStarted := make(chan struct{})
	writeDone := make(chan struct{})
	detection := runtime.Detection{Kind: runtime.AdapterKindHermes, State: runtime.AdapterStateMCPVerified}
	statuses := []externalagentprotocol.ReadinessStatus{}
	var statusesMu sync.Mutex
	var wakeAt *time.Time
	wakeProbeAt := func() *time.Time {
		statusesMu.Lock()
		defer statusesMu.Unlock()
		if wakeAt == nil {
			return nil
		}
		value := wakeAt.UTC()
		return &value
	}
	write := func(frame externalagentprotocol.Frame) error {
		if frame.Capabilities == nil || len(frame.Capabilities.Capabilities) != 1 {
			t.Fatalf("unexpected capabilities frame: %+v", frame)
		}
		statusesMu.Lock()
		statuses = append(statuses, frame.Capabilities.Capabilities[0].Status)
		statusesMu.Unlock()
		return nil
	}

	go func() {
		_ = reporter.writeIfChanged(context.Background(), Runner{}, session, slowCapabilityAdapter{
			started: discoveryStarted,
			release: blockDiscovery,
		}, config.Binding{NativeMCPServer: "personastack-conn-1"}, detection, wakeProbeAt, write)
		close(writeDone)
	}()
	select {
	case <-discoveryStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for startup discovery")
	}
	probedAt := time.Now().UTC()
	statusesMu.Lock()
	wakeAt = &probedAt
	statusesMu.Unlock()
	wakeWriteDone := make(chan struct{})
	go func() {
		_ = reporter.writeIfChanged(context.Background(), Runner{}, session, emptyCapabilityAdapter{}, config.Binding{NativeMCPServer: "personastack-conn-1"}, detection, wakeProbeAt, write)
		close(wakeWriteDone)
	}()
	select {
	case <-wakeWriteDone:
		t.Fatal("wakeable report wrote while startup report was still in flight")
	case <-time.After(25 * time.Millisecond):
	}
	close(blockDiscovery)
	select {
	case <-writeDone:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for startup report")
	}
	select {
	case <-wakeWriteDone:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for wakeable report")
	}
	statusesMu.Lock()
	defer statusesMu.Unlock()
	if len(statuses) != 2 {
		t.Fatalf("writes = %d, want startup plus native capability change", len(statuses))
	}
	if statuses[0] != externalagentprotocol.ReadinessStatusWakeable || statuses[1] != externalagentprotocol.ReadinessStatusWakeable {
		t.Fatalf("unexpected serialized capability readiness transition: %#v", statuses)
	}
}

func TestNativeCapabilityChangeReporterWritesRuntimeCapabilityChanges(t *testing.T) {
	session := newCapabilityTestSession(t)
	reporter := nativeCapabilityChangeReporter{}
	writes := 0
	for _, detection := range []runtime.Detection{
		{Kind: runtime.AdapterKindHermes, State: runtime.AdapterStateReady, Note: "ready"},
		{Kind: runtime.AdapterKindHermes, State: runtime.AdapterStateRuntimeStopped, Note: "stopped"},
	} {
		err := reporter.writeIfChanged(
			context.Background(),
			Runner{},
			session,
			emptyCapabilityAdapter{},
			config.Binding{NativeMCPServer: "personastack-conn-1"},
			detection,
			nil,
			func(externalagentprotocol.Frame) error {
				writes++
				return nil
			},
		)
		if err != nil {
			t.Fatalf("writeIfChanged() error = %v", err)
		}
	}
	if writes != 2 {
		t.Fatalf("expected runtime capability change to write twice, wrote %d", writes)
	}
}

func TestNativeCapabilityChangeReporterDistinguishesFailedFromKnownEmpty(t *testing.T) {
	session := newCapabilityTestSession(t)
	reporter := nativeCapabilityChangeReporter{}
	var nativeCapabilityLengths []int
	var reportedSources [][]externalagentprotocol.NativeCapabilitySource
	for _, adapter := range []runtime.Adapter{failingCapabilityAdapter{}, emptyCapabilityAdapter{}} {
		err := reporter.writeIfChanged(
			context.Background(),
			Runner{},
			session,
			adapter,
			config.Binding{NativeMCPServer: "personastack-conn-1"},
			runtime.Detection{Kind: runtime.AdapterKindHermes, State: runtime.AdapterStateReady},
			nil,
			func(frame externalagentprotocol.Frame) error {
				nativeCapabilityLengths = append(nativeCapabilityLengths, len(frame.Capabilities.NativeCapabilities))
				reportedSources = append(reportedSources, frame.Capabilities.NativeCapabilityReportedSources)
				return nil
			},
		)
		if err != nil {
			t.Fatalf("writeIfChanged() error = %v", err)
		}
	}
	if len(nativeCapabilityLengths) != 2 {
		t.Fatalf("expected unknown and known-empty writes, got %d", len(nativeCapabilityLengths))
	}
	if len(reportedSources[0]) != 0 {
		t.Fatalf("expected no failed reported sources, got %#v", reportedSources[0])
	}
	if len(reportedSources[1]) != 1 || reportedSources[1][0] != externalagentprotocol.NativeCapabilitySourceHermesToolsList {
		t.Fatalf("expected known-empty tools-list source, got %#v", reportedSources[1])
	}
}

func TestWriteCapabilitiesFrameReportsPartialEmptyNativeSource(t *testing.T) {
	session := newCapabilityTestSession(t)
	var got externalagentprotocol.Frame
	err := (Runner{}).writeCapabilitiesFrame(
		context.Background(),
		session,
		partialEmptyCapabilityAdapter{},
		config.Binding{NativeMCPServer: "personastack-conn-1"},
		runtime.Detection{Kind: runtime.AdapterKindHermes, State: runtime.AdapterStateReady},
		func(frame externalagentprotocol.Frame) error {
			got = frame
			return nil
		},
	)
	if err != nil {
		t.Fatalf("write capabilities frame: %v", err)
	}
	if len(got.Capabilities.NativeCapabilities) != 0 {
		t.Fatalf("expected no emitted native capabilities, got %#v", got.Capabilities.NativeCapabilities)
	}
	if got.Capabilities.NativeCapabilityDiscoveryStatus != externalagentprotocol.NativeCapabilityDiscoveryPartial {
		t.Fatalf("expected partial discovery status, got %s", got.Capabilities.NativeCapabilityDiscoveryStatus)
	}
	if len(got.Capabilities.NativeCapabilityReportedSources) != 1 || got.Capabilities.NativeCapabilityReportedSources[0] != externalagentprotocol.NativeCapabilitySourceHermesToolsList {
		t.Fatalf("expected tools-list reported source, got %#v", got.Capabilities.NativeCapabilityReportedSources)
	}
}

func TestWriteCapabilitiesFrameReportsPartialNativeCapabilitiesWithSources(t *testing.T) {
	session := newCapabilityTestSession(t)
	var got externalagentprotocol.Frame
	err := (Runner{}).writeCapabilitiesFrame(
		context.Background(),
		session,
		partialCapabilityAdapter{},
		config.Binding{NativeMCPServer: "personastack-conn-1"},
		runtime.Detection{Kind: runtime.AdapterKindHermes, State: runtime.AdapterStateReady},
		func(frame externalagentprotocol.Frame) error {
			got = frame
			return nil
		},
	)
	if err != nil {
		t.Fatalf("write capabilities frame: %v", err)
	}
	if len(got.Capabilities.NativeCapabilities) != 1 {
		t.Fatalf("expected partial native capabilities, got %#v", got.Capabilities.NativeCapabilities)
	}
	nativeCapability := got.Capabilities.NativeCapabilities[0]
	if nativeCapability.Source != externalagentprotocol.NativeCapabilitySourceHermesRuntimeAPI || nativeCapability.CapabilityID != "run_submission" {
		t.Fatalf("unexpected partial native capability: %#v", nativeCapability)
	}
	if got.Capabilities.NativeCapabilityDiscoveryStatus != externalagentprotocol.NativeCapabilityDiscoveryPartial {
		t.Fatalf("expected partial discovery status, got %s", got.Capabilities.NativeCapabilityDiscoveryStatus)
	}
	if len(got.Capabilities.NativeCapabilityReportedSources) != 1 || got.Capabilities.NativeCapabilityReportedSources[0] != externalagentprotocol.NativeCapabilitySourceHermesRuntimeAPI {
		t.Fatalf("expected runtime API reported source only, got %#v", got.Capabilities.NativeCapabilityReportedSources)
	}
}

func newCapabilityTestSession(t *testing.T) bridge.Session {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	session, err := bridge.NewSession(config.Binding{
		ConnectionID: "conn-1",
		PersonaID:    "persona-1",
		RuntimeKind:  runtime.AdapterKindHermes,
	}, bridge.Credential{
		ID:         "cred-1",
		PrivateKey: privateKey,
		PublicKey:  publicKey,
	})
	if err != nil {
		t.Fatalf("new session: %v", err)
	}
	return session
}

type failingCapabilityAdapter struct{}

func (failingCapabilityAdapter) Kind() runtime.AdapterKind {
	return runtime.AdapterKindHermes
}

func (failingCapabilityAdapter) Detect() runtime.Detection {
	return runtime.Detection{Kind: runtime.AdapterKindHermes, State: runtime.AdapterStateReady}
}

func (failingCapabilityAdapter) StartRun(runtime.RunRequest) (string, error) {
	return "", fmt.Errorf("not used")
}

func (failingCapabilityAdapter) StreamOrPollRun(context.Context, string, runtime.RunEventHandler) (runtime.RunResult, error) {
	return runtime.RunResult{}, fmt.Errorf("not used")
}

func (failingCapabilityAdapter) CancelRun(string) error {
	return fmt.Errorf("not used")
}

func (failingCapabilityAdapter) Diagnose() runtime.Detection {
	return runtime.Detection{Kind: runtime.AdapterKindHermes, State: runtime.AdapterStateReady}
}

func (failingCapabilityAdapter) DescribeNativeCapabilities(context.Context, string) ([]runtime.NativeCapability, error) {
	return nil, fmt.Errorf("catalog unavailable")
}

type recordFailureAdapter struct {
	cancelledNativeRunID string
}

func (adapter *recordFailureAdapter) Kind() runtime.AdapterKind {
	return runtime.AdapterKindHermes
}

func (adapter *recordFailureAdapter) Detect() runtime.Detection {
	return runtime.Detection{Kind: runtime.AdapterKindHermes, State: runtime.AdapterStateReady}
}

func (adapter *recordFailureAdapter) StartRun(runtime.RunRequest) (string, error) {
	return "native-1", nil
}

func (adapter *recordFailureAdapter) StreamOrPollRun(context.Context, string, runtime.RunEventHandler) (runtime.RunResult, error) {
	return runtime.RunResult{}, fmt.Errorf("not used")
}

func (adapter *recordFailureAdapter) CancelRun(nativeRunID string) error {
	adapter.cancelledNativeRunID = nativeRunID
	return nil
}

func (adapter *recordFailureAdapter) Diagnose() runtime.Detection {
	return runtime.Detection{Kind: runtime.AdapterKindHermes, State: runtime.AdapterStateReady}
}

func TestRunRecordFailureTerminalCancelsNativeRunAndPreservesNativeID(t *testing.T) {
	adapter := &recordFailureAdapter{}
	session := newCapabilityTestSession(t)
	frame := externalagentprotocol.Frame{
		MessageID:    "start-1",
		RunID:        "run-1",
		AssignmentID: "assignment-1",
	}

	failed := runRecordFailureTerminal(adapter, session, frame, " native-1 ", errors.New("record failed"))

	if adapter.cancelledNativeRunID != "native-1" {
		t.Fatalf("native run was not cancelled: %q", adapter.cancelledNativeRunID)
	}
	if failed.MessageType != externalagentprotocol.FrameTypeRunFailed || failed.RunTerminal == nil {
		t.Fatalf("expected failed terminal frame, got %+v", failed)
	}
	if failed.RunTerminal.NativeRunID != "native-1" {
		t.Fatalf("native run id not preserved in terminal frame: %+v", failed.RunTerminal)
	}
}

type partialCapabilityAdapter struct {
	failingCapabilityAdapter
}

func (partialCapabilityAdapter) DescribeNativeCapabilities(context.Context, string) ([]runtime.NativeCapability, error) {
	return []runtime.NativeCapability{{
		Source:       runtime.NativeCapabilitySourceHermesRuntimeAPI,
		Kind:         runtime.NativeCapabilityKindRuntimeFeature,
		CapabilityID: "run_submission",
		Label:        "Task delegation",
		Summary:      "can accept delegated tasks",
	}}, fmt.Errorf("Hermes tools list: unavailable")
}

type partialEmptyCapabilityAdapter struct {
	failingCapabilityAdapter
}

func (partialEmptyCapabilityAdapter) DescribeNativeCapabilities(context.Context, string) ([]runtime.NativeCapability, error) {
	return []runtime.NativeCapability{{
		Source: runtime.NativeCapabilitySourceHermesToolsList,
	}}, fmt.Errorf("Hermes runtime API: unavailable")
}

type emptyCapabilityAdapter struct{}

func (emptyCapabilityAdapter) Kind() runtime.AdapterKind {
	return runtime.AdapterKindHermes
}

func (emptyCapabilityAdapter) Detect() runtime.Detection {
	return runtime.Detection{Kind: runtime.AdapterKindHermes, State: runtime.AdapterStateReady}
}

func (emptyCapabilityAdapter) StartRun(runtime.RunRequest) (string, error) {
	return "", fmt.Errorf("not used")
}

func (emptyCapabilityAdapter) StreamOrPollRun(context.Context, string, runtime.RunEventHandler) (runtime.RunResult, error) {
	return runtime.RunResult{}, fmt.Errorf("not used")
}

func (emptyCapabilityAdapter) CancelRun(string) error {
	return fmt.Errorf("not used")
}

func (emptyCapabilityAdapter) Diagnose() runtime.Detection {
	return runtime.Detection{Kind: runtime.AdapterKindHermes, State: runtime.AdapterStateReady}
}

func (emptyCapabilityAdapter) DescribeNativeCapabilities(context.Context, string) ([]runtime.NativeCapability, error) {
	return []runtime.NativeCapability{{
		Source: runtime.NativeCapabilitySourceHermesToolsList,
	}}, nil
}

type slowCapabilityAdapter struct {
	emptyCapabilityAdapter
	started chan struct{}
	release chan struct{}
}

func (adapter slowCapabilityAdapter) DescribeNativeCapabilities(context.Context, string) ([]runtime.NativeCapability, error) {
	adapter.started <- struct{}{}
	<-adapter.release
	return []runtime.NativeCapability{{
		Source:       runtime.NativeCapabilitySourceHermesToolsList,
		Kind:         runtime.NativeCapabilityKindNativeTool,
		CapabilityID: "tool-1",
		Label:        "Tool",
	}}, nil
}

func TestRunnerConnectsAndSendsHeartbeat(t *testing.T) {
	keyring.MockInit()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	seenHeartbeat := make(chan externalagentprotocol.Frame, 1)
	seenCapabilities := make(chan externalagentprotocol.Frame, 1)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()
		var connectFrame externalagentprotocol.Frame
		if err := conn.ReadJSON(&connectFrame); err != nil {
			t.Fatalf("read connect: %v", err)
		}
		if connectFrame.MessageType != externalagentprotocol.FrameTypeConnect {
			t.Fatalf("unexpected connect frame: %+v", connectFrame)
		}
		if connectFrame.Connect.ConnectionGeneration != 2 {
			t.Fatalf("expected first session to advance generation to 2, got %+v", connectFrame.Connect)
		}
		_ = conn.WriteJSON(externalagentprotocol.Frame{
			MessageType:  externalagentprotocol.FrameTypeConnectAccepted,
			PersonaID:    "persona-1",
			ConnectionID: "conn-1",
			SentAt:       time.Now(),
			ConnectAccepted: &externalagentprotocol.ConnectAcceptedPayload{
				ProtocolVersion:      externalagentprotocol.ProtocolVersionV4,
				ConnectionGeneration: connectFrame.Connect.ConnectionGeneration,
				HeartbeatSeconds:     15,
			},
		})
		var heartbeat externalagentprotocol.Frame
		if err := conn.ReadJSON(&heartbeat); err != nil {
			t.Fatalf("read heartbeat: %v", err)
		}
		seenHeartbeat <- heartbeat
		capabilities := readFrameOfType(t, conn, externalagentprotocol.FrameTypeCapabilitiesReport)
		seenCapabilities <- capabilities
		cancel()
	}))
	defer server.Close()

	binding := config.Binding{
		ConnectionID:            "conn-1",
		PersonaID:               "persona-1",
		ConnectionGeneration:    1,
		GatewayWebsocketURL:     "ws" + server.URL[len("http"):],
		BridgeCredentialID:      "cred-1",
		BridgePrivateKey:        base64.StdEncoding.EncodeToString(privateKey),
		BridgePublicKey:         base64.StdEncoding.EncodeToString(publicKey),
		LastWakeProbeAt:         time.Now().UTC().Add(-time.Minute),
		LastWakeProbeGeneration: 1,
		RuntimeKind:             runtime.AdapterKindAuto,
	}
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{binding}})
	if err := (Runner{Store: &store, ReconnectMin: time.Millisecond, ReconnectMax: time.Millisecond}).RunForeground(ctx); err != nil {
		t.Fatalf("run foreground: %v", err)
	}
	heartbeat := <-seenHeartbeat
	if heartbeat.MessageType != externalagentprotocol.FrameTypeHeartbeat || heartbeat.ConnectionID != "conn-1" {
		t.Fatalf("unexpected heartbeat: %+v", heartbeat)
	}
	if heartbeat.Heartbeat.ReadinessStatus == externalagentprotocol.ReadinessStatusWakeable {
		t.Fatalf("heartbeat reused persisted wake probe before current-session probe: %+v", heartbeat.Heartbeat)
	}
	capabilities := <-seenCapabilities
	if capabilities.Capabilities == nil {
		t.Fatalf("expected startup capabilities report, got %+v", capabilities)
	}
	stored, ok := store.Binding("conn-1")
	if !ok {
		t.Fatal("binding missing after run")
	}
	if stored.ConnectionGeneration != 2 || stored.LastWakeProbeGeneration == stored.ConnectionGeneration {
		t.Fatalf("expected first session generation to invalidate persisted wake probe: %+v", stored)
	}
}

func TestRunnerReloadsFileBackedBindingAfterRestart(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	heartbeatCount := make(chan externalagentprotocol.Frame, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()
		var connectFrame externalagentprotocol.Frame
		if err := conn.ReadJSON(&connectFrame); err != nil {
			t.Fatalf("read connect: %v", err)
		}
		if connectFrame.MessageType != externalagentprotocol.FrameTypeConnect || connectFrame.ConnectionID != "conn-1" {
			t.Fatalf("unexpected connect frame: %+v", connectFrame)
		}
		_ = conn.WriteJSON(externalagentprotocol.Frame{
			MessageType:  externalagentprotocol.FrameTypeConnectAccepted,
			PersonaID:    "persona-1",
			ConnectionID: "conn-1",
			SentAt:       time.Now(),
			ConnectAccepted: &externalagentprotocol.ConnectAcceptedPayload{
				ProtocolVersion:      externalagentprotocol.ProtocolVersionV4,
				ConnectionGeneration: 1,
				HeartbeatSeconds:     15,
			},
		})
		var heartbeat externalagentprotocol.Frame
		if err := conn.ReadJSON(&heartbeat); err != nil {
			t.Fatalf("read heartbeat: %v", err)
		}
		if heartbeat.MessageType != externalagentprotocol.FrameTypeHeartbeat {
			t.Fatalf("unexpected heartbeat: %+v", heartbeat)
		}
		if heartbeat.Heartbeat == nil {
			t.Fatalf("missing heartbeat payload: %+v", heartbeat)
		}
		if heartbeat.Heartbeat.ConnectionStatus != externalagentprotocol.ConnectionStatusBridgeConnected {
			t.Fatalf("expected connected heartbeat, got %+v", heartbeat.Heartbeat)
		}
		heartbeatCount <- heartbeat
	}))
	defer server.Close()

	path := t.TempDir() + "/state.json"
	raw, err := json.Marshal(config.State{Bindings: []config.Binding{{
		ConnectionID:         "conn-1",
		PersonaID:            "persona-1",
		ConnectionGeneration: 1,
		GatewayWebsocketURL:  "ws" + server.URL[len("http"):],
		BridgeCredentialID:   "cred-1",
		BridgePrivateKey:     base64.StdEncoding.EncodeToString(privateKey),
		BridgePublicKey:      base64.StdEncoding.EncodeToString(publicKey),
		NativeMCPServer:      "personastack-conn-1",
		NativeMCPNamespace:   "personastack",
		RuntimeKind:          runtime.AdapterKindAuto,
		ReadinessState:       runtime.AdapterStateReady,
	}}})
	if err != nil {
		t.Fatalf("encode state: %v", err)
	}
	err = os.WriteFile(path, raw, 0o600)
	if err != nil {
		t.Fatalf("write state: %v", err)
	}

	for i := 0; i < 2; i++ {
		ctx, cancel := context.WithCancel(t.Context())
		errCh := make(chan error, 1)
		go func() {
			errCh <- (Runner{Store: config.NewFileStore(path), ReconnectMin: time.Millisecond, ReconnectMax: time.Millisecond}).RunForeground(ctx)
		}()
		select {
		case <-heartbeatCount:
			cancel()
		case <-time.After(2 * time.Second):
			cancel()
			t.Fatalf("timeout waiting for restart connection %d", i+1)
		}
		if err := <-errCh; err != nil {
			t.Fatalf("run foreground restart %d: %v", i+1, err)
		}
	}
}

func TestRunnerReconnectsAfterServerDrainingWithoutOverlap(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	secondConnected := make(chan struct{}, 1)
	firstClosed := make(chan struct{})
	var closeFirst sync.Once
	var connectionCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()
		var connectionNumber int32
		defer func() {
			if connectionNumber == 1 {
				closeFirst.Do(func() {
					close(firstClosed)
				})
			}
		}()
		connectionNumber = connectionCount.Add(1)
		var connectFrame externalagentprotocol.Frame
		if err := conn.ReadJSON(&connectFrame); err != nil {
			t.Fatalf("read connect: %v", err)
		}
		_ = conn.WriteJSON(externalagentprotocol.Frame{
			MessageType:  externalagentprotocol.FrameTypeConnectAccepted,
			PersonaID:    "persona-1",
			ConnectionID: "conn-1",
			SentAt:       time.Now().UTC(),
			ConnectAccepted: &externalagentprotocol.ConnectAcceptedPayload{
				ProtocolVersion:      externalagentprotocol.ProtocolVersionV4,
				ConnectionGeneration: connectFrame.Connect.ConnectionGeneration,
				HeartbeatSeconds:     15,
			},
		})
		if connectionNumber != 1 {
			select {
			case <-firstClosed:
			case <-time.After(2 * time.Second):
				t.Fatal("replacement connected before first drain session closed")
			}
			secondConnected <- struct{}{}
			for {
				if _, _, err := conn.ReadMessage(); err != nil {
					return
				}
			}
		}
		var heartbeat externalagentprotocol.Frame
		if err := conn.ReadJSON(&heartbeat); err != nil {
			t.Fatalf("read heartbeat: %v", err)
		}
		_ = conn.WriteJSON(externalagentprotocol.Frame{
			MessageType:  externalagentprotocol.FrameTypeServerDraining,
			PersonaID:    "persona-1",
			ConnectionID: "conn-1",
			SentAt:       time.Now().UTC(),
			ServerDraining: &externalagentprotocol.ServerDrainingPayload{
				DeadlineAt: time.Now().UTC().Add(time.Second),
				Reason:     "test",
			},
		})
	}))
	defer server.Close()

	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{{
		ConnectionID:        "conn-1",
		PersonaID:           "persona-1",
		GatewayWebsocketURL: "ws" + server.URL[len("http"):],
		BridgeCredentialID:  "cred-1",
		BridgePrivateKey:    base64.StdEncoding.EncodeToString(privateKey),
		BridgePublicKey:     base64.StdEncoding.EncodeToString(publicKey),
		RuntimeKind:         runtime.AdapterKindAuto,
	}}})
	ctx, cancel := context.WithCancel(t.Context())
	errCh := make(chan error, 1)
	go func() {
		errCh <- (Runner{Store: &store, ReconnectMin: time.Millisecond, ReconnectMax: time.Millisecond}).RunForeground(ctx)
	}()
	select {
	case <-secondConnected:
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("timed out waiting for tracked replacement connection")
	}
	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("RunForeground() after draining replacement: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for runner shutdown")
	}
}

func TestRunnerForwardsHermesRunEventsAfterMCPVerification(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)
	t.Setenv("HERMES_API_SERVER_KEY", "hermes-key-1")

	mcpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer mcp-token-1" {
			t.Fatalf("unexpected mcp authorization: %q", got)
		}
		var request struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("decode mcp request: %v", err)
		}
		switch request.Method {
		case "initialize":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-11-25","capabilities":{},"serverInfo":{"name":"personastack-connector","version":"verify"}}}`))
		case "notifications/initialized":
			w.WriteHeader(http.StatusOK)
		case "tools/list":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":2,"result":{"tools":[]}}`))
		default:
			t.Fatalf("unexpected mcp method: %s", request.Method)
		}
	}))
	defer mcpServer.Close()

	err = os.MkdirAll(filepath.Join(homeDir, ".hermes"), 0o700)
	if err != nil {
		t.Fatalf("create hermes config dir: %v", err)
	}
	err = os.WriteFile(filepath.Join(homeDir, ".hermes", "config.yaml"), []byte(
		"mcp_servers:\n"+
			"  personastack-conn-1:\n"+
			"    command: /opt/personastack-connector\n"+
			"    args:\n"+
			"      - mcp\n"+
			"      - stdio\n"+
			"      - --binding\n"+
			"      - conn-1\n",
	), 0o600)
	if err != nil {
		t.Fatalf("write hermes config: %v", err)
	}

	hermesServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health", "/health/detailed":
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		case "/v1/models":
			_, _ = w.Write([]byte(`{"data":[]}`))
		case "/v1/capabilities":
			_, _ = w.Write([]byte(`{"features":{"run_submission":true,"run_status":true,"run_events_sse":true,"run_stop":true}}`))
		case "/v1/runs":
			if got := r.Header.Get("Authorization"); got != "Bearer hermes-key-1" {
				t.Fatalf("unexpected hermes run authorization: %q", got)
			}
			var request map[string]any
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Fatalf("decode hermes run request: %v", err)
			}
			if request["input"] != "prompt" || request["session_id"] != "run-1" || request["native_mcp_server"] != "personastack-conn-1" {
				t.Fatalf("unexpected hermes run request: %#v", request)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"run_id":"hermes-native-1"}`))
		case "/v1/runs/hermes-native-1/events":
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("data: {\"status\":\"running\",\"data\":{\"deltaText\":\"chunk\"}}\n\n"))
			_, _ = w.Write([]byte("data: {\"status\":\"running\",\"data\":{\"toolName\":\"browser\",\"phase\":\"started\",\"summary\":\"opening\"}}\n\n"))
			_, _ = w.Write([]byte("data: {\"status\":\"completed\",\"output\":\"done\"}\n\n"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer hermesServer.Close()
	t.Setenv("PERSONASTACK_CONNECTOR_HERMES_URL", hermesServer.URL)

	terminalSeen := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	readNextFrame := func(conn *websocket.Conn) (externalagentprotocol.Frame, error) {
		for {
			var frame externalagentprotocol.Frame
			if err := conn.ReadJSON(&frame); err != nil {
				return externalagentprotocol.Frame{}, err
			}
			if frame.MessageType == externalagentprotocol.FrameTypeHeartbeat || frame.MessageType == externalagentprotocol.FrameTypeCapabilitiesReport {
				continue
			}
			return frame, nil
		}
	}
	gatewayServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()

		var connectFrame externalagentprotocol.Frame
		if err := conn.ReadJSON(&connectFrame); err != nil {
			t.Fatalf("read connect: %v", err)
		}
		if connectFrame.MessageType != externalagentprotocol.FrameTypeConnect {
			t.Fatalf("unexpected connect frame: %+v", connectFrame)
		}
		err = conn.WriteJSON(externalagentprotocol.Frame{
			MessageType:  externalagentprotocol.FrameTypeConnectAccepted,
			PersonaID:    "persona-1",
			ConnectionID: "conn-1",
			SentAt:       time.Now().UTC(),
			ConnectAccepted: &externalagentprotocol.ConnectAcceptedPayload{
				ProtocolVersion:      externalagentprotocol.ProtocolVersionV4,
				ConnectionGeneration: 1,
				HeartbeatSeconds:     15,
			},
		})
		if err != nil {
			t.Fatalf("write connect accepted: %v", err)
		}

		var heartbeat externalagentprotocol.Frame
		if err := conn.ReadJSON(&heartbeat); err != nil {
			t.Fatalf("read heartbeat: %v", err)
		}
		if heartbeat.MessageType != externalagentprotocol.FrameTypeHeartbeat || heartbeat.Heartbeat == nil {
			t.Fatalf("unexpected heartbeat: %+v", heartbeat)
		}

		err = conn.WriteJSON(externalagentprotocol.Frame{
			MessageID:    "probe-1",
			MessageType:  externalagentprotocol.FrameTypeWakeProbe,
			PersonaID:    "persona-1",
			ConnectionID: "conn-1",
			SentAt:       time.Now().UTC(),
			WakeProbe: &externalagentprotocol.WakeProbePayload{
				ProbeID:    "probe-1",
				DeadlineAt: time.Now().UTC().Add(time.Second),
			},
		})
		if err != nil {
			t.Fatalf("write wake probe: %v", err)
		}
		wakeAck := readFrameOfType(t, conn, externalagentprotocol.FrameTypeWakeProbeAccepted)
		if wakeAck.MessageType != externalagentprotocol.FrameTypeWakeProbeAccepted || wakeAck.WakeProbeAccepted == nil {
			t.Fatalf("unexpected wake probe accepted: %+v", wakeAck)
		}

		runStart := externalagentprotocol.Frame{
			MessageID:    "start-1",
			MessageType:  externalagentprotocol.FrameTypeRunStart,
			PersonaID:    "persona-1",
			ConnectionID: "conn-1",
			RunID:        "run-1",
			AssignmentID: "assignment-1",
			SentAt:       time.Now().UTC(),
			RunStart: &externalagentprotocol.RunStartPayload{
				FullyComposedPrompt:    "prompt",
				PromptContext:          externalagentprotocol.PromptContext{PromptVersion: "test", PromptHash: "hash"},
				TriggerKind:            "persona_chat",
				MCPURL:                 mcpServer.URL,
				NativeMCPServerName:    "personastack-conn-1",
				NativeMCPToolNamespace: "personastack",
				DeadlineAt:             time.Now().UTC().Add(time.Minute),
			},
		}
		err = conn.WriteJSON(runStart)
		if err != nil {
			t.Fatalf("write run start: %v", err)
		}

		gotFrames := []externalagentprotocol.Frame{}
		for {
			frame, err := readNextFrame(conn)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				t.Fatalf("read run frame: %v", err)
			}
			gotFrames = append(gotFrames, frame)
			if frame.MessageType == externalagentprotocol.FrameTypeRunCompleted {
				break
			}
		}
		if len(gotFrames) < 5 {
			t.Fatalf("unexpected run frames: %+v", gotFrames)
		}
		if gotFrames[0].MessageType != externalagentprotocol.FrameTypeRunAccepted || gotFrames[0].RunAccepted == nil || gotFrames[0].RunAccepted.NativeRunID != "hermes-native-1" {
			t.Fatalf("unexpected run accepted frame: %+v", gotFrames[0])
		}
		if gotFrames[1].MessageType != externalagentprotocol.FrameTypeRunStarted || gotFrames[1].RunStarted == nil || gotFrames[1].RunStarted.NativeRunID != "hermes-native-1" {
			t.Fatalf("unexpected run started frame: %+v", gotFrames[1])
		}
		seenDelta := false
		seenTool := false
		for i := 2; i < len(gotFrames)-1; i++ {
			frame := gotFrames[i]
			if frame.MessageType == externalagentprotocol.FrameTypeRunOutputDelta && frame.RunOutputDelta != nil && frame.RunOutputDelta.Delta != "" {
				seenDelta = true
			}
			if frame.MessageType == externalagentprotocol.FrameTypeRunToolEvent && frame.RunToolEvent != nil && frame.RunToolEvent.ToolName == "browser" && frame.RunToolEvent.Summary != "" {
				seenTool = true
			}
		}
		if !seenDelta {
			t.Fatalf("missing run output delta: %+v", gotFrames)
		}
		if !seenTool {
			t.Fatalf("missing run tool event: %+v", gotFrames)
		}
		terminal := gotFrames[len(gotFrames)-1]
		if terminal.MessageType != externalagentprotocol.FrameTypeRunCompleted || terminal.RunTerminal == nil || terminal.RunTerminal.Status != externalagentprotocol.RunStatusCompleted || terminal.RunTerminal.FinalMessage != "done" {
			t.Fatalf("unexpected run terminal frame: %+v", terminal)
		}
		if err := conn.WriteJSON(externalagentprotocol.Frame{
			MessageID:    "terminal-ack-1",
			MessageType:  externalagentprotocol.FrameTypeRunTerminalAck,
			PersonaID:    "persona-1",
			ConnectionID: "conn-1",
			RunID:        terminal.RunID,
			AssignmentID: terminal.AssignmentID,
			SentAt:       time.Now().UTC(),
			RunTerminalAck: &externalagentprotocol.RunTerminalAckPayload{
				AcknowledgedAt: time.Now().UTC(),
			},
		}); err != nil {
			t.Fatalf("write terminal ack: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
		terminalSeen <- struct{}{}
	}))
	defer gatewayServer.Close()

	binding := config.Binding{
		ConnectionID:        "conn-1",
		PersonaID:           "persona-1",
		BridgeCredentialID:  "cred-1",
		BridgePrivateKey:    base64.StdEncoding.EncodeToString(privateKey),
		BridgePublicKey:     base64.StdEncoding.EncodeToString(publicKey),
		NativeMCPServer:     "personastack-conn-1",
		NativeMCPNamespace:  "personastack",
		PersonaMCPURL:       mcpServer.URL,
		PersonaMCPToken:     "mcp-token-1",
		GatewayWebsocketURL: "ws" + gatewayServer.URL[len("http"):],
		RuntimeKind:         runtime.AdapterKindHermes,
	}
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{binding}})
	keyring.MockInit()
	t.Setenv("PERSONASTACK_CONNECTOR_DISABLE_HERMES_GATEWAY_START", "1")
	if _, err := (mcp.Installer{Store: &store, HomeDir: homeDir, ExecutablePath: "/usr/local/bin/personastack-connector", GOOS: "linux"}).InstallAll(); err != nil {
		t.Fatalf("install mcp: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- (Runner{Store: &store, ReconnectMin: time.Millisecond, ReconnectMax: time.Millisecond}).RunForeground(ctx)
	}()

	select {
	case <-terminalSeen:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for run terminal frame")
	}
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
		stored, ok := store.Binding("conn-1")
		if ok && stored.ActiveRunID == "" && stored.ActiveNativeRunID == "" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run foreground: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for runner shutdown")
	}
	stored, ok := store.Binding("conn-1")
	if !ok || stored.ActiveRunID != "" || stored.ActiveNativeRunID != "" {
		t.Fatalf("active run was not cleared after terminal ack: %+v", stored)
	}
}

func TestContextForRunDeadlineExpires(t *testing.T) {
	ctx, cancel := contextForRunDeadline(t.Context(), time.Now().Add(10*time.Millisecond))
	defer cancel()

	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for deadline context")
	}
	if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatalf("ctx err = %v", ctx.Err())
	}
}

func TestContextForRunDeadlineHonorsLongDeadlines(t *testing.T) {
	before := time.Now().UTC()
	upstream := before.Add(15 * time.Minute)
	ctx, cancel := contextForRunDeadline(t.Context(), upstream)
	defer cancel()
	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("deadline missing")
	}
	if deadline.Before(upstream.Add(-time.Second)) || deadline.After(upstream.Add(time.Second)) {
		t.Fatalf("deadline = %s, want upstream deadline", deadline)
	}
}

func TestContextForRunDeadlineAllowsNoDeadline(t *testing.T) {
	ctx, cancel := contextForRunDeadline(t.Context(), time.Time{})
	defer cancel()
	if deadline, ok := ctx.Deadline(); ok {
		t.Fatalf("deadline = %s, want none", deadline)
	}
}

func TestRunObservationRegistryCancelsNoDeadlineContext(t *testing.T) {
	registry := newRunObservationRegistry()
	ctx, cancel := contextForRunDeadline(t.Context(), time.Time{})
	cleanup := registry.track("run-1", cancel)
	defer cleanup()

	if !registry.cancel("run-1") {
		t.Fatal("expected registered run observation to cancel")
	}
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for observation context cancellation")
	}
}

func TestObserveReplayedActiveRunExpiresWithStoredDeadlineAndCancelsAsync(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	session, err := bridge.NewSession(config.Binding{
		ConnectionID: "conn-1",
		PersonaID:    "persona-1",
		RuntimeKind:  runtime.AdapterKindHermes,
	}, bridge.Credential{
		ID:         "cred-1",
		PrivateKey: privateKey,
		PublicKey:  publicKey,
	})
	if err != nil {
		t.Fatalf("new session: %v", err)
	}
	binding := config.Binding{ConnectionID: "conn-1", PersonaID: "persona-1", ActiveRunID: "run-1"}
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{binding}})
	cancelStarted := make(chan struct{})
	releaseCancel := make(chan struct{})
	adapter := deadlineReplayAdapter{cancelStarted: cancelStarted, releaseCancel: releaseCancel}
	defer close(releaseCancel)
	frames := make(chan externalagentprotocol.Frame, 1)

	Runner{Store: &store}.observeReplayedActiveRun(
		context.Background(),
		binding,
		session,
		adapter,
		externalagentprotocol.Frame{RunID: "run-1", AssignmentID: "assignment-1"},
		"native-1",
		time.Now().Add(10*time.Millisecond),
		func(frame externalagentprotocol.Frame) error {
			frames <- frame
			return nil
		},
	)

	select {
	case frame := <-frames:
		if frame.RunTerminal == nil || frame.RunTerminal.Reason != externalagentprotocol.TerminalReasonExpired {
			t.Fatalf("unexpected terminal frame: %+v", frame)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for expired terminal frame")
	}
	select {
	case <-cancelStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for async cancel")
	}
}

func TestObserveReplayedActiveRunSessionCancelDoesNotCancelNativeRun(t *testing.T) {
	session := newCapabilityTestSession(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancelStarted := make(chan struct{}, 1)
	done := make(chan struct{}, 1)
	frames := make(chan externalagentprotocol.Frame, 1)

	go func() {
		Runner{}.observeReplayedActiveRun(
			ctx,
			config.Binding{ConnectionID: "conn-1", PersonaID: "persona-1", ActiveRunID: "run-1"},
			session,
			sessionCancelReplayAdapter{cancelStarted: cancelStarted},
			externalagentprotocol.Frame{RunID: "run-1", AssignmentID: "assignment-1"},
			"native-1",
			time.Time{},
			func(frame externalagentprotocol.Frame) error {
				frames <- frame
				return nil
			},
		)
		done <- struct{}{}
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for replay observer shutdown")
	}
	select {
	case <-cancelStarted:
		t.Fatal("session cancellation cancelled native run")
	default:
	}
	select {
	case frame := <-frames:
		t.Fatalf("session cancellation emitted stale terminal frame: %+v", frame)
	default:
	}
}

func TestObserveReplayedActiveRunKeepsStateWhenTerminalWriteFails(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	session, err := bridge.NewSession(config.Binding{
		ConnectionID: "conn-1",
		PersonaID:    "persona-1",
		RuntimeKind:  runtime.AdapterKindHermes,
	}, bridge.Credential{
		ID:         "cred-1",
		PrivateKey: privateKey,
		PublicKey:  publicKey,
	})
	if err != nil {
		t.Fatalf("new session: %v", err)
	}
	binding := config.Binding{
		ConnectionID:       "conn-1",
		PersonaID:          "persona-1",
		ActiveRunID:        "run-1",
		ActiveAssignmentID: "assignment-1",
		ActiveNativeRunID:  "native-1",
	}
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{binding}})

	Runner{Store: &store}.observeReplayedActiveRun(
		context.Background(),
		binding,
		session,
		completingReplayAdapter{},
		externalagentprotocol.Frame{RunID: "run-1", AssignmentID: "assignment-1"},
		"native-1",
		time.Time{},
		func(externalagentprotocol.Frame) error {
			return fmt.Errorf("websocket closed")
		},
	)

	active, ok := store.Binding("conn-1")
	if !ok || active.ActiveRunID != "run-1" || active.ActiveNativeRunID != "native-1" {
		t.Fatalf("active run was cleared after failed terminal write: %+v", active)
	}
}

func TestObserveReplayedActiveRunKeepsStateUntilTerminalAck(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	session, err := bridge.NewSession(config.Binding{
		ConnectionID: "conn-1",
		PersonaID:    "persona-1",
		RuntimeKind:  runtime.AdapterKindHermes,
	}, bridge.Credential{
		ID:         "cred-1",
		PrivateKey: privateKey,
		PublicKey:  publicKey,
	})
	if err != nil {
		t.Fatalf("new session: %v", err)
	}
	binding := config.Binding{
		ConnectionID:       "conn-1",
		PersonaID:          "persona-1",
		ActiveRunID:        "run-1",
		ActiveAssignmentID: "assignment-1",
		ActiveNativeRunID:  "native-1",
	}
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{binding}})

	Runner{Store: &store}.observeReplayedActiveRun(
		context.Background(),
		binding,
		session,
		completingReplayAdapter{},
		externalagentprotocol.Frame{RunID: "run-1", AssignmentID: "assignment-1"},
		"native-1",
		time.Time{},
		func(externalagentprotocol.Frame) error {
			return nil
		},
	)

	active, ok := store.Binding("conn-1")
	if !ok || active.ActiveRunID != "run-1" || active.ActiveNativeRunID != "native-1" {
		t.Fatalf("active run was cleared before terminal ack: %+v", active)
	}
}

func TestObserveReplayedActiveRunDoesNotDuplicateStarted(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	session, err := bridge.NewSession(config.Binding{
		ConnectionID: "conn-1",
		PersonaID:    "persona-1",
		RuntimeKind:  runtime.AdapterKindHermes,
	}, bridge.Credential{
		ID:         "cred-1",
		PrivateKey: privateKey,
		PublicKey:  publicKey,
	})
	if err != nil {
		t.Fatalf("new session: %v", err)
	}
	frames := make([]externalagentprotocol.Frame, 0, 2)

	Runner{}.observeReplayedActiveRun(
		context.Background(),
		config.Binding{ConnectionID: "conn-1", PersonaID: "persona-1"},
		session,
		startingReplayAdapter{},
		externalagentprotocol.Frame{RunID: "run-1", AssignmentID: "assignment-1"},
		"native-1",
		time.Time{},
		func(frame externalagentprotocol.Frame) error {
			frames = append(frames, frame)
			return nil
		},
	)

	for _, frame := range frames {
		if frame.MessageType == externalagentprotocol.FrameTypeRunStarted {
			t.Fatalf("replay observer emitted duplicate started frame: %+v", frames)
		}
	}
}

func TestRunnerKeepsMissingNativeRunStateUntilTerminalAck(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	terminalSent := make(chan struct{})
	ackSent := make(chan struct{})
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()
		var connectFrame externalagentprotocol.Frame
		if err := conn.ReadJSON(&connectFrame); err != nil {
			t.Fatalf("read connect: %v", err)
		}
		_ = conn.WriteJSON(externalagentprotocol.Frame{
			MessageType:  externalagentprotocol.FrameTypeConnectAccepted,
			PersonaID:    "persona-1",
			ConnectionID: "conn-1",
			SentAt:       time.Now().UTC(),
			ConnectAccepted: &externalagentprotocol.ConnectAcceptedPayload{
				ProtocolVersion:      externalagentprotocol.ProtocolVersionV4,
				ConnectionGeneration: 1,
				HeartbeatSeconds:     15,
			},
		})
		for {
			var frame externalagentprotocol.Frame
			if err := conn.ReadJSON(&frame); err != nil {
				t.Fatalf("read heartbeat: %v", err)
			}
			if frame.MessageType == externalagentprotocol.FrameTypeHeartbeat {
				break
			}
		}
		_ = conn.WriteJSON(externalagentprotocol.Frame{
			MessageID:    "run-start-1",
			MessageType:  externalagentprotocol.FrameTypeRunStart,
			PersonaID:    "persona-1",
			ConnectionID: "conn-1",
			RunID:        "run-1",
			AssignmentID: "assignment-1",
			SentAt:       time.Now().UTC(),
			RunStart: &externalagentprotocol.RunStartPayload{
				FullyComposedPrompt: "prompt",
			},
		})
		for {
			var frame externalagentprotocol.Frame
			if err := conn.ReadJSON(&frame); err != nil {
				t.Fatalf("read terminal: %v", err)
			}
			if frame.MessageType != externalagentprotocol.FrameTypeRunFailed {
				continue
			}
			if frame.RunTerminal == nil || !strings.Contains(frame.RunTerminal.FinalMessage, "native id missing") {
				t.Fatalf("unexpected terminal: %+v", frame)
			}
			close(terminalSent)
			<-ackSent
			_ = conn.WriteJSON(externalagentprotocol.Frame{
				MessageID:    "terminal-ack-1",
				MessageType:  externalagentprotocol.FrameTypeRunTerminalAck,
				PersonaID:    "persona-1",
				ConnectionID: "conn-1",
				RunID:        frame.RunID,
				AssignmentID: frame.AssignmentID,
				SentAt:       time.Now().UTC(),
				RunTerminalAck: &externalagentprotocol.RunTerminalAckPayload{
					AcknowledgedAt: time.Now().UTC(),
				},
			})
			return
		}
	}))
	defer gateway.Close()

	binding := config.Binding{
		ConnectionID:        "conn-1",
		PersonaID:           "persona-1",
		GatewayWebsocketURL: "ws" + gateway.URL[len("http"):],
		BridgeCredentialID:  "cred-1",
		BridgePrivateKey:    base64.StdEncoding.EncodeToString(privateKey),
		BridgePublicKey:     base64.StdEncoding.EncodeToString(publicKey),
		RuntimeKind:         runtime.AdapterKindHermes,
		ActiveRunID:         "run-1",
		ActiveAssignmentID:  "assignment-1",
	}
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{binding}})
	session, err := bridge.NewSession(binding, bridge.Credential{
		ID:         "cred-1",
		PrivateKey: privateKey,
		PublicKey:  publicKey,
	})
	if err != nil {
		t.Fatalf("new session: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- (Runner{Store: &store}).runBindingSession(ctx, binding, session)
	}()

	select {
	case <-terminalSent:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for terminal")
	}
	active, ok := store.Binding("conn-1")
	if !ok || active.ActiveRunID != "run-1" || active.ActiveAssignmentID != "assignment-1" {
		t.Fatalf("active run cleared before terminal ack: %+v", active)
	}
	close(ackSent)
	select {
	case err := <-errCh:
		if err == nil || !strings.Contains(err.Error(), "read gateway websocket frame") {
			t.Fatalf("run binding session error = %v, want retryable read failure", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for runner shutdown")
	}
	cleared, ok := store.Binding("conn-1")
	if !ok || cleared.ActiveRunID != "" || cleared.ActiveAssignmentID != "" {
		t.Fatalf("active run not cleared after terminal ack: %+v", cleared)
	}
}

type deadlineReplayAdapter struct {
	cancelStarted chan struct{}
	releaseCancel chan struct{}
}

func (deadlineReplayAdapter) Kind() runtime.AdapterKind {
	return runtime.AdapterKindHermes
}

func (deadlineReplayAdapter) Detect() runtime.Detection {
	return runtime.Detection{Kind: runtime.AdapterKindHermes, State: runtime.AdapterStateReady}
}

func (deadlineReplayAdapter) StartRun(runtime.RunRequest) (string, error) {
	return "", fmt.Errorf("not used")
}

func (deadlineReplayAdapter) StreamOrPollRun(ctx context.Context, _ string, _ runtime.RunEventHandler) (runtime.RunResult, error) {
	<-ctx.Done()
	return runtime.RunResult{}, ctx.Err()
}

func (adapter deadlineReplayAdapter) CancelRun(string) error {
	close(adapter.cancelStarted)
	<-adapter.releaseCancel
	return nil
}

func (deadlineReplayAdapter) Diagnose() runtime.Detection {
	return runtime.Detection{Kind: runtime.AdapterKindHermes, State: runtime.AdapterStateReady}
}

type sessionCancelReplayAdapter struct {
	cancelStarted chan struct{}
}

func (sessionCancelReplayAdapter) Kind() runtime.AdapterKind {
	return runtime.AdapterKindHermes
}

func (sessionCancelReplayAdapter) Detect() runtime.Detection {
	return runtime.Detection{Kind: runtime.AdapterKindHermes, State: runtime.AdapterStateReady}
}

func (sessionCancelReplayAdapter) StartRun(runtime.RunRequest) (string, error) {
	return "", fmt.Errorf("not used")
}

func (sessionCancelReplayAdapter) StreamOrPollRun(ctx context.Context, _ string, _ runtime.RunEventHandler) (runtime.RunResult, error) {
	<-ctx.Done()
	return runtime.RunResult{}, ctx.Err()
}

func (adapter sessionCancelReplayAdapter) CancelRun(string) error {
	adapter.cancelStarted <- struct{}{}
	return nil
}

func (sessionCancelReplayAdapter) Diagnose() runtime.Detection {
	return runtime.Detection{Kind: runtime.AdapterKindHermes, State: runtime.AdapterStateReady}
}

type completingReplayAdapter struct{}

func (completingReplayAdapter) Kind() runtime.AdapterKind {
	return runtime.AdapterKindHermes
}

func (completingReplayAdapter) Detect() runtime.Detection {
	return runtime.Detection{Kind: runtime.AdapterKindHermes, State: runtime.AdapterStateReady}
}

func (completingReplayAdapter) StartRun(runtime.RunRequest) (string, error) {
	return "", fmt.Errorf("not used")
}

func (completingReplayAdapter) StreamOrPollRun(context.Context, string, runtime.RunEventHandler) (runtime.RunResult, error) {
	return runtime.RunResult{Status: runtime.RunStatusSucceeded, Output: "done"}, nil
}

func (completingReplayAdapter) CancelRun(string) error {
	return nil
}

func (completingReplayAdapter) Diagnose() runtime.Detection {
	return runtime.Detection{Kind: runtime.AdapterKindHermes, State: runtime.AdapterStateReady}
}

type startingReplayAdapter struct{}

func (startingReplayAdapter) Kind() runtime.AdapterKind {
	return runtime.AdapterKindHermes
}

func (startingReplayAdapter) Detect() runtime.Detection {
	return runtime.Detection{Kind: runtime.AdapterKindHermes, State: runtime.AdapterStateReady}
}

func (startingReplayAdapter) StartRun(runtime.RunRequest) (string, error) {
	return "", fmt.Errorf("not used")
}

func (startingReplayAdapter) StreamOrPollRun(_ context.Context, _ string, handler runtime.RunEventHandler) (runtime.RunResult, error) {
	if err := handler(runtime.RunEvent{Kind: runtime.RunEventStarted, StartedAt: time.Now().UTC()}); err != nil {
		return runtime.RunResult{}, err
	}
	return runtime.RunResult{Status: runtime.RunStatusSucceeded, Output: "done"}, nil
}

func (startingReplayAdapter) CancelRun(string) error {
	return nil
}

func (startingReplayAdapter) Diagnose() runtime.Detection {
	return runtime.Detection{Kind: runtime.AdapterKindHermes, State: runtime.AdapterStateReady}
}

func TestRunnerForwardsStreamingRunEventsAfterAccepted(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	mcpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			w.Header().Set("Content-Type", "text/event-stream")
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
			<-r.Context().Done()
			return
		}
		var message map[string]any
		if err := json.NewDecoder(r.Body).Decode(&message); err != nil {
			t.Fatalf("decode mcp request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		switch message["method"] {
		case "initialize":
			w.Header().Set("MCP-Session-Id", "session-1")
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-11-25","capabilities":{}}}`))
		case "notifications/initialized":
			w.WriteHeader(http.StatusAccepted)
		case "tools/list":
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":2,"result":{"tools":[]}}`))
		default:
			t.Fatalf("unexpected mcp method: %v", message["method"])
		}
	}))
	defer mcpServer.Close()

	hermesServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health", "/health/detailed", "/v1/models":
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		case "/v1/capabilities":
			_, _ = w.Write([]byte(`{"features":{"run_submission":true,"run_status":true,"run_events_sse":true,"run_stop":true}}`))
		case "/v1/runs":
			if r.Method != http.MethodPost {
				http.NotFound(w, r)
				return
			}
			_, _ = w.Write([]byte(`{"run_id":"native-run-1"}`))
		case "/v1/runs/native-run-1/events":
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("data: {\"type\":\"progress\",\"data\":{\"deltaText\":\"chunk\",\"toolName\":\"browser\",\"phase\":\"started\",\"summary\":\"opening\"}}\n\n"))
			_, _ = w.Write([]byte("data: {\"type\":\"run.completed\",\"output\":\"done\"}\n\n"))
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer hermesServer.Close()

	t.Setenv("PERSONASTACK_CONNECTOR_HERMES_URL", hermesServer.URL)
	t.Setenv("HERMES_API_SERVER_KEY", "key-1")

	frames := make(chan externalagentprotocol.Frame, 8)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()
		var connectFrame externalagentprotocol.Frame
		if err := conn.ReadJSON(&connectFrame); err != nil {
			t.Fatalf("read connect: %v", err)
		}
		_ = conn.WriteJSON(externalagentprotocol.Frame{
			MessageType:  externalagentprotocol.FrameTypeConnectAccepted,
			PersonaID:    "persona-1",
			ConnectionID: "conn-1",
			SentAt:       time.Now(),
			ConnectAccepted: &externalagentprotocol.ConnectAcceptedPayload{
				ProtocolVersion:      externalagentprotocol.ProtocolVersionV4,
				ConnectionGeneration: 1,
				HeartbeatSeconds:     15,
			},
		})
		var heartbeat externalagentprotocol.Frame
		if err := conn.ReadJSON(&heartbeat); err != nil {
			if connectFrame.Connect.ConnectionGeneration != 2 && ctx.Err() != nil {
				return
			}
			t.Fatalf("read heartbeat: %v", err)
		}
		_ = conn.WriteJSON(externalagentprotocol.Frame{
			MessageID:    "probe-1",
			MessageType:  externalagentprotocol.FrameTypeWakeProbe,
			PersonaID:    "persona-1",
			ConnectionID: "conn-1",
			SentAt:       time.Now(),
			WakeProbe: &externalagentprotocol.WakeProbePayload{
				ProbeID:    "probe-1",
				DeadlineAt: time.Now().Add(time.Second),
			},
		})
		wakeAck := readFrameOfType(t, conn, externalagentprotocol.FrameTypeWakeProbeAccepted)
		if wakeAck.MessageType != externalagentprotocol.FrameTypeWakeProbeAccepted {
			t.Fatalf("unexpected wake probe accepted: %+v", wakeAck)
		}
		_ = conn.WriteJSON(externalagentprotocol.Frame{
			MessageID:    "run-start-1",
			MessageType:  externalagentprotocol.FrameTypeRunStart,
			PersonaID:    "persona-1",
			ConnectionID: "conn-1",
			SentAt:       time.Now(),
			RunID:        "run-1",
			AssignmentID: "assignment-1",
			RunStart: &externalagentprotocol.RunStartPayload{
				FullyComposedPrompt:    "prompt",
				NativeMCPServerName:    "personastack-conn-1",
				NativeMCPToolNamespace: "personastack",
				Metadata:               map[string]string{"source": "test"},
			},
		})
		for {
			var frame externalagentprotocol.Frame
			if err := conn.ReadJSON(&frame); err != nil {
				return
			}
			if frame.MessageType == externalagentprotocol.FrameTypeHeartbeat || frame.MessageType == externalagentprotocol.FrameTypeCapabilitiesReport {
				continue
			}
			frames <- frame
			if frame.MessageType == externalagentprotocol.FrameTypeRunCompleted {
				cancel()
				return
			}
		}
	}))
	defer gateway.Close()

	binding := config.Binding{
		ConnectionID:         "conn-1",
		PersonaID:            "persona-1",
		ConnectionGeneration: 1,
		GatewayWebsocketURL:  "ws" + gateway.URL[len("http"):],
		BridgeCredentialID:   "cred-1",
		BridgePrivateKey:     base64.StdEncoding.EncodeToString(privateKey),
		BridgePublicKey:      base64.StdEncoding.EncodeToString(publicKey),
		NativeMCPServer:      "personastack-conn-1",
		PersonaMCPURL:        mcpServer.URL,
		PersonaMCPToken:      "stable-token",
		HasPersonaMCPToken:   true,
		RuntimeKind:          runtime.AdapterKindHermes,
	}
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{binding}})
	keyring.MockInit()
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)
	t.Setenv("PERSONASTACK_CONNECTOR_DISABLE_HERMES_GATEWAY_START", "1")
	if _, err := (mcp.Installer{Store: &store, HomeDir: homeDir, ExecutablePath: "/usr/local/bin/personastack-connector", GOOS: "linux"}).InstallAll(); err != nil {
		t.Fatalf("install mcp: %v", err)
	}
	if err := (Runner{Store: &store, ReconnectMin: time.Millisecond, ReconnectMax: time.Millisecond}).RunForeground(ctx); err != nil {
		t.Fatalf("run foreground: %v", err)
	}
	got := []externalagentprotocol.Frame{}
	close(frames)
	for frame := range frames {
		got = append(got, frame)
	}
	if len(got) != 5 {
		t.Fatalf("unexpected frames: %+v", got)
	}
	if got[0].MessageType != externalagentprotocol.FrameTypeRunAccepted {
		t.Fatalf("first frame = %+v", got[0])
	}
	if got[1].MessageType != externalagentprotocol.FrameTypeRunStarted {
		t.Fatalf("second frame = %+v", got[1])
	}
	if got[2].MessageType != externalagentprotocol.FrameTypeRunOutputDelta {
		t.Fatalf("third frame = %+v", got[2])
	}
	if got[3].MessageType != externalagentprotocol.FrameTypeRunToolEvent {
		t.Fatalf("fourth frame = %+v", got[3])
	}
	if got[4].MessageType != externalagentprotocol.FrameTypeRunCompleted {
		t.Fatalf("fifth frame = %+v", got[4])
	}
	if got[4].RunTerminal == nil || got[4].RunTerminal.FinalMessage != "done" {
		t.Fatalf("unexpected terminal output: %+v", got[4])
	}
}

func TestRunnerTokenRevokedDeletesBindingAndStopsReconnect(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)
	t.Setenv("HERMES_API_SERVER_KEY", "test-hermes-api-key")
	t.Setenv("PERSONASTACK_CONNECTOR_DISABLE_HERMES_GATEWAY_START", "1")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()
		var connectFrame externalagentprotocol.Frame
		if err := conn.ReadJSON(&connectFrame); err != nil {
			t.Fatalf("read connect: %v", err)
		}
		_ = conn.WriteJSON(externalagentprotocol.Frame{
			MessageType:  externalagentprotocol.FrameTypeConnectAccepted,
			PersonaID:    "persona-1",
			ConnectionID: "conn-1",
			SentAt:       time.Now(),
			ConnectAccepted: &externalagentprotocol.ConnectAcceptedPayload{
				ProtocolVersion:      externalagentprotocol.ProtocolVersionV4,
				ConnectionGeneration: 1,
				HeartbeatSeconds:     15,
			},
		})
		var heartbeat externalagentprotocol.Frame
		if err := conn.ReadJSON(&heartbeat); err != nil {
			t.Fatalf("read heartbeat: %v", err)
		}
		_ = conn.WriteJSON(externalagentprotocol.Frame{
			MessageID:     "refresh-1",
			MessageType:   externalagentprotocol.FrameTypeConfigRefresh,
			PersonaID:     "persona-1",
			ConnectionID:  "conn-1",
			SentAt:        time.Now(),
			ConfigRefresh: &externalagentprotocol.ConfigRefreshPayload{},
		})
		deadline := time.After(2 * time.Second)
		for {
			if _, err := os.ReadFile(filepath.Join(homeDir, ".hermes", "config.yaml")); err == nil {
				break
			}
			select {
			case <-deadline:
				t.Fatal("timed out waiting for config refresh")
			case <-time.After(time.Millisecond):
			}
		}
		_ = conn.WriteJSON(externalagentprotocol.Frame{
			MessageID:    "revoke-1",
			MessageType:  externalagentprotocol.FrameTypeTokenRevoked,
			PersonaID:    "persona-1",
			ConnectionID: "conn-1",
			SentAt:       time.Now(),
			TokenRevoked: &externalagentprotocol.TokenRevokedPayload{
				TokenKind: "bridge",
				Reason:    "user_requested",
			},
		})
	}))
	defer server.Close()

	binding := config.Binding{
		ConnectionID:         "conn-1",
		PersonaID:            "persona-1",
		ConnectionGeneration: 1,
		GatewayWebsocketURL:  "ws" + server.URL[len("http"):],
		BridgeCredentialID:   "cred-1",
		BridgePrivateKey:     base64.StdEncoding.EncodeToString(privateKey),
		BridgePublicKey:      base64.StdEncoding.EncodeToString(publicKey),
		NativeMCPServer:      "personastack-conn-1",
		PersonaMCPURL:        "https://mcp.personastack.ai/mcp",
		PersonaMCPToken:      "stable-token",
		HasPersonaMCPToken:   true,
		RuntimeKind:          runtime.AdapterKindHermes,
	}
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{binding}})
	ctx, cancel := context.WithCancel(t.Context())
	errCh := make(chan error, 1)
	go func() {
		errCh <- (Runner{Store: &store, ReconnectMin: time.Millisecond, ReconnectMax: time.Millisecond}).RunForeground(ctx)
	}()
	deadline := time.After(2 * time.Second)
	for {
		if _, ok := store.Binding("conn-1"); !ok {
			break
		}
		select {
		case err := <-errCh:
			t.Fatalf("run foreground returned before token revocation was stored: %v", err)
		case <-deadline:
			t.Fatal("timed out waiting for token revocation to delete binding")
		case <-time.After(time.Millisecond):
		}
	}
	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run foreground: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for runner shutdown")
	}
	if _, ok := store.Binding("conn-1"); ok {
		t.Fatalf("expected token revocation to delete binding")
	}
	if _, err := os.ReadFile(filepath.Join(homeDir, ".hermes", "config.yaml")); err != nil {
		t.Fatalf("expected config refresh to write Hermes config: %v", err)
	}
}

func TestCanStartRunWithReadiness(t *testing.T) {
	now := time.Now().UTC()
	if !canStartRunWithReadiness(runtime.AdapterStateMCPVerified, &now) {
		t.Fatalf("mcp verified should be runnable")
	}
	if canStartRunWithReadiness(runtime.AdapterStateMCPVerified, nil) {
		t.Fatalf("mcp verified without wake probe should not be runnable")
	}
	if !canStartRunWithReadiness(runtime.AdapterStateReady, nil) {
		t.Fatalf("ready should be runnable")
	}
	for _, state := range []runtime.AdapterState{
		runtime.AdapterStateRuntimeMissing,
		runtime.AdapterStateRuntimeStopped,
		runtime.AdapterStateAuthMissing,
		runtime.AdapterStateCapabilityMissing,
		runtime.AdapterStateMCPConfigMissing,
		runtime.AdapterStateMCPRestartRequired,
		runtime.AdapterStateWakeProbeFailed,
	} {
		if canStartRunWithReadiness(state, &now) {
			t.Fatalf("%s should not be runnable", state)
		}
	}
}

func TestRunLifecycleUpdatesBinding(t *testing.T) {
	binding := config.Binding{ConnectionID: "conn-1", PersonaID: "persona-1", PersonaMCPToken: "stable-token"}
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{binding}})
	runner := Runner{Store: &store}
	frame := externalagentprotocol.Frame{
		RunID:        "run-1",
		AssignmentID: "assignment-1",
		RunStart: &externalagentprotocol.RunStartPayload{
			DeadlineAt: time.Now().UTC().Add(time.Minute),
		},
	}
	if err := runner.activateRun(binding, frame); err != nil {
		t.Fatalf("activate run: %v", err)
	}
	active, ok := store.Binding("conn-1")
	if !ok || active.ActiveRunID != "run-1" || active.ActiveAssignmentID != "assignment-1" || active.ActiveRunDeadlineAt.IsZero() {
		t.Fatalf("active run not stored: %+v", active)
	}
	if err := runner.recordNativeRunID(binding, "run-1", "native-1"); err != nil {
		t.Fatalf("record native run id: %v", err)
	}
	active, ok = store.Binding("conn-1")
	if !ok || active.ActiveNativeRunID != "native-1" {
		t.Fatalf("native run id not stored: %+v", active)
	}
	nativeRunID, err := runner.nativeRunIDForCancel(binding, "run-1")
	if err != nil {
		t.Fatalf("native run id for cancel: %v", err)
	}
	if nativeRunID != "native-1" {
		t.Fatalf("native run id for cancel = %q", nativeRunID)
	}
	if err := runner.clearRunState(binding, "run-1"); err != nil {
		t.Fatalf("clear run state: %v", err)
	}
	cleared, ok := store.Binding("conn-1")
	if !ok || cleared.ActiveRunID != "" || cleared.ActiveAssignmentID != "" || cleared.ActiveNativeRunID != "" || !cleared.ActiveRunDeadlineAt.IsZero() {
		t.Fatalf("active run not cleared: %+v", cleared)
	}
}

func TestRecordNativeRunIDFailsWhenBindingMissing(t *testing.T) {
	store := config.NewMemoryStore(config.State{})
	runner := Runner{Store: &store}
	err := runner.recordNativeRunID(config.Binding{ConnectionID: "conn-missing"}, "run-1", "native-1")
	if err == nil {
		t.Fatalf("expected missing binding error")
	}
}

func TestRecordNativeRunIDFailsWhenActiveRunChanges(t *testing.T) {
	binding := config.Binding{
		ConnectionID: "conn-1",
		PersonaID:    "persona-1",
		ActiveRunID:  "run-2",
	}
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{binding}})
	runner := Runner{Store: &store}
	err := runner.recordNativeRunID(binding, "run-1", "native-1")
	if err == nil {
		t.Fatalf("expected active run mismatch error")
	}
	active, ok := store.Binding("conn-1")
	if !ok || active.ActiveNativeRunID != "" {
		t.Fatalf("native run id should not be stored after mismatch: %+v", active)
	}
}

func TestActiveNativeRunIDForRunStartDeduplicatesRedelivery(t *testing.T) {
	binding := config.Binding{
		ConnectionID:       "conn-1",
		PersonaID:          "persona-1",
		ActiveRunID:        "run-1",
		ActiveAssignmentID: "assignment-1",
		ActiveNativeRunID:  "native-1",
	}
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{binding}})
	runner := Runner{Store: &store}
	frame := externalagentprotocol.Frame{
		RunID:        "run-1",
		AssignmentID: "assignment-1",
		RunStart:     &externalagentprotocol.RunStartPayload{},
	}

	nativeRunID, ok := runner.activeNativeRunIDForRunStart(binding, frame)
	if !ok || nativeRunID != "native-1" {
		t.Fatalf("expected redelivered native run id, got ok=%t native=%q", ok, nativeRunID)
	}

	frame.RunID = "run-2"
	nativeRunID, ok = runner.activeNativeRunIDForRunStart(binding, frame)
	if ok || nativeRunID != "" {
		t.Fatalf("unexpected match for different run, got ok=%t native=%q", ok, nativeRunID)
	}
	frame.RunID = "run-1"
	frame.AssignmentID = "assignment-2"
	nativeRunID, ok = runner.activeNativeRunIDForRunStart(binding, frame)
	if ok || nativeRunID != "" {
		t.Fatalf("unexpected match for different assignment, got ok=%t native=%q", ok, nativeRunID)
	}
}

func TestCommandFrameCacheReplaysMultipleRunReplies(t *testing.T) {
	cache := newCommandFrameCache()
	request := externalagentprotocol.Frame{MessageID: "message-1"}
	accepted := externalagentprotocol.Frame{MessageType: externalagentprotocol.FrameTypeRunAccepted}
	started := externalagentprotocol.Frame{MessageType: externalagentprotocol.FrameTypeRunStarted}

	cache.storeReplies(request, []externalagentprotocol.Frame{accepted, started})
	replies, ok := cache.cachedReplies(request)
	if !ok || len(replies) != 2 {
		t.Fatalf("expected two cached replies, got ok=%t replies=%+v", ok, replies)
	}
	if replies[0].MessageType != externalagentprotocol.FrameTypeRunAccepted || replies[1].MessageType != externalagentprotocol.FrameTypeRunStarted {
		t.Fatalf("unexpected cached replies: %+v", replies)
	}
}

func TestDuplicateRunStartMessageIDReplaysCachedStartedFrame(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)
	t.Setenv("HERMES_API_SERVER_KEY", "key-1")
	t.Setenv("PERSONASTACK_CONNECTOR_DISABLE_HERMES_GATEWAY_START", "1")

	mcpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			w.Header().Set("Content-Type", "text/event-stream")
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
			<-r.Context().Done()
			return
		}
		var message map[string]any
		if err := json.NewDecoder(r.Body).Decode(&message); err != nil {
			t.Fatalf("decode mcp request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		switch message["method"] {
		case "initialize":
			w.Header().Set("MCP-Session-Id", "session-1")
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-11-25","capabilities":{}}}`))
		case "notifications/initialized":
			w.WriteHeader(http.StatusAccepted)
		case "tools/list":
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":2,"result":{"tools":[]}}`))
		default:
			t.Fatalf("unexpected mcp method: %v", message["method"])
		}
	}))
	defer mcpServer.Close()

	startedAt := time.Date(2026, 5, 23, 12, 30, 0, 0, time.UTC)
	hermesServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health", "/health/detailed", "/v1/models":
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		case "/v1/capabilities":
			_, _ = w.Write([]byte(`{"features":{"run_submission":true,"run_status":true,"run_events_sse":true,"run_stop":true}}`))
		case "/v1/runs":
			_, _ = w.Write([]byte(`{"run_id":"native-run-1"}`))
		case "/v1/runs/native-run-1/events":
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte(`data: {"type":"run.started","data":{"started_at":"2026-05-23T12:30:00Z"}}` + "\n\n"))
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
			<-r.Context().Done()
		default:
			http.NotFound(w, r)
		}
	}))
	defer hermesServer.Close()
	t.Setenv("PERSONASTACK_CONNECTOR_HERMES_URL", hermesServer.URL)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	replayed := make(chan externalagentprotocol.Frame, 1)
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()
		var connectFrame externalagentprotocol.Frame
		if err := conn.ReadJSON(&connectFrame); err != nil {
			t.Fatalf("read connect: %v", err)
		}
		_ = conn.WriteJSON(externalagentprotocol.Frame{
			MessageType:  externalagentprotocol.FrameTypeConnectAccepted,
			PersonaID:    "persona-1",
			ConnectionID: "conn-1",
			SentAt:       time.Now().UTC(),
			ConnectAccepted: &externalagentprotocol.ConnectAcceptedPayload{
				ProtocolVersion:      externalagentprotocol.ProtocolVersionV4,
				ConnectionGeneration: 1,
				HeartbeatSeconds:     15,
			},
		})
		_ = conn.WriteJSON(externalagentprotocol.Frame{
			MessageID:    "probe-1",
			MessageType:  externalagentprotocol.FrameTypeWakeProbe,
			PersonaID:    "persona-1",
			ConnectionID: "conn-1",
			SentAt:       time.Now().UTC(),
			WakeProbe:    &externalagentprotocol.WakeProbePayload{ProbeID: "probe-1", DeadlineAt: time.Now().UTC().Add(time.Second)},
		})
		for {
			var frame externalagentprotocol.Frame
			if err := conn.ReadJSON(&frame); err != nil {
				return
			}
			if frame.MessageType == externalagentprotocol.FrameTypeWakeProbeAccepted {
				break
			}
		}
		start := externalagentprotocol.Frame{
			MessageID:    "run-start-1",
			MessageType:  externalagentprotocol.FrameTypeRunStart,
			PersonaID:    "persona-1",
			ConnectionID: "conn-1",
			RunID:        "run-1",
			AssignmentID: "assignment-1",
			SentAt:       time.Now().UTC(),
			RunStart: &externalagentprotocol.RunStartPayload{
				FullyComposedPrompt:    "prompt",
				NativeMCPServerName:    "personastack-conn-1",
				NativeMCPToolNamespace: "personastack",
			},
		}
		_ = conn.WriteJSON(start)
		for {
			var frame externalagentprotocol.Frame
			if err := conn.ReadJSON(&frame); err != nil {
				return
			}
			if frame.MessageType == externalagentprotocol.FrameTypeRunStarted {
				break
			}
		}
		_ = conn.WriteJSON(start)
		for {
			var frame externalagentprotocol.Frame
			if err := conn.ReadJSON(&frame); err != nil {
				return
			}
			if frame.MessageType != externalagentprotocol.FrameTypeRunStarted {
				continue
			}
			replayed <- frame
			cancel()
			return
		}
	}))
	defer gateway.Close()

	binding := config.Binding{
		ConnectionID:         "conn-1",
		PersonaID:            "persona-1",
		ConnectionGeneration: 1,
		GatewayWebsocketURL:  "ws" + gateway.URL[len("http"):],
		BridgeCredentialID:   "cred-1",
		BridgePrivateKey:     base64.StdEncoding.EncodeToString(privateKey),
		BridgePublicKey:      base64.StdEncoding.EncodeToString(publicKey),
		NativeMCPServer:      "personastack-conn-1",
		NativeMCPNamespace:   "personastack",
		PersonaMCPURL:        mcpServer.URL,
		PersonaMCPToken:      "stable-token",
		HasPersonaMCPToken:   true,
		RuntimeKind:          runtime.AdapterKindHermes,
	}
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{binding}})
	keyring.MockInit()
	if _, err := (mcp.Installer{Store: &store, HomeDir: homeDir, ExecutablePath: "/usr/local/bin/personastack-connector", GOOS: "linux"}).InstallAll(); err != nil {
		t.Fatalf("install mcp: %v", err)
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- (Runner{Store: &store, ReconnectMin: time.Millisecond, ReconnectMax: time.Millisecond}).RunForeground(ctx)
	}()

	select {
	case frame := <-replayed:
		if frame.RunStarted == nil || !frame.RunStarted.StartedAt.Equal(startedAt) {
			t.Fatalf("duplicate run.start did not replay cached started frame: %+v", frame)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for duplicate run.start replay")
	}
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run foreground: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for runner shutdown")
	}
}

func TestRunnerReplaysActiveRunOnReconnect(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	session, err := bridge.NewSession(config.Binding{
		ConnectionID:         "conn-1",
		PersonaID:            "persona-1",
		RuntimeKind:          runtime.AdapterKindHermes,
		NativeMCPServer:      "personastack-conn-1",
		NativeMCPNamespace:   "personastack",
		ConnectionGeneration: 2,
	}, bridge.Credential{
		ID:         "cred-1",
		PrivateKey: privateKey,
		PublicKey:  publicKey,
	})
	if err != nil {
		t.Fatalf("new session: %v", err)
	}
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{{
		ConnectionID:         "conn-1",
		PersonaID:            "persona-1",
		ActiveRunID:          "run-1",
		ActiveAssignmentID:   "assignment-1",
		ActiveNativeRunID:    "native-1",
		ConnectionGeneration: 2,
	}}})
	runner := Runner{Store: &store}
	frames := make([]externalagentprotocol.Frame, 0, 2)
	if err := runner.replayActiveRun(context.Background(), config.Binding{ConnectionID: "conn-1", ConnectionGeneration: 2}, session, nil, newRunObservationRegistry(), func(frame externalagentprotocol.Frame) error {
		frames = append(frames, frame)
		return nil
	}); err != nil {
		t.Fatalf("replay active run: %v", err)
	}
	if len(frames) != 2 {
		t.Fatalf("unexpected replay frames: %+v", frames)
	}
	if frames[0].MessageType != externalagentprotocol.FrameTypeRunAccepted || frames[0].RunAccepted == nil || frames[0].RunAccepted.NativeRunID != "native-1" {
		t.Fatalf("unexpected accepted replay: %+v", frames[0])
	}
	if frames[1].MessageType != externalagentprotocol.FrameTypeRunStarted || frames[1].RunStarted == nil || frames[1].RunStarted.NativeRunID != "native-1" {
		t.Fatalf("unexpected started replay: %+v", frames[1])
	}
	frames = frames[:0]
	if err := runner.replayActiveRun(context.Background(), config.Binding{ConnectionID: "conn-1", ConnectionGeneration: 1}, session, nil, newRunObservationRegistry(), func(frame externalagentprotocol.Frame) error {
		frames = append(frames, frame)
		return nil
	}); err != nil {
		t.Fatalf("stale replay active run: %v", err)
	}
	if len(frames) != 0 {
		t.Fatalf("stale generation replayed active run: %+v", frames)
	}
}

func TestRunnerClearsExpiredActiveRunBeforeReplay(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	now := time.Date(2026, 6, 23, 2, 40, 0, 0, time.UTC)
	session, err := bridge.NewSession(config.Binding{
		ConnectionID:         "conn-1",
		PersonaID:            "persona-1",
		RuntimeKind:          runtime.AdapterKindHermes,
		NativeMCPServer:      "personastack-conn-1",
		NativeMCPNamespace:   "personastack",
		ConnectionGeneration: 2,
	}, bridge.Credential{
		ID:         "cred-1",
		PrivateKey: privateKey,
		PublicKey:  publicKey,
	})
	if err != nil {
		t.Fatalf("new session: %v", err)
	}
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{{
		ConnectionID:         "conn-1",
		PersonaID:            "persona-1",
		ActiveRunID:          "run-1",
		ActiveAssignmentID:   "assignment-1",
		ActiveNativeRunID:    "native-1",
		ActiveRunDeadlineAt:  now.Add(-time.Minute),
		ConnectionGeneration: 2,
	}}})
	adapter := sessionCancelReplayAdapter{cancelStarted: make(chan struct{}, 1)}
	runner := Runner{Store: &store, Now: func() time.Time { return now }}
	frames := make([]externalagentprotocol.Frame, 0, 2)
	if err := runner.replayActiveRun(context.Background(), config.Binding{ConnectionID: "conn-1", ConnectionGeneration: 2}, session, adapter, newRunObservationRegistry(), func(frame externalagentprotocol.Frame) error {
		frames = append(frames, frame)
		return nil
	}); err != nil {
		t.Fatalf("replay active run: %v", err)
	}
	if len(frames) != 0 {
		t.Fatalf("expired active run replayed frames: %+v", frames)
	}
	cleared, ok := store.Binding("conn-1")
	if !ok || cleared.ActiveRunID != "" || cleared.ActiveAssignmentID != "" || cleared.ActiveNativeRunID != "" || !cleared.ActiveRunDeadlineAt.IsZero() {
		t.Fatalf("expired active run was not cleared: %+v", cleared)
	}
	select {
	case <-adapter.cancelStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for expired native run cancellation")
	}
}

func TestRunnerGatesRedeliveredRunStartUntilWakeProbe(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)
	t.Setenv("HERMES_API_SERVER_KEY", "hermes-key-1")
	if err := os.MkdirAll(filepath.Join(homeDir, ".hermes"), 0o700); err != nil {
		t.Fatalf("create hermes config dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(homeDir, ".hermes", "config.yaml"), []byte(
		"mcp_servers:\n"+
			"  personastack-conn-1:\n"+
			"    command: /opt/personastack-connector\n"+
			"    args:\n"+
			"      - mcp\n"+
			"      - stdio\n"+
			"      - --binding\n"+
			"      - conn-1\n",
	), 0o600); err != nil {
		t.Fatalf("write hermes config: %v", err)
	}
	mcpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Method string `json:"method"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("decode mcp request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		switch request.Method {
		case "initialize":
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-11-25","capabilities":{}}}`))
		case "notifications/initialized":
			w.WriteHeader(http.StatusOK)
		case "tools/list":
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":2,"result":{"tools":[]}}`))
		default:
			t.Fatalf("unexpected mcp method: %s", request.Method)
		}
	}))
	defer mcpServer.Close()
	hermesServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health", "/health/detailed", "/v1/models":
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		case "/v1/capabilities":
			_, _ = w.Write([]byte(`{"features":{"run_submission":true,"run_status":true,"run_events_sse":true,"run_stop":true}}`))
		case "/v1/runs/native-1":
			_, _ = w.Write([]byte(`{"status":"running"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer hermesServer.Close()
	t.Setenv("PERSONASTACK_CONNECTOR_HERMES_URL", hermesServer.URL)

	runReplay := make(chan []externalagentprotocol.Frame, 1)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()
		var connectFrame externalagentprotocol.Frame
		if err := conn.ReadJSON(&connectFrame); err != nil {
			t.Fatalf("read connect: %v", err)
		}
		_ = conn.WriteJSON(externalagentprotocol.Frame{
			MessageType:  externalagentprotocol.FrameTypeConnectAccepted,
			PersonaID:    "persona-1",
			ConnectionID: "conn-1",
			SentAt:       time.Now().UTC(),
			ConnectAccepted: &externalagentprotocol.ConnectAcceptedPayload{
				ProtocolVersion:      externalagentprotocol.ProtocolVersionV4,
				ConnectionGeneration: connectFrame.Connect.ConnectionGeneration,
				HeartbeatSeconds:     15,
			},
		})
		readRunFrame := func() externalagentprotocol.Frame {
			for {
				var frame externalagentprotocol.Frame
				if err := conn.ReadJSON(&frame); err != nil {
					t.Fatalf("read frame: %v", err)
				}
				if frame.MessageType != externalagentprotocol.FrameTypeHeartbeat && frame.MessageType != externalagentprotocol.FrameTypeCapabilitiesReport {
					return frame
				}
			}
		}
		if replay := readRunFrame(); replay.MessageType != externalagentprotocol.FrameTypeRunAccepted {
			t.Fatalf("expected replay accepted, got %+v", replay)
		}
		if replay := readRunFrame(); replay.MessageType != externalagentprotocol.FrameTypeRunStarted {
			t.Fatalf("expected replay started, got %+v", replay)
		}
		_ = conn.WriteJSON(externalagentprotocol.Frame{
			MessageID:    "redelivery-1",
			MessageType:  externalagentprotocol.FrameTypeRunStart,
			PersonaID:    "persona-1",
			ConnectionID: "conn-1",
			RunID:        "run-1",
			AssignmentID: "assignment-1",
			SentAt:       time.Now().UTC(),
			RunStart: &externalagentprotocol.RunStartPayload{
				FullyComposedPrompt: "prompt",
			},
		})
		runReplay <- []externalagentprotocol.Frame{readRunFrame(), readRunFrame()}
		cancel()
	}))
	defer gateway.Close()

	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{{
		ConnectionID:         "conn-1",
		PersonaID:            "persona-1",
		ConnectionGeneration: 1,
		GatewayWebsocketURL:  "ws" + gateway.URL[len("http"):],
		BridgeCredentialID:   "cred-1",
		BridgePrivateKey:     base64.StdEncoding.EncodeToString(privateKey),
		BridgePublicKey:      base64.StdEncoding.EncodeToString(publicKey),
		NativeMCPServer:      "personastack-conn-1",
		PersonaMCPURL:        mcpServer.URL,
		PersonaMCPToken:      "mcp-token-1",
		ActiveRunID:          "run-1",
		ActiveAssignmentID:   "assignment-1",
		ActiveNativeRunID:    "native-1",
		RuntimeKind:          runtime.AdapterKindHermes,
	}}})
	errCh := make(chan error, 1)
	go func() {
		errCh <- (Runner{Store: &store, ReconnectMin: time.Millisecond, ReconnectMax: time.Millisecond}).RunForeground(ctx)
	}()

	select {
	case frames := <-runReplay:
		if len(frames) != 2 || frames[0].MessageType != externalagentprotocol.FrameTypeRunAccepted || frames[1].MessageType != externalagentprotocol.FrameTypeRunStarted {
			t.Fatalf("expected redelivered run.start accepted and started, got %+v", frames)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for redelivered run.start replay")
	}
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run foreground: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for runner shutdown")
	}
}

func TestRunnerReconnectsWithFreshGenerationAfterDrainHint(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)
	t.Setenv("HERMES_API_SERVER_KEY", "hermes-key-1")
	t.Setenv("PERSONASTACK_CONNECTOR_DISABLE_HERMES_GATEWAY_START", "1")

	mcpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer mcp-token-1" {
			t.Fatalf("unexpected mcp authorization: %q", got)
		}
		var request struct {
			Method string `json:"method"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("decode mcp request: %v", err)
		}
		switch request.Method {
		case "initialize":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-11-25","capabilities":{},"serverInfo":{"name":"personastack-connector","version":"test"}}}`))
		case "notifications/initialized":
			w.WriteHeader(http.StatusOK)
		case "tools/list":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":2,"result":{"tools":[]}}`))
		default:
			t.Fatalf("unexpected mcp method: %s", request.Method)
		}
	}))
	defer mcpServer.Close()

	err = os.MkdirAll(filepath.Join(homeDir, ".hermes"), 0o700)
	if err != nil {
		t.Fatalf("create hermes config dir: %v", err)
	}
	err = os.WriteFile(filepath.Join(homeDir, ".hermes", "config.yaml"), []byte(
		"mcp_servers:\n"+
			"  personastack-conn-1:\n"+
			"    command: /opt/personastack-connector\n"+
			"    args:\n"+
			"      - mcp\n"+
			"      - stdio\n"+
			"      - --binding\n"+
			"      - conn-1\n",
	), 0o600)
	if err != nil {
		t.Fatalf("write hermes config: %v", err)
	}

	hermesServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health", "/health/detailed", "/v1/models":
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		case "/v1/capabilities":
			_, _ = w.Write([]byte(`{"features":{"run_submission":true,"run_status":true,"run_events_sse":true,"run_stop":true}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer hermesServer.Close()
	t.Setenv("PERSONASTACK_CONNECTOR_HERMES_URL", hermesServer.URL)

	connectGenerations := make(chan int64, 2)
	firstHandshakeDone := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()

		var connectFrame externalagentprotocol.Frame
		if err := conn.ReadJSON(&connectFrame); err != nil {
			t.Fatalf("read connect: %v", err)
		}
		if connectFrame.MessageType != externalagentprotocol.FrameTypeConnect {
			t.Fatalf("unexpected connect frame: %+v", connectFrame)
		}
		if err := conn.WriteJSON(externalagentprotocol.Frame{
			MessageType:  externalagentprotocol.FrameTypeConnectAccepted,
			PersonaID:    "persona-1",
			ConnectionID: "conn-1",
			SentAt:       time.Now().UTC(),
			ConnectAccepted: &externalagentprotocol.ConnectAcceptedPayload{
				ProtocolVersion:      externalagentprotocol.ProtocolVersionV4,
				ConnectionGeneration: connectFrame.Connect.ConnectionGeneration,
				HeartbeatSeconds:     15,
			},
		}); err != nil {
			t.Fatalf("write connect accepted: %v", err)
		}

		var heartbeat externalagentprotocol.Frame
		if err := conn.ReadJSON(&heartbeat); err != nil {
			t.Fatalf("read heartbeat: %v", err)
		}
		if heartbeat.MessageType != externalagentprotocol.FrameTypeHeartbeat || heartbeat.Heartbeat == nil {
			t.Fatalf("unexpected heartbeat: %+v", heartbeat)
		}
		if heartbeat.Heartbeat.ReadinessStatus == externalagentprotocol.ReadinessStatusWakeable {
			t.Fatalf("heartbeat reused stale wake probe for Hermes MCP verification: %+v", heartbeat.Heartbeat)
		}
		connectGenerations <- connectFrame.Connect.ConnectionGeneration
		if connectFrame.Connect.ConnectionGeneration == 2 {
			firstHandshakeDone <- struct{}{}
			if err := conn.WriteJSON(externalagentprotocol.Frame{
				MessageType:  externalagentprotocol.FrameTypeServerDraining,
				PersonaID:    "persona-1",
				ConnectionID: "conn-1",
				SentAt:       time.Now().UTC(),
				ServerDraining: &externalagentprotocol.ServerDrainingPayload{
					DeadlineAt: time.Now().UTC().Add(500 * time.Millisecond),
					Reason:     "agent-gateway draining",
				},
			}); err != nil {
				t.Fatalf("write draining hint: %v", err)
			}
			time.Sleep(75 * time.Millisecond)
			return
		}
		cancel()
	}))
	defer gateway.Close()

	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{{
		ConnectionID:            "conn-1",
		PersonaID:               "persona-1",
		ConnectionGeneration:    1,
		GatewayWebsocketURL:     "ws" + gateway.URL[len("http"):],
		BridgeCredentialID:      "cred-1",
		BridgePrivateKey:        base64.StdEncoding.EncodeToString(privateKey),
		BridgePublicKey:         base64.StdEncoding.EncodeToString(publicKey),
		NativeMCPServer:         "personastack-conn-1",
		NativeMCPNamespace:      "personastack",
		PersonaMCPURL:           mcpServer.URL,
		PersonaMCPToken:         "mcp-token-1",
		LastWakeProbeAt:         time.Now().UTC().Add(-time.Minute),
		LastWakeProbeGeneration: 1,
		RuntimeKind:             runtime.AdapterKindHermes,
		HasBridgeSecret:         true,
		HasPersonaMCPToken:      true,
	}}})
	keyring.MockInit()
	errCh := make(chan error, 1)
	go func() {
		errCh <- (Runner{Store: &store, ReconnectMin: time.Millisecond, ReconnectMax: time.Millisecond}).RunForeground(ctx)
	}()
	select {
	case <-firstHandshakeDone:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for drain handshake")
	}
	firstGeneration := <-connectGenerations
	secondGeneration := <-connectGenerations
	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run foreground: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for runner shutdown")
	}
	if firstGeneration != 2 || secondGeneration != 3 {
		t.Fatalf("unexpected reconnect generations: first=%d second=%d", firstGeneration, secondGeneration)
	}
}

func TestRunnerReconnectsWhenWebsocketReadDeadlineExpires(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	connectGenerations := make(chan int64, 2)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()

		var connectFrame externalagentprotocol.Frame
		if err := conn.ReadJSON(&connectFrame); err != nil {
			t.Fatalf("read connect: %v", err)
		}
		if err := conn.WriteJSON(externalagentprotocol.Frame{
			MessageType:  externalagentprotocol.FrameTypeConnectAccepted,
			PersonaID:    "persona-1",
			ConnectionID: "conn-1",
			SentAt:       time.Now().UTC(),
			ConnectAccepted: &externalagentprotocol.ConnectAcceptedPayload{
				ProtocolVersion:      externalagentprotocol.ProtocolVersionV4,
				ConnectionGeneration: connectFrame.Connect.ConnectionGeneration,
				HeartbeatSeconds:     15,
			},
		}); err != nil {
			t.Fatalf("write connect accepted: %v", err)
		}
		var heartbeat externalagentprotocol.Frame
		if err := conn.ReadJSON(&heartbeat); err != nil {
			t.Fatalf("read heartbeat: %v", err)
		}
		connectGenerations <- connectFrame.Connect.ConnectionGeneration
		if connectFrame.Connect.ConnectionGeneration >= 3 {
			cancel()
			return
		}
		<-ctx.Done()
	}))
	defer gateway.Close()

	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{{
		ConnectionID:         "conn-1",
		PersonaID:            "persona-1",
		ConnectionGeneration: 1,
		GatewayWebsocketURL:  "ws" + gateway.URL[len("http"):],
		BridgeCredentialID:   "cred-1",
		BridgePrivateKey:     base64.StdEncoding.EncodeToString(privateKey),
		BridgePublicKey:      base64.StdEncoding.EncodeToString(publicKey),
		RuntimeKind:          runtime.AdapterKindHermes,
		HasBridgeSecret:      true,
	}}})
	errCh := make(chan error, 1)
	go func() {
		errCh <- (Runner{Store: &store, ReconnectMin: time.Millisecond, ReconnectMax: time.Millisecond, ReadTimeout: 20 * time.Millisecond}).RunForeground(ctx)
	}()
	firstGeneration := <-connectGenerations
	secondGeneration := <-connectGenerations
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run foreground: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for runner shutdown")
	}
	if firstGeneration != 2 || secondGeneration != 3 {
		t.Fatalf("unexpected reconnect generations: first=%d second=%d", firstGeneration, secondGeneration)
	}
}

func TestRunnerEstablishedWebsocketReadFailuresBackoff(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	connectTimes := make(chan time.Time, 3)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()

		var connectFrame externalagentprotocol.Frame
		if err := conn.ReadJSON(&connectFrame); err != nil {
			t.Fatalf("read connect: %v", err)
		}
		if err := conn.WriteJSON(externalagentprotocol.Frame{
			MessageType:  externalagentprotocol.FrameTypeConnectAccepted,
			PersonaID:    "persona-1",
			ConnectionID: "conn-1",
			SentAt:       time.Now().UTC(),
			ConnectAccepted: &externalagentprotocol.ConnectAcceptedPayload{
				ProtocolVersion:      externalagentprotocol.ProtocolVersionV4,
				ConnectionGeneration: connectFrame.Connect.ConnectionGeneration,
				HeartbeatSeconds:     15,
			},
		}); err != nil {
			t.Fatalf("write connect accepted: %v", err)
		}
		var heartbeat externalagentprotocol.Frame
		if err := conn.ReadJSON(&heartbeat); err != nil {
			t.Fatalf("read heartbeat: %v", err)
		}
		connectTimes <- time.Now()
	}))
	defer gateway.Close()

	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{{
		ConnectionID:         "conn-1",
		PersonaID:            "persona-1",
		ConnectionGeneration: 1,
		GatewayWebsocketURL:  "ws" + gateway.URL[len("http"):],
		BridgeCredentialID:   "cred-1",
		BridgePrivateKey:     base64.StdEncoding.EncodeToString(privateKey),
		BridgePublicKey:      base64.StdEncoding.EncodeToString(publicKey),
		RuntimeKind:          runtime.AdapterKindAuto,
		HasBridgeSecret:      true,
	}}})
	errCh := make(chan error, 1)
	go func() {
		errCh <- (Runner{Store: &store, ReconnectMin: 20 * time.Millisecond, ReconnectMax: 80 * time.Millisecond}).RunForeground(ctx)
	}()

	times := make([]time.Time, 0, 3)
	for len(times) < 3 {
		select {
		case at := <-connectTimes:
			times = append(times, at)
		case <-time.After(2 * time.Second):
			cancel()
			t.Fatal("timed out waiting for reconnect attempts")
		}
	}
	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run foreground: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for runner shutdown")
	}
	firstDelay := times[1].Sub(times[0])
	secondDelay := times[2].Sub(times[1])
	if firstDelay < 15*time.Millisecond {
		t.Fatalf("first reconnect delay = %s, want at least reconnect min", firstDelay)
	}
	if secondDelay < 35*time.Millisecond {
		t.Fatalf("second reconnect delay = %s, want increased backoff", secondDelay)
	}
}

func TestRunnerServerDrainingReconnectWaitsBeforeFreshGeneration(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	connectTimes := make(chan time.Time, 2)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()

		var connectFrame externalagentprotocol.Frame
		if err := conn.ReadJSON(&connectFrame); err != nil {
			t.Fatalf("read connect: %v", err)
		}
		if err := conn.WriteJSON(externalagentprotocol.Frame{
			MessageType:  externalagentprotocol.FrameTypeConnectAccepted,
			PersonaID:    "persona-1",
			ConnectionID: "conn-1",
			SentAt:       time.Now().UTC(),
			ConnectAccepted: &externalagentprotocol.ConnectAcceptedPayload{
				ProtocolVersion:      externalagentprotocol.ProtocolVersionV4,
				ConnectionGeneration: connectFrame.Connect.ConnectionGeneration,
				HeartbeatSeconds:     15,
			},
		}); err != nil {
			t.Fatalf("write connect accepted: %v", err)
		}
		var heartbeat externalagentprotocol.Frame
		if err := conn.ReadJSON(&heartbeat); err != nil {
			t.Fatalf("read heartbeat: %v", err)
		}
		connectTimes <- time.Now()
		if connectFrame.Connect.ConnectionGeneration == 2 {
			_ = conn.WriteJSON(externalagentprotocol.Frame{
				MessageType:  externalagentprotocol.FrameTypeServerDraining,
				PersonaID:    "persona-1",
				ConnectionID: "conn-1",
				SentAt:       time.Now().UTC(),
				ServerDraining: &externalagentprotocol.ServerDrainingPayload{
					DeadlineAt: time.Now().UTC().Add(45 * time.Millisecond),
					Reason:     "test drain",
				},
			})
			return
		}
		cancel()
	}))
	defer gateway.Close()

	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{{
		ConnectionID:         "conn-1",
		PersonaID:            "persona-1",
		ConnectionGeneration: 1,
		GatewayWebsocketURL:  "ws" + gateway.URL[len("http"):],
		BridgeCredentialID:   "cred-1",
		BridgePrivateKey:     base64.StdEncoding.EncodeToString(privateKey),
		BridgePublicKey:      base64.StdEncoding.EncodeToString(publicKey),
		RuntimeKind:          runtime.AdapterKindAuto,
		HasBridgeSecret:      true,
	}}})
	errCh := make(chan error, 1)
	go func() {
		errCh <- (Runner{Store: &store, ReconnectMin: 20 * time.Millisecond, ReconnectMax: 100 * time.Millisecond}).RunForeground(ctx)
	}()

	times := make([]time.Time, 0, 2)
	for len(times) < 2 {
		select {
		case at := <-connectTimes:
			times = append(times, at)
		case <-time.After(2 * time.Second):
			cancel()
			t.Fatal("timed out waiting for drain reconnect")
		}
	}
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run foreground: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for runner shutdown")
	}
	if delay := times[1].Sub(times[0]); delay < 35*time.Millisecond {
		t.Fatalf("drain reconnect delay = %s, want drain deadline wait", delay)
	}
}

func TestRunnerStartupFailureBackoffSkipsForegroundTicks(t *testing.T) {
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{{
		ConnectionID:         "conn-1",
		PersonaID:            "persona-1",
		ConnectionGeneration: 1,
		GatewayWebsocketURL:  "ws://127.0.0.1:1",
		BridgeCredentialID:   "cred-1",
		BridgePrivateKey:     "not-base64",
		BridgePublicKey:      "not-base64",
		RuntimeKind:          runtime.AdapterKindAuto,
	}}})
	ctx, cancel := context.WithCancel(t.Context())
	errCh := make(chan error, 1)
	go func() {
		errCh <- (Runner{Store: &store, ReconnectMin: 20 * time.Millisecond, ReconnectMax: 80 * time.Millisecond}).RunForeground(ctx)
	}()

	time.Sleep(55 * time.Millisecond)
	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run foreground: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for runner shutdown")
	}
	binding, ok := store.Binding("conn-1")
	if !ok {
		t.Fatal("binding missing")
	}
	if binding.ConnectionGeneration > 3 {
		t.Fatalf("connection generation = %d, want startup failure backoff to skip foreground ticks", binding.ConnectionGeneration)
	}
}

func TestActiveRunConflictClearsStaleDifferentActiveRun(t *testing.T) {
	binding := config.Binding{
		ConnectionID:       "conn-1",
		PersonaID:          "persona-1",
		ActiveRunID:        "run-1",
		ActiveAssignmentID: "assignment-1",
	}
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{binding}})
	runner := Runner{Store: &store}

	activeRunID, conflict := runner.activeRunConflict(binding, externalagentprotocol.Frame{RunID: "run-2"})
	if conflict || activeRunID != "" {
		t.Fatalf("expected stale active run to clear, got conflict=%t active=%q", conflict, activeRunID)
	}
	cleared, ok := store.Binding("conn-1")
	if !ok || cleared.ActiveRunID != "" || cleared.ActiveAssignmentID != "" {
		t.Fatalf("stale active run not cleared: %+v", cleared)
	}

	binding.ActiveRunID = "run-1"
	binding.ActiveAssignmentID = "assignment-1"
	store = config.NewMemoryStore(config.State{Bindings: []config.Binding{binding}})
	runner = Runner{Store: &store}
	activeRunID, conflict = runner.activeRunConflict(binding, externalagentprotocol.Frame{RunID: "run-1", AssignmentID: "assignment-1"})
	if conflict || activeRunID != "" {
		t.Fatalf("did not expect same-run conflict, got conflict=%t active=%q", conflict, activeRunID)
	}
	activeRunID, conflict = runner.activeRunConflict(binding, externalagentprotocol.Frame{RunID: "run-1", AssignmentID: "assignment-2"})
	if conflict || activeRunID != "" {
		t.Fatalf("expected stale assignment to clear, got conflict=%t active=%q", conflict, activeRunID)
	}
	cleared, ok = store.Binding("conn-1")
	if !ok || cleared.ActiveRunID != "" || cleared.ActiveAssignmentID != "" || cleared.ActiveNativeRunID != "" || !cleared.ActiveRunDeadlineAt.IsZero() {
		t.Fatalf("stale assignment not cleared: %+v", cleared)
	}
}

func TestRunnerIgnoresStateWritesFromStaleGeneration(t *testing.T) {
	staleBinding := config.Binding{
		ConnectionID:            "conn-1",
		PersonaID:               "persona-1",
		ConnectionGeneration:    1,
		ActiveRunID:             "run-1",
		ActiveAssignmentID:      "assignment-1",
		ActiveNativeRunID:       "native-1",
		LastHeartbeatAt:         time.Date(2026, 5, 24, 1, 0, 0, 0, time.UTC),
		LastWakeProbeAt:         time.Date(2026, 5, 24, 1, 1, 0, 0, time.UTC),
		LastWakeProbeGeneration: 1,
	}
	currentBinding := staleBinding
	currentBinding.ConnectionGeneration = 2
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{currentBinding}})
	runner := Runner{Store: &store}

	newTime := time.Date(2026, 5, 24, 2, 0, 0, 0, time.UTC)
	if err := runner.recordHeartbeat(staleBinding, newTime); err != nil {
		t.Fatalf("record heartbeat: %v", err)
	}
	if err := runner.recordWakeProbe(staleBinding, newTime); err != nil {
		t.Fatalf("record wake probe: %v", err)
	}
	if err := runner.clearRunState(staleBinding, "run-1"); err != nil {
		t.Fatalf("clear run state: %v", err)
	}
	if err := runner.refreshMCPConfig(staleBinding); err != nil {
		t.Fatalf("stale refresh mcp config: %v", err)
	}
	if _, err := runner.nativeRunIDForCancel(staleBinding, "run-1"); err == nil {
		t.Fatal("expected stale cancel lookup error")
	}
	if nativeRunID, ok := runner.activeNativeRunIDForRunStart(staleBinding, externalagentprotocol.Frame{RunID: "run-1", AssignmentID: "assignment-1"}); ok || nativeRunID != "" {
		t.Fatalf("stale active native run lookup succeeded: nativeRunID=%q ok=%t", nativeRunID, ok)
	}
	if runner.activeRunMatchesWithoutNativeRunID(staleBinding, externalagentprotocol.Frame{RunID: "run-1", AssignmentID: "assignment-1"}) {
		t.Fatal("stale active run without native ID lookup succeeded")
	}
	if activeRunID, conflict := runner.activeRunConflict(staleBinding, externalagentprotocol.Frame{RunID: "run-2", AssignmentID: "assignment-2"}); conflict || activeRunID != "" {
		t.Fatalf("stale active run conflict lookup succeeded: activeRunID=%q conflict=%t", activeRunID, conflict)
	}
	if err := runner.activateRun(staleBinding, externalagentprotocol.Frame{
		RunID:        "run-2",
		AssignmentID: "assignment-2",
		RunStart:     &externalagentprotocol.RunStartPayload{},
	}); err == nil {
		t.Fatal("expected stale activate run error")
	}
	if err := runner.recordNativeRunID(staleBinding, "run-1", "native-2"); err == nil {
		t.Fatal("expected stale native run journal error")
	}
	if err := runner.revokeBinding(staleBinding, failingCapabilityAdapter{}, "revoked"); err != nil {
		t.Fatalf("stale revoke binding: %v", err)
	}

	latest, ok := store.Binding("conn-1")
	if !ok {
		t.Fatal("binding missing")
	}
	if latest.LastHeartbeatAt.Equal(newTime) || latest.LastWakeProbeAt.Equal(newTime) {
		t.Fatalf("stale liveness write mutated binding: %+v", latest)
	}
	if latest.LastWakeProbeGeneration != 1 {
		t.Fatalf("stale wake probe generation mutated binding: %+v", latest)
	}
	if latest.ActiveRunID != "run-1" || latest.ActiveAssignmentID != "assignment-1" || latest.ActiveNativeRunID != "native-1" {
		t.Fatalf("stale run write mutated binding: %+v", latest)
	}
}

func TestRunStartClearsStaleActiveRunBeforeReadinessRejection(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	hermesServer := httptest.NewServer(http.NotFoundHandler())
	defer hermesServer.Close()
	t.Setenv("PERSONASTACK_CONNECTOR_HERMES_URL", hermesServer.URL)
	t.Setenv("HERMES_API_SERVER_KEY", "key-1")

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	failed := make(chan externalagentprotocol.Frame, 1)
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()
		var connectFrame externalagentprotocol.Frame
		if err := conn.ReadJSON(&connectFrame); err != nil {
			t.Fatalf("read connect: %v", err)
		}
		_ = conn.WriteJSON(externalagentprotocol.Frame{
			MessageType:  externalagentprotocol.FrameTypeConnectAccepted,
			PersonaID:    "persona-1",
			ConnectionID: "conn-1",
			SentAt:       time.Now().UTC(),
			ConnectAccepted: &externalagentprotocol.ConnectAcceptedPayload{
				ProtocolVersion:      externalagentprotocol.ProtocolVersionV4,
				ConnectionGeneration: 1,
				HeartbeatSeconds:     15,
			},
		})
		_ = conn.WriteJSON(externalagentprotocol.Frame{
			MessageID:    "run-start-2",
			MessageType:  externalagentprotocol.FrameTypeRunStart,
			PersonaID:    "persona-1",
			ConnectionID: "conn-1",
			RunID:        "run-2",
			AssignmentID: "assignment-2",
			SentAt:       time.Now().UTC(),
			RunStart:     &externalagentprotocol.RunStartPayload{FullyComposedPrompt: "prompt"},
		})
		for {
			var frame externalagentprotocol.Frame
			if err := conn.ReadJSON(&frame); err != nil {
				return
			}
			if frame.MessageType != externalagentprotocol.FrameTypeRunFailed || frame.RunID != "run-2" {
				continue
			}
			failed <- frame
			cancel()
			return
		}
	}))
	defer gateway.Close()

	binding := config.Binding{
		ConnectionID:        "conn-1",
		PersonaID:           "persona-1",
		GatewayWebsocketURL: "ws" + gateway.URL[len("http"):],
		BridgeCredentialID:  "cred-1",
		BridgePrivateKey:    base64.StdEncoding.EncodeToString(privateKey),
		BridgePublicKey:     base64.StdEncoding.EncodeToString(publicKey),
		RuntimeKind:         runtime.AdapterKindHermes,
		ActiveRunID:         "run-1",
		ActiveAssignmentID:  "assignment-1",
		ActiveNativeRunID:   "native-1",
	}
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{binding}})
	session, err := bridge.NewSession(binding, bridge.Credential{ID: "cred-1", PrivateKey: privateKey, PublicKey: publicKey})
	if err != nil {
		t.Fatalf("new session: %v", err)
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- (Runner{Store: &store}).runBindingSession(ctx, binding, session)
	}()

	select {
	case frame := <-failed:
		if frame.RunTerminal == nil || !strings.Contains(frame.RunTerminal.FinalMessage, "external runtime is not ready") {
			t.Fatalf("unexpected failure frame: %+v", frame)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for readiness failure")
	}
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run binding session: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for runner shutdown")
	}
	cleared, ok := store.Binding("conn-1")
	if !ok || cleared.ActiveRunID != "" || cleared.ActiveAssignmentID != "" || cleared.ActiveNativeRunID != "" {
		t.Fatalf("stale active run not cleared before readiness rejection: %+v", cleared)
	}
}

func TestCommandFrameCacheReplaysRepliesAndSuppressesSideEffects(t *testing.T) {
	cache := newCommandFrameCache()
	request := externalagentprotocol.Frame{
		MessageID:    "msg-1",
		MessageType:  externalagentprotocol.FrameTypeWakeProbe,
		ConnectionID: "conn-1",
		PersonaID:    "persona-1",
	}
	reply := externalagentprotocol.Frame{
		MessageID:    "msg-1",
		MessageType:  externalagentprotocol.FrameTypeWakeProbeAccepted,
		ConnectionID: "conn-1",
		PersonaID:    "persona-1",
	}

	if _, ok := cache.cachedReply(request); ok {
		t.Fatalf("unexpected cached reply before store")
	}
	cache.storeReply(request, reply)
	cached, ok := cache.cachedReply(request)
	if !ok || cached.MessageType != externalagentprotocol.FrameTypeWakeProbeAccepted {
		t.Fatalf("unexpected cached reply: ok=%t frame=%+v", ok, cached)
	}
	if !cache.seen(request) {
		t.Fatalf("stored reply should mark command as seen")
	}

	cancel := externalagentprotocol.Frame{MessageID: "cancel-1", MessageType: externalagentprotocol.FrameTypeRunCancel}
	if cache.seen(cancel) {
		t.Fatalf("cancel should not be seen before mark")
	}
	cache.mark(cancel)
	if !cache.seen(cancel) {
		t.Fatalf("cancel should be seen after mark")
	}
}

func TestTargetRuntimeURLUsesProfileScopedEndpointForSystemService(t *testing.T) {
	t.Setenv("PERSONASTACK_CONNECTOR_HERMES_URL", "http://127.0.0.1:8642")
	target := &externalagentprotocol.RuntimeTarget{
		RuntimeKind:        externalagentprotocol.RuntimeKindHermes,
		AccountCandidateID: "account-a",
		ProfileCandidateID: "profile-a",
	}
	binding := config.Binding{RuntimeKind: runtime.AdapterKindHermes, BridgePrivateKey: "installation-secret"}
	runner := Runner{ServiceScope: externalagentprotocol.ServiceScopeLinuxSystemService}
	endpoint, err := runner.targetRuntimeURL(binding, target)
	if err != nil {
		t.Fatalf("targetRuntimeURL() error = %v", err)
	}
	if endpoint == "http://127.0.0.1:8642" || !strings.HasPrefix(endpoint, "http://127.0.0.1:") {
		t.Fatalf("system target endpoint = %q", endpoint)
	}
}
