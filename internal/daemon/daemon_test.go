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

func TestRunnerFailsFastWhenLoopbackProxyCannotBind(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()
	binding := config.Binding{
		ConnectionID:          "conn-1",
		PersonaID:             "persona-1",
		LocalMCPProxyURL:      "http://" + listener.Addr().String() + "/mcp/conn-1",
		LocalMCPProxyToken:    "local-token",
		HasLocalMCPProxyToken: true,
		PersonaMCPURL:         "http://127.0.0.1:1/mcp",
		PersonaMCPToken:       "persona-token",
	}
	err = (Runner{Store: config.NewMemoryStore(config.State{Bindings: []config.Binding{binding}})}).RunForeground(t.Context())
	if err == nil {
		t.Fatal("RunForeground() expected loopback bind error")
	}
}

func TestRunnerConfigRefreshUsesLatestLoopbackProxyState(t *testing.T) {
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
	if err != nil {
		t.Fatalf("refreshMCPConfig() error = %v", err)
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
	if !strings.Contains(string(raw), latest.LocalMCPProxyURL) {
		t.Fatalf("refresh did not use latest loopback url:\n%s", string(raw))
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
	if got.Capabilities.NativeCapabilities != nil {
		t.Fatalf("expected nil native capabilities on discovery error, got %#v", got.Capabilities.NativeCapabilities)
	}
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

func TestRunnerConnectsAndSendsHeartbeat(t *testing.T) {
	keyring.MockInit()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	seenHeartbeat := make(chan externalagentprotocol.Frame, 1)
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
				ProtocolVersion:      externalagentprotocol.ProtocolVersionV1,
				ConnectionGeneration: connectFrame.Connect.ConnectionGeneration,
				HeartbeatSeconds:     15,
			},
		})
		var heartbeat externalagentprotocol.Frame
		if err := conn.ReadJSON(&heartbeat); err != nil {
			t.Fatalf("read heartbeat: %v", err)
		}
		seenHeartbeat <- heartbeat
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
		RuntimeKind:             runtime.AdapterKindHermes,
	}
	store := config.NewFileStore(t.TempDir() + "/state.json")
	if err := store.SaveBinding(binding); err != nil {
		t.Fatalf("save binding: %v", err)
	}
	if err := (Runner{Store: store, ReconnectMin: time.Millisecond, ReconnectMax: time.Millisecond}).RunForeground(ctx); err != nil {
		t.Fatalf("run foreground: %v", err)
	}
	heartbeat := <-seenHeartbeat
	if heartbeat.MessageType != externalagentprotocol.FrameTypeHeartbeat || heartbeat.ConnectionID != "conn-1" {
		t.Fatalf("unexpected heartbeat: %+v", heartbeat)
	}
	if heartbeat.Heartbeat.ReadinessStatus == externalagentprotocol.ReadinessStatusWakeable {
		t.Fatalf("heartbeat reused persisted wake probe before current-session probe: %+v", heartbeat.Heartbeat)
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
				ProtocolVersion:      externalagentprotocol.ProtocolVersionV1,
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
		RuntimeKind:          runtime.AdapterKindHermes,
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
	readNextFrame := func(conn *websocket.Conn) (externalagentprotocol.Frame, error) {
		for {
			var frame externalagentprotocol.Frame
			if err := conn.ReadJSON(&frame); err != nil {
				return externalagentprotocol.Frame{}, err
			}
			if frame.MessageType == externalagentprotocol.FrameTypeHeartbeat {
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
				ProtocolVersion:      externalagentprotocol.ProtocolVersionV1,
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
		var wakeAck externalagentprotocol.Frame
		if err := conn.ReadJSON(&wakeAck); err != nil {
			t.Fatalf("read wake probe accepted: %v", err)
		}
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
		if terminal.MessageType != externalagentprotocol.FrameTypeRunCompleted || terminal.RunTerminal == nil || terminal.RunTerminal.Status != externalagentprotocol.RunStatusCompleted || terminal.RunTerminal.Output != "done" {
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

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- (Runner{Store: &store, ReconnectMin: time.Millisecond, ReconnectMax: time.Millisecond}).RunForeground(ctx)
	}()

	select {
	case <-terminalSeen:
		cancel()
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for run terminal frame")
	}

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
				ProtocolVersion:      externalagentprotocol.ProtocolVersionV1,
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
			if frame.RunTerminal == nil || !strings.Contains(frame.RunTerminal.Output, "native id missing") {
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
		if err != nil {
			t.Fatalf("run binding session: %v", err)
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
				ProtocolVersion:      externalagentprotocol.ProtocolVersionV1,
				ConnectionGeneration: 1,
				HeartbeatSeconds:     15,
			},
		})
		var heartbeat externalagentprotocol.Frame
		if err := conn.ReadJSON(&heartbeat); err != nil {
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
		var wakeAck externalagentprotocol.Frame
		if err := conn.ReadJSON(&wakeAck); err != nil {
			t.Fatalf("read wake probe accepted: %v", err)
		}
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
			if frame.MessageType == externalagentprotocol.FrameTypeHeartbeat {
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
	if len(got) != 7 {
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
	if got[4].MessageType != externalagentprotocol.FrameTypeRunOutputDelta {
		t.Fatalf("fifth frame = %+v", got[4])
	}
	if got[5].MessageType != externalagentprotocol.FrameTypeRunToolEvent {
		t.Fatalf("sixth frame = %+v", got[5])
	}
	if got[6].MessageType != externalagentprotocol.FrameTypeRunCompleted {
		t.Fatalf("seventh frame = %+v", got[6])
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
				ProtocolVersion:      externalagentprotocol.ProtocolVersionV1,
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
	err = (Runner{Store: &store, ReconnectMin: time.Millisecond, ReconnectMax: time.Millisecond}).RunForeground(t.Context())
	if err != nil {
		t.Fatalf("run foreground: %v", err)
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
	if err := runner.replayActiveRun(context.Background(), config.Binding{ConnectionID: "conn-1"}, session, nil, func(frame externalagentprotocol.Frame) error {
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
				ProtocolVersion:      externalagentprotocol.ProtocolVersionV1,
				ConnectionGeneration: connectFrame.Connect.ConnectionGeneration,
				HeartbeatSeconds:     15,
			},
		})
		readNonHeartbeat := func() externalagentprotocol.Frame {
			for {
				var frame externalagentprotocol.Frame
				if err := conn.ReadJSON(&frame); err != nil {
					t.Fatalf("read frame: %v", err)
				}
				if frame.MessageType != externalagentprotocol.FrameTypeHeartbeat {
					return frame
				}
			}
		}
		if replay := readNonHeartbeat(); replay.MessageType != externalagentprotocol.FrameTypeRunAccepted {
			t.Fatalf("expected replay accepted, got %+v", replay)
		}
		if replay := readNonHeartbeat(); replay.MessageType != externalagentprotocol.FrameTypeRunStarted {
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
		runReplay <- []externalagentprotocol.Frame{readNonHeartbeat()}
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
		if len(frames) != 1 || frames[0].MessageType != externalagentprotocol.FrameTypeRunAccepted {
			t.Fatalf("expected redelivered run.start accepted without duplicate started, got %+v", frames)
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
		connectGenerations <- connectFrame.Connect.ConnectionGeneration
		if err := conn.WriteJSON(externalagentprotocol.Frame{
			MessageType:  externalagentprotocol.FrameTypeConnectAccepted,
			PersonaID:    "persona-1",
			ConnectionID: "conn-1",
			SentAt:       time.Now().UTC(),
			ConnectAccepted: &externalagentprotocol.ConnectAcceptedPayload{
				ProtocolVersion:      externalagentprotocol.ProtocolVersionV1,
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
		PersonaMCPToken:      "mcp-token-1",
		RuntimeKind:          runtime.AdapterKindHermes,
		HasBridgeSecret:      true,
		HasPersonaMCPToken:   true,
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

func TestActiveRunConflictRejectsDifferentActiveRun(t *testing.T) {
	binding := config.Binding{
		ConnectionID:       "conn-1",
		PersonaID:          "persona-1",
		ActiveRunID:        "run-1",
		ActiveAssignmentID: "assignment-1",
	}
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{binding}})
	runner := Runner{Store: &store}

	activeRunID, conflict := runner.activeRunConflict(binding, externalagentprotocol.Frame{RunID: "run-2"})
	if !conflict || activeRunID != "run-1" {
		t.Fatalf("expected active run conflict, got conflict=%t active=%q", conflict, activeRunID)
	}

	activeRunID, conflict = runner.activeRunConflict(binding, externalagentprotocol.Frame{RunID: "run-1", AssignmentID: "assignment-1"})
	if conflict || activeRunID != "" {
		t.Fatalf("did not expect same-run conflict, got conflict=%t active=%q", conflict, activeRunID)
	}
	activeRunID, conflict = runner.activeRunConflict(binding, externalagentprotocol.Frame{RunID: "run-1", AssignmentID: "assignment-2"})
	if !conflict || activeRunID != "run-1" {
		t.Fatalf("expected assignment conflict, got conflict=%t active=%q", conflict, activeRunID)
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
