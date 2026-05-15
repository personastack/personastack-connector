package daemon

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/personastack/agent-gateway/pkg/externalagentprotocol"
	"github.com/personastack/personastack-connector/internal/config"
	"github.com/personastack/personastack-connector/internal/mcp"
	"github.com/personastack/personastack-connector/internal/runtime"
	"github.com/zalando/go-keyring"
)

func TestRunnerConnectsAndSendsHeartbeat(t *testing.T) {
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
		seenHeartbeat <- heartbeat
		_ = conn.WriteJSON(externalagentprotocol.Frame{
			MessageID:    "probe-msg-1",
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
			t.Fatalf("read wake ack: %v", err)
		}
		if wakeAck.MessageType != externalagentprotocol.FrameTypeWakeProbeAccepted {
			t.Fatalf("unexpected wake ack: %+v", wakeAck)
		}
		cancel()
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
		RuntimeKind:          runtime.AdapterKindHermes,
	}
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{binding}})
	if err := (Runner{Store: store, ReconnectMin: time.Millisecond, ReconnectMax: time.Millisecond}).RunForeground(ctx); err != nil {
		t.Fatalf("run foreground: %v", err)
	}
	heartbeat := <-seenHeartbeat
	if heartbeat.MessageType != externalagentprotocol.FrameTypeHeartbeat || heartbeat.ConnectionID != "conn-1" {
		t.Fatalf("unexpected heartbeat: %+v", heartbeat)
	}
}

func TestRunnerReloadsFileBackedBindingAfterRestart(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	connectionCount := make(chan struct{}, 2)
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
		connectionCount <- struct{}{}
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
		RuntimeKind:          runtime.AdapterKindHermes,
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
		case <-connectionCount:
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
				RunScopedMCPToken:      "run-token-1",
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
			MessageID:    "run-start-1",
			MessageType:  externalagentprotocol.FrameTypeRunStart,
			PersonaID:    "persona-1",
			ConnectionID: "conn-1",
			SentAt:       time.Now(),
			RunID:        "run-1",
			AssignmentID: "assignment-1",
			RunStart: &externalagentprotocol.RunStartPayload{
				FullyComposedPrompt:    "prompt",
				RunScopedMCPToken:      "run-token",
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
}

func TestCanStartRunWithReadiness(t *testing.T) {
	if !canStartRunWithReadiness(runtime.AdapterStateMCPVerified) {
		t.Fatalf("mcp verified should be runnable")
	}
	if !canStartRunWithReadiness(runtime.AdapterStateReady) {
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
		if canStartRunWithReadiness(state) {
			t.Fatalf("%s should not be runnable", state)
		}
	}
}

func TestRunMCPTokenLifecycleUpdatesBinding(t *testing.T) {
	binding := config.Binding{ConnectionID: "conn-1", PersonaID: "persona-1", PersonaMCPToken: "stable-token"}
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{binding}})
	runner := Runner{Store: &store}
	frame := externalagentprotocol.Frame{
		RunID:        "run-1",
		AssignmentID: "assignment-1",
		RunStart: &externalagentprotocol.RunStartPayload{
			RunScopedMCPToken: "run-token",
		},
	}
	if err := runner.activateRunMCPToken(binding, frame); err != nil {
		t.Fatalf("activate run token: %v", err)
	}
	active, ok := store.Binding("conn-1")
	if !ok || active.ActiveRunID != "run-1" || active.ActiveAssignmentID != "assignment-1" || active.ActiveRunMCPToken != "run-token" || !active.HasActiveRunMCPToken {
		t.Fatalf("active token not stored: %+v", active)
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
	if err := runner.clearRunMCPToken(binding, "run-1"); err != nil {
		t.Fatalf("clear run token: %v", err)
	}
	cleared, ok := store.Binding("conn-1")
	if !ok || cleared.ActiveRunID != "" || cleared.ActiveAssignmentID != "" || cleared.ActiveNativeRunID != "" || cleared.ActiveRunMCPToken != "" || cleared.HasActiveRunMCPToken {
		t.Fatalf("active token not cleared: %+v", cleared)
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
		RunStart:     &externalagentprotocol.RunStartPayload{RunScopedMCPToken: "run-token"},
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
