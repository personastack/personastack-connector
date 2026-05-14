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
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/personastack/agent-gateway/pkg/externalagentprotocol"
	"github.com/personastack/personastack-connector/internal/config"
	"github.com/personastack/personastack-connector/internal/runtime"
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
		RunID: "run-1",
		RunStart: &externalagentprotocol.RunStartPayload{
			RunScopedMCPToken: "run-token",
		},
	}
	if err := runner.activateRunMCPToken(binding, frame); err != nil {
		t.Fatalf("activate run token: %v", err)
	}
	active, ok := store.Binding("conn-1")
	if !ok || active.ActiveRunID != "run-1" || active.ActiveRunMCPToken != "run-token" || !active.HasActiveRunMCPToken {
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
	if !ok || cleared.ActiveRunID != "" || cleared.ActiveNativeRunID != "" || cleared.ActiveRunMCPToken != "" || cleared.HasActiveRunMCPToken {
		t.Fatalf("active token not cleared: %+v", cleared)
	}
}

func TestActiveNativeRunIDForRunStartDeduplicatesRedelivery(t *testing.T) {
	binding := config.Binding{
		ConnectionID:      "conn-1",
		PersonaID:         "persona-1",
		ActiveRunID:       "run-1",
		ActiveNativeRunID: "native-1",
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
}
