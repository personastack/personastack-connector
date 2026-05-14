package daemon

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
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
