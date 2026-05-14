package daemon

import (
	"context"
	"fmt"
	"time"

	"github.com/gorilla/websocket"
	"github.com/personastack/agent-gateway/pkg/externalagentprotocol"
	"github.com/personastack/personastack-connector/internal/bridge"
	"github.com/personastack/personastack-connector/internal/config"
	"github.com/personastack/personastack-connector/internal/runtime"
)

type Runner struct {
	Store config.Store
	Now   func() time.Time
}

func (r Runner) RunForeground(ctx context.Context) error {
	bindings := r.Store.ListBindings()
	if len(bindings) == 0 {
		return fmt.Errorf("no paired bindings")
	}
	for _, binding := range bindings {
		if err := r.runBinding(ctx, binding); err != nil {
			return err
		}
	}
	return nil
}

func (r Runner) runBinding(ctx context.Context, binding config.Binding) error {
	credential, err := bridge.CredentialFromBinding(binding)
	if err != nil {
		return err
	}
	session, err := bridge.NewSession(binding, credential)
	if err != nil {
		return err
	}
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, binding.GatewayWebsocketURL, nil)
	if err != nil {
		return fmt.Errorf("connect gateway websocket: %w", err)
	}
	defer conn.Close()

	connectFrame, err := session.ConnectFrame("connector-startup")
	if err != nil {
		return err
	}
	if err := conn.WriteJSON(connectFrame); err != nil {
		return fmt.Errorf("write connect frame: %w", err)
	}
	var accepted externalagentprotocol.Frame
	if err := conn.ReadJSON(&accepted); err != nil {
		return fmt.Errorf("read connect response: %w", err)
	}
	if accepted.MessageType != externalagentprotocol.FrameTypeConnectAccepted {
		return fmt.Errorf("connector rejected: %s", accepted.MessageType)
	}
	heartbeat := session.HeartbeatFrame(runtime.AdapterStateRuntimeMissing, nil)
	if err := conn.WriteJSON(heartbeat); err != nil {
		return fmt.Errorf("write heartbeat frame: %w", err)
	}
	return nil
}
