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
	adapter := runtime.NewAdapter(binding.RuntimeKind)
	for {
		var frame externalagentprotocol.Frame
		if err := conn.ReadJSON(&frame); err != nil {
			return nil
		}
		switch frame.MessageType {
		case externalagentprotocol.FrameTypeWakeProbe:
			if frame.WakeProbe == nil {
				continue
			}
			if err := conn.WriteJSON(session.WakeProbeAcceptedFrame(frame.WakeProbe.ProbeID)); err != nil {
				return fmt.Errorf("write wake probe ack: %w", err)
			}
		case externalagentprotocol.FrameTypeRunStart:
			if frame.RunStart == nil {
				continue
			}
			nativeRunID, err := adapter.StartRun(frame.AssignmentID, frame.RunStart.FullyComposedPrompt)
			if err != nil {
				failed := session.RunTerminalFrame(frame, externalagentprotocol.RunStatusFailed, externalagentprotocol.TerminalReasonFailed, err.Error())
				if writeErr := conn.WriteJSON(failed); writeErr != nil {
					return fmt.Errorf("write run failure: %w", writeErr)
				}
				continue
			}
			if err := conn.WriteJSON(session.RunAcceptedFrame(frame, nativeRunID)); err != nil {
				return fmt.Errorf("write run accepted: %w", err)
			}
		case externalagentprotocol.FrameTypeRunCancel:
			if frame.RunCancel == nil {
				continue
			}
			if err := adapter.CancelRun(frame.RunID); err != nil {
				return fmt.Errorf("cancel local run: %w", err)
			}
		}
	}
}
