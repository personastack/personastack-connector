package daemon

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/personastack/agent-gateway/pkg/externalagentprotocol"
	"github.com/personastack/personastack-connector/internal/bridge"
	"github.com/personastack/personastack-connector/internal/config"
	"github.com/personastack/personastack-connector/internal/mcp"
	"github.com/personastack/personastack-connector/internal/runtime"
)

type Runner struct {
	Store        config.Store
	Now          func() time.Time
	ReconnectMin time.Duration
	ReconnectMax time.Duration
}

func (r Runner) RunForeground(ctx context.Context) error {
	bindings := r.Store.ListBindings()
	if len(bindings) == 0 {
		return fmt.Errorf("no paired bindings")
	}
	errs := make(chan error, len(bindings))
	var wg sync.WaitGroup
	for _, binding := range bindings {
		wg.Add(1)
		go func(binding config.Binding) {
			defer wg.Done()
			errs <- r.runBinding(ctx, binding)
		}(binding)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
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
	backoff := r.reconnectMin()
	for {
		err = r.runBindingSession(ctx, binding, session)
		if ctx.Err() != nil {
			return nil
		}
		if err == nil {
			backoff = r.reconnectMin()
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
		backoff = minDuration(backoff*2, r.reconnectMax())
	}
}

func (r Runner) runBindingSession(ctx context.Context, binding config.Binding, session bridge.Session) error {
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, binding.GatewayWebsocketURL, nil)
	if err != nil {
		return fmt.Errorf("connect gateway websocket: %w", err)
	}
	defer conn.Close()
	var writeMu sync.Mutex
	writeFrame := func(frame externalagentprotocol.Frame) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		return conn.WriteJSON(frame)
	}

	connectFrame, err := session.ConnectFrame("connector-startup")
	if err != nil {
		return err
	}
	if err := writeFrame(connectFrame); err != nil {
		return fmt.Errorf("write connect frame: %w", err)
	}
	var accepted externalagentprotocol.Frame
	if err := conn.ReadJSON(&accepted); err != nil {
		return fmt.Errorf("read connect response: %w", err)
	}
	if accepted.MessageType != externalagentprotocol.FrameTypeConnectAccepted {
		return fmt.Errorf("connector rejected: %s", accepted.MessageType)
	}
	adapter := runtime.NewAdapter(binding.RuntimeKind)
	detection := r.bindingReadiness(adapter, binding)
	heartbeat := session.HeartbeatFrame(detection.State, nil)
	if err := writeFrame(heartbeat); err != nil {
		return fmt.Errorf("write heartbeat frame: %w", err)
	}
	heartbeatStop := make(chan struct{})
	defer close(heartbeatStop)
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-heartbeatStop:
				return
			case <-ticker.C:
				state := r.bindingReadiness(adapter, binding).State
				_ = writeFrame(session.HeartbeatFrame(state, nil))
			}
		}
	}()
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
			if err := writeFrame(session.WakeProbeAcceptedFrame(frame.WakeProbe.ProbeID)); err != nil {
				return fmt.Errorf("write wake probe ack: %w", err)
			}
		case externalagentprotocol.FrameTypeRunStart:
			if frame.RunStart == nil {
				continue
			}
			if readiness := r.bindingReadiness(adapter, binding); !canStartRunWithReadiness(readiness.State) {
				failed := session.RunTerminalFrame(frame, externalagentprotocol.RunStatusFailed, externalagentprotocol.TerminalReasonFailed, "external runtime is not ready: "+readiness.State.String())
				if writeErr := writeFrame(failed); writeErr != nil {
					return fmt.Errorf("write run readiness failure: %w", writeErr)
				}
				continue
			}
			if err := r.activateRunMCPToken(binding, frame); err != nil {
				failed := session.RunTerminalFrame(frame, externalagentprotocol.RunStatusFailed, externalagentprotocol.TerminalReasonFailed, err.Error())
				if writeErr := writeFrame(failed); writeErr != nil {
					return fmt.Errorf("write run mcp token failure: %w", writeErr)
				}
				continue
			}
			nativeRunID, err := adapter.StartRun(frame.AssignmentID, frame.RunStart.FullyComposedPrompt)
			if err != nil {
				_ = r.clearRunMCPToken(binding, frame.RunID)
				failed := session.RunTerminalFrame(frame, externalagentprotocol.RunStatusFailed, externalagentprotocol.TerminalReasonFailed, err.Error())
				if writeErr := writeFrame(failed); writeErr != nil {
					return fmt.Errorf("write run failure: %w", writeErr)
				}
				continue
			}
			if err := r.recordNativeRunID(binding, frame.RunID, nativeRunID); err != nil {
				_ = r.clearRunMCPToken(binding, frame.RunID)
				failed := session.RunTerminalFrame(frame, externalagentprotocol.RunStatusFailed, externalagentprotocol.TerminalReasonFailed, err.Error())
				if writeErr := writeFrame(failed); writeErr != nil {
					return fmt.Errorf("write run journal failure: %w", writeErr)
				}
				continue
			}
			if err := writeFrame(session.RunAcceptedFrame(frame, nativeRunID)); err != nil {
				return fmt.Errorf("write run accepted: %w", err)
			}
			if err := writeFrame(session.RunStartedFrame(frame, nativeRunID)); err != nil {
				return fmt.Errorf("write run started: %w", err)
			}
			go func(frame externalagentprotocol.Frame, nativeRunID string) {
				defer func() {
					_ = r.clearRunMCPToken(binding, frame.RunID)
				}()
				result, err := adapter.WaitRun(ctx, nativeRunID)
				if err != nil {
					failed := session.RunTerminalFrame(frame, externalagentprotocol.RunStatusFailed, externalagentprotocol.TerminalReasonFailed, err.Error())
					_ = writeFrame(failed)
					return
				}
				status := externalagentprotocol.RunStatusCompleted
				reason := externalagentprotocol.TerminalReasonSucceeded
				if result.Status == runtime.RunStatusFailed {
					status = externalagentprotocol.RunStatusFailed
					reason = externalagentprotocol.TerminalReasonFailed
				}
				if result.Status == runtime.RunStatusCancelled {
					status = externalagentprotocol.RunStatusCancelled
					reason = externalagentprotocol.TerminalReasonCancelled
				}
				_ = writeFrame(session.RunTerminalFrame(frame, status, reason, result.Output))
			}(frame, nativeRunID)
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

func (r Runner) activateRunMCPToken(binding config.Binding, frame externalagentprotocol.Frame) error {
	token := ""
	if frame.RunStart != nil {
		token = strings.TrimSpace(frame.RunStart.RunScopedMCPToken)
	}
	if token == "" {
		return fmt.Errorf("run scoped mcp token required")
	}
	writable, ok := r.Store.(config.WritableStore)
	if !ok {
		return fmt.Errorf("writable connector store required")
	}
	active := binding
	active.ActiveRunID = strings.TrimSpace(frame.RunID)
	active.ActiveRunMCPToken = token
	active.HasActiveRunMCPToken = true
	return writable.SaveBinding(active)
}

func (r Runner) recordNativeRunID(binding config.Binding, runID string, nativeRunID string) error {
	writable, ok := r.Store.(config.WritableStore)
	if !ok {
		return nil
	}
	latest, ok := r.Store.Binding(binding.ConnectionID)
	if !ok {
		return nil
	}
	if strings.TrimSpace(latest.ActiveRunID) != strings.TrimSpace(runID) {
		return nil
	}
	latest.ActiveNativeRunID = strings.TrimSpace(nativeRunID)
	return writable.SaveBinding(latest)
}

func (r Runner) clearRunMCPToken(binding config.Binding, runID string) error {
	writable, ok := r.Store.(config.WritableStore)
	if !ok {
		return nil
	}
	latest, ok := r.Store.Binding(binding.ConnectionID)
	if !ok {
		return nil
	}
	if strings.TrimSpace(latest.ActiveRunID) != strings.TrimSpace(runID) {
		return nil
	}
	latest.ActiveRunID = ""
	latest.ActiveNativeRunID = ""
	latest.ActiveRunMCPToken = ""
	latest.HasActiveRunMCPToken = false
	return writable.SaveBinding(latest)
}

func (r Runner) bindingReadiness(adapter runtime.Adapter, binding config.Binding) runtime.Detection {
	detection := adapter.Detect()
	if detection.State != runtime.AdapterStateReady {
		return detection
	}
	verify := mcp.VerifyBindingInUserHome(binding)
	detection.State = verify.State
	detection.Note = verify.Note
	if verify.State != runtime.AdapterStateMCPVerified {
		return detection
	}
	verifyCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	live := mcp.VerifyBindingLive(verifyCtx, binding, nil)
	if !live.OK {
		detection.State = runtime.AdapterStateMCPConfigMissing
		detection.Note = live.Note
		return detection
	}
	detection.Note = live.Note
	return detection
}

func canStartRunWithReadiness(state runtime.AdapterState) bool {
	return state == runtime.AdapterStateMCPVerified || state == runtime.AdapterStateReady
}

func (r Runner) reconnectMin() time.Duration {
	if r.ReconnectMin > 0 {
		return r.ReconnectMin
	}
	return time.Second
}

func (r Runner) reconnectMax() time.Duration {
	if r.ReconnectMax > 0 {
		return r.ReconnectMax
	}
	return 30 * time.Second
}

func minDuration(a time.Duration, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
