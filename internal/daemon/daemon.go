package daemon

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/personastack/personastack-connector/internal/bridge"
	"github.com/personastack/personastack-connector/internal/config"
	"github.com/personastack/personastack-connector/internal/externalagentprotocol"
	"github.com/personastack/personastack-connector/internal/mcp"
	"github.com/personastack/personastack-connector/internal/openclawauth"
	"github.com/personastack/personastack-connector/internal/runtime"
)

type Runner struct {
	Store        config.Store
	Now          func() time.Time
	ReconnectMin time.Duration
	ReconnectMax time.Duration
}

var errConnectorDraining = errors.New("connector draining")

const minimumRunDeadline = 15 * time.Minute

func (r Runner) RunForeground(ctx context.Context) error {
	bindings := r.Store.ListBindings()
	if len(bindings) == 0 {
		return fmt.Errorf("no paired bindings")
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errs := make(chan error, len(bindings)*2)
	var wg sync.WaitGroup
	for _, binding := range bindings {
		if strings.TrimSpace(binding.LocalMCPProxyURL) == "" {
			continue
		}
		proxyErrs, err := mcp.StartLoopbackHTTPProxyWithStore(runCtx, r.Store, binding, nil)
		if err != nil {
			return err
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- <-proxyErrs
		}()
	}
	for _, binding := range bindings {
		wg.Add(1)
		go func(binding config.Binding) {
			defer wg.Done()
			errs <- r.runBinding(runCtx, binding)
		}(binding)
	}
	go func() {
		wg.Wait()
		close(errs)
	}()
	var firstErr error
	for err := range errs {
		if err != nil && firstErr == nil {
			firstErr = err
			cancel()
		}
	}
	return firstErr
}

func (r Runner) runBinding(ctx context.Context, binding config.Binding) error {
	return r.runBindingLoop(ctx, binding)
}

func (r Runner) runBindingLoop(ctx context.Context, binding config.Binding) error {
	connectionID := binding.ConnectionID
	backoff := r.reconnectMin()
	for {
		latest, ok := r.Store.Binding(connectionID)
		if !ok {
			return nil
		}
		current := latest
		current.ConnectionGeneration++
		if current.ConnectionGeneration <= 0 {
			current.ConnectionGeneration = 1
		}
		writable, ok := r.Store.(config.WritableStore)
		if !ok {
			return fmt.Errorf("writable connector store required")
		}
		if err := writable.SaveBinding(current); err != nil {
			return err
		}
		credential, err := bridge.CredentialFromBinding(current)
		if err != nil {
			return err
		}
		session, err := bridge.NewSession(current, credential)
		if err != nil {
			return err
		}
		err = r.runBindingSession(ctx, current, session)
		if ctx.Err() != nil {
			return nil
		}
		if errors.Is(err, errConnectorDraining) {
			return nil
		}
		if err == nil {
			backoff = r.reconnectMin()
		}
		timer := time.NewTimer(jitterDuration(backoff))
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
	if accepted.ConnectAccepted == nil {
		return fmt.Errorf("connect response payload required")
	}
	if !externalagentprotocol.ProtocolVersionSupported(accepted.ConnectAccepted.ProtocolVersion) {
		return fmt.Errorf("unsupported protocol version: %s", accepted.ConnectAccepted.ProtocolVersion)
	}
	adapter := r.adapterForBinding(binding)
	detection := r.bindingReadiness(adapter, binding)
	_ = r.recordHeartbeat(binding.ConnectionID, r.now())
	var wakeProbeMu sync.Mutex
	var sessionWakeProbeAt *time.Time
	lastSessionWakeProbeAt := func() *time.Time {
		wakeProbeMu.Lock()
		defer wakeProbeMu.Unlock()
		if sessionWakeProbeAt == nil {
			return nil
		}
		value := sessionWakeProbeAt.UTC()
		return &value
	}
	recordSessionWakeProbe := func(at time.Time) {
		value := at.UTC()
		wakeProbeMu.Lock()
		sessionWakeProbeAt = &value
		wakeProbeMu.Unlock()
	}
	heartbeat := session.HeartbeatFrame(detection.State, lastSessionWakeProbeAt())
	if err := writeFrame(heartbeat); err != nil {
		return fmt.Errorf("write heartbeat frame: %w", err)
	}
	if err := r.replayActiveRun(ctx, binding, session, adapter, writeFrame); err != nil {
		return err
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
				_ = r.recordHeartbeat(binding.ConnectionID, r.now())
				readiness := r.bindingReadiness(adapter, binding)
				_ = writeFrame(session.HeartbeatFrame(readiness.State, lastSessionWakeProbeAt()))
				_ = r.writeCapabilitiesFrame(ctx, session, adapter, binding, readiness, writeFrame)
			}
		}
	}()
	draining := false
	drainOnce := sync.Once{}
	commandCache := newCommandFrameCache()
	for {
		var frame externalagentprotocol.Frame
		if err := conn.ReadJSON(&frame); err != nil {
			if draining {
				return errConnectorDraining
			}
			return nil
		}
		switch frame.MessageType {
		case externalagentprotocol.FrameTypeServerDraining:
			if frame.ServerDraining == nil {
				continue
			}
			draining = true
			drainOnce.Do(func() {
				go func() {
					_ = r.runBindingLoop(ctx, binding)
				}()
			})
			if deadline := frame.ServerDraining.DeadlineAt; !deadline.IsZero() {
				go func(deadline time.Time) {
					timer := time.NewTimer(time.Until(deadline))
					defer timer.Stop()
					select {
					case <-ctx.Done():
						return
					case <-timer.C:
						_ = conn.Close()
					}
				}(deadline.UTC())
			}
			continue
		case externalagentprotocol.FrameTypeWakeProbe:
			if frame.WakeProbe == nil {
				continue
			}
			if cached, ok := commandCache.cachedReply(frame); ok {
				if err := writeFrame(cached); err != nil {
					return fmt.Errorf("write cached wake probe ack: %w", err)
				}
				continue
			}
			readiness := r.bindingReadiness(adapter, binding)
			if !wakeProbeSucceeded(readiness.State) {
				continue
			}
			probedAt := r.now()
			_ = r.recordWakeProbe(binding.ConnectionID, binding.ConnectionGeneration, probedAt)
			recordSessionWakeProbe(probedAt)
			accepted := session.WakeProbeAcceptedFrame(frame.WakeProbe.ProbeID)
			commandCache.storeReply(frame, accepted)
			if err := writeFrame(accepted); err != nil {
				return fmt.Errorf("write wake probe ack: %w", err)
			}
			state := r.bindingReadiness(adapter, binding).State
			if err := writeFrame(session.HeartbeatFrame(state, lastSessionWakeProbeAt())); err != nil {
				return fmt.Errorf("write wake probe heartbeat: %w", err)
			}
		case externalagentprotocol.FrameTypeConfigRefresh:
			if frame.ConfigRefresh == nil {
				continue
			}
			if err := r.refreshMCPConfig(binding); err != nil {
				return fmt.Errorf("refresh mcp config: %w", err)
			}
		case externalagentprotocol.FrameTypeRunStart:
			if frame.RunStart == nil {
				continue
			}
			if cached, ok := commandCache.cachedReply(frame); ok {
				if err := writeFrame(cached); err != nil {
					return fmt.Errorf("write cached run response: %w", err)
				}
				continue
			}
			if r.activeRunMatchesWithoutNativeRunID(binding, frame) {
				_ = r.clearRunMCPToken(binding, frame.RunID)
				failed := session.RunTerminalFrame(frame, externalagentprotocol.RunStatusFailed, externalagentprotocol.TerminalReasonFailed, "active run native id missing")
				commandCache.storeReply(frame, failed)
				if writeErr := writeFrame(failed); writeErr != nil {
					return fmt.Errorf("write active run missing native id: %w", writeErr)
				}
				continue
			}
			readiness := r.bindingReadiness(adapter, binding)
			if !canStartRunWithReadiness(readiness.State, lastSessionWakeProbeAt()) {
				failed := session.RunTerminalFrame(frame, externalagentprotocol.RunStatusFailed, externalagentprotocol.TerminalReasonFailed, "external runtime is not ready: "+readiness.State.String())
				commandCache.storeReply(frame, failed)
				if writeErr := writeFrame(failed); writeErr != nil {
					return fmt.Errorf("write run readiness failure: %w", writeErr)
				}
				continue
			}
			if nativeRunID, ok := r.activeNativeRunIDForRunStart(binding, frame); ok {
				accepted := session.RunAcceptedFrame(frame, nativeRunID)
				commandCache.storeReply(frame, accepted)
				if err := writeFrame(accepted); err != nil {
					return fmt.Errorf("write redelivered run accepted: %w", err)
				}
				continue
			}
			if activeRunID, conflict := r.activeRunConflict(binding, frame); conflict {
				failed := session.RunTerminalFrame(frame, externalagentprotocol.RunStatusFailed, externalagentprotocol.TerminalReasonFailed, "external persona already has active run "+activeRunID)
				commandCache.storeReply(frame, failed)
				if writeErr := writeFrame(failed); writeErr != nil {
					return fmt.Errorf("write active run conflict: %w", writeErr)
				}
				continue
			}
			if err := r.activateRunMCPToken(binding, frame); err != nil {
				failed := session.RunTerminalFrame(frame, externalagentprotocol.RunStatusFailed, externalagentprotocol.TerminalReasonFailed, err.Error())
				commandCache.storeReply(frame, failed)
				if writeErr := writeFrame(failed); writeErr != nil {
					return fmt.Errorf("write run mcp token failure: %w", writeErr)
				}
				continue
			}
			nativeRunID, err := adapter.StartRun(runtime.RunRequest{
				RunID:                  frame.RunID,
				AssignmentID:           frame.AssignmentID,
				FullyComposedPrompt:    frame.RunStart.FullyComposedPrompt,
				NativeMCPServerName:    frame.RunStart.NativeMCPServerName,
				NativeMCPToolNamespace: frame.RunStart.NativeMCPToolNamespace,
				Metadata:               frame.RunStart.Metadata,
			})
			if err != nil {
				_ = r.clearRunMCPToken(binding, frame.RunID)
				failed := session.RunTerminalFrame(frame, externalagentprotocol.RunStatusFailed, externalagentprotocol.TerminalReasonFailed, err.Error())
				commandCache.storeReply(frame, failed)
				if writeErr := writeFrame(failed); writeErr != nil {
					return fmt.Errorf("write run failure: %w", writeErr)
				}
				continue
			}
			if err := r.recordNativeRunID(binding, frame.RunID, nativeRunID); err != nil {
				_ = r.clearRunMCPToken(binding, frame.RunID)
				failed := session.RunTerminalFrame(frame, externalagentprotocol.RunStatusFailed, externalagentprotocol.TerminalReasonFailed, err.Error())
				commandCache.storeReply(frame, failed)
				if writeErr := writeFrame(failed); writeErr != nil {
					return fmt.Errorf("write run journal failure: %w", writeErr)
				}
				continue
			}
			accepted := session.RunAcceptedFrame(frame, nativeRunID)
			commandCache.storeReply(frame, accepted)
			if err := writeFrame(accepted); err != nil {
				return fmt.Errorf("write run accepted: %w", err)
			}
			started := false
			writeStarted := func(startedAt time.Time) error {
				if started {
					return nil
				}
				started = true
				return writeFrame(session.RunStartedFrame(frame, nativeRunID, startedAt))
			}
			go func(frame externalagentprotocol.Frame, nativeRunID string) {
				defer func() {
					_ = r.clearRunMCPToken(binding, frame.RunID)
				}()
				observeCtx, cancelObserve := contextForRunDeadline(ctx, frame.RunStart.DeadlineAt)
				defer cancelObserve()
				result, err := adapter.StreamOrPollRun(observeCtx, nativeRunID, func(event runtime.RunEvent) error {
					switch event.Kind {
					case runtime.RunEventStarted:
						return writeStarted(event.StartedAt)
					case runtime.RunEventOutputDelta:
						return writeFrame(session.RunOutputDeltaFrame(frame, event.Delta))
					case runtime.RunEventToolEvent:
						return writeFrame(session.RunToolEventFrame(frame, event.ToolName, event.ToolPhase, event.Summary))
					default:
						return nil
					}
				})
				if err != nil {
					reason := externalagentprotocol.TerminalReasonFailed
					output := err.Error()
					if errors.Is(err, context.DeadlineExceeded) {
						reason = externalagentprotocol.TerminalReasonExpired
						output = "external agent run deadline exceeded"
					}
					failed := session.RunTerminalFrame(frame, externalagentprotocol.RunStatusFailed, reason, output)
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
			if commandCache.seen(frame) {
				continue
			}
			nativeRunID, err := r.nativeRunIDForCancel(binding, frame.RunID)
			if err != nil {
				continue
			}
			commandCache.mark(frame)
			if err := adapter.CancelRun(nativeRunID); err != nil {
				continue
			}
		case externalagentprotocol.FrameTypeTokenRevoked:
			if frame.TokenRevoked == nil {
				continue
			}
			if commandCache.seen(frame) {
				continue
			}
			commandCache.mark(frame)
			if err := r.revokeBinding(binding, adapter, frame.TokenRevoked.Reason); err != nil {
				return err
			}
			return nil
		}
	}
}

func (r Runner) refreshMCPConfig(binding config.Binding) error {
	latest, ok := r.Store.Binding(binding.ConnectionID)
	if !ok {
		return nil
	}
	_, err := (mcp.Installer{Store: r.Store}).InstallBinding(latest)
	return err
}

func (r Runner) now() time.Time {
	if r.Now != nil {
		return r.Now().UTC()
	}
	return time.Now().UTC()
}

type commandFrameCache struct {
	replies map[string]externalagentprotocol.Frame
	seenIDs map[string]struct{}
}

func newCommandFrameCache() *commandFrameCache {
	return &commandFrameCache{
		replies: map[string]externalagentprotocol.Frame{},
		seenIDs: map[string]struct{}{},
	}
}

func (cache *commandFrameCache) key(frame externalagentprotocol.Frame) string {
	return strings.TrimSpace(frame.MessageID)
}

func (cache *commandFrameCache) cachedReply(frame externalagentprotocol.Frame) (externalagentprotocol.Frame, bool) {
	key := cache.key(frame)
	if key == "" {
		return externalagentprotocol.Frame{}, false
	}
	reply, ok := cache.replies[key]
	return reply, ok
}

func (cache *commandFrameCache) storeReply(frame externalagentprotocol.Frame, reply externalagentprotocol.Frame) {
	key := cache.key(frame)
	if key == "" {
		return
	}
	cache.replies[key] = reply
	cache.seenIDs[key] = struct{}{}
}

func (cache *commandFrameCache) seen(frame externalagentprotocol.Frame) bool {
	key := cache.key(frame)
	if key == "" {
		return false
	}
	_, ok := cache.seenIDs[key]
	return ok
}

func (cache *commandFrameCache) mark(frame externalagentprotocol.Frame) {
	key := cache.key(frame)
	if key == "" {
		return
	}
	cache.seenIDs[key] = struct{}{}
}

func (r Runner) activeNativeRunIDForRunStart(binding config.Binding, frame externalagentprotocol.Frame) (string, bool) {
	if r.Store == nil {
		return "", false
	}
	latest, ok := r.Store.Binding(binding.ConnectionID)
	if !ok {
		return "", false
	}
	if strings.TrimSpace(latest.ActiveRunID) != strings.TrimSpace(frame.RunID) {
		return "", false
	}
	if strings.TrimSpace(latest.ActiveAssignmentID) != strings.TrimSpace(frame.AssignmentID) {
		return "", false
	}
	nativeRunID := strings.TrimSpace(latest.ActiveNativeRunID)
	if nativeRunID == "" {
		return "", false
	}
	return nativeRunID, true
}

func (r Runner) activeRunMatchesWithoutNativeRunID(binding config.Binding, frame externalagentprotocol.Frame) bool {
	if r.Store == nil {
		return false
	}
	latest, ok := r.Store.Binding(binding.ConnectionID)
	if !ok {
		return false
	}
	if strings.TrimSpace(latest.ActiveRunID) != strings.TrimSpace(frame.RunID) {
		return false
	}
	if strings.TrimSpace(latest.ActiveAssignmentID) != strings.TrimSpace(frame.AssignmentID) {
		return false
	}
	return strings.TrimSpace(latest.ActiveNativeRunID) == ""
}

func (r Runner) activeRunConflict(binding config.Binding, frame externalagentprotocol.Frame) (string, bool) {
	if r.Store == nil {
		return "", false
	}
	latest, ok := r.Store.Binding(binding.ConnectionID)
	if !ok {
		return "", false
	}
	activeRunID := strings.TrimSpace(latest.ActiveRunID)
	if activeRunID == "" {
		return "", false
	}
	if activeRunID == strings.TrimSpace(frame.RunID) && strings.TrimSpace(latest.ActiveAssignmentID) == strings.TrimSpace(frame.AssignmentID) {
		return "", false
	}
	return activeRunID, true
}

func (r Runner) replayActiveRun(ctx context.Context, binding config.Binding, session bridge.Session, adapter runtime.Adapter, writeFrame func(externalagentprotocol.Frame) error) error {
	if r.Store == nil {
		return nil
	}
	latest, ok := r.Store.Binding(binding.ConnectionID)
	if !ok {
		return nil
	}
	if strings.TrimSpace(latest.ActiveRunID) == "" || strings.TrimSpace(latest.ActiveAssignmentID) == "" || strings.TrimSpace(latest.ActiveNativeRunID) == "" {
		return nil
	}
	accepted := session.RunAcceptedFrame(externalagentprotocol.Frame{
		MessageID:    uuid.NewString(),
		RunID:        latest.ActiveRunID,
		AssignmentID: latest.ActiveAssignmentID,
	}, latest.ActiveNativeRunID)
	if err := writeFrame(accepted); err != nil {
		return err
	}
	replayRequest := externalagentprotocol.Frame{
		MessageID:    uuid.NewString(),
		RunID:        latest.ActiveRunID,
		AssignmentID: latest.ActiveAssignmentID,
	}
	go r.observeReplayedActiveRun(ctx, binding, session, adapter, replayRequest, latest.ActiveNativeRunID, writeFrame)
	return nil
}

func (r Runner) observeReplayedActiveRun(ctx context.Context, binding config.Binding, session bridge.Session, adapter runtime.Adapter, frame externalagentprotocol.Frame, nativeRunID string, writeFrame func(externalagentprotocol.Frame) error) {
	if adapter == nil {
		return
	}
	defer func() {
		_ = r.clearRunMCPToken(binding, frame.RunID)
	}()
	result, err := adapter.StreamOrPollRun(ctx, nativeRunID, func(event runtime.RunEvent) error {
		switch event.Kind {
		case runtime.RunEventOutputDelta:
			return writeFrame(session.RunOutputDeltaFrame(frame, event.Delta))
		case runtime.RunEventToolEvent:
			return writeFrame(session.RunToolEventFrame(frame, event.ToolName, event.ToolPhase, event.Summary))
		default:
			return nil
		}
	})
	if err != nil {
		_ = writeFrame(session.RunTerminalFrame(frame, externalagentprotocol.RunStatusFailed, externalagentprotocol.TerminalReasonFailed, err.Error()))
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
}

func contextForRunDeadline(parent context.Context, deadline time.Time) (context.Context, context.CancelFunc) {
	if deadline.IsZero() {
		return context.WithCancel(parent)
	}
	now := time.Now().UTC()
	if deadline.After(now) && deadline.Before(now.Add(minimumRunDeadline)) {
		deadline = now.Add(minimumRunDeadline)
	}
	return context.WithDeadline(parent, deadline.UTC())
}

func (r Runner) revokeBinding(binding config.Binding, adapter runtime.Adapter, reason string) error {
	latest, ok := r.Store.Binding(binding.ConnectionID)
	if ok && strings.TrimSpace(latest.ActiveNativeRunID) != "" {
		_ = adapter.CancelRun(strings.TrimSpace(latest.ActiveNativeRunID))
	}
	deleting, ok := r.Store.(config.DeletingStore)
	if !ok {
		return fmt.Errorf("deletable connector store required")
	}
	return deleting.DeleteBinding(binding.ConnectionID)
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
	active, ok := r.Store.Binding(binding.ConnectionID)
	if !ok {
		return fmt.Errorf("binding %s not found", binding.ConnectionID)
	}
	active.ActiveRunID = strings.TrimSpace(frame.RunID)
	active.ActiveAssignmentID = strings.TrimSpace(frame.AssignmentID)
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
	latest.ActiveAssignmentID = ""
	latest.ActiveNativeRunID = ""
	latest.ActiveRunMCPToken = ""
	latest.HasActiveRunMCPToken = false
	return writable.SaveBinding(latest)
}

func (r Runner) recordHeartbeat(connectionID config.ConnectionID, at time.Time) error {
	writable, ok := r.Store.(config.WritableStore)
	if !ok {
		return nil
	}
	latest, ok := r.Store.Binding(connectionID)
	if !ok {
		return nil
	}
	latest.LastHeartbeatAt = at.UTC()
	return writable.SaveBinding(latest)
}

func (r Runner) recordWakeProbe(connectionID config.ConnectionID, generation int64, at time.Time) error {
	writable, ok := r.Store.(config.WritableStore)
	if !ok {
		return nil
	}
	latest, ok := r.Store.Binding(connectionID)
	if !ok {
		return nil
	}
	latest.LastWakeProbeAt = at.UTC()
	latest.LastWakeProbeGeneration = generation
	return writable.SaveBinding(latest)
}

func (r Runner) lastWakeProbeAt(connectionID config.ConnectionID) *time.Time {
	latest, ok := r.Store.Binding(connectionID)
	if !ok || latest.LastWakeProbeAt.IsZero() {
		return nil
	}
	value := latest.LastWakeProbeAt.UTC()
	return &value
}

func (r Runner) nativeRunIDForCancel(binding config.Binding, runID string) (string, error) {
	latest, ok := r.Store.Binding(binding.ConnectionID)
	if !ok {
		return "", fmt.Errorf("binding %s not found", binding.ConnectionID)
	}
	if strings.TrimSpace(latest.ActiveRunID) != strings.TrimSpace(runID) {
		return "", fmt.Errorf("run %s is not active", strings.TrimSpace(runID))
	}
	nativeRunID := strings.TrimSpace(latest.ActiveNativeRunID)
	if nativeRunID == "" {
		return "", fmt.Errorf("native run id missing for run %s", strings.TrimSpace(runID))
	}
	return nativeRunID, nil
}

func (r Runner) bindingReadiness(adapter runtime.Adapter, binding config.Binding) runtime.Detection {
	detection := adapter.Detect()
	if detection.State != runtime.AdapterStateReady {
		return detection
	}
	homeDir, err := os.UserHomeDir()
	if err != nil {
		detection.State = runtime.AdapterStateMCPConfigMissing
		detection.Note = "resolve home dir: " + err.Error()
		return detection
	}
	verifyCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	verify := mcp.VerifyBindingWithLive(verifyCtx, homeDir, binding, nil)
	detection.State = verify.State
	detection.Note = verify.Note
	if verify.State != runtime.AdapterStateMCPVerified {
		return detection
	}
	return detection
}

func (r Runner) adapterForBinding(binding config.Binding) runtime.Adapter {
	if binding.RuntimeKind != runtime.AdapterKindOpenClaw {
		return runtime.NewAdapter(binding.RuntimeKind)
	}
	resolved := openclawauth.Result{}
	if openclawauth.GatewayIsLoopback(getenv("PERSONASTACK_CONNECTOR_OPENCLAW_GATEWAY_URL")) {
		var err error
		resolved, err = openclawauth.Resolve(openclawauth.Options{
			Binding: binding,
			Env:     getenv,
		})
		if err != nil {
			return runtime.NewErrorAdapter(runtime.AdapterKindOpenClaw, runtime.AdapterStateAuthMissing, err.Error())
		}
	}
	adapter := runtime.NewOpenClawAdapterWithAuth(getenv("PERSONASTACK_CONNECTOR_OPENCLAW_GATEWAY_URL"), resolved.Auth, binding.OpenClawAgentID)
	adapter.DeviceTokenSink = func(deviceToken string) error {
		writable, ok := r.Store.(config.WritableStore)
		if !ok {
			return nil
		}
		latest, ok := r.Store.Binding(binding.ConnectionID)
		if !ok {
			return nil
		}
		latest.OpenClawDeviceToken = strings.TrimSpace(deviceToken)
		latest.HasOpenClawDevice = latest.OpenClawDeviceToken != ""
		return writable.SaveBinding(latest)
	}
	return adapter
}

func (r Runner) writeCapabilitiesFrame(
	ctx context.Context,
	session bridge.Session,
	adapter runtime.Adapter,
	binding config.Binding,
	detection runtime.Detection,
	writeFrame func(externalagentprotocol.Frame) error,
) error {
	var nativeCapabilities []externalagentprotocol.NativeCapabilityReport
	describer, ok := adapter.(runtime.NativeCapabilityDescriber)
	if ok {
		describeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		capabilities, err := describer.DescribeNativeCapabilities(describeCtx, binding.NativeMCPServer)
		cancel()
		if err == nil {
			nativeCapabilities = nativeCapabilityReports(capabilities, r.now())
		}
	}
	frame := session.CapabilitiesFrame(detectionCapabilities(detection, r.now()), nativeCapabilities)
	if err := writeFrame(frame); err != nil {
		return fmt.Errorf("write capabilities frame: %w", err)
	}
	return nil
}

func detectionCapabilities(detection runtime.Detection, reportedAt time.Time) []externalagentprotocol.CapabilityReport {
	status := externalagentprotocol.ReadinessStatusRuntimeHealthy
	if detection.State == runtime.AdapterStateReady {
		status = externalagentprotocol.ReadinessStatusWakeable
	}
	if detection.State == runtime.AdapterStateMCPVerified || detection.State == runtime.AdapterStateMCPRestartRequired {
		status = externalagentprotocol.ReadinessStatusMCPConfigured
	}
	if detection.State == runtime.AdapterStateRuntimeMissing || detection.State == runtime.AdapterStateRuntimeStopped || detection.State == runtime.AdapterStateAuthMissing || detection.State == runtime.AdapterStateCapabilityMissing || detection.State == runtime.AdapterStateWakeProbeFailed {
		status = externalagentprotocol.ReadinessStatusRuntimeError
	}
	return []externalagentprotocol.CapabilityReport{
		{
			Kind:       externalagentprotocol.CapabilityKindRuntimeHealth,
			Status:     status,
			Label:      strings.TrimSpace(detection.Note),
			ReportedAt: reportedAt.UTC(),
		},
	}
}

func nativeCapabilityReports(capabilities []runtime.NativeCapability, reportedAt time.Time) []externalagentprotocol.NativeCapabilityReport {
	reports := make([]externalagentprotocol.NativeCapabilityReport, 0, len(capabilities))
	for _, capability := range capabilities {
		report := externalagentprotocol.NativeCapabilityReport{
			Source:       externalagentprotocol.NativeCapabilitySource(strings.TrimSpace(string(capability.Source))),
			Kind:         externalagentprotocol.NativeCapabilityKind(strings.TrimSpace(string(capability.Kind))),
			CapabilityID: strings.TrimSpace(capability.CapabilityID),
			Label:        strings.TrimSpace(capability.Label),
			Summary:      strings.TrimSpace(capability.Summary),
			Status:       externalagentprotocol.ReadinessStatusWakeable,
			ReportedAt:   reportedAt.UTC(),
		}
		if report.Source == "" || report.Kind == "" || report.CapabilityID == "" || report.Label == "" {
			continue
		}
		if report.Summary == "" {
			report.Summary = report.Label
		}
		reports = append(reports, report)
	}
	return reports
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func getenv(key string) string {
	return strings.TrimSpace(os.Getenv(key))
}

func canStartRunWithReadiness(state runtime.AdapterState, lastWakeProbeAt *time.Time) bool {
	if state == runtime.AdapterStateReady {
		return true
	}
	return state == runtime.AdapterStateMCPVerified && lastWakeProbeAt != nil && !lastWakeProbeAt.IsZero()
}

func wakeProbeSucceeded(state runtime.AdapterState) bool {
	return state == runtime.AdapterStateReady || state == runtime.AdapterStateMCPVerified
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

func jitterDuration(base time.Duration) time.Duration {
	if base <= 0 {
		return 0
	}
	if base < 100*time.Millisecond {
		return base
	}
	half := base / 2
	if half <= 0 {
		return base
	}
	return half + time.Duration(rand.Int63n(int64(base-half)+1))
}
