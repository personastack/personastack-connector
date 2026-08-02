package daemon

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/personastack/personastack-connector/internal/bridge"
	"github.com/personastack/personastack-connector/internal/config"
	"github.com/personastack/personastack-connector/internal/externalagentprotocol"
	"github.com/personastack/personastack-connector/internal/hermessetup"
	"github.com/personastack/personastack-connector/internal/mcp"
	"github.com/personastack/personastack-connector/internal/openclawauth"
	"github.com/personastack/personastack-connector/internal/runtime"
	"github.com/personastack/personastack-connector/internal/targetinventory"
	"github.com/personastack/personastack-connector/internal/targetruntime"
)

type Runner struct {
	Store                   config.Store
	ServiceScope            externalagentprotocol.ServiceScope
	Now                     func() time.Time
	ReconnectMin            time.Duration
	ReconnectMax            time.Duration
	ReadTimeout             time.Duration
	ReconcileMin            time.Duration
	ReconcileMax            time.Duration
	ReconcileAttemptTimeout time.Duration
}

var errConnectorBindingRemoved = errors.New("connector binding removed")
var errConnectorDraining = errors.New("connector draining")

const websocketReadTimeout = 60 * time.Second

type bindingRun struct {
	cancel       func()
	connectionID config.ConnectionID
	token        int
}

type bindingRunResult struct {
	connectionID config.ConnectionID
	token        int
	err          error
}

type bindingStartBackoff struct {
	nextAttemptAt time.Time
	backoff       time.Duration
}

type connectorDrainingError struct {
	deadlineAt time.Time
	reason     string
}

func (err connectorDrainingError) Error() string {
	reason := strings.TrimSpace(err.reason)
	if reason == "" {
		return errConnectorDraining.Error()
	}
	return errConnectorDraining.Error() + ": " + reason
}

func (err connectorDrainingError) Is(target error) bool {
	return target == errConnectorDraining
}

type observedRunCancel struct {
	token  int
	cancel context.CancelFunc
}

type runObservationRegistry struct {
	mu      sync.Mutex
	next    int
	cancels map[string]observedRunCancel
}

func newRunObservationRegistry() *runObservationRegistry {
	return &runObservationRegistry{cancels: map[string]observedRunCancel{}}
}

func (registry *runObservationRegistry) track(runID string, cancel context.CancelFunc) func() {
	runID = strings.TrimSpace(runID)
	if registry == nil || runID == "" {
		return cancel
	}
	registry.mu.Lock()
	registry.next++
	entry := observedRunCancel{token: registry.next, cancel: cancel}
	registry.cancels[runID] = entry
	registry.mu.Unlock()
	return func() {
		registry.mu.Lock()
		if current, ok := registry.cancels[runID]; ok && current.token == entry.token {
			delete(registry.cancels, runID)
		}
		registry.mu.Unlock()
		cancel()
	}
}

func (registry *runObservationRegistry) cancel(runID string) bool {
	runID = strings.TrimSpace(runID)
	if registry == nil || runID == "" {
		return false
	}
	registry.mu.Lock()
	entry, ok := registry.cancels[runID]
	if ok {
		delete(registry.cancels, runID)
	}
	registry.mu.Unlock()
	if ok {
		entry.cancel()
	}
	return ok
}

func (r Runner) RunForeground(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errs := make(chan bindingRunResult, 16)
	var wg sync.WaitGroup
	active := map[int]bindingRun{}
	startBackoffs := map[config.ConnectionID]bindingStartBackoff{}
	nextToken := 0
	sendResult := func(result bindingRunResult) {
		select {
		case errs <- result:
		case <-runCtx.Done():
		}
	}
	canStartBinding := func(binding config.Binding) bool {
		state, ok := startBackoffs[binding.ConnectionID]
		if !ok || state.nextAttemptAt.IsZero() {
			return true
		}
		return !r.now().Before(state.nextAttemptAt)
	}
	recordStartFailure := func(binding config.Binding) {
		state := startBackoffs[binding.ConnectionID]
		backoff := state.backoff
		if backoff <= 0 {
			backoff = r.reconnectMin()
		}
		startBackoffs[binding.ConnectionID] = bindingStartBackoff{
			nextAttemptAt: r.now().Add(jitterDuration(backoff)),
			backoff:       minDuration(backoff*2, r.reconnectMax()),
		}
	}
	hasActiveBinding := func(connectionID config.ConnectionID) bool {
		for _, run := range active {
			if run.connectionID == connectionID {
				return true
			}
		}
		return false
	}
	startBinding := func(binding config.Binding) error {
		if hasActiveBinding(binding.ConnectionID) {
			return nil
		}
		if !canStartBinding(binding) {
			return nil
		}
		nextToken++
		token := nextToken
		bindingCtx, bindingCancel := context.WithCancel(runCtx)
		logCancel := func() {
			bindingCancel()
		}
		componentResults := make(chan error, 2)
		componentCount := 1
		if strings.TrimSpace(binding.LocalMCPProxyURL) != "" {
			proxyErrs, err := mcp.StartLoopbackHTTPProxyWithStore(bindingCtx, r.Store, binding, nil)
			if err != nil {
				logCancel()
				return err
			}
			componentCount++
			go func() {
				componentResults <- <-proxyErrs
			}()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			go func() {
				componentResults <- r.runBinding(bindingCtx, binding)
			}()
			var resultErr error
			for completed := 0; completed < componentCount; completed++ {
				componentErr := <-componentResults
				if resultErr == nil && componentErr != nil {
					resultErr = componentErr
				}
				// Stop the sibling component as soon as either component exits.
				bindingCancel()
			}
			sendResult(bindingRunResult{connectionID: binding.ConnectionID, token: token, err: resultErr})
		}()
		active[token] = bindingRun{cancel: logCancel, connectionID: binding.ConnectionID, token: token}
		delete(startBackoffs, binding.ConnectionID)
		return nil
	}
	cancelMissingBindings := func(bindings []config.Binding) {
		present := map[config.ConnectionID]struct{}{}
		for _, binding := range bindings {
			present[binding.ConnectionID] = struct{}{}
		}
		for token, run := range active {
			if _, ok := present[run.connectionID]; ok {
				continue
			}
			run.cancel()
			delete(active, token)
		}
		for connectionID := range startBackoffs {
			if _, ok := present[connectionID]; ok {
				continue
			}
			delete(startBackoffs, connectionID)
		}
	}

	bindings := r.Store.ListBindings()
	for _, binding := range bindings {
		if err := startBinding(binding); err != nil {
			log.Printf("connector binding start failed connection_id=%s diagnostic=%s", binding.ConnectionID, safeDiagnosticNote(err.Error()))
			recordStartFailure(binding)
		}
	}

	ticker := time.NewTicker(r.reconnectMin())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			cancel()
			wg.Wait()
			return nil
		case result := <-errs:
			current, ok := active[result.token]
			if ok && current.connectionID != result.connectionID {
				continue
			}
			if ok {
				current.cancel()
				delete(active, result.token)
			}
			if ok && result.err != nil {
				log.Printf("connector binding stopped connection_id=%s diagnostic=%s", result.connectionID, safeDiagnosticNote(result.err.Error()))
				if binding, exists := r.Store.Binding(result.connectionID); exists {
					recordStartFailure(binding)
				}
			}
		case <-ticker.C:
			if len(active) > 0 && !supportsExternalBindingReload(r.Store) {
				continue
			}
			bindings := r.Store.ListBindings()
			cancelMissingBindings(bindings)
			for _, binding := range bindings {
				if err := startBinding(binding); err != nil {
					log.Printf("connector binding start failed connection_id=%s diagnostic=%s", binding.ConnectionID, safeDiagnosticNote(err.Error()))
					recordStartFailure(binding)
				}
			}
		}
	}
}

func supportsExternalBindingReload(store config.Store) bool {
	_, ok := store.(config.FileStore)
	return ok
}

func (r Runner) runBinding(ctx context.Context, binding config.Binding) error {
	return r.runBindingLoop(ctx, binding)
}

func (r Runner) runBindingLoop(ctx context.Context, binding config.Binding) error {
	connectionID := binding.ConnectionID
	backoff := r.reconnectMin()
	waitForBackoff := func() bool {
		delay := jitterDuration(backoff)
		if delay <= 0 {
			backoff = minDuration(backoff*2, r.reconnectMax())
			return true
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return false
		case <-timer.C:
		}
		backoff = minDuration(backoff*2, r.reconnectMax())
		return true
	}
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
			if !waitForBackoff() {
				return nil
			}
			continue
		}
		if err := writable.SaveBinding(current); err != nil {
			if !waitForBackoff() {
				return nil
			}
			continue
		}
		credential, err := bridge.CredentialFromBinding(current)
		if err != nil {
			if !waitForBackoff() {
				return nil
			}
			continue
		}
		session, err := bridge.NewSession(current, credential)
		if err != nil {
			if !waitForBackoff() {
				return nil
			}
			continue
		}
		session.ServiceScope = r.serviceScope()
		err = r.runBindingSession(ctx, current, session)
		if ctx.Err() != nil {
			return nil
		}
		if errors.Is(err, errConnectorBindingRemoved) {
			return nil
		}
		if errors.Is(err, errConnectorDraining) {
			delay := r.drainReconnectDelay(err)
			if delay > 0 {
				timer := time.NewTimer(delay)
				select {
				case <-ctx.Done():
					timer.Stop()
					return nil
				case <-timer.C:
				}
			}
			continue
		}
		if err == nil {
			backoff = r.reconnectMin()
		}
		if !waitForBackoff() {
			return nil
		}
	}
}

func (r Runner) drainReconnectDelay(err error) time.Duration {
	minDelay := jitterDuration(r.reconnectMin())
	var drainErr connectorDrainingError
	if errors.As(err, &drainErr) && !drainErr.deadlineAt.IsZero() {
		untilDeadline := drainErr.deadlineAt.Sub(r.now())
		if untilDeadline > minDelay {
			minDelay = untilDeadline
		}
	}
	return minDuration(minDelay, r.reconnectMax())
}

func (r Runner) serviceScope() externalagentprotocol.ServiceScope {
	if r.ServiceScope != "" {
		return r.ServiceScope
	}
	return externalagentprotocol.ServiceScopeUserLaunchAgent
}

func (r Runner) readTimeout() time.Duration {
	if r.ReadTimeout > 0 {
		return r.ReadTimeout
	}
	return websocketReadTimeout
}

func (r Runner) runBindingSession(ctx context.Context, binding config.Binding, session bridge.Session) error {
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, binding.GatewayWebsocketURL, nil)
	if err != nil {
		return fmt.Errorf("connect gateway websocket: %w", err)
	}
	defer conn.Close()
	sessionCtx, cancelSession := context.WithCancel(ctx)
	defer cancelSession()
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-sessionCtx.Done():
			_ = conn.Close()
		case <-done:
		}
	}()
	var writeMu sync.Mutex
	var writeFailureOnce sync.Once
	writeFrame := func(frame externalagentprotocol.Frame) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		err := conn.WriteJSON(frame)
		if err != nil {
			writeFailureOnce.Do(func() {
				cancelSession()
				_ = conn.Close()
			})
		}
		return err
	}
	extendReadDeadline := func() error {
		return conn.SetReadDeadline(r.now().Add(r.readTimeout()))
	}
	if err := extendReadDeadline(); err != nil {
		return fmt.Errorf("set websocket read deadline: %w", err)
	}
	conn.SetPongHandler(func(string) error {
		return extendReadDeadline()
	})
	writePing := func() error {
		writeMu.Lock()
		defer writeMu.Unlock()
		deadline := r.now().Add(5 * time.Second)
		err := conn.WriteControl(websocket.PingMessage, []byte("personastack-connector"), deadline)
		if err != nil {
			writeFailureOnce.Do(func() {
				cancelSession()
				_ = conn.Close()
			})
		}
		return err
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
	initialCtx, cancelInitial := context.WithTimeout(sessionCtx, 10*time.Second)
	initialDetection := safeDetection(runtime.DetectContext(initialCtx, adapter))
	cancelInitial()
	var reconciler *sessionReconciler
	capabilityReporter := newNativeCapabilityChangeReporter()
	if accepted.ConnectAccepted.ProtocolVersion == externalagentprotocol.ProtocolVersionV4 {
		reconciler = newSessionReconciler(sessionCtx, r, binding, adapter, initialDetection)
		reconciler.start(sessionCtx)
		defer reconciler.close()
	}
	sessionRuntime := func() (runtime.Adapter, runtime.Detection) {
		if accepted.ConnectAccepted.ProtocolVersion != externalagentprotocol.ProtocolVersionV4 {
			return adapter, safeDetection(r.bindingReadiness(adapter, binding))
		}
		snapshot := reconciler.snapshotCopy()
		if snapshot.Adapter == nil {
			return adapter, selectionRequiredDetection(snapshot.Detection)
		}
		return snapshot.Adapter, snapshot.Detection
	}
	_, detection := sessionRuntime()
	_ = r.recordHeartbeat(binding, r.now())
	var wakeProbeMu sync.Mutex
	var sessionWakeProbeAt *time.Time
	var runStartedMu sync.Mutex
	runStarted := map[string]bool{}
	lastSessionWakeProbeAt := func() *time.Time {
		if reconciler != nil {
			// V4 wake evidence is scoped to the reconciler epoch. Do not fall
			// back to the legacy session-wide value after a target changes.
			if value := reconciler.snapshotCopy().WakeProbeAt; value != nil {
				copy := value.UTC()
				return &copy
			}
			return nil
		}
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
	markRunStarted := func(runID string) bool {
		runID = strings.TrimSpace(runID)
		if runID == "" {
			return false
		}
		runStartedMu.Lock()
		defer runStartedMu.Unlock()
		if runStarted[runID] {
			return false
		}
		runStarted[runID] = true
		return true
	}
	heartbeat := session.HeartbeatFrameWithDetection(detection, lastSessionWakeProbeAt())
	if err := writeFrame(heartbeat); err != nil {
		return fmt.Errorf("write heartbeat frame: %w", err)
	}
	reportTargetInventory := func() error {
		inventory, warnings := targetinventory.Discover(binding.RuntimeKind, binding.BridgePrivateKey)
		for _, warning := range warnings {
			log.Printf("connector runtime target discovery warning connection_id=%s diagnostic=%s", binding.ConnectionID, safeDiagnosticNote(warning.Error()))
		}
		inventory.InventoryGeneration = r.now().UnixNano()
		if inventory.InventoryGeneration <= 0 {
			inventory.InventoryGeneration = 1
		}
		if err := writeFrame(externalagentprotocol.Frame{
			MessageID:            uuid.NewString(),
			MessageType:          externalagentprotocol.FrameTypeTargetInventory,
			PersonaID:            string(binding.PersonaID),
			ConnectionID:         string(binding.ConnectionID),
			ConnectionGeneration: binding.ConnectionGeneration,
			SentAt:               r.now(),
			TargetInventory:      &inventory,
		}); err != nil {
			return fmt.Errorf("write runtime target inventory: %w", err)
		}
		return nil
	}
	if accepted.ConnectAccepted.ProtocolVersion == externalagentprotocol.ProtocolVersionV4 {
		capabilityAdapter, capabilityDetection := sessionRuntime()
		if err := capabilityReporter.writeIfChanged(sessionCtx, r, session, capabilityAdapter, binding, capabilityDetection, lastSessionWakeProbeAt, writeFrame); err != nil {
			return err
		}
		if err := reportTargetInventory(); err != nil {
			return err
		}
	}
	runObservations := newRunObservationRegistry()
	type pendingRefresh struct {
		target   *externalagentprotocol.RuntimeTarget
		clear    bool
		revision int64
		epoch    uint64
	}
	var pendingRuntimeRefresh *pendingRefresh
	applyConfigRefresh := func(refresh pendingRefresh) error {
		if refresh.clear {
			reconciler.clearTarget(refresh.revision, refresh.epoch)
			return nil
		}
		if refresh.target == nil {
			return fmt.Errorf("runtime target required for config refresh")
		}
		reconciler.setTarget(refresh.target, refresh.epoch)
		return nil
	}
	if accepted.ConnectAccepted.ProtocolVersion != externalagentprotocol.ProtocolVersionV4 {
		if err := r.replayActiveRun(sessionCtx, binding, session, adapter, runObservations, writeFrame); err != nil {
			return err
		}
	} else {
		// V4 replay is fenced behind the API-selected target and its reconciler.
		go func() {
			ticker := time.NewTicker(250 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-sessionCtx.Done():
					return
				case <-ticker.C:
					snapshot := reconciler.snapshotCopy()
					if snapshot.Target == nil || snapshot.Adapter == nil || !canStartRunWithReadiness(snapshot.Detection.State, snapshot.WakeProbeAt) {
						continue
					}
					if err := r.replayActiveRun(sessionCtx, binding, session, snapshot.Adapter, runObservations, writeFrame); err != nil {
						return
					}
					return
				}
			}
		}()
	}
	if r.Store != nil {
		if latest, ok := r.Store.Binding(binding.ConnectionID); ok && strings.TrimSpace(latest.ActiveRunID) != "" && strings.TrimSpace(latest.ActiveNativeRunID) != "" {
			_ = markRunStarted(latest.ActiveRunID)
		}
	}
	heartbeatStop := make(chan struct{})
	defer close(heartbeatStop)
	go func() {
		heartbeatAdapter, readiness := sessionRuntime()
		_ = capabilityReporter.writeIfChanged(sessionCtx, r, session, heartbeatAdapter, binding, readiness, lastSessionWakeProbeAt, writeFrame)
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		inventoryTicker := time.NewTicker(5 * time.Minute)
		defer inventoryTicker.Stop()
		for {
			select {
			case <-heartbeatStop:
				return
			case <-sessionCtx.Done():
				return
			case <-ticker.C:
				_ = r.recordHeartbeat(binding, r.now())
				heartbeatAdapter, readiness := sessionRuntime()
				snapshot := runtimeSnapshot{}
				if reconciler != nil {
					snapshot = reconciler.snapshotCopy()
				}
				_ = writeFrame(session.HeartbeatFrameWithDetectionAndTarget(readiness, lastSessionWakeProbeAt(), snapshot.TargetRevision, snapshot.TargetEpoch))
				_ = writePing()
				_ = capabilityReporter.writeIfChanged(sessionCtx, r, session, heartbeatAdapter, binding, readiness, lastSessionWakeProbeAt, writeFrame)
			case <-inventoryTicker.C:
				if accepted.ConnectAccepted.ProtocolVersion == externalagentprotocol.ProtocolVersionV4 {
					_ = reportTargetInventory()
				}
			}
		}
	}()
	commandCache := newCommandFrameCache()
	for {
		var frame externalagentprotocol.Frame
		if err := conn.ReadJSON(&frame); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("read gateway websocket frame: %w", err)
		}
		_ = extendReadDeadline()
		switch frame.MessageType {
		case externalagentprotocol.FrameTypeServerDraining:
			if frame.ServerDraining == nil {
				continue
			}
			return connectorDrainingError{
				deadlineAt: frame.ServerDraining.DeadlineAt,
				reason:     frame.ServerDraining.Reason,
			}
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
			wakeSnapshot := runtimeSnapshot{}
			if reconciler != nil {
				initialSnapshot := reconciler.snapshotCopy()
				if initialSnapshot.Target == nil {
					continue
				}
				wakeContext := sessionCtx
				cancelWake := func() {}
				if !frame.WakeProbe.DeadlineAt.IsZero() {
					wakeContext, cancelWake = context.WithDeadline(sessionCtx, frame.WakeProbe.DeadlineAt)
				}
				var ok bool
				wakeSnapshot, ok = reconciler.waitForWakeable(wakeContext, initialSnapshot.Epoch)
				cancelWake()
				if !ok {
					continue
				}
			}
			wakeAdapter, readiness := sessionRuntime()
			if reconciler != nil {
				wakeAdapter = wakeSnapshot.Adapter
				readiness = wakeSnapshot.Detection
			}
			if !wakeProbeSucceeded(readiness.State) {
				continue
			}
			probedAt := r.now()
			if reconciler != nil && !reconciler.recordWakeProbe(probedAt, wakeSnapshot.Epoch) {
				continue
			}
			_ = r.recordWakeProbe(binding, probedAt)
			recordSessionWakeProbe(probedAt)
			accepted := session.WakeProbeAcceptedFrameForRequestWithTarget(frame, wakeSnapshot.TargetRevision, wakeSnapshot.TargetEpoch)
			commandCache.storeReply(frame, accepted)
			if err := writeFrame(accepted); err != nil {
				return fmt.Errorf("write wake probe ack: %w", err)
			}
			_, wakeReadiness := sessionRuntime()
			if err := writeFrame(session.HeartbeatFrameWithDetectionAndTarget(wakeReadiness, lastSessionWakeProbeAt(), wakeSnapshot.TargetRevision, wakeSnapshot.TargetEpoch)); err != nil {
				return fmt.Errorf("write wake probe heartbeat: %w", err)
			}
			_ = capabilityReporter.writeIfChanged(sessionCtx, r, session, wakeAdapter, binding, wakeReadiness, lastSessionWakeProbeAt, writeFrame)
		case externalagentprotocol.FrameTypeConfigRefresh:
			if frame.ConfigRefresh == nil {
				continue
			}
			if frame.ConfigRefresh.RuntimeTarget == nil && !frame.ConfigRefresh.ClearRuntimeTarget {
				return fmt.Errorf("runtime target required for config refresh")
			}
			refresh := pendingRefresh{
				target:   cloneRuntimeTarget(frame.ConfigRefresh.RuntimeTarget),
				clear:    frame.ConfigRefresh.ClearRuntimeTarget,
				revision: frame.ConfigRefresh.SelectionRevision,
				epoch:    frame.ConfigRefresh.TargetEpoch,
			}
			if refresh.target != nil && refresh.revision == 0 {
				refresh.revision = refresh.target.SelectionRevision
			}
			deferRefresh := r.bindingHasActiveRun(binding)
			if deferRefresh && reconciler != nil && reconciler.snapshotCopy().Target == nil {
				// A reconnect has no in-memory target. Apply the first API-selected
				// target so a persisted active run can be replayed and recovered.
				deferRefresh = false
			}
			if deferRefresh {
				pendingRuntimeRefresh = &refresh
				log.Printf("connector runtime target refresh deferred connection_id=%s active_run_id=%s", binding.ConnectionID, r.activeRunID(binding))
				continue
			}
			if err := applyConfigRefresh(refresh); err != nil {
				return err
			}
		case externalagentprotocol.FrameTypeConfigClear:
			if frame.ConfigClear == nil || frame.ConfigClear.TargetSelectionRevision <= 0 {
				return fmt.Errorf("target selection revision required for config clear")
			}
			refresh := pendingRefresh{
				clear:    true,
				revision: frame.ConfigClear.TargetSelectionRevision,
				epoch:    frame.ConfigClear.TargetEpoch,
			}
			if r.bindingHasActiveRun(binding) {
				pendingRuntimeRefresh = &refresh
				log.Printf("connector runtime target clear deferred connection_id=%s active_run_id=%s", binding.ConnectionID, r.activeRunID(binding))
				continue
			}
			if err := applyConfigRefresh(refresh); err != nil {
				return err
			}
		case externalagentprotocol.FrameTypeRunTerminalAck:
			if frame.RunTerminalAck == nil {
				continue
			}
			runObservations.cancel(frame.RunID)
			if err := r.clearRunState(binding, frame.RunID); err != nil {
				return fmt.Errorf("clear acknowledged run state: %w", err)
			}
			if pendingRuntimeRefresh != nil {
				refresh := *pendingRuntimeRefresh
				pendingRuntimeRefresh = nil
				if err := applyConfigRefresh(refresh); err != nil {
					return err
				}
			}
		case externalagentprotocol.FrameTypeRunStart:
			if frame.RunStart == nil {
				continue
			}
			if cached, ok := commandCache.cachedReplies(frame); ok {
				for _, reply := range cached {
					if err := writeFrame(reply); err != nil {
						return fmt.Errorf("write cached run response: %w", err)
					}
				}
				continue
			}
			if nativeRunID, ok := r.activeNativeRunIDForRunStart(binding, frame); ok {
				accepted := session.RunAcceptedFrame(frame, nativeRunID)
				started := session.RunStartedFrame(frame, nativeRunID, time.Time{})
				commandCache.storeReplies(frame, []externalagentprotocol.Frame{accepted, started})
				if err := writeFrame(accepted); err != nil {
					return fmt.Errorf("write redelivered run accepted: %w", err)
				}
				_ = markRunStarted(frame.RunID)
				if err := writeFrame(started); err != nil {
					return fmt.Errorf("write redelivered run started: %w", err)
				}
				continue
			}
			if r.activeRunMatchesWithoutNativeRunID(binding, frame) {
				failed := session.RunTerminalFrame(frame, externalagentprotocol.RunStatusFailed, externalagentprotocol.TerminalReasonFailed, "active run native id missing")
				commandCache.storeReply(frame, failed)
				if writeErr := writeFrame(failed); writeErr != nil {
					return fmt.Errorf("write active run missing native id: %w", writeErr)
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
			runAdapter := adapter
			readiness := runtime.Detection{}
			if accepted.ConnectAccepted.ProtocolVersion == externalagentprotocol.ProtocolVersionV4 {
				snapshot := reconciler.snapshotCopy()
				if snapshot.Target == nil || !runtimeTargetsEqual(snapshot.Target, frame.RunStart.RuntimeTarget) || snapshot.Adapter == nil {
					failed := session.RunTerminalFrame(frame, externalagentprotocol.RunStatusFailed, externalagentprotocol.TerminalReasonFailed, "external runtime is not ready: selected target is unavailable")
					commandCache.storeReply(frame, failed)
					if writeErr := writeFrame(failed); writeErr != nil {
						return fmt.Errorf("write runtime target failure: %w", writeErr)
					}
					continue
				}
				runAdapter = snapshot.Adapter
				binding.OpenClawAgentID = snapshot.Resolved.OpenClawAgentID
				readiness = snapshot.Detection
			} else {
				readiness = r.bindingReadiness(runAdapter, binding)
			}
			if !canStartRunWithReadiness(readiness.State, lastSessionWakeProbeAt()) {
				failed := session.RunTerminalFrame(frame, externalagentprotocol.RunStatusFailed, externalagentprotocol.TerminalReasonFailed, "external runtime is not ready: "+readiness.State.String())
				commandCache.storeReply(frame, failed)
				if writeErr := writeFrame(failed); writeErr != nil {
					return fmt.Errorf("write run readiness failure: %w", writeErr)
				}
				continue
			}
			if err := r.activateRun(binding, frame); err != nil {
				failed := session.RunTerminalFrame(frame, externalagentprotocol.RunStatusFailed, externalagentprotocol.TerminalReasonFailed, safeDiagnosticNote(err.Error()))
				commandCache.storeReply(frame, failed)
				if writeErr := writeFrame(failed); writeErr != nil {
					return fmt.Errorf("write run activation failure: %w", writeErr)
				}
				continue
			}
			nativeRunID, err := runAdapter.StartRun(runtime.RunRequest{
				RunID:                  frame.RunID,
				AssignmentID:           frame.AssignmentID,
				FullyComposedPrompt:    frame.RunStart.FullyComposedPrompt,
				NativeMCPServerName:    frame.RunStart.NativeMCPServerName,
				NativeMCPToolNamespace: frame.RunStart.NativeMCPToolNamespace,
				Metadata:               frame.RunStart.Metadata,
			})
			if err != nil {
				failed := session.RunTerminalFrame(frame, externalagentprotocol.RunStatusFailed, externalagentprotocol.TerminalReasonFailed, safeDiagnosticNote(err.Error()))
				commandCache.storeReply(frame, failed)
				if writeErr := writeFrame(failed); writeErr != nil {
					return fmt.Errorf("write run failure: %w", writeErr)
				}
				continue
			}
			if err := r.recordNativeRunID(binding, frame.RunID, nativeRunID); err != nil {
				failed := runRecordFailureTerminal(runAdapter, session, frame, nativeRunID, err)
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
			writeStarted := func(startedAt time.Time) error {
				if !markRunStarted(frame.RunID) {
					return nil
				}
				started := session.RunStartedFrame(frame, nativeRunID, startedAt)
				commandCache.storeReplies(frame, []externalagentprotocol.Frame{accepted, started})
				return writeFrame(started)
			}
			go func(frame externalagentprotocol.Frame, nativeRunID string, executionAdapter runtime.Adapter) {
				observeCtx, cancelObserve := contextForRunDeadline(sessionCtx, frame.RunStart.DeadlineAt)
				defer runObservations.track(frame.RunID, cancelObserve)()
				result, err := executionAdapter.StreamOrPollRun(observeCtx, nativeRunID, func(event runtime.RunEvent) error {
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
					if errors.Is(observeCtx.Err(), context.Canceled) {
						return
					}
					reason := externalagentprotocol.TerminalReasonFailed
					output := safeDiagnosticNote(err.Error())
					if errors.Is(observeCtx.Err(), context.DeadlineExceeded) {
						reason = externalagentprotocol.TerminalReasonExpired
						output = "external agent run deadline exceeded"
					}
					failed := session.RunTerminalFrame(frame, externalagentprotocol.RunStatusFailed, reason, output)
					if writeErr := writeFrame(failed); writeErr != nil {
						return
					}
					if reason == externalagentprotocol.TerminalReasonExpired {
						go func() {
							_ = executionAdapter.CancelRun(nativeRunID)
						}()
					}
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
				if writeErr := writeFrame(session.RunTerminalFrame(frame, status, reason, result.Output)); writeErr != nil {
					return
				}
			}(frame, nativeRunID, runAdapter)
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
			_ = adapter.CancelRun(nativeRunID)
			runObservations.cancel(frame.RunID)
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
			return errConnectorBindingRemoved
		}
	}
}

func (r Runner) refreshMCPConfig(binding config.Binding, targets ...*externalagentprotocol.RuntimeTarget) error {
	latest, ok := r.Store.Binding(binding.ConnectionID)
	if !ok {
		return nil
	}
	if latest.ConnectionGeneration != binding.ConnectionGeneration {
		return nil
	}
	var target *externalagentprotocol.RuntimeTarget
	if len(targets) > 0 {
		target = targets[0]
	}
	if target == nil {
		return fmt.Errorf("runtime target required for MCP refresh")
	}
	resolved, err := targetinventory.Resolve(latest.RuntimeKind, target, latest.BridgePrivateKey)
	if err != nil {
		return err
	}
	transport := mcp.MCPProxyTransportAuto
	if strings.TrimSpace(latest.LocalMCPProxyURL) != "" || strings.TrimSpace(latest.LocalMCPProxyToken) != "" {
		transport = mcp.MCPProxyTransportLoopbackHTTP
	}
	// The selected OpenClaw agent is operation-scoped. Installer calls do not
	// save Binding, and config.Store scrubs this legacy field on every write.
	latest.OpenClawAgentID = resolved.OpenClawAgentID
	_, err = (mcp.Installer{Store: r.Store, Transport: transport}).InstallBindingForTarget(
		latest,
		resolved.HomeDir,
		resolved.HermesHome,
		hermessetup.ProcessIdentity{Username: resolved.Username, HomeDir: resolved.HomeDir, UID: resolved.UID, GID: resolved.GID, GroupIDs: resolved.GroupIDs},
	)
	return err
}

func (r Runner) now() time.Time {
	if r.Now != nil {
		return r.Now().UTC()
	}
	return time.Now().UTC()
}

func (r Runner) bindingHasActiveRun(binding config.Binding) bool {
	return r.activeRunID(binding) != ""
}

func (r Runner) activeRunID(binding config.Binding) string {
	if r.Store == nil {
		return ""
	}
	latest, ok := r.Store.Binding(binding.ConnectionID)
	if !ok || latest.ConnectionGeneration != binding.ConnectionGeneration {
		return ""
	}
	return strings.TrimSpace(latest.ActiveRunID)
}

func cloneRuntimeTarget(target *externalagentprotocol.RuntimeTarget) *externalagentprotocol.RuntimeTarget {
	if target == nil {
		return nil
	}
	copy := *target
	return &copy
}

type commandFrameCache struct {
	mu      sync.Mutex
	replies map[string][]externalagentprotocol.Frame
	seenIDs map[string]struct{}
}

func newCommandFrameCache() *commandFrameCache {
	return &commandFrameCache{
		replies: map[string][]externalagentprotocol.Frame{},
		seenIDs: map[string]struct{}{},
	}
}

func (cache *commandFrameCache) key(frame externalagentprotocol.Frame) string {
	return strings.TrimSpace(frame.MessageID)
}

func (cache *commandFrameCache) cachedReply(frame externalagentprotocol.Frame) (externalagentprotocol.Frame, bool) {
	replies, ok := cache.cachedReplies(frame)
	if !ok || len(replies) == 0 {
		return externalagentprotocol.Frame{}, false
	}
	return replies[0], true
}

func (cache *commandFrameCache) cachedReplies(frame externalagentprotocol.Frame) ([]externalagentprotocol.Frame, bool) {
	key := cache.key(frame)
	if key == "" {
		return nil, false
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	replies, ok := cache.replies[key]
	if !ok {
		return nil, false
	}
	return append([]externalagentprotocol.Frame(nil), replies...), true
}

func (cache *commandFrameCache) storeReply(frame externalagentprotocol.Frame, reply externalagentprotocol.Frame) {
	cache.storeReplies(frame, []externalagentprotocol.Frame{reply})
}

func (cache *commandFrameCache) storeReplies(frame externalagentprotocol.Frame, replies []externalagentprotocol.Frame) {
	key := cache.key(frame)
	if key == "" {
		return
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	cache.replies[key] = append([]externalagentprotocol.Frame(nil), replies...)
	cache.seenIDs[key] = struct{}{}
}

func (cache *commandFrameCache) seen(frame externalagentprotocol.Frame) bool {
	key := cache.key(frame)
	if key == "" {
		return false
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	_, ok := cache.seenIDs[key]
	return ok
}

func (cache *commandFrameCache) mark(frame externalagentprotocol.Frame) {
	key := cache.key(frame)
	if key == "" {
		return
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
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
	if latest.ConnectionGeneration != binding.ConnectionGeneration {
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
	if latest.ConnectionGeneration != binding.ConnectionGeneration {
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
	if latest.ConnectionGeneration != binding.ConnectionGeneration {
		return "", false
	}
	activeRunID := strings.TrimSpace(latest.ActiveRunID)
	if activeRunID == "" {
		return "", false
	}
	if activeRunID == strings.TrimSpace(frame.RunID) && strings.TrimSpace(latest.ActiveAssignmentID) == strings.TrimSpace(frame.AssignmentID) {
		return "", false
	}
	if r.clearStaleActiveRunConflict(binding, latest) {
		return "", false
	}
	return activeRunID, true
}

func (r Runner) clearStaleActiveRunConflict(binding config.Binding, latest config.Binding) bool {
	return r.clearRunState(binding, latest.ActiveRunID) == nil
}

func (r Runner) replayActiveRun(ctx context.Context, binding config.Binding, session bridge.Session, adapter runtime.Adapter, runObservations *runObservationRegistry, writeFrame func(externalagentprotocol.Frame) error) error {
	if r.Store == nil {
		return nil
	}
	latest, ok := r.Store.Binding(binding.ConnectionID)
	if !ok {
		return nil
	}
	if latest.ConnectionGeneration != binding.ConnectionGeneration {
		return nil
	}
	if strings.TrimSpace(latest.ActiveRunID) == "" || strings.TrimSpace(latest.ActiveAssignmentID) == "" || strings.TrimSpace(latest.ActiveNativeRunID) == "" {
		return nil
	}
	if activeRunDeadlineExpired(latest.ActiveRunDeadlineAt, r.now()) {
		nativeRunID := strings.TrimSpace(latest.ActiveNativeRunID)
		if adapter != nil && nativeRunID != "" {
			go func() {
				_ = adapter.CancelRun(nativeRunID)
			}()
		}
		return r.clearRunState(binding, latest.ActiveRunID)
	}
	accepted := session.RunAcceptedFrame(externalagentprotocol.Frame{
		MessageID:    uuid.NewString(),
		RunID:        latest.ActiveRunID,
		AssignmentID: latest.ActiveAssignmentID,
	}, latest.ActiveNativeRunID)
	if err := writeFrame(accepted); err != nil {
		return err
	}
	started := session.RunStartedFrame(externalagentprotocol.Frame{
		MessageID:    uuid.NewString(),
		RunID:        latest.ActiveRunID,
		AssignmentID: latest.ActiveAssignmentID,
	}, latest.ActiveNativeRunID, time.Time{})
	if err := writeFrame(started); err != nil {
		return err
	}
	replayRequest := externalagentprotocol.Frame{
		MessageID:    uuid.NewString(),
		RunID:        latest.ActiveRunID,
		AssignmentID: latest.ActiveAssignmentID,
	}
	go r.observeActiveRun(ctx, binding, session, adapter, replayRequest, latest.ActiveNativeRunID, latest.ActiveRunDeadlineAt, runObservations, writeFrame)
	return nil
}

func activeRunDeadlineExpired(deadline time.Time, now time.Time) bool {
	return !deadline.IsZero() && !now.Before(deadline.UTC())
}

func (r Runner) observeReplayedActiveRun(ctx context.Context, binding config.Binding, session bridge.Session, adapter runtime.Adapter, frame externalagentprotocol.Frame, nativeRunID string, deadline time.Time, writeFrame func(externalagentprotocol.Frame) error) {
	r.observeActiveRun(ctx, binding, session, adapter, frame, nativeRunID, deadline, nil, writeFrame)
}

func (r Runner) observeActiveRun(ctx context.Context, binding config.Binding, session bridge.Session, adapter runtime.Adapter, frame externalagentprotocol.Frame, nativeRunID string, deadline time.Time, runObservations *runObservationRegistry, writeFrame func(externalagentprotocol.Frame) error) {
	if adapter == nil {
		return
	}
	observeCtx, cancelObserve := contextForRunDeadline(ctx, deadline)
	defer runObservations.track(frame.RunID, cancelObserve)()
	result, err := adapter.StreamOrPollRun(observeCtx, nativeRunID, func(event runtime.RunEvent) error {
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
		if errors.Is(observeCtx.Err(), context.Canceled) {
			return
		}
		reason := externalagentprotocol.TerminalReasonFailed
		output := safeDiagnosticNote(err.Error())
		if errors.Is(observeCtx.Err(), context.DeadlineExceeded) {
			reason = externalagentprotocol.TerminalReasonExpired
			output = "external agent run deadline exceeded"
		}
		if writeErr := writeFrame(session.RunTerminalFrame(frame, externalagentprotocol.RunStatusFailed, reason, output)); writeErr != nil {
			return
		}
		if reason == externalagentprotocol.TerminalReasonExpired {
			go func() {
				_ = adapter.CancelRun(nativeRunID)
			}()
		}
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
	if writeErr := writeFrame(session.RunTerminalFrame(frame, status, reason, result.Output)); writeErr != nil {
		return
	}
}

func contextForRunDeadline(parent context.Context, deadline time.Time) (context.Context, context.CancelFunc) {
	if deadline.IsZero() {
		return context.WithCancel(parent)
	}
	return context.WithDeadline(parent, deadline.UTC())
}

func (r Runner) revokeBinding(binding config.Binding, adapter runtime.Adapter, reason string) error {
	latest, ok := r.Store.Binding(binding.ConnectionID)
	if ok && latest.ConnectionGeneration != binding.ConnectionGeneration {
		return nil
	}
	if ok && strings.TrimSpace(latest.ActiveNativeRunID) != "" {
		_ = adapter.CancelRun(strings.TrimSpace(latest.ActiveNativeRunID))
	}
	deleting, ok := r.Store.(config.DeletingStore)
	if !ok {
		return fmt.Errorf("deletable connector store required")
	}
	return deleting.DeleteBinding(binding.ConnectionID)
}

func (r Runner) activateRun(binding config.Binding, frame externalagentprotocol.Frame) error {
	writable, ok := r.Store.(config.WritableStore)
	if !ok {
		return fmt.Errorf("writable connector store required")
	}
	active, ok := r.Store.Binding(binding.ConnectionID)
	if !ok {
		return fmt.Errorf("binding %s not found", binding.ConnectionID)
	}
	if active.ConnectionGeneration != binding.ConnectionGeneration {
		return fmt.Errorf("stale connection generation")
	}
	active.ActiveRunID = strings.TrimSpace(frame.RunID)
	active.ActiveAssignmentID = strings.TrimSpace(frame.AssignmentID)
	if frame.RunStart != nil {
		active.ActiveRunDeadlineAt = frame.RunStart.DeadlineAt.UTC()
	}
	return writable.SaveBinding(active)
}

func runRecordFailureTerminal(adapter runtime.Adapter, session bridge.Session, frame externalagentprotocol.Frame, nativeRunID string, recordErr error) externalagentprotocol.Frame {
	_ = adapter.CancelRun(strings.TrimSpace(nativeRunID))
	failed := session.RunTerminalFrame(frame, externalagentprotocol.RunStatusFailed, externalagentprotocol.TerminalReasonFailed, safeDiagnosticNote(recordErr.Error()))
	if failed.RunTerminal != nil {
		failed.RunTerminal.NativeRunID = strings.TrimSpace(nativeRunID)
	}
	return failed
}

func (r Runner) recordNativeRunID(binding config.Binding, runID string, nativeRunID string) error {
	writable, ok := r.Store.(config.WritableStore)
	if !ok {
		return fmt.Errorf("writable connector store required")
	}
	latest, ok := r.Store.Binding(binding.ConnectionID)
	if !ok {
		return fmt.Errorf("binding %s not found", binding.ConnectionID)
	}
	if latest.ConnectionGeneration != binding.ConnectionGeneration {
		return fmt.Errorf("stale connection generation")
	}
	if strings.TrimSpace(latest.ActiveRunID) != strings.TrimSpace(runID) {
		return fmt.Errorf("active run changed before native run id journal")
	}
	latest.ActiveNativeRunID = strings.TrimSpace(nativeRunID)
	return writable.SaveBinding(latest)
}

func (r Runner) clearRunState(binding config.Binding, runID string) error {
	writable, ok := r.Store.(config.WritableStore)
	if !ok {
		return nil
	}
	latest, ok := r.Store.Binding(binding.ConnectionID)
	if !ok {
		return nil
	}
	if latest.ConnectionGeneration != binding.ConnectionGeneration {
		return nil
	}
	if strings.TrimSpace(latest.ActiveRunID) != strings.TrimSpace(runID) {
		return nil
	}
	latest.ActiveRunID = ""
	latest.ActiveAssignmentID = ""
	latest.ActiveNativeRunID = ""
	latest.ActiveRunDeadlineAt = time.Time{}
	return writable.SaveBinding(latest)
}

func (r Runner) recordHeartbeat(binding config.Binding, at time.Time) error {
	writable, ok := r.Store.(config.WritableStore)
	if !ok {
		return nil
	}
	latest, ok := r.Store.Binding(binding.ConnectionID)
	if !ok {
		return nil
	}
	if latest.ConnectionGeneration != binding.ConnectionGeneration {
		return nil
	}
	latest.LastHeartbeatAt = at.UTC()
	return writable.SaveBinding(latest)
}

func (r Runner) recordWakeProbe(binding config.Binding, at time.Time) error {
	writable, ok := r.Store.(config.WritableStore)
	if !ok {
		return nil
	}
	latest, ok := r.Store.Binding(binding.ConnectionID)
	if !ok {
		return nil
	}
	if latest.ConnectionGeneration != binding.ConnectionGeneration {
		return nil
	}
	latest.LastWakeProbeAt = at.UTC()
	latest.LastWakeProbeGeneration = binding.ConnectionGeneration
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
	if latest.ConnectionGeneration != binding.ConnectionGeneration {
		return "", fmt.Errorf("stale connection generation")
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
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return runtime.Detection{Kind: binding.RuntimeKind, State: runtime.AdapterStateMCPConfigMissing, Note: "resolve home dir: " + err.Error()}
	}
	return r.bindingReadinessAtHome(adapter, binding, homeDir, binding.HermesHome)
}

func (r Runner) bindingReadinessAtHome(adapter runtime.Adapter, binding config.Binding, homeDir string, hermesHome string, runtimeURLs ...string) runtime.Detection {
	ctx, cancel := context.WithTimeout(context.Background(), r.reconcileAttemptTimeout())
	defer cancel()
	return r.bindingReadinessAtHomeContext(ctx, adapter, binding, homeDir, hermesHome, runtimeURLs...)
}

func (r Runner) bindingReadinessAtHomeContext(ctx context.Context, adapter runtime.Adapter, binding config.Binding, homeDir string, hermesHome string, runtimeURLs ...string) runtime.Detection {
	detection := runtime.DetectContext(ctx, adapter)
	if detection.State != runtime.AdapterStateReady {
		return detection
	}
	verificationBinding := binding
	verificationBinding.HermesHome = strings.TrimSpace(hermesHome)
	runtimeURL := ""
	if len(runtimeURLs) > 0 {
		runtimeURL = runtimeURLs[0]
	}
	verify := mcp.VerifyBindingWithLiveAt(ctx, homeDir, verificationBinding, nil, runtimeURL)
	detection.State = verify.State
	detection.Note = verify.Note
	detection.DiagnosticCode = verify.DiagnosticCode
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
	adapter := runtime.NewOpenClawAdapterWithAuth(getenv("PERSONASTACK_CONNECTOR_OPENCLAW_GATEWAY_URL"), resolved.Auth, "")
	adapter.DeviceTokenSink = func(deviceToken string) error {
		writable, ok := r.Store.(config.WritableStore)
		if !ok {
			return nil
		}
		latest, ok := r.Store.Binding(binding.ConnectionID)
		if !ok {
			return nil
		}
		if latest.ConnectionGeneration != binding.ConnectionGeneration {
			return nil
		}
		latest.OpenClawDeviceToken = strings.TrimSpace(deviceToken)
		latest.HasOpenClawDevice = latest.OpenClawDeviceToken != ""
		return writable.SaveBinding(latest)
	}
	return adapter
}

// adapterForRuntimeTarget constructs an adapter for the API-selected
// account/profile. It deliberately keeps home and profile paths out of Binding
// storage. Runtime readiness and startup belong to the session reconciler.
func (r Runner) adapterForRuntimeTarget(binding config.Binding, target *externalagentprotocol.RuntimeTarget) (runtime.Adapter, error) {
	adapter, _, err := r.targetAdapter(binding, target)
	return adapter, err
}

func (r Runner) targetAdapter(binding config.Binding, target *externalagentprotocol.RuntimeTarget) (runtime.Adapter, targetinventory.ResolvedTarget, error) {
	resolvedTarget, err := targetinventory.Resolve(binding.RuntimeKind, target, binding.BridgePrivateKey)
	if err != nil {
		return nil, targetinventory.ResolvedTarget{}, err
	}
	switch binding.RuntimeKind {
	case runtime.AdapterKindHermes:
		if strings.TrimSpace(resolvedTarget.HermesHome) == "" {
			return nil, targetinventory.ResolvedTarget{}, fmt.Errorf("selected Hermes profile is unavailable")
		}
		runtimeURL, err := r.targetRuntimeURL(binding, target)
		if err != nil {
			return nil, targetinventory.ResolvedTarget{}, err
		}
		adapter := runtime.NewHermesAdapterForHome(runtimeURL, resolvedTarget.HermesHome)
		return adapter, resolvedTarget, nil
	case runtime.AdapterKindOpenClaw:
		resolved, err := openclawauth.Resolve(openclawauth.Options{
			HomeDir: resolvedTarget.HomeDir,
			Env: func(name string) string {
				switch name {
				case "OPENCLAW_GATEWAY_TOKEN", "OPENCLAW_GATEWAY_PASSWORD", "OPENCLAW_GATEWAY_DEVICE_TOKEN", "OPENCLAW_CONFIG_PATH":
					return ""
				default:
					return getenv(name)
				}
			},
		})
		if err != nil {
			return nil, targetinventory.ResolvedTarget{}, fmt.Errorf("resolve selected OpenClaw credentials: %w", err)
		}
		if !resolved.Found() {
			return nil, targetinventory.ResolvedTarget{}, fmt.Errorf("selected OpenClaw profile has no usable local credential")
		}
		runtimeURL, err := r.targetRuntimeURL(binding, target)
		if err != nil {
			return nil, targetinventory.ResolvedTarget{}, err
		}
		adapter := runtime.NewOpenClawAdapterWithAuth(runtimeURL, resolved.Auth, resolvedTarget.OpenClawAgentID)
		return adapter, resolvedTarget, nil
	default:
		return nil, targetinventory.ResolvedTarget{}, fmt.Errorf("unsupported runtime target")
	}
}

func (r Runner) targetRuntimeURL(binding config.Binding, target *externalagentprotocol.RuntimeTarget) (string, error) {
	if r.ServiceScope != externalagentprotocol.ServiceScopeSystemLaunchDaemon && r.ServiceScope != externalagentprotocol.ServiceScopeLinuxSystemService {
		switch binding.RuntimeKind {
		case runtime.AdapterKindHermes:
			return getenv("PERSONASTACK_CONNECTOR_HERMES_URL"), nil
		case runtime.AdapterKindOpenClaw:
			return getenv("PERSONASTACK_CONNECTOR_OPENCLAW_GATEWAY_URL"), nil
		default:
			return "", fmt.Errorf("unsupported runtime target")
		}
	}
	return targetruntime.LoopbackURL(target, binding.BridgePrivateKey)
}

func (r Runner) writeCapabilitiesFrame(
	ctx context.Context,
	session bridge.Session,
	adapter runtime.Adapter,
	binding config.Binding,
	detection runtime.Detection,
	writeFrame func(externalagentprotocol.Frame) error,
) error {
	frame, _ := r.capabilitiesFrame(ctx, session, adapter, binding, detection, nil)
	if err := writeFrame(frame); err != nil {
		return fmt.Errorf("write capabilities frame: %w", err)
	}
	return nil
}

type nativeCapabilityChangeReporter struct {
	mu              sync.Mutex
	lastFingerprint string
	lastWriteAt     time.Time
	seen            bool
}

const nativeCapabilityReportRetryInterval = time.Minute

func newNativeCapabilityChangeReporter() *nativeCapabilityChangeReporter {
	return &nativeCapabilityChangeReporter{}
}

func (reporter *nativeCapabilityChangeReporter) writeIfChanged(
	ctx context.Context,
	r Runner,
	session bridge.Session,
	adapter runtime.Adapter,
	binding config.Binding,
	detection runtime.Detection,
	lastWakeProbeAt func() *time.Time,
	writeFrame func(externalagentprotocol.Frame) error,
) error {
	reporter.mu.Lock()
	defer reporter.mu.Unlock()
	frame, fingerprint := r.capabilitiesFrame(ctx, session, adapter, binding, detection, lastWakeProbeAt)
	now := r.now()
	if now.IsZero() {
		now = time.Now()
	}
	if reporter.seen && reporter.lastFingerprint == fingerprint && now.Sub(reporter.lastWriteAt) < nativeCapabilityReportRetryInterval {
		return nil
	}
	if err := writeFrame(frame); err != nil {
		return fmt.Errorf("write capabilities frame: %w", err)
	}
	reporter.seen = true
	reporter.lastFingerprint = fingerprint
	reporter.lastWriteAt = now.UTC()
	return nil
}

func (r Runner) capabilitiesFrame(
	ctx context.Context,
	session bridge.Session,
	adapter runtime.Adapter,
	binding config.Binding,
	detection runtime.Detection,
	lastWakeProbeAt func() *time.Time,
) (externalagentprotocol.Frame, string) {
	var nativeCapabilities []externalagentprotocol.NativeCapabilityReport
	var nativeReportedSources []externalagentprotocol.NativeCapabilitySource
	nativeDiscoveryStatus := externalagentprotocol.NativeCapabilityDiscoveryUnsupported
	describer, ok := adapter.(runtime.NativeCapabilityDescriber)
	if ok {
		nativeDiscoveryStatus = externalagentprotocol.NativeCapabilityDiscoveryFailed
		describeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		capabilities, err := describer.DescribeNativeCapabilities(describeCtx, binding.NativeMCPServer)
		cancel()
		nativeCapabilities = nativeCapabilityReports(capabilities, r.now())
		nativeReportedSources = nativeCapabilityReportedSources(capabilities)
		if err == nil {
			nativeDiscoveryStatus = externalagentprotocol.NativeCapabilityDiscoveryComplete
		} else if len(nativeReportedSources) > 0 {
			nativeDiscoveryStatus = externalagentprotocol.NativeCapabilityDiscoveryPartial
		}
	}
	capabilities := detectionCapabilities(detection, currentWakeProbeAt(lastWakeProbeAt), r.now())
	frame := session.CapabilitiesFrame(capabilities, nativeCapabilities, nativeDiscoveryStatus, nativeReportedSources)
	return frame, capabilityFrameFingerprint(capabilities, nativeCapabilities, nativeDiscoveryStatus, nativeReportedSources)
}

func detectionCapabilities(detection runtime.Detection, lastWakeProbeAt *time.Time, reportedAt time.Time) []externalagentprotocol.CapabilityReport {
	status := externalagentprotocol.ReadinessStatusRuntimeHealthy
	if detection.State == runtime.AdapterStateReady {
		status = externalagentprotocol.ReadinessStatusWakeable
	}
	if detection.State == runtime.AdapterStateMCPVerified || detection.State == runtime.AdapterStateMCPRestartRequired {
		status = externalagentprotocol.ReadinessStatusMCPConfigured
	}
	if detection.State == runtime.AdapterStateMCPVerified && lastWakeProbeAt != nil && !lastWakeProbeAt.IsZero() {
		status = externalagentprotocol.ReadinessStatusWakeable
	}
	if detection.State == runtime.AdapterStateRuntimeMissing || detection.State == runtime.AdapterStateRuntimeStopped || detection.State == runtime.AdapterStateAuthMissing || detection.State == runtime.AdapterStateCapabilityMissing || detection.State == runtime.AdapterStateWakeProbeFailed {
		status = externalagentprotocol.ReadinessStatusRuntimeError
	}
	return []externalagentprotocol.CapabilityReport{
		{
			Kind:       externalagentprotocol.CapabilityKindRuntimeHealth,
			Status:     status,
			Label:      safeDiagnosticNote(detection.Note),
			ReportedAt: reportedAt.UTC(),
		},
	}
}

func currentWakeProbeAt(lastWakeProbeAt func() *time.Time) *time.Time {
	if lastWakeProbeAt == nil {
		return nil
	}
	return lastWakeProbeAt()
}

func nativeCapabilityReports(capabilities []runtime.NativeCapability, reportedAt time.Time) []externalagentprotocol.NativeCapabilityReport {
	reports := make([]externalagentprotocol.NativeCapabilityReport, 0, len(capabilities))
	for _, capability := range capabilities {
		status := externalagentprotocol.ReadinessStatusWakeable
		if capability.Degraded {
			status = externalagentprotocol.ReadinessStatusRuntimeHealthy
		}
		report := externalagentprotocol.NativeCapabilityReport{
			Source:       externalagentprotocol.NativeCapabilitySource(strings.TrimSpace(string(capability.Source))),
			Kind:         externalagentprotocol.NativeCapabilityKind(strings.TrimSpace(string(capability.Kind))),
			CapabilityID: strings.TrimSpace(capability.CapabilityID),
			Label:        strings.TrimSpace(capability.Label),
			Summary:      strings.TrimSpace(capability.Summary),
			Status:       status,
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
	sort.Slice(reports, func(i, j int) bool {
		left := nativeCapabilityFingerprintKey(reports[i])
		right := nativeCapabilityFingerprintKey(reports[j])
		return left < right
	})
	return reports
}

func nativeCapabilityFingerprint(reports []externalagentprotocol.NativeCapabilityReport) string {
	keys := make([]string, 0, len(reports))
	for _, report := range reports {
		keys = append(keys, nativeCapabilityFingerprintKey(report))
	}
	sort.Strings(keys)
	return strings.Join(keys, "\n")
}

func nativeCapabilityReportedSources(capabilities []runtime.NativeCapability) []externalagentprotocol.NativeCapabilitySource {
	sources := make([]externalagentprotocol.NativeCapabilitySource, 0, len(capabilities))
	seen := map[externalagentprotocol.NativeCapabilitySource]struct{}{}
	for _, capability := range capabilities {
		source := externalagentprotocol.NativeCapabilitySource(strings.TrimSpace(string(capability.Source)))
		if source == "" {
			continue
		}
		if _, ok := seen[source]; ok {
			continue
		}
		seen[source] = struct{}{}
		sources = append(sources, source)
	}
	sort.Slice(sources, func(i, j int) bool {
		return string(sources[i]) < string(sources[j])
	})
	return sources
}

func capabilityFrameFingerprint(
	capabilities []externalagentprotocol.CapabilityReport,
	nativeCapabilities []externalagentprotocol.NativeCapabilityReport,
	nativeDiscoveryStatus externalagentprotocol.NativeCapabilityDiscoveryStatus,
	nativeReportedSources []externalagentprotocol.NativeCapabilitySource,
) string {
	keys := make([]string, 0, len(capabilities)+len(nativeCapabilities)+len(nativeReportedSources)+1)
	keys = append(keys, "native_discovery\x1f"+strings.TrimSpace(string(nativeDiscoveryStatus)))
	for _, capability := range capabilities {
		keys = append(keys, capabilityFingerprintKey(capability))
	}
	for _, capability := range nativeCapabilities {
		keys = append(keys, nativeCapabilityFingerprintKey(capability))
	}
	for _, source := range nativeReportedSources {
		keys = append(keys, "native_reported_source\x1f"+strings.TrimSpace(string(source)))
	}
	sort.Strings(keys)
	return strings.Join(keys, "\n")
}

func capabilityFingerprintKey(report externalagentprotocol.CapabilityReport) string {
	parts := []string{
		"capability",
		strings.TrimSpace(string(report.Kind)),
		strings.TrimSpace(string(report.Status)),
		strings.TrimSpace(report.Label),
		strings.TrimSpace(string(report.Reason)),
	}
	return strings.Join(parts, "\x1f")
}

func nativeCapabilityFingerprintKey(report externalagentprotocol.NativeCapabilityReport) string {
	parts := []string{
		"native_capability",
		strings.TrimSpace(string(report.Source)),
		strings.TrimSpace(string(report.Kind)),
		strings.TrimSpace(report.CapabilityID),
		strings.TrimSpace(report.Label),
		strings.TrimSpace(report.Summary),
		strings.TrimSpace(string(report.Status)),
	}
	return strings.Join(parts, "\x1f")
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
	return state == runtime.AdapterStateMCPVerified && lastWakeProbeAt != nil && !lastWakeProbeAt.IsZero()
}

func wakeProbeSucceeded(state runtime.AdapterState) bool {
	return state == runtime.AdapterStateMCPVerified
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
