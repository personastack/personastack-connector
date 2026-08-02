package daemon

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/personastack/personastack-connector/internal/config"
	"github.com/personastack/personastack-connector/internal/externalagentprotocol"
	"github.com/personastack/personastack-connector/internal/hermessetup"
	"github.com/personastack/personastack-connector/internal/openclawauth"
	"github.com/personastack/personastack-connector/internal/openclawsetup"
	"github.com/personastack/personastack-connector/internal/runtime"
	"github.com/personastack/personastack-connector/internal/targetinventory"
	"github.com/personastack/personastack-connector/internal/targetruntime"
)

type runtimeSnapshot struct {
	Generation     int64
	TargetRevision int64
	TargetEpoch    uint64
	Epoch          uint64
	Target         *externalagentprotocol.RuntimeTarget
	Adapter        runtime.Adapter
	Resolved       targetinventory.ResolvedTarget
	RuntimeURL     string
	Detection      runtime.Detection
	MCPApplied     bool
	WakeProbeAt    *time.Time
}

func (snapshot runtimeSnapshot) clone() runtimeSnapshot {
	copy := snapshot
	copy.Target = cloneRuntimeTarget(snapshot.Target)
	if snapshot.WakeProbeAt != nil {
		value := snapshot.WakeProbeAt.UTC()
		copy.WakeProbeAt = &value
	}
	return copy
}

type sessionReconciler struct {
	runner  Runner
	binding config.Binding
	base    runtime.Adapter

	mu            sync.RWMutex
	snapshot      runtimeSnapshot
	wake          chan struct{}
	stop          chan struct{}
	done          chan struct{}
	cancel        context.CancelFunc
	attemptCancel context.CancelFunc
}

func newSessionReconciler(ctx context.Context, runner Runner, binding config.Binding, base runtime.Adapter, initial runtime.Detection) *sessionReconciler {
	return &sessionReconciler{
		runner:  runner,
		binding: binding,
		base:    base,
		snapshot: runtimeSnapshot{
			Generation: binding.ConnectionGeneration,
			Detection:  initial,
		},
		wake:   make(chan struct{}, 1),
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
		cancel: func() {},
	}
}

func (reconciler *sessionReconciler) start(parent context.Context) {
	ctx, cancel := context.WithCancel(parent)
	reconciler.mu.Lock()
	reconciler.cancel = cancel
	reconciler.mu.Unlock()
	go reconciler.loop(ctx)
}

func (reconciler *sessionReconciler) close() {
	reconciler.mu.RLock()
	cancel := reconciler.cancel
	reconciler.mu.RUnlock()
	cancel()
	select {
	case <-reconciler.done:
	case <-time.After(2 * time.Second):
	}
}

func (reconciler *sessionReconciler) wakeNow() {
	select {
	case reconciler.wake <- struct{}{}:
	default:
	}
}

func (reconciler *sessionReconciler) waitForWakeable(ctx context.Context, epoch uint64) (runtimeSnapshot, bool) {
	reconciler.wakeNow()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		snapshot := reconciler.snapshotCopy()
		if snapshot.Epoch != epoch || snapshot.Target == nil {
			return snapshot, false
		}
		if wakeProbeSucceeded(snapshot.Detection.State) {
			return snapshot, true
		}
		select {
		case <-ctx.Done():
			return snapshot, false
		case <-ticker.C:
		}
	}
}

func (reconciler *sessionReconciler) snapshotCopy() runtimeSnapshot {
	reconciler.mu.RLock()
	defer reconciler.mu.RUnlock()
	return reconciler.snapshot.clone()
}

func (reconciler *sessionReconciler) setTarget(target *externalagentprotocol.RuntimeTarget, targetEpoch ...uint64) bool {
	if target == nil {
		return false
	}
	reconciler.mu.Lock()
	defer reconciler.mu.Unlock()
	wireEpoch := uint64(0)
	if len(targetEpoch) > 0 {
		wireEpoch = targetEpoch[0]
	}
	if wireEpoch > 0 && reconciler.snapshot.TargetEpoch > 0 && wireEpoch < reconciler.snapshot.TargetEpoch {
		return false
	}
	if target.SelectionRevision < reconciler.snapshot.TargetRevision || (target.SelectionRevision == reconciler.snapshot.TargetRevision && reconciler.snapshot.Target != nil) {
		if wireEpoch <= reconciler.snapshot.TargetEpoch || !runtimeTargetsEqual(target, reconciler.snapshot.Target) {
			return false
		}
	}
	if target.SelectionRevision == reconciler.snapshot.TargetRevision && runtimeTargetsEqual(target, reconciler.snapshot.Target) && wireEpoch <= reconciler.snapshot.TargetEpoch {
		return false
	}
	if reconciler.attemptCancel != nil {
		reconciler.attemptCancel()
		reconciler.attemptCancel = nil
	}
	if target.SelectionRevision == reconciler.snapshot.TargetRevision && runtimeTargetsEqual(target, reconciler.snapshot.Target) {
		return false
	}
	reconciler.snapshot.TargetRevision = target.SelectionRevision
	reconciler.snapshot.TargetEpoch = wireEpoch
	reconciler.snapshot.Epoch++
	reconciler.snapshot.Target = cloneRuntimeTarget(target)
	reconciler.snapshot.Adapter = nil
	reconciler.snapshot.Resolved = targetinventory.ResolvedTarget{}
	reconciler.snapshot.RuntimeURL = ""
	reconciler.snapshot.Detection = runtime.Detection{Kind: reconciler.binding.RuntimeKind, State: runtime.AdapterStateRuntimeMissing, DiagnosticCode: string(externalagentprotocol.DiagnosticCodeRuntimeMissing), Note: "selected runtime target is being reconciled"}
	reconciler.snapshot.MCPApplied = false
	reconciler.snapshot.WakeProbeAt = nil
	reconciler.wakeNow()
	return true
}

func (reconciler *sessionReconciler) clearTarget(revision int64, targetEpoch ...uint64) bool {
	wireEpoch := uint64(0)
	if len(targetEpoch) > 0 {
		wireEpoch = targetEpoch[0]
	}
	reconciler.mu.Lock()
	if wireEpoch > 0 && reconciler.snapshot.TargetEpoch > 0 && wireEpoch < reconciler.snapshot.TargetEpoch {
		reconciler.mu.Unlock()
		return false
	}
	if revision < reconciler.snapshot.TargetRevision || (reconciler.snapshot.Target == nil && revision == reconciler.snapshot.TargetRevision) {
		reconciler.mu.Unlock()
		return false
	}
	if reconciler.attemptCancel != nil {
		reconciler.attemptCancel()
		reconciler.attemptCancel = nil
	}
	defer reconciler.mu.Unlock()
	reconciler.snapshot.TargetRevision = revision
	reconciler.snapshot.TargetEpoch = wireEpoch
	reconciler.snapshot.Epoch++
	reconciler.snapshot.Target = nil
	reconciler.snapshot.Adapter = nil
	reconciler.snapshot.Resolved = targetinventory.ResolvedTarget{}
	reconciler.snapshot.RuntimeURL = ""
	reconciler.snapshot.Detection = selectionRequiredDetection(reconciler.snapshot.Detection)
	reconciler.snapshot.MCPApplied = false
	reconciler.snapshot.WakeProbeAt = nil
	reconciler.wakeNow()
	return true
}

func (reconciler *sessionReconciler) recordWakeProbe(at time.Time, epoch uint64) bool {
	reconciler.mu.Lock()
	defer reconciler.mu.Unlock()
	if reconciler.snapshot.Epoch != epoch || reconciler.snapshot.Target == nil || !wakeProbeSucceeded(reconciler.snapshot.Detection.State) {
		return false
	}
	value := at.UTC()
	reconciler.snapshot.WakeProbeAt = &value
	return true
}

func (reconciler *sessionReconciler) loop(ctx context.Context) {
	defer close(reconciler.done)
	backoff := reconciler.runner.reconcileMin()
	for {
		select {
		case <-ctx.Done():
			return
		case <-reconciler.stop:
			return
		case <-reconciler.wake:
		}
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			snapshot := reconciler.snapshotCopy()
			if snapshot.Target == nil {
				backoff = reconciler.runner.reconcileMin()
				break
			}
			attemptCtx, cancelAttempt := context.WithTimeout(ctx, reconciler.runner.reconcileAttemptTimeout())
			reconciler.mu.Lock()
			reconciler.attemptCancel = cancelAttempt
			reconciler.mu.Unlock()
			result, err := reconciler.runner.reconcileTarget(attemptCtx, reconciler.binding, snapshot)
			cancelAttempt()
			reconciler.mu.Lock()
			reconciler.attemptCancel = nil
			reconciler.mu.Unlock()
			if err != nil {
				result.Detection = detectionForReconcileError(reconciler.binding.RuntimeKind, err)
			}
			reconciler.publish(snapshot, result)
			if err == nil && result.Detection.State == runtime.AdapterStateMCPVerified {
				backoff = reconciler.runner.reconcileMin()
			} else {
				backoff = minDuration(maxDuration(backoff*2, reconciler.runner.reconcileMin()), reconciler.runner.reconcileMax())
			}
			timer := time.NewTimer(jitterDuration(backoff))
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-reconciler.wake:
				timer.Stop()
				continue
			case <-timer.C:
			}
		}
	}
}

type reconcileResult struct {
	Adapter    runtime.Adapter
	Resolved   targetinventory.ResolvedTarget
	RuntimeURL string
	Detection  runtime.Detection
	MCPApplied bool
}

func (reconciler *sessionReconciler) publish(start runtimeSnapshot, result reconcileResult) {
	reconciler.mu.Lock()
	defer reconciler.mu.Unlock()
	if reconciler.snapshot.Generation != start.Generation || reconciler.snapshot.Epoch != start.Epoch || reconciler.snapshot.TargetRevision != start.TargetRevision || !runtimeTargetsEqual(reconciler.snapshot.Target, start.Target) {
		return
	}
	reconciler.snapshot.Adapter = result.Adapter
	reconciler.snapshot.Resolved = result.Resolved
	reconciler.snapshot.RuntimeURL = result.RuntimeURL
	reconciler.snapshot.Detection = result.Detection
	reconciler.snapshot.MCPApplied = result.MCPApplied
	if result.Detection.State != runtime.AdapterStateMCPVerified && result.Detection.State != runtime.AdapterStateReady {
		reconciler.snapshot.WakeProbeAt = nil
	}
}

func (r Runner) reconcileTarget(ctx context.Context, binding config.Binding, snapshot runtimeSnapshot) (reconcileResult, error) {
	adapter, resolved, err := r.targetAdapter(binding, snapshot.Target)
	if err != nil {
		return reconcileResult{}, err
	}
	runtimeURL, err := r.targetRuntimeURL(binding, snapshot.Target)
	if err != nil {
		return reconcileResult{Adapter: adapter, Resolved: resolved}, err
	}
	detection := safeDetection(runtime.DetectContext(ctx, adapter))
	if detection.State == runtime.AdapterStateRuntimeMissing || detection.State == runtime.AdapterStateRuntimeStopped {
		if startErr := r.startTargetRuntime(ctx, binding, snapshot.Target, resolved, runtimeURL); startErr != nil {
			return reconcileResult{Adapter: adapter, Resolved: resolved, RuntimeURL: runtimeURL, Detection: detection}, startErr
		}
		detection = safeDetection(runtime.DetectContext(ctx, adapter))
	}
	result := reconcileResult{Adapter: adapter, Resolved: resolved, RuntimeURL: runtimeURL, Detection: detection, MCPApplied: snapshot.MCPApplied}
	if detection.State != runtime.AdapterStateReady {
		return result, nil
	}
	if !snapshot.MCPApplied {
		if err := r.refreshMCPConfig(binding, snapshot.Target); err != nil {
			return result, err
		}
		result.MCPApplied = true
	}
	result.Detection = safeDetection(r.bindingReadinessAtHomeContext(ctx, adapter, binding, resolved.HomeDir, resolved.HermesHome, runtimeURL))
	return result, nil
}

func (r Runner) startTargetRuntime(ctx context.Context, binding config.Binding, target *externalagentprotocol.RuntimeTarget, resolved targetinventory.ResolvedTarget, runtimeURL string) error {
	identity := hermessetup.ProcessIdentity{Username: resolved.Username, HomeDir: resolved.HomeDir, UID: resolved.UID, GID: resolved.GID, GroupIDs: resolved.GroupIDs}
	switch binding.RuntimeKind {
	case runtime.AdapterKindHermes:
		paths := hermessetup.ResolvePaths(resolved.HomeDir, resolved.HermesHome)
		_, err := hermessetup.TryStartGatewayForPathsAtContext(ctx, paths, identity, runtimeURL)
		return err
	case runtime.AdapterKindOpenClaw:
		port, err := targetruntime.Port(target, binding.BridgePrivateKey)
		if err != nil {
			return err
		}
		_, err = openclawsetup.TryStartGatewayForHomeAtContext(ctx, resolved.HomeDir, identity, port, func() bool { return openclawauth.GatewayReachable(runtimeURL) })
		return err
	default:
		return fmt.Errorf("unsupported runtime target")
	}
}

func (r Runner) reconcileMin() time.Duration {
	if r.ReconcileMin > 0 {
		return r.ReconcileMin
	}
	return 2 * time.Second
}

func (r Runner) reconcileMax() time.Duration {
	if r.ReconcileMax > 0 {
		return r.ReconcileMax
	}
	return 60 * time.Second
}

func (r Runner) reconcileAttemptTimeout() time.Duration {
	if r.ReconcileAttemptTimeout > 0 {
		return r.ReconcileAttemptTimeout
	}
	return 35 * time.Second
}

func runtimeTargetsEqual(left, right *externalagentprotocol.RuntimeTarget) bool {
	if left == nil || right == nil {
		return left == right
	}
	return strings.TrimSpace(left.AccountCandidateID) == strings.TrimSpace(right.AccountCandidateID) && strings.TrimSpace(left.ProfileCandidateID) == strings.TrimSpace(right.ProfileCandidateID) && left.RuntimeKind == right.RuntimeKind && left.SelectionRevision == right.SelectionRevision
}

func detectionForReconcileError(kind runtime.AdapterKind, err error) runtime.Detection {
	code := externalagentprotocol.DiagnosticCodeRuntimeError
	state := runtime.AdapterStateRuntimeStopped
	text := strings.ToLower(strings.TrimSpace(err.Error()))
	switch {
	case strings.Contains(text, "credential"), strings.Contains(text, "auth"):
		state = runtime.AdapterStateAuthMissing
		code = externalagentprotocol.DiagnosticCodeAuthMissing
	case strings.Contains(text, "mcp"), strings.Contains(text, "config"):
		state = runtime.AdapterStateMCPConfigMissing
		code = externalagentprotocol.DiagnosticCodeMCPConfigMissing
	case strings.Contains(text, "target"), strings.Contains(text, "profile"), strings.Contains(text, "account"):
		state = runtime.AdapterStateRuntimeMissing
		code = externalagentprotocol.DiagnosticCodeRuntimeMissing
	}
	return runtime.Detection{Kind: kind, State: state, DiagnosticCode: string(code), Note: safeDiagnosticNote(err.Error())}
}

func maxDuration(left, right time.Duration) time.Duration {
	if left > right {
		return left
	}
	return right
}
