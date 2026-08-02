package daemon

import (
	"strings"
	"testing"
	"time"

	"github.com/personastack/personastack-connector/internal/config"
	"github.com/personastack/personastack-connector/internal/externalagentprotocol"
	"github.com/personastack/personastack-connector/internal/runtime"
)

func TestSessionReconcilerFencesTargetEpochAndRevision(t *testing.T) {
	binding := config.Binding{ConnectionID: "conn-1", ConnectionGeneration: 9, RuntimeKind: runtime.AdapterKindHermes}
	reconciler := newSessionReconciler(t.Context(), Runner{}, binding, emptyCapabilityAdapter{}, runtime.Detection{Kind: runtime.AdapterKindHermes, State: runtime.AdapterStateRuntimeMissing})
	targetA := &externalagentprotocol.RuntimeTarget{AccountCandidateID: "account-a", ProfileCandidateID: "profile-a", RuntimeKind: externalagentprotocol.RuntimeKindHermes, SelectionRevision: 4}
	if !reconciler.setTarget(targetA) {
		t.Fatal("first target was not accepted")
	}
	first := reconciler.snapshotCopy()
	if first.Generation != 9 || first.TargetRevision != 4 || first.Epoch == 0 {
		t.Fatalf("unexpected first snapshot: %+v", first)
	}
	targetStale := *targetA
	targetStale.AccountCandidateID = "account-stale"
	if reconciler.setTarget(&targetStale) {
		t.Fatal("same-revision target replaced current target")
	}
	if reconciler.snapshotCopy().Target.AccountCandidateID != "account-a" {
		t.Fatal("stale target changed the snapshot")
	}
	targetB := *targetA
	targetB.AccountCandidateID = "account-b"
	targetB.SelectionRevision = 5
	if !reconciler.setTarget(&targetB) {
		t.Fatal("newer target was not accepted")
	}
	second := reconciler.snapshotCopy()
	if second.Epoch <= first.Epoch || second.Target.AccountCandidateID != "account-b" || second.WakeProbeAt != nil {
		t.Fatalf("target change did not fence snapshot: first=%+v second=%+v", first, second)
	}
	reconciler.mu.Lock()
	reconciler.snapshot.Detection = runtime.Detection{Kind: runtime.AdapterKindHermes, State: runtime.AdapterStateMCPVerified}
	reconciler.mu.Unlock()
	if reconciler.recordWakeProbe(time.Now(), first.Epoch) {
		t.Fatal("old target epoch accepted a wake probe")
	}
	if !reconciler.recordWakeProbe(time.Now(), second.Epoch) {
		t.Fatal("current target epoch rejected a wake probe")
	}
	if !reconciler.clearTarget(6) {
		t.Fatal("target clear was not accepted")
	}
	cleared := reconciler.snapshotCopy()
	if cleared.Target != nil || cleared.WakeProbeAt != nil || cleared.Epoch <= second.Epoch {
		t.Fatalf("target clear did not invalidate session evidence: %+v", cleared)
	}
}

func TestSafeDiagnosticNoteRedactsHostDetails(t *testing.T) {
	note := safeDiagnosticNote("openclaw failed path=/Users/alice/.openclaw token=secret-value url=http://127.0.0.1:18789")
	for _, forbidden := range []string{"/Users/alice", "secret-value", "127.0.0.1:18789"} {
		if strings.Contains(note, forbidden) {
			t.Fatalf("diagnostic leaked %q: %s", forbidden, note)
		}
	}
}
