package externalagentprotocol

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestProtocolVersionSupportedRecognizesCurrentVersion(t *testing.T) {
	if !ProtocolVersionSupported(ProtocolVersionV4) {
		t.Fatal("expected current protocol version to be supported")
	}
	if ProtocolVersionSupported(ProtocolVersionV2) {
		t.Fatal("did not expect legacy protocol version to be supported")
	}
}

func TestTargetInventoryDiscoveryStatusRoundTrips(t *testing.T) {
	payload := TargetInventoryPayload{InventoryGeneration: 3, DiscoveryStatus: DiscoveryStatusDegraded}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal inventory: %v", err)
	}
	var decoded TargetInventoryPayload
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal inventory: %v", err)
	}
	if decoded.DiscoveryStatus != DiscoveryStatusDegraded {
		t.Fatalf("discovery status = %q", decoded.DiscoveryStatus)
	}
}

func TestConfigRefreshTargetClearIsExplicit(t *testing.T) {
	payload := ConfigRefreshPayload{ClearRuntimeTarget: true, SelectionRevision: 8}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal refresh: %v", err)
	}
	if !strings.Contains(string(raw), `"clear_runtime_target":true`) || !strings.Contains(string(raw), `"selection_revision":8`) {
		t.Fatalf("clear marker missing: %s", raw)
	}
}

func TestConfigClearRoundTrips(t *testing.T) {
	payload := ConfigClearPayload{TargetSelectionRevision: 8, TargetEpoch: 3}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal config clear: %v", err)
	}
	var decoded ConfigClearPayload
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal config clear: %v", err)
	}
	if decoded.TargetSelectionRevision != 8 || decoded.TargetEpoch != 3 {
		t.Fatalf("config clear = %+v", decoded)
	}
}
