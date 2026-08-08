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

func TestConnectAndHeartbeatPreserveManualOnlyUpdateMetadata(t *testing.T) {
	tests := []string{
		`{"message_type":"connect","connect":{"protocol_version":"external-agent-v4","connector_version":"v0.1.0","runtime_kind":"hermes","connection_generation":3,"device_public_key":"key","credential_id":"credential","credential_proof":"proof","install_channel":"homebrew","executable_path_class":"homebrew_opt","update_capability":"manual_required","update_state":"idle"}}`,
		`{"message_type":"heartbeat","heartbeat":{"connection_status":"bridge_connected","readiness_status":"wakeable","runtime_kind":"hermes","connection_generation":3,"connector_version":"v0.1.0","install_channel":"deb","executable_path_class":"package_managed","update_capability":"manual_required","update_state":"idle","update_reason":"unknown_install_channel"}}`,
	}
	for _, raw := range tests {
		var frame Frame
		err := json.Unmarshal([]byte(raw), &frame)
		if err != nil {
			t.Fatalf("unmarshal frame: %v", err)
		}
		roundTrip, err := json.Marshal(frame)
		if err != nil {
			t.Fatalf("marshal frame: %v", err)
		}
		for _, field := range []string{"install_channel", "executable_path_class", "update_capability", "update_state"} {
			if !strings.Contains(string(roundTrip), `"`+field+`"`) {
				t.Fatalf("round trip dropped %s: %s", field, roundTrip)
			}
		}
		if strings.Contains(raw, "update_reason") && !strings.Contains(string(roundTrip), `"update_reason"`) {
			t.Fatalf("round trip dropped update_reason: %s", roundTrip)
		}
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
