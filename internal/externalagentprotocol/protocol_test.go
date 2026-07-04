package externalagentprotocol

import (
	"encoding/json"
	"testing"
	"time"
)

func TestProtocolVersionSupportedRecognizesCurrentVersion(t *testing.T) {
	if !ProtocolVersionSupported(ProtocolVersionV3) {
		t.Fatal("expected current protocol version to be supported")
	}
	if ProtocolVersionSupported(ProtocolVersionV2) {
		t.Fatal("did not expect legacy protocol version to be supported")
	}
}

func TestHeartbeatPayloadCarriesUpdateMetadata(t *testing.T) {
	frame := Frame{
		MessageID:    "msg-1",
		MessageType:  FrameTypeHeartbeat,
		PersonaID:    "persona-1",
		ConnectionID: "conn-1",
		SentAt:       time.Unix(1, 0).UTC(),
		Heartbeat: &HeartbeatPayload{
			ConnectionStatus:     ConnectionStatusBridgeConnected,
			ReadinessStatus:      ReadinessStatusWakeable,
			RuntimeKind:          RuntimeKindHermes,
			ConnectionGeneration: 7,
			ConnectorVersion:     "v1.2.3",
			InstallChannel:       InstallChannelHomebrew,
			ExecutablePathClass:  ExecutablePathClassHomebrewOpt,
			UpdateCapability:     UpdateCapabilityOneClickAvailable,
			UpdateState:          UpdateStateAvailable,
			UpdateReason:         UpdateReasonReleaseMetadataUnavailable,
			LastUpdateRequestID:  "update-1",
			LastUpdateSummary:    "release metadata unavailable",
		},
	}

	encoded, err := json.Marshal(frame)
	if err != nil {
		t.Fatalf("marshal frame: %v", err)
	}
	var decoded Frame
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal frame: %v", err)
	}
	if decoded.Heartbeat == nil {
		t.Fatal("heartbeat payload missing")
	}
	if decoded.Heartbeat.InstallChannel != InstallChannelHomebrew ||
		decoded.Heartbeat.ExecutablePathClass != ExecutablePathClassHomebrewOpt ||
		decoded.Heartbeat.UpdateCapability != UpdateCapabilityOneClickAvailable ||
		decoded.Heartbeat.UpdateState != UpdateStateAvailable ||
		decoded.Heartbeat.UpdateReason != UpdateReasonReleaseMetadataUnavailable ||
		decoded.Heartbeat.LastUpdateRequestID != "update-1" {
		t.Fatalf("unexpected update metadata: %+v", decoded.Heartbeat)
	}
}

func TestUpdateFramePayloadsRoundTrip(t *testing.T) {
	frame := Frame{
		MessageID:            "msg-1",
		MessageType:          FrameTypeUpdateRequest,
		PersonaID:            "persona-1",
		ConnectionID:         "conn-1",
		ConnectionGeneration: 7,
		SentAt:               time.Unix(1, 0).UTC(),
		UpdateRequest: &UpdateRequestPayload{
			RequestID:            "update-1",
			TargetVersion:        "v1.2.4",
			InstallChannel:       InstallChannelHomebrew,
			PackageKind:          "homebrew",
			AssetURL:             "https://downloads.personastack.ai/connector/v1.2.4/personastack-connector.tar.gz",
			ChecksumURL:          "https://downloads.personastack.ai/connector/v1.2.4/checksums.txt",
			SignatureURL:         "https://downloads.personastack.ai/connector/v1.2.4/checksums.txt.sig",
			InstallCommandSource: "api_release_metadata",
			RequestedAt:          time.Unix(2, 0).UTC(),
		},
	}

	encoded, err := json.Marshal(frame)
	if err != nil {
		t.Fatalf("marshal frame: %v", err)
	}
	var decoded Frame
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal frame: %v", err)
	}
	if decoded.MessageType != FrameTypeUpdateRequest || decoded.UpdateRequest == nil {
		t.Fatalf("unexpected update frame: %+v", decoded)
	}
	if decoded.UpdateRequest.RequestID != "update-1" ||
		decoded.UpdateRequest.TargetVersion != "v1.2.4" ||
		decoded.UpdateRequest.InstallChannel != InstallChannelHomebrew ||
		decoded.UpdateRequest.PackageKind != "homebrew" ||
		decoded.UpdateRequest.InstallCommandSource != "api_release_metadata" {
		t.Fatalf("unexpected update request payload: %+v", decoded.UpdateRequest)
	}
}
