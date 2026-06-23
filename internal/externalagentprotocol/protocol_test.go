package externalagentprotocol

import "testing"

func TestProtocolVersionSupportedRecognizesCurrentVersion(t *testing.T) {
	if !ProtocolVersionSupported(ProtocolVersionV3) {
		t.Fatal("expected current protocol version to be supported")
	}
	if ProtocolVersionSupported(ProtocolVersionV2) {
		t.Fatal("did not expect legacy protocol version to be supported")
	}
}
