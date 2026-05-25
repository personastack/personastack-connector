package runtime

import (
	"fmt"
	"strings"
	"testing"
)

func TestParseAdapterKind(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want AdapterKind
	}{
		{name: "auto", in: "auto", want: AdapterKindAuto},
		{name: "empty", in: "", want: AdapterKindAuto},
		{name: "hermes", in: "hermes", want: AdapterKindHermes},
		{name: "openclaw", in: "openclaw", want: AdapterKindOpenClaw},
		{name: "trim case", in: " OpenClaw ", want: AdapterKindOpenClaw},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := ParseAdapterKind(test.in)
			if err != nil {
				t.Fatalf("ParseAdapterKind returned error: %v", err)
			}
			if got != test.want {
				t.Fatalf("ParseAdapterKind = %v, want %v", got, test.want)
			}
			if got.String() == "unknown" {
				t.Fatalf("String returned unknown for %v", got)
			}
		})
	}
}

func TestParseAdapterKindRejectsUnknown(t *testing.T) {
	_, err := ParseAdapterKind("fake")
	if err == nil {
		t.Fatal("ParseAdapterKind returned nil error")
	}
}

func TestAdapterStateString(t *testing.T) {
	if AdapterStateReady.String() != "ready" {
		t.Fatalf("AdapterStateReady.String = %q", AdapterStateReady.String())
	}
	if AdapterStateMCPConfigMissing.String() != "mcp_config_missing" {
		t.Fatalf("AdapterStateMCPConfigMissing.String = %q", AdapterStateMCPConfigMissing.String())
	}
}

func TestRunMetadataBoundsCallerAndCanonicalValues(t *testing.T) {
	longCallerKey := "0" + strings.Repeat("k", maxRunMetadataKeyRunes+10)
	callerMetadata := map[string]string{
		longCallerKey: strings.Repeat("v", maxRunMetadataValueRunes+10),
	}
	for i := 0; i < maxRunMetadataEntries+10; i++ {
		callerMetadata[fmt.Sprintf("caller_%02d", i)] = "value"
	}

	request := RunRequest{
		RunID:                  strings.Repeat("r", maxRunMetadataValueRunes+10),
		AssignmentID:           " assignment-1 ",
		NativeMCPServerName:    strings.Repeat("s", maxRunMetadataValueRunes+10),
		NativeMCPToolNamespace: " personastack ",
		Metadata:               callerMetadata,
	}

	metadata := runMetadata(request)
	if len(metadata) > maxRunMetadataEntries {
		t.Fatalf("metadata entry count = %d", len(metadata))
	}
	if len([]rune(metadata["personastack_run_id"])) != maxRunMetadataValueRunes {
		t.Fatalf("run id length = %d", len([]rune(metadata["personastack_run_id"])))
	}
	if metadata["personastack_assignment_id"] != "assignment-1" {
		t.Fatalf("assignment id = %q", metadata["personastack_assignment_id"])
	}
	if len([]rune(metadata["native_mcp_server"])) != maxRunMetadataValueRunes {
		t.Fatalf("native mcp server length = %d", len([]rune(metadata["native_mcp_server"])))
	}
	if metadata["native_mcp_namespace"] != "personastack" {
		t.Fatalf("native mcp namespace = %q", metadata["native_mcp_namespace"])
	}

	foundBoundedCallerKey := false
	for key, value := range metadata {
		if key == string([]rune(longCallerKey)[:maxRunMetadataKeyRunes]) {
			foundBoundedCallerKey = true
			if len([]rune(value)) != maxRunMetadataValueRunes {
				t.Fatalf("caller metadata value length = %d", len([]rune(value)))
			}
		}
	}
	if !foundBoundedCallerKey {
		t.Fatalf("bounded caller metadata key missing: %#v", metadata)
	}
}
