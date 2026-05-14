package runtime

import "testing"

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
