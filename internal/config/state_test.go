package config

import "testing"

func TestExternalAgentKindString(t *testing.T) {
	if ExternalAgentKindHermes.String() != "hermes" {
		t.Fatalf("ExternalAgentKindHermes.String = %q", ExternalAgentKindHermes.String())
	}
	if ExternalAgentKindOpenClaw.String() != "openclaw" {
		t.Fatalf("ExternalAgentKindOpenClaw.String = %q", ExternalAgentKindOpenClaw.String())
	}
}
