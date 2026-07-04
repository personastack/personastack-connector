package menubar

import (
	"strings"
	"testing"
	"time"

	"github.com/personastack/personastack-connector/internal/externalagentprotocol"
)

func TestEnabledOnlyForDarwinUserLaunchAgent(t *testing.T) {
	if !Enabled(Options{GOOS: "darwin", ServiceScope: externalagentprotocol.ServiceScopeUserLaunchAgent}) {
		t.Fatal("expected darwin user launch agent to enable menu bar")
	}
	if Enabled(Options{GOOS: "darwin", ServiceScope: externalagentprotocol.ServiceScopeSystemLaunchDaemon}) {
		t.Fatal("did not expect system launch daemon to enable menu bar")
	}
	if Enabled(Options{GOOS: "linux", ServiceScope: externalagentprotocol.ServiceScopeLinuxSystemService}) {
		t.Fatal("did not expect linux to enable menu bar")
	}
}

func TestStatusTextIncludesRedactedBoundedFields(t *testing.T) {
	text := StatusText(State{
		ActiveRunID:      "run-1",
		ConnectionStatus: externalagentprotocol.ConnectionStatusBridgeConnected,
		CurrentVersion:   "v1.2.3",
		LatestVersion:    "v1.2.4",
		LastHeartbeatAt:  time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC),
		PersonaID:        "persona-1",
		RuntimeKind:      externalagentprotocol.RuntimeKindHermes,
		UpdateCapability: externalagentprotocol.UpdateCapabilityOneClickAvailable,
		UpdateState:      externalagentprotocol.UpdateStateAvailable,
		WakeReadiness:    externalagentprotocol.ReadinessStatusWakeable,
	})
	for _, want := range []string{
		"Connection: bridge_connected",
		"Runtime: hermes",
		"Persona: persona-1",
		"Wake: wakeable",
		"Active run: run-1",
		"Current version: v1.2.3",
		"Latest version: v1.2.4",
		"Update state: available",
		"Last heartbeat: 2026-07-03T12:00:00Z",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("StatusText missing %q in:\n%s", want, text)
		}
	}
}
