package openclawsetup

import (
	"strings"
	"testing"

	"github.com/personastack/personastack-connector/internal/hermessetup"
)

func TestProcessEnvUsesSelectedAccountHome(t *testing.T) {
	t.Setenv("HOME", "/root")
	t.Setenv("OPENCLAW_HOME", "/root/.openclaw")
	env := strings.Join(processEnv("/Users/alice", hermessetup.ProcessIdentity{Username: "alice", HomeDir: "/Users/alice"}), "\n")
	for _, want := range []string{"HOME=/Users/alice", "OPENCLAW_HOME=/Users/alice", "OPENCLAW_CONFIG_PATH=/Users/alice/.openclaw/openclaw.json", "USER=alice", "LOGNAME=alice"} {
		if !strings.Contains(env, want) {
			t.Fatalf("environment missing %q: %s", want, env)
		}
	}
	if strings.Contains(env, "HOME=/root") || strings.Contains(env, "OPENCLAW_HOME=/root") {
		t.Fatalf("root environment leaked: %s", env)
	}
}

func TestTryStartGatewaySkipsReachableGateway(t *testing.T) {
	called := false
	oldStart := startGateway
	startGateway = func(string, hermessetup.ProcessIdentity, string, int) error { called = true; return nil }
	t.Cleanup(func() { startGateway = oldStart })
	started, err := TryStartGatewayForHome(t.TempDir(), hermessetup.ProcessIdentity{}, func() bool { return true })
	if err != nil || started || called {
		t.Fatalf("TryStartGatewayForHome() started=%t called=%t err=%v", started, called, err)
	}
}

func TestTryStartGatewayForHomeAtUsesTargetPort(t *testing.T) {
	oldStart := startGateway
	oldLookPath := lookPath
	t.Cleanup(func() {
		startGateway = oldStart
		lookPath = oldLookPath
	})
	lookPath = func(string) (string, error) { return "/usr/local/bin/openclaw", nil }
	gotPort := 0
	startGateway = func(_ string, _ hermessetup.ProcessIdentity, _ string, port int) error {
		gotPort = port
		return nil
	}
	started, err := TryStartGatewayForHomeAt(t.TempDir(), hermessetup.ProcessIdentity{}, 25001, func() bool { return false })
	if err != nil || !started {
		t.Fatalf("TryStartGatewayForHomeAt() started=%t err=%v", started, err)
	}
	if gotPort != 25001 {
		t.Fatalf("gateway port = %d, want 25001", gotPort)
	}
}
