package targetinventory

import (
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"testing"

	"github.com/personastack/personastack-connector/internal/externalagentprotocol"
	"github.com/personastack/personastack-connector/internal/runtime"
)

func TestDiscoverNonRootOnlyReportsCurrentUserHermesProfiles(t *testing.T) {
	homeDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(homeDir, ".hermes", "profiles", "work"), 0o700); err != nil {
		t.Fatalf("create Hermes profile: %v", err)
	}
	oldCurrentUser := currentUser
	oldEffectiveUID := effectiveUID
	currentUser = func() (*user.User, error) { return &user.User{Username: "alice", HomeDir: homeDir, Uid: "501"}, nil }
	effectiveUID = func() int { return 501 }
	t.Setenv("HOME", homeDir)
	t.Cleanup(func() {
		currentUser = oldCurrentUser
		effectiveUID = oldEffectiveUID
	})

	inventory, warnings := Discover(runtime.AdapterKindHermes)
	if len(warnings) != 0 {
		t.Fatalf("unexpected warnings: %v", warnings)
	}
	if len(inventory.Accounts) != 1 || inventory.Accounts[0].Label != "alice" {
		t.Fatalf("unexpected accounts: %+v", inventory.Accounts)
	}
	profiles := inventory.Accounts[0].Profiles
	if len(profiles) != 2 || profiles[0].Label != "Default" || profiles[1].Label != "work" {
		t.Fatalf("unexpected profiles: %+v", profiles)
	}
	target := &externalagentprotocol.RuntimeTarget{AccountCandidateID: inventory.Accounts[0].CandidateID, ProfileCandidateID: profiles[1].CandidateID, RuntimeKind: externalagentprotocol.RuntimeKindHermes, SelectionRevision: 1}
	resolved, err := Resolve(runtime.AdapterKindHermes, target)
	if err != nil {
		t.Fatalf("resolve selected Hermes profile: %v", err)
	}
	if resolved.HermesHome != filepath.Join(homeDir, ".hermes", "profiles", "work") {
		t.Fatalf("profile = %q", resolved.HermesHome)
	}
}

func TestDiscoverOmitsUnreadableOrMissingNonRootHomeWithoutFailure(t *testing.T) {
	oldCurrentUser := currentUser
	oldEffectiveUID := effectiveUID
	currentUser = func() (*user.User, error) {
		return &user.User{Username: "alice", HomeDir: filepath.Join(t.TempDir(), "missing"), Uid: "501"}, nil
	}
	effectiveUID = func() int { return 501 }
	t.Setenv("HOME", filepath.Join(t.TempDir(), "missing"))
	t.Cleanup(func() {
		currentUser = oldCurrentUser
		effectiveUID = oldEffectiveUID
	})
	inventory, warnings := Discover(runtime.AdapterKindHermes)
	if len(warnings) == 0 || len(inventory.Accounts) != 0 || inventory.DiscoveryStatus != externalagentprotocol.DiscoveryStatusDegraded {
		t.Fatalf("missing home must be degraded but usable: inventory=%+v warnings=%v", inventory, warnings)
	}
}

func TestResolveRejectsSymlinkedHermesProfile(t *testing.T) {
	homeDir := t.TempDir()
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(homeDir, ".hermes")); err != nil {
		t.Fatalf("symlink Hermes home: %v", err)
	}
	oldCurrentUser := currentUser
	oldEffectiveUID := effectiveUID
	currentUser = func() (*user.User, error) {
		return &user.User{Username: "alice", HomeDir: homeDir, Uid: "501", Gid: "20"}, nil
	}
	effectiveUID = func() int { return 501 }
	t.Setenv("HOME", homeDir)
	t.Cleanup(func() {
		currentUser = oldCurrentUser
		effectiveUID = oldEffectiveUID
	})
	inventory, _ := Discover(runtime.AdapterKindHermes, "secret")
	if len(inventory.Accounts) != 1 || len(inventory.Accounts[0].Profiles) != 1 {
		t.Fatalf("inventory = %+v", inventory)
	}
	_, err := Resolve(runtime.AdapterKindHermes, &externalagentprotocol.RuntimeTarget{AccountCandidateID: inventory.Accounts[0].CandidateID, ProfileCandidateID: inventory.Accounts[0].Profiles[0].CandidateID, RuntimeKind: externalagentprotocol.RuntimeKindHermes, SelectionRevision: 1}, "secret")
	if err == nil || !strings.Contains(err.Error(), "unsafe") {
		t.Fatalf("Resolve() error = %v, want unsafe path", err)
	}
}

func TestDiscoverRootIncludesRootAndRegularUsersOnly(t *testing.T) {
	rootHome := t.TempDir()
	aliceHome := t.TempDir()
	for _, homeDir := range []string{rootHome, aliceHome} {
		if err := os.MkdirAll(filepath.Join(homeDir, ".hermes"), 0o700); err != nil {
			t.Fatalf("create Hermes home: %v", err)
		}
	}
	oldReadFile := readFile
	oldEffectiveUID := effectiveUID
	readFile = func(path string) ([]byte, error) {
		if path != "/etc/passwd" {
			t.Fatalf("unexpected passwd path %q", path)
		}
		return []byte("root:x:0:0:root:" + rootHome + ":/bin/zsh\nservice:x:999:999::/nonexistent:/usr/sbin/nologin\nalice:x:1001:1001::" + aliceHome + ":/bin/zsh\n"), nil
	}
	effectiveUID = func() int { return 0 }
	t.Cleanup(func() {
		readFile = oldReadFile
		effectiveUID = oldEffectiveUID
	})

	inventory, warnings := Discover(runtime.AdapterKindHermes, "installation-secret")
	if len(warnings) == 0 || inventory.DiscoveryStatus != externalagentprotocol.DiscoveryStatusDegraded {
		t.Fatalf("expected degraded discovery warnings: inventory=%+v warnings=%v", inventory, warnings)
	}
	if len(inventory.Accounts) != 2 || inventory.Accounts[0].Label != "alice" || inventory.Accounts[1].Label != "root" {
		t.Fatalf("root discovery accounts = %+v", inventory.Accounts)
	}
}

func TestDiscoverNonRootIgnoresSudoUser(t *testing.T) {
	homeDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(homeDir, ".hermes"), 0o700); err != nil {
		t.Fatalf("create Hermes home: %v", err)
	}
	oldCurrentUser := currentUser
	oldEffectiveUID := effectiveUID
	currentUser = func() (*user.User, error) { return &user.User{Username: "alice", HomeDir: homeDir, Uid: "501"}, nil }
	effectiveUID = func() int { return 501 }
	t.Setenv("HOME", homeDir)
	t.Setenv("SUDO_USER", "root")
	t.Cleanup(func() {
		currentUser = oldCurrentUser
		effectiveUID = oldEffectiveUID
	})
	inventory, _ := Discover(runtime.AdapterKindHermes)
	if len(inventory.Accounts) != 1 || inventory.Accounts[0].Label != "alice" {
		t.Fatalf("SUDO_USER changed non-root discovery: %+v", inventory.Accounts)
	}
}

func TestDiscoverAndResolveOpenClawAgentProfiles(t *testing.T) {
	homeDir := t.TempDir()
	openClawDir := filepath.Join(homeDir, ".openclaw")
	if err := os.MkdirAll(openClawDir, 0o700); err != nil {
		t.Fatalf("create OpenClaw home: %v", err)
	}
	if err := os.WriteFile(filepath.Join(openClawDir, "openclaw.json"), []byte(`{"agents":{"list":[{"id":"main","name":"Personal"},{"id":"work"}]}}`), 0o600); err != nil {
		t.Fatalf("write OpenClaw config: %v", err)
	}
	oldCurrentUser := currentUser
	oldEffectiveUID := effectiveUID
	currentUser = func() (*user.User, error) {
		return &user.User{Username: "alice", HomeDir: homeDir, Uid: "501", Gid: "20"}, nil
	}
	effectiveUID = func() int { return 501 }
	t.Setenv("HOME", homeDir)
	t.Cleanup(func() {
		currentUser = oldCurrentUser
		effectiveUID = oldEffectiveUID
	})

	inventory, warnings := Discover(runtime.AdapterKindOpenClaw, "installation-secret")
	if len(warnings) != 0 || len(inventory.Accounts) != 1 {
		t.Fatalf("inventory = %+v warnings=%v", inventory, warnings)
	}
	profiles := inventory.Accounts[0].Profiles
	if len(profiles) != 2 || profiles[0].Label != "Personal" || profiles[1].Label != "work" {
		t.Fatalf("OpenClaw profiles = %+v", profiles)
	}
	resolved, err := Resolve(runtime.AdapterKindOpenClaw, &externalagentprotocol.RuntimeTarget{
		RuntimeKind:        externalagentprotocol.RuntimeKindOpenClaw,
		AccountCandidateID: inventory.Accounts[0].CandidateID,
		ProfileCandidateID: profiles[1].CandidateID,
		SelectionRevision:  1,
	}, "installation-secret")
	if err != nil {
		t.Fatalf("resolve OpenClaw agent: %v", err)
	}
	if resolved.HomeDir != homeDir || resolved.OpenClawAgentID != "work" {
		t.Fatalf("resolved target = %+v", resolved)
	}
}
