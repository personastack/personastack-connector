package service

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type recordingRunner struct {
	commands []string
	err      error
}

func (runner *recordingRunner) Run(name string, args ...string) error {
	runner.commands = append(runner.commands, name+" "+strings.Join(args, " "))
	return runner.err
}

func TestInstallerWritesSystemdUserUnit(t *testing.T) {
	homeDir := t.TempDir()
	runner := &recordingRunner{}
	result, err := (Installer{
		HomeDir:        homeDir,
		ExecutablePath: "/opt/PersonaStack Connector/personastack-connector",
		GOOS:           "linux",
		Runner:         runner,
	}).Install()
	if err != nil {
		t.Fatalf("Install() error = %v", err)
	}
	if result.Kind != "systemd-user" {
		t.Fatalf("kind = %q", result.Kind)
	}
	raw, err := os.ReadFile(filepath.Join(homeDir, ".config", "systemd", "user", "personastack-connector.service"))
	if err != nil {
		t.Fatalf("read unit: %v", err)
	}
	if !strings.Contains(string(raw), `ExecStart="/opt/PersonaStack Connector/personastack-connector" run --foreground`) {
		t.Fatalf("unexpected unit:\n%s", raw)
	}
	if len(runner.commands) != 2 || !strings.Contains(runner.commands[1], "enable --now") {
		t.Fatalf("unexpected commands: %+v", runner.commands)
	}
}

func TestInstallerPlansServiceWithoutWritingFiles(t *testing.T) {
	tests := []struct {
		goos     string
		wantKind string
		wantPath string
	}{
		{
			goos:     "darwin",
			wantKind: "launchagent",
			wantPath: filepath.Join("Library", "LaunchAgents", "ai.personastack.connector.plist"),
		},
		{
			goos:     "linux",
			wantKind: "systemd-user",
			wantPath: filepath.Join(".config", "systemd", "user", "personastack-connector.service"),
		},
		{
			goos:     "windows",
			wantKind: "windows-scheduled-task",
			wantPath: filepath.Join("AppData", "Local", "PersonaStack", "Connector", "install-task.ps1"),
		},
	}

	for _, test := range tests {
		t.Run(test.goos, func(t *testing.T) {
			homeDir := t.TempDir()
			result, err := (Installer{
				HomeDir:        homeDir,
				ExecutablePath: "/opt/personastack-connector",
				GOOS:           test.goos,
			}).Plan()
			if err != nil {
				t.Fatalf("Plan() error = %v", err)
			}
			if result.Kind != test.wantKind {
				t.Fatalf("kind = %q, want %q", result.Kind, test.wantKind)
			}
			if result.Path != filepath.Join(homeDir, test.wantPath) {
				t.Fatalf("path = %q, want suffix %q", result.Path, test.wantPath)
			}
			if _, err := os.Stat(filepath.Dir(result.Path)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("Plan() wrote service directory or stat failed: %v", err)
			}
		})
	}
}

func TestInstallerFallsBackToLinuxAutostart(t *testing.T) {
	homeDir := t.TempDir()
	runner := &recordingRunner{err: errors.New("systemd unavailable")}
	result, err := (Installer{
		HomeDir:        homeDir,
		ExecutablePath: "/opt/PersonaStack Connector/personastack-connector",
		GOOS:           "linux",
		Runner:         runner,
	}).Install()
	if err != nil {
		t.Fatalf("Install() error = %v", err)
	}
	if result.Kind != "no_user_service_manager" {
		t.Fatalf("kind = %q", result.Kind)
	}
	raw, err := os.ReadFile(filepath.Join(homeDir, ".config", "autostart", "personastack-connector.desktop"))
	if err != nil {
		t.Fatalf("read desktop entry: %v", err)
	}
	if !strings.Contains(string(raw), `Exec="/opt/PersonaStack Connector/personastack-connector" run --foreground`) {
		t.Fatalf("unexpected desktop entry:\n%s", raw)
	}
}

func TestInstallerWritesLaunchAgent(t *testing.T) {
	homeDir := t.TempDir()
	runner := &recordingRunner{}
	result, err := (Installer{
		HomeDir:        homeDir,
		ExecutablePath: "/Applications/PersonaStack & Connector.app/Contents/MacOS/personastack-connector",
		GOOS:           "darwin",
		Runner:         runner,
	}).Install()
	if err != nil {
		t.Fatalf("Install() error = %v", err)
	}
	if result.Kind != "launchagent" {
		t.Fatalf("kind = %q", result.Kind)
	}
	raw, err := os.ReadFile(filepath.Join(homeDir, "Library", "LaunchAgents", "ai.personastack.connector.plist"))
	if err != nil {
		t.Fatalf("read plist: %v", err)
	}
	if !strings.Contains(string(raw), "PersonaStack &amp; Connector.app") {
		t.Fatalf("unexpected plist:\n%s", raw)
	}
	if len(runner.commands) != 4 || !strings.Contains(runner.commands[3], "kickstart") {
		t.Fatalf("unexpected commands: %+v", runner.commands)
	}
}

func TestInstallerWritesWindowsScheduledTask(t *testing.T) {
	homeDir := t.TempDir()
	runner := &recordingRunner{}
	result, err := (Installer{
		HomeDir:        homeDir,
		ExecutablePath: `C:\Program Files\PersonaStack\personastack-connector.exe`,
		GOOS:           "windows",
		Runner:         runner,
	}).Install()
	if err != nil {
		t.Fatalf("Install() error = %v", err)
	}
	if result.Kind != "windows-scheduled-task" {
		t.Fatalf("kind = %q", result.Kind)
	}
	raw, err := os.ReadFile(filepath.Join(homeDir, "AppData", "Local", "PersonaStack", "Connector", "install-task.ps1"))
	if err != nil {
		t.Fatalf("read task script: %v", err)
	}
	if !strings.Contains(string(raw), `Register-ScheduledTask -TaskName "PersonaStack Connector"`) {
		t.Fatalf("unexpected task script:\n%s", raw)
	}
	if !strings.Contains(string(raw), `-RestartCount 999`) || !strings.Contains(string(raw), `Start-ScheduledTask -TaskName "PersonaStack Connector"`) {
		t.Fatalf("missing restart persistence in task script:\n%s", raw)
	}
	if len(runner.commands) != 1 || !strings.Contains(runner.commands[0], "powershell.exe -NoProfile -ExecutionPolicy Bypass -File") {
		t.Fatalf("unexpected commands: %+v", runner.commands)
	}
}

func TestEnsureShimWritesStableUserExecutable(t *testing.T) {
	homeDir := t.TempDir()
	result, err := EnsureShim(homeDir, "/opt/PersonaStack Connector/personastack-connector", "linux")
	if err != nil {
		t.Fatalf("EnsureShim() error = %v", err)
	}
	wantPath := filepath.Join(homeDir, ".local", "bin", "personastack-connector")
	if result.Path != wantPath {
		t.Fatalf("path = %q, want %q", result.Path, wantPath)
	}
	raw, err := os.ReadFile(wantPath)
	if err != nil {
		t.Fatalf("read shim: %v", err)
	}
	if !strings.Contains(string(raw), "exec '/opt/PersonaStack Connector/personastack-connector'") {
		t.Fatalf("unexpected shim:\n%s", raw)
	}
	info, err := os.Stat(wantPath)
	if err != nil {
		t.Fatalf("stat shim: %v", err)
	}
	if info.Mode().Perm() != 0o700 {
		t.Fatalf("mode = %o, want 700", info.Mode().Perm())
	}
}

func TestEnsureShimDoesNotOverwriteTargetExecutable(t *testing.T) {
	homeDir := t.TempDir()
	path := filepath.Join(homeDir, ".local", "bin", "personastack-connector")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("mkdir shim dir: %v", err)
	}
	if err := os.WriteFile(path, []byte("binary"), 0o700); err != nil {
		t.Fatalf("write executable: %v", err)
	}
	result, err := EnsureShim(homeDir, path, "linux")
	if err != nil {
		t.Fatalf("EnsureShim() error = %v", err)
	}
	if result.Path != path {
		t.Fatalf("path = %q, want %q", result.Path, path)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read executable: %v", err)
	}
	if string(raw) != "binary" {
		t.Fatalf("executable overwritten:\n%s", raw)
	}
}

func TestEnsureShimDoesNotOverwriteSymlinkedTargetExecutable(t *testing.T) {
	homeDir := t.TempDir()
	target := filepath.Join(homeDir, "tools", "personastack-connector")
	if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
		t.Fatalf("mkdir target dir: %v", err)
	}
	if err := os.WriteFile(target, []byte("binary"), 0o700); err != nil {
		t.Fatalf("write target: %v", err)
	}
	path := filepath.Join(homeDir, ".local", "bin", "personastack-connector")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("mkdir shim dir: %v", err)
	}
	if err := os.Symlink(target, path); err != nil {
		t.Fatalf("symlink target: %v", err)
	}

	result, err := EnsureShim(homeDir, target, "linux")
	if err != nil {
		t.Fatalf("EnsureShim() error = %v", err)
	}
	if result.Path != path {
		t.Fatalf("path = %q, want %q", result.Path, path)
	}
	raw, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("read target: %v", err)
	}
	if string(raw) != "binary" {
		t.Fatalf("target overwritten:\n%s", raw)
	}
}
