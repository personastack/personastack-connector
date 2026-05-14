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
	if result.Kind != "linux-autostart" {
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
