package service

import (
	"errors"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"testing"
)

type recordingRunner struct {
	commands     []string
	err          error
	errByCommand map[string]error
}

func (runner *recordingRunner) Run(name string, args ...string) error {
	command := name + " " + strings.Join(args, " ")
	runner.commands = append(runner.commands, command)
	if runner.errByCommand != nil {
		if err, ok := runner.errByCommand[command]; ok {
			return err
		}
	}
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
		scope    ServiceScope
		wantKind string
		wantPath string
	}{
		{
			goos:     "darwin",
			scope:    ServiceScopeUserLaunchAgent,
			wantKind: "launchagent",
			wantPath: filepath.Join("Library", "LaunchAgents", "ai.personastack.connector.plist"),
		},
		{
			goos:     "darwin",
			scope:    ServiceScopeSystemLaunchDaemon,
			wantKind: "launchdaemon",
			wantPath: filepath.Join("Library", "LaunchDaemons", "ai.personastack.connector.plist"),
		},
		{
			goos:     "linux",
			scope:    ServiceScopeUserLaunchAgent,
			wantKind: "systemd-user",
			wantPath: filepath.Join(".config", "systemd", "user", "personastack-connector.service"),
		},
		{
			goos:     "windows",
			scope:    ServiceScopeUserLaunchAgent,
			wantKind: "windows-scheduled-task",
			wantPath: filepath.Join("AppData", "Local", "PersonaStack", "Connector", "install-task.ps1"),
		},
	}

	for _, test := range tests {
		t.Run(test.goos+"-"+string(test.scope), func(t *testing.T) {
			homeDir := t.TempDir()
			systemRoot := homeDir
			result, err := (Installer{
				HomeDir:        homeDir,
				ExecutablePath: "/opt/personastack-connector",
				GOOS:           test.goos,
				ServiceScope:   test.scope,
				SystemRoot:     systemRoot,
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

func TestInstallerPlansSystemLaunchDaemon(t *testing.T) {
	homeDir := t.TempDir()
	result, err := (Installer{
		HomeDir:        homeDir,
		ExecutablePath: "/opt/personastack-connector",
		GOOS:           "darwin",
		Scope:          ServiceScopeSystemLaunchDaemon,
	}).Plan()
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if result.Kind != "launchdaemon" {
		t.Fatalf("kind = %q", result.Kind)
	}
	wantPath := filepath.Join(string(os.PathSeparator), "Library", "LaunchDaemons", "ai.personastack.connector.plist")
	if result.Path != wantPath {
		t.Fatalf("path = %q, want %q", result.Path, wantPath)
	}
	if _, err := os.Stat(filepath.Join(homeDir, "Library")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Plan() wrote home directory or stat failed: %v", err)
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
	systemRoot := t.TempDir()
	oppositePath := filepath.Join(systemRoot, "Library", "LaunchDaemons", "ai.personastack.connector.plist")
	if err := os.MkdirAll(filepath.Dir(oppositePath), 0o755); err != nil {
		t.Fatalf("mkdir opposite service: %v", err)
	}
	if err := os.WriteFile(oppositePath, []byte("old"), 0o644); err != nil {
		t.Fatalf("write opposite service: %v", err)
	}
	runner := &recordingRunner{}
	result, err := (Installer{
		HomeDir:        homeDir,
		ExecutablePath: "/Applications/PersonaStack & Connector.app/Contents/MacOS/personastack-connector",
		GOOS:           "darwin",
		Runner:         runner,
		SystemRoot:     systemRoot,
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
	if _, err := os.Stat(oppositePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("opposite launchdaemon still exists or stat failed: %v", err)
	}
	if len(runner.commands) != 6 || runner.commands[0] != "launchctl bootout system "+oppositePath || runner.commands[1] != "launchctl disable system/ai.personastack.connector" || !strings.Contains(runner.commands[5], "kickstart") {
		t.Fatalf("unexpected commands: %+v", runner.commands)
	}
}

func TestInstallerWritesLaunchDaemon(t *testing.T) {
	systemRoot := t.TempDir()
	homeDir := t.TempDir()
	oppositePath := filepath.Join(homeDir, "Library", "LaunchAgents", "ai.personastack.connector.plist")
	if err := os.MkdirAll(filepath.Dir(oppositePath), 0o700); err != nil {
		t.Fatalf("mkdir opposite service: %v", err)
	}
	if err := os.WriteFile(oppositePath, []byte("old"), 0o600); err != nil {
		t.Fatalf("write opposite service: %v", err)
	}
	runner := &recordingRunner{}
	result, err := (Installer{
		HomeDir:        homeDir,
		ExecutablePath: "/usr/local/bin/personastack-connector",
		GOOS:           "darwin",
		Runner:         runner,
		ServiceScope:   ServiceScopeSystemLaunchDaemon,
		SystemRoot:     systemRoot,
	}).Install()
	if err != nil {
		t.Fatalf("Install() error = %v", err)
	}
	if result.Kind != "launchdaemon" || result.Scope != ServiceScopeSystemLaunchDaemon {
		t.Fatalf("result = %+v", result)
	}
	raw, err := os.ReadFile(filepath.Join(systemRoot, "Library", "LaunchDaemons", "ai.personastack.connector.plist"))
	if err != nil {
		t.Fatalf("read plist: %v", err)
	}
	content := string(raw)
	for _, want := range []string{
		"<string>--service-scope</string>",
		"<string>system</string>",
		"<key>KeepAlive</key><true/>",
		"<key>RunAtLoad</key><true/>",
		"<key>ThrottleInterval</key><integer>30</integer>",
		"<key>ProcessType</key><string>Background</string>",
		"Library/Logs/PersonaStack/personastack-connector.log",
		"Library/Logs/PersonaStack/personastack-connector.err.log",
	} {
		if !strings.Contains(content, want) {
			t.Fatalf("launchdaemon plist missing %q:\n%s", want, content)
		}
	}
	info, err := os.Stat(filepath.Join(systemRoot, "Library", "LaunchDaemons", "ai.personastack.connector.plist"))
	if err != nil {
		t.Fatalf("stat plist: %v", err)
	}
	if info.Mode().Perm() != 0o644 {
		t.Fatalf("plist mode = %o, want 644", info.Mode().Perm())
	}
	logInfo, err := os.Stat(filepath.Join(systemRoot, "Library", "Logs", "PersonaStack"))
	if err != nil {
		t.Fatalf("stat log dir: %v", err)
	}
	if logInfo.Mode().Perm() != 0o755 {
		t.Fatalf("log dir mode = %o, want 755", logInfo.Mode().Perm())
	}
	if _, err := os.Stat(oppositePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("opposite launchagent still exists or stat failed: %v", err)
	}
	guiDomain := fmt.Sprintf("gui/%d", os.Getuid())
	if len(runner.commands) != 6 || runner.commands[0] != "launchctl bootout "+guiDomain+" "+oppositePath || runner.commands[1] != "launchctl disable "+guiDomain+"/ai.personastack.connector" || runner.commands[2] != "launchctl bootout system "+result.Path || runner.commands[3] != "launchctl bootstrap system "+result.Path || runner.commands[4] != "launchctl enable system/ai.personastack.connector" || !strings.Contains(runner.commands[5], "kickstart -k system/ai.personastack.connector") {
		t.Fatalf("unexpected commands: %+v", runner.commands)
	}
}

func TestInstallerUninstallsLaunchAgent(t *testing.T) {
	homeDir := t.TempDir()
	path := filepath.Join(homeDir, "Library", "LaunchAgents", "ai.personastack.connector.plist")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("mkdir launchagent dir: %v", err)
	}
	if err := os.WriteFile(path, []byte("old"), 0o600); err != nil {
		t.Fatalf("write launchagent: %v", err)
	}
	runner := &recordingRunner{}
	results, err := (Installer{
		HomeDir: homeDir,
		GOOS:    "darwin",
		Runner:  runner,
	}).Uninstall()
	if err != nil {
		t.Fatalf("Uninstall() error = %v", err)
	}
	if len(results) != 1 || results[0].Kind != "launchagent" || !results[0].Removed {
		t.Fatalf("unexpected results: %+v", results)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("launchagent still exists or stat failed: %v", err)
	}
	guiDomain := fmt.Sprintf("gui/%d", os.Getuid())
	if len(runner.commands) != 2 || runner.commands[0] != "launchctl bootout "+guiDomain+" "+path || runner.commands[1] != "launchctl disable "+guiDomain+"/ai.personastack.connector" {
		t.Fatalf("unexpected commands: %+v", runner.commands)
	}
}

func TestInstallerUninstallsMissingLaunchAgentByLabel(t *testing.T) {
	homeDir := t.TempDir()
	runner := &recordingRunner{
		errByCommand: map[string]error{
			fmt.Sprintf("launchctl bootout gui/%d/ai.personastack.connector", os.Getuid()): errors.New("service could not be found"),
			fmt.Sprintf("launchctl disable gui/%d/ai.personastack.connector", os.Getuid()): errors.New("service could not be found"),
		},
	}
	results, err := (Installer{
		HomeDir: homeDir,
		GOOS:    "darwin",
		Runner:  runner,
	}).Uninstall()
	if err != nil {
		t.Fatalf("Uninstall() error = %v", err)
	}
	if len(results) != 1 || results[0].Kind != "launchagent" || results[0].Removed {
		t.Fatalf("unexpected results: %+v", results)
	}
	wantBootout := fmt.Sprintf("launchctl bootout gui/%d/ai.personastack.connector", os.Getuid())
	wantDisable := fmt.Sprintf("launchctl disable gui/%d/ai.personastack.connector", os.Getuid())
	if len(runner.commands) != 2 || runner.commands[0] != wantBootout || runner.commands[1] != wantDisable {
		t.Fatalf("unexpected commands: %+v", runner.commands)
	}
}

func TestInstallerUninstallLaunchAgentSurfacesManagerFailure(t *testing.T) {
	homeDir := t.TempDir()
	path := filepath.Join(homeDir, "Library", "LaunchAgents", "ai.personastack.connector.plist")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("mkdir launchagent dir: %v", err)
	}
	if err := os.WriteFile(path, []byte("old"), 0o600); err != nil {
		t.Fatalf("write launchagent: %v", err)
	}
	guiDomain := fmt.Sprintf("gui/%d", os.Getuid())
	runner := &recordingRunner{
		errByCommand: map[string]error{
			"launchctl bootout " + guiDomain + " " + path: errors.New("permission denied"),
		},
	}
	_, err := (Installer{
		HomeDir: homeDir,
		GOOS:    "darwin",
		Runner:  runner,
	}).Uninstall()
	if err == nil || !strings.Contains(err.Error(), "launchctl bootout") {
		t.Fatalf("Uninstall() error = %v, want launchctl failure", err)
	}
}

func TestInstallerLaunchAgentTargetUsesSudoUserHome(t *testing.T) {
	sudoUser := os.Getenv("USER")
	if strings.TrimSpace(sudoUser) == "" {
		t.Skip("USER is not set")
	}
	account, err := user.Lookup(sudoUser)
	if err != nil {
		t.Fatalf("lookup user: %v", err)
	}
	t.Setenv("SUDO_USER", sudoUser)
	guiDomain, homeDir := (Installer{}).launchAgentTarget(t.TempDir())
	if guiDomain != "gui/"+account.Uid {
		t.Fatalf("gui domain = %q, want gui/%s", guiDomain, account.Uid)
	}
	if homeDir != account.HomeDir {
		t.Fatalf("home dir = %q, want %q", homeDir, account.HomeDir)
	}
}

func TestInstallerUninstallsLaunchDaemon(t *testing.T) {
	systemRoot := t.TempDir()
	path := filepath.Join(systemRoot, "Library", "LaunchDaemons", "ai.personastack.connector.plist")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir launchdaemon dir: %v", err)
	}
	if err := os.WriteFile(path, []byte("old"), 0o644); err != nil {
		t.Fatalf("write launchdaemon: %v", err)
	}
	runner := &recordingRunner{}
	results, err := (Installer{
		HomeDir:      t.TempDir(),
		GOOS:         "darwin",
		Runner:       runner,
		ServiceScope: ServiceScopeSystemLaunchDaemon,
		SystemRoot:   systemRoot,
	}).Uninstall()
	if err != nil {
		t.Fatalf("Uninstall() error = %v", err)
	}
	if len(results) != 1 || results[0].Kind != "launchdaemon" || !results[0].Removed {
		t.Fatalf("unexpected results: %+v", results)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("launchdaemon still exists or stat failed: %v", err)
	}
	if len(runner.commands) != 2 || runner.commands[0] != "launchctl bootout system "+path || runner.commands[1] != "launchctl disable system/ai.personastack.connector" {
		t.Fatalf("unexpected commands: %+v", runner.commands)
	}
}

func TestInstallerUninstallsMissingLaunchDaemonByLabel(t *testing.T) {
	systemRoot := t.TempDir()
	runner := &recordingRunner{
		errByCommand: map[string]error{
			"launchctl bootout system/ai.personastack.connector": errors.New("service could not be found"),
			"launchctl disable system/ai.personastack.connector": errors.New("service could not be found"),
		},
	}
	results, err := (Installer{
		HomeDir:      t.TempDir(),
		GOOS:         "darwin",
		Runner:       runner,
		ServiceScope: ServiceScopeSystemLaunchDaemon,
		SystemRoot:   systemRoot,
	}).Uninstall()
	if err != nil {
		t.Fatalf("Uninstall() error = %v", err)
	}
	if len(results) != 1 || results[0].Kind != "launchdaemon" || results[0].Removed {
		t.Fatalf("unexpected results: %+v", results)
	}
	if len(runner.commands) != 2 || runner.commands[0] != "launchctl bootout system/ai.personastack.connector" || runner.commands[1] != "launchctl disable system/ai.personastack.connector" {
		t.Fatalf("unexpected commands: %+v", runner.commands)
	}
}

func TestInstallerUninstallsLinuxUserServices(t *testing.T) {
	homeDir := t.TempDir()
	systemdPath := filepath.Join(homeDir, ".config", "systemd", "user", "personastack-connector.service")
	systemdWantsPath := filepath.Join(homeDir, ".config", "systemd", "user", "default.target.wants", "personastack-connector.service")
	autostartPath := filepath.Join(homeDir, ".config", "autostart", "personastack-connector.desktop")
	for _, path := range []string{systemdPath, systemdWantsPath, autostartPath} {
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatalf("mkdir service dir: %v", err)
		}
		if err := os.WriteFile(path, []byte("old"), 0o600); err != nil {
			t.Fatalf("write service file: %v", err)
		}
	}
	runner := &recordingRunner{}
	results, err := (Installer{
		HomeDir: homeDir,
		GOOS:    "linux",
		Runner:  runner,
	}).Uninstall()
	if err != nil {
		t.Fatalf("Uninstall() error = %v", err)
	}
	if len(results) != 3 || results[0].Kind != "systemd-user" || results[1].Kind != "systemd-user-wants" || results[2].Kind != "no_user_service_manager" {
		t.Fatalf("unexpected results: %+v", results)
	}
	for _, result := range results {
		if !result.Removed {
			t.Fatalf("result was not removed: %+v", result)
		}
		if _, err := os.Stat(result.Path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("service file still exists or stat failed: %v", err)
		}
	}
	if len(runner.commands) != 2 || runner.commands[0] != "systemctl --user disable --now personastack-connector.service" || runner.commands[1] != "systemctl --user daemon-reload" {
		t.Fatalf("unexpected commands: %+v", runner.commands)
	}
}

func TestInstallerUninstallLinuxRemovesFallbackFilesWhenSystemdUnavailable(t *testing.T) {
	homeDir := t.TempDir()
	systemdPath := filepath.Join(homeDir, ".config", "systemd", "user", "personastack-connector.service")
	systemdWantsPath := filepath.Join(homeDir, ".config", "systemd", "user", "default.target.wants", "personastack-connector.service")
	autostartPath := filepath.Join(homeDir, ".config", "autostart", "personastack-connector.desktop")
	for _, path := range []string{systemdPath, systemdWantsPath, autostartPath} {
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatalf("mkdir service dir: %v", err)
		}
		if err := os.WriteFile(path, []byte("old"), 0o600); err != nil {
			t.Fatalf("write service file: %v", err)
		}
	}
	runner := &recordingRunner{errByCommand: map[string]error{
		"systemctl --user disable --now personastack-connector.service": errors.New("Failed to connect to bus: No medium found"),
		"systemctl --user daemon-reload":                                errors.New("Failed to connect to bus: No medium found"),
	}}

	results, err := (Installer{
		HomeDir: homeDir,
		GOOS:    "linux",
		Runner:  runner,
	}).Uninstall()
	if err != nil {
		t.Fatalf("Uninstall() error = %v", err)
	}
	if len(results) != 3 || results[0].Kind != "systemd-user" || results[1].Kind != "systemd-user-wants" || results[2].Kind != "no_user_service_manager" {
		t.Fatalf("unexpected results: %+v", results)
	}
	for _, result := range results {
		if !result.Removed {
			t.Fatalf("result was not removed: %+v", result)
		}
		if _, err := os.Stat(result.Path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("service file still exists or stat failed: %v", err)
		}
	}
}

func TestInstallerUninstallIsIdempotent(t *testing.T) {
	results, err := (Installer{
		HomeDir: t.TempDir(),
		GOOS:    "linux",
		Runner:  &recordingRunner{},
	}).Uninstall()
	if err != nil {
		t.Fatalf("Uninstall() error = %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("unexpected results: %+v", results)
	}
	for _, result := range results {
		if result.Removed {
			t.Fatalf("missing service reported removed: %+v", result)
		}
	}
}

func TestInstallerUninstallLinuxOmitsMissingSystemdWantsResult(t *testing.T) {
	homeDir := t.TempDir()
	systemdPath := filepath.Join(homeDir, ".config", "systemd", "user", "personastack-connector.service")
	autostartPath := filepath.Join(homeDir, ".config", "autostart", "personastack-connector.desktop")
	for _, path := range []string{systemdPath, autostartPath} {
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatalf("mkdir service dir: %v", err)
		}
		if err := os.WriteFile(path, []byte("old"), 0o600); err != nil {
			t.Fatalf("write service file: %v", err)
		}
	}
	results, err := (Installer{
		HomeDir: homeDir,
		GOOS:    "linux",
		Runner:  &recordingRunner{},
	}).Uninstall()
	if err != nil {
		t.Fatalf("Uninstall() error = %v", err)
	}
	if len(results) != 2 || results[0].Kind != "systemd-user" || results[1].Kind != "no_user_service_manager" {
		t.Fatalf("unexpected results: %+v", results)
	}
}

func TestInstallerUninstallLinuxSurfacesSystemctlFailure(t *testing.T) {
	homeDir := t.TempDir()
	systemdPath := filepath.Join(homeDir, ".config", "systemd", "user", "personastack-connector.service")
	if err := os.MkdirAll(filepath.Dir(systemdPath), 0o700); err != nil {
		t.Fatalf("mkdir service dir: %v", err)
	}
	if err := os.WriteFile(systemdPath, []byte("old"), 0o600); err != nil {
		t.Fatalf("write service file: %v", err)
	}
	runner := &recordingRunner{
		errByCommand: map[string]error{
			"systemctl --user disable --now personastack-connector.service": errors.New("permission denied"),
		},
	}
	_, err := (Installer{
		HomeDir: homeDir,
		GOOS:    "linux",
		Runner:  runner,
	}).Uninstall()
	if err == nil || !strings.Contains(err.Error(), "systemctl disable") {
		t.Fatalf("Uninstall() error = %v, want systemctl failure", err)
	}
}

func TestInstallerRejectsSystemScopeOutsideDarwin(t *testing.T) {
	_, err := (Installer{
		HomeDir:        t.TempDir(),
		ExecutablePath: "/opt/personastack-connector",
		GOOS:           "linux",
		ServiceScope:   ServiceScopeSystemLaunchDaemon,
	}).Plan()
	if err == nil || !strings.Contains(err.Error(), "requires darwin") {
		t.Fatalf("Plan() error = %v, want darwin scope rejection", err)
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
