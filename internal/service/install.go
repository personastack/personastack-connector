package service

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

const serviceName = "personastack-connector"

type CommandRunner interface {
	Run(name string, args ...string) error
}

type ExecRunner struct{}

func (ExecRunner) Run(name string, args ...string) error {
	cmd := exec.Command(name, args...)
	return cmd.Run()
}

type Installer struct {
	HomeDir        string
	ExecutablePath string
	GOOS           string
	Runner         CommandRunner
}

type InstallResult struct {
	Kind string
	Path string
}

func (installer Installer) Install() (InstallResult, error) {
	executablePath, err := installer.executablePath()
	if err != nil {
		return InstallResult{}, err
	}
	homeDir, err := installer.homeDir()
	if err != nil {
		return InstallResult{}, err
	}
	goos := strings.TrimSpace(installer.GOOS)
	if goos == "" {
		goos = runtime.GOOS
	}
	switch goos {
	case "darwin":
		return installer.installLaunchAgent(homeDir, executablePath)
	case "linux":
		return installer.installSystemdUser(homeDir, executablePath)
	case "windows":
		return installer.installWindowsTask(homeDir, executablePath)
	default:
		return InstallResult{}, fmt.Errorf("unsupported service platform: %s", goos)
	}
}

func (installer Installer) installLaunchAgent(homeDir string, executablePath string) (InstallResult, error) {
	path := filepath.Join(homeDir, "Library", "LaunchAgents", "ai.personastack.connector.plist")
	plist := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>ai.personastack.connector</string>
  <key>ProgramArguments</key>
  <array>
    <string>%s</string>
    <string>run</string>
    <string>--foreground</string>
  </array>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>StandardOutPath</key><string>%s</string>
  <key>StandardErrorPath</key><string>%s</string>
</dict>
</plist>
`, executablePath, filepath.Join(homeDir, "Library", "Logs", "personastack-connector.log"), filepath.Join(homeDir, "Library", "Logs", "personastack-connector.err.log"))
	if err := writeOwnerOnly(path, []byte(plist)); err != nil {
		return InstallResult{}, err
	}
	runner := installer.runner()
	target := fmt.Sprintf("gui/%d/ai.personastack.connector", os.Getuid())
	_ = runner.Run("launchctl", "bootout", fmt.Sprintf("gui/%d", os.Getuid()), path)
	if err := runner.Run("launchctl", "bootstrap", fmt.Sprintf("gui/%d", os.Getuid()), path); err != nil {
		return InstallResult{}, fmt.Errorf("launchctl bootstrap: %w", err)
	}
	if err := runner.Run("launchctl", "enable", target); err != nil {
		return InstallResult{}, fmt.Errorf("launchctl enable: %w", err)
	}
	if err := runner.Run("launchctl", "kickstart", "-k", target); err != nil {
		return InstallResult{}, fmt.Errorf("launchctl kickstart: %w", err)
	}
	return InstallResult{Kind: "launchagent", Path: path}, nil
}

func (installer Installer) installSystemdUser(homeDir string, executablePath string) (InstallResult, error) {
	path := filepath.Join(homeDir, ".config", "systemd", "user", serviceName+".service")
	unit := fmt.Sprintf(`[Unit]
Description=PersonaStack Connector

[Service]
ExecStart=%s run --foreground
Restart=always
RestartSec=5

[Install]
WantedBy=default.target
`, executablePath)
	if err := writeOwnerOnly(path, []byte(unit)); err != nil {
		return InstallResult{}, err
	}
	runner := installer.runner()
	if err := runner.Run("systemctl", "--user", "daemon-reload"); err != nil {
		return InstallResult{}, fmt.Errorf("systemctl daemon-reload: %w", err)
	}
	if err := runner.Run("systemctl", "--user", "enable", "--now", serviceName+".service"); err != nil {
		return InstallResult{}, fmt.Errorf("systemctl enable: %w", err)
	}
	return InstallResult{Kind: "systemd-user", Path: path}, nil
}

func (installer Installer) installWindowsTask(homeDir string, executablePath string) (InstallResult, error) {
	path := filepath.Join(homeDir, "AppData", "Local", "PersonaStack", "Connector", "install-task.ps1")
	script := fmt.Sprintf(`$Action = New-ScheduledTaskAction -Execute %q -Argument "run --foreground"
$Trigger = New-ScheduledTaskTrigger -AtLogOn
$Settings = New-ScheduledTaskSettingsSet -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 1)
Register-ScheduledTask -TaskName "PersonaStack Connector" -Action $Action -Trigger $Trigger -Settings $Settings -Force
Start-ScheduledTask -TaskName "PersonaStack Connector"
`, executablePath)
	if err := writeOwnerOnly(path, []byte(script)); err != nil {
		return InstallResult{}, err
	}
	if err := installer.runner().Run("powershell.exe", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", path); err != nil {
		return InstallResult{}, fmt.Errorf("register scheduled task: %w", err)
	}
	return InstallResult{Kind: "windows-scheduled-task", Path: path}, nil
}

func (installer Installer) executablePath() (string, error) {
	value := strings.TrimSpace(installer.ExecutablePath)
	if value != "" {
		return value, nil
	}
	path, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("resolve connector executable: %w", err)
	}
	return path, nil
}

func (installer Installer) homeDir() (string, error) {
	value := strings.TrimSpace(installer.HomeDir)
	if value != "" {
		return value, nil
	}
	dir, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home dir: %w", err)
	}
	return dir, nil
}

func (installer Installer) runner() CommandRunner {
	if installer.Runner != nil {
		return installer.Runner
	}
	return ExecRunner{}
}

func writeOwnerOnly(path string, raw []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create service dir: %w", err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		return fmt.Errorf("write service file: %w", err)
	}
	return nil
}
