package service

import (
	"fmt"
	"html"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/personastack/personastack-connector/internal/updater"
)

const serviceName = "personastack-connector"
const launchdLabel = "ai.personastack.connector"

var launchdSystemRoot = string(filepath.Separator)

type ServiceScope string

const (
	ServiceScopeUserLaunchAgent    ServiceScope = "user_launch_agent"
	ServiceScopeSystemLaunchDaemon ServiceScope = "system_launch_daemon"
	ServiceScopeLinuxSystemService ServiceScope = "linux_system_service"
)

func ParseServiceScope(value string) (ServiceScope, error) {
	switch strings.TrimSpace(value) {
	case "", "user", string(ServiceScopeUserLaunchAgent):
		return ServiceScopeUserLaunchAgent, nil
	case "system":
		if runtime.GOOS == "linux" {
			return ServiceScopeLinuxSystemService, nil
		}
		return ServiceScopeSystemLaunchDaemon, nil
	case string(ServiceScopeSystemLaunchDaemon):
		return ServiceScopeSystemLaunchDaemon, nil
	case "linux-system", string(ServiceScopeLinuxSystemService):
		return ServiceScopeLinuxSystemService, nil
	default:
		return "", fmt.Errorf("unsupported service scope: %s", value)
	}
}

type CommandRunner interface {
	Run(name string, args ...string) error
}

type ExecRunner struct{}

func (ExecRunner) Run(name string, args ...string) error {
	cmd := exec.Command(name, args...)
	output, err := cmd.CombinedOutput()
	if err == nil {
		return nil
	}
	message := strings.TrimSpace(string(output))
	if message == "" {
		return err
	}
	return fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, message)
}

type Installer struct {
	HomeDir        string
	HermesHome     string
	ExecutablePath string
	GOOS           string
	Scope          ServiceScope
	ServiceScope   ServiceScope
	SystemRoot     string
	Runner         CommandRunner
}

type SystemServiceTarget struct {
	Username string
	HomeDir  string
	UID      int
	GID      int
}

type InstallResult struct {
	Kind  string
	Path  string
	Scope ServiceScope
}

type UninstallResult struct {
	Kind    string
	Path    string
	Scope   ServiceScope
	Removed bool
}

type ShimResult struct {
	Path string
}

func EnsureShim(homeDir string, executablePath string, goos string) (ShimResult, error) {
	executablePath = strings.TrimSpace(executablePath)
	if executablePath == "" {
		return ShimResult{}, fmt.Errorf("resolve connector executable: empty path")
	}
	homeDir = strings.TrimSpace(homeDir)
	if homeDir == "" {
		return ShimResult{}, fmt.Errorf("resolve home dir: empty path")
	}
	goos = strings.TrimSpace(goos)
	if goos == "" {
		goos = runtime.GOOS
	}
	switch goos {
	case "darwin", "linux":
		path := filepath.Join(homeDir, ".local", "bin", serviceName)
		if samePath(path, executablePath) {
			return ShimResult{Path: path}, nil
		}
		script := fmt.Sprintf("#!/bin/sh\nexec %s \"$@\"\n", shellQuote(executablePath))
		if err := writeOwnerExecutable(path, []byte(script)); err != nil {
			return ShimResult{}, err
		}
		return ShimResult{Path: path}, nil
	case "windows":
		path := filepath.Join(homeDir, "AppData", "Local", "PersonaStack", "Connector", serviceName+".cmd")
		script := fmt.Sprintf("@echo off\r\n%q %%*\r\n", executablePath)
		if err := writeOwnerOnly(path, []byte(script)); err != nil {
			return ShimResult{}, err
		}
		return ShimResult{Path: path}, nil
	default:
		return ShimResult{}, fmt.Errorf("unsupported shim platform: %s", goos)
	}
}

func samePath(left string, right string) bool {
	left = filepath.Clean(strings.TrimSpace(left))
	right = filepath.Clean(strings.TrimSpace(right))
	if left == right {
		return true
	}
	leftEval, leftErr := filepath.EvalSymlinks(left)
	rightEval, rightErr := filepath.EvalSymlinks(right)
	if leftErr == nil && rightErr == nil {
		leftInfo, leftStatErr := os.Stat(leftEval)
		rightInfo, rightStatErr := os.Stat(rightEval)
		return leftStatErr == nil && rightStatErr == nil && os.SameFile(leftInfo, rightInfo)
	}
	return false
}

func (installer Installer) Plan() (InstallResult, error) {
	executablePath, err := installer.executablePath()
	if err != nil {
		return InstallResult{}, err
	}
	if strings.TrimSpace(executablePath) == "" {
		return InstallResult{}, fmt.Errorf("resolve connector executable: empty path")
	}
	homeDir, err := installer.homeDir()
	if err != nil {
		return InstallResult{}, err
	}
	goos := strings.TrimSpace(installer.GOOS)
	if goos == "" {
		goos = runtime.GOOS
	}
	scope, err := installer.serviceScope(goos)
	if err != nil {
		return InstallResult{}, err
	}
	executablePath = serviceExecutablePath(executablePath, goos, scope)
	switch goos {
	case "darwin":
		if scope == ServiceScopeSystemLaunchDaemon {
			return InstallResult{Kind: "launchdaemon", Path: filepath.Join(installer.systemRoot(), "Library", "LaunchDaemons", launchdLabel+".plist"), Scope: scope}, nil
		}
		return InstallResult{Kind: "launchagent", Path: filepath.Join(homeDir, "Library", "LaunchAgents", launchdLabel+".plist"), Scope: scope}, nil
	case "linux":
		if scope == ServiceScopeLinuxSystemService {
			return InstallResult{Kind: "systemd-system", Path: filepath.Join(installer.systemRoot(), "etc", "systemd", "system", serviceName+".service"), Scope: scope}, nil
		}
		return InstallResult{Kind: "systemd-user", Path: filepath.Join(homeDir, ".config", "systemd", "user", serviceName+".service"), Scope: scope}, nil
	case "windows":
		return InstallResult{Kind: "windows-scheduled-task", Path: filepath.Join(homeDir, "AppData", "Local", "PersonaStack", "Connector", "install-task.ps1"), Scope: scope}, nil
	default:
		return InstallResult{}, fmt.Errorf("unsupported service platform: %s", goos)
	}
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
	scope, err := installer.serviceScope(goos)
	if err != nil {
		return InstallResult{}, err
	}
	executablePath = serviceExecutablePath(executablePath, goos, scope)
	switch goos {
	case "darwin":
		if scope == ServiceScopeSystemLaunchDaemon {
			return installer.installLaunchDaemon(executablePath)
		}
		return installer.installLaunchAgent(homeDir, executablePath)
	case "linux":
		if scope == ServiceScopeLinuxSystemService {
			return installer.installSystemdSystem(homeDir, executablePath)
		}
		return installer.installSystemdUser(homeDir, executablePath)
	case "windows":
		return installer.installWindowsTask(homeDir, executablePath)
	default:
		return InstallResult{}, fmt.Errorf("unsupported service platform: %s", goos)
	}
}

func serviceExecutablePath(executablePath string, goos string, scope ServiceScope) string {
	if goos != "darwin" || scope != ServiceScopeUserLaunchAgent {
		return executablePath
	}
	stablePath, ok := updater.StableHomebrewOptExecutablePath(executablePath)
	if !ok {
		return executablePath
	}
	return stablePath
}

func (installer Installer) Uninstall() ([]UninstallResult, error) {
	homeDir, err := installer.homeDir()
	if err != nil {
		return nil, err
	}
	goos := strings.TrimSpace(installer.GOOS)
	if goos == "" {
		goos = runtime.GOOS
	}
	scope, err := installer.serviceScope(goos)
	if err != nil {
		return nil, err
	}
	switch goos {
	case "darwin":
		if scope == ServiceScopeSystemLaunchDaemon {
			result, err := installer.uninstallLaunchDaemon()
			return singleUninstallResult(result, err)
		}
		result, err := installer.uninstallLaunchAgent(homeDir)
		return singleUninstallResult(result, err)
	case "linux":
		if scope == ServiceScopeLinuxSystemService {
			result, err := installer.uninstallLinuxSystem()
			return singleUninstallResult(result, err)
		}
		return installer.uninstallLinuxUser(homeDir)
	default:
		return nil, fmt.Errorf("unsupported service platform: %s", goos)
	}
}

func singleUninstallResult(result UninstallResult, err error) ([]UninstallResult, error) {
	if err != nil {
		return nil, err
	}
	return []UninstallResult{result}, nil
}

func (installer Installer) installLaunchAgent(homeDir string, executablePath string) (InstallResult, error) {
	path := filepath.Join(homeDir, "Library", "LaunchAgents", launchdLabel+".plist")
	if err := installer.disableOppositeLaunchdService(ServiceScopeUserLaunchAgent, homeDir); err != nil {
		return InstallResult{}, err
	}
	plist := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>%s</string>
  <key>ProgramArguments</key>
  <array>
    <string>%s</string>
    <string>run</string>
    <string>--foreground</string>
  </array>
%s
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>StandardOutPath</key><string>%s</string>
  <key>StandardErrorPath</key><string>%s</string>
</dict>
</plist>
`, launchdLabel, xmlEscape(executablePath), launchdEnvironmentVariables(installer.HermesHome), xmlEscape(filepath.Join(homeDir, "Library", "Logs", "personastack-connector.log")), xmlEscape(filepath.Join(homeDir, "Library", "Logs", "personastack-connector.err.log")))
	if err := writeOwnerOnly(path, []byte(plist)); err != nil {
		return InstallResult{}, err
	}
	runner := installer.runner()
	target := fmt.Sprintf("gui/%d/%s", os.Getuid(), launchdLabel)
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
	return InstallResult{Kind: "launchagent", Path: path, Scope: ServiceScopeUserLaunchAgent}, nil
}

func (installer Installer) installLaunchDaemon(executablePath string) (InstallResult, error) {
	systemRoot := installer.systemRoot()
	path := filepath.Join(systemRoot, "Library", "LaunchDaemons", launchdLabel+".plist")
	logDir := filepath.Join(systemRoot, "Library", "Logs", "PersonaStack")
	homeDir, err := installer.homeDir()
	if err != nil {
		return InstallResult{}, err
	}
	if err := installer.disableOppositeLaunchdService(ServiceScopeSystemLaunchDaemon, homeDir); err != nil {
		return InstallResult{}, err
	}
	plist := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>%s</string>
  <key>ProgramArguments</key>
  <array>
    <string>%s</string>
    <string>run</string>
    <string>--foreground</string>
    <string>--service-scope</string>
    <string>system</string>
  </array>
%s
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>ThrottleInterval</key><integer>30</integer>
  <key>ProcessType</key><string>Background</string>
  <key>StandardOutPath</key><string>%s</string>
  <key>StandardErrorPath</key><string>%s</string>
</dict>
</plist>
`, launchdLabel, xmlEscape(executablePath), launchdEnvironmentVariables(installer.HermesHome), xmlEscape(filepath.Join(logDir, "personastack-connector.log")), xmlEscape(filepath.Join(logDir, "personastack-connector.err.log")))
	err = os.MkdirAll(logDir, 0o755)
	if err != nil {
		return InstallResult{}, fmt.Errorf("create service log dir: %w", err)
	}
	if err := writeLaunchDaemonPlist(path, []byte(plist)); err != nil {
		return InstallResult{}, err
	}
	runner := installer.runner()
	_ = runner.Run("launchctl", "bootout", "system", path)
	err = runner.Run("launchctl", "bootstrap", "system", path)
	if err != nil {
		return InstallResult{}, fmt.Errorf("launchctl bootstrap system: %w", err)
	}
	err = runner.Run("launchctl", "enable", "system/"+launchdLabel)
	if err != nil {
		return InstallResult{}, fmt.Errorf("launchctl enable system: %w", err)
	}
	err = runner.Run("launchctl", "kickstart", "-k", "system/"+launchdLabel)
	if err != nil {
		return InstallResult{}, fmt.Errorf("launchctl kickstart system: %w", err)
	}
	return InstallResult{Kind: "launchdaemon", Path: path, Scope: ServiceScopeSystemLaunchDaemon}, nil
}

func (installer Installer) uninstallLaunchAgent(homeDir string) (UninstallResult, error) {
	guiDomain, homeDir := installer.launchAgentTarget(homeDir)
	path := filepath.Join(homeDir, "Library", "LaunchAgents", launchdLabel+".plist")
	labelTarget := guiDomain + "/" + launchdLabel
	if err := installer.unloadLaunchdService(path, guiDomain, labelTarget); err != nil {
		return UninstallResult{}, err
	}
	removed, err := removeServiceFile(path)
	if err != nil {
		return UninstallResult{}, err
	}
	return UninstallResult{Kind: "launchagent", Path: path, Scope: ServiceScopeUserLaunchAgent, Removed: removed}, nil
}

func (installer Installer) uninstallLaunchDaemon() (UninstallResult, error) {
	path := filepath.Join(installer.systemRoot(), "Library", "LaunchDaemons", launchdLabel+".plist")
	if err := installer.unloadLaunchdService(path, "system", "system/"+launchdLabel); err != nil {
		return UninstallResult{}, err
	}
	removed, err := removeServiceFile(path)
	if err != nil {
		return UninstallResult{}, err
	}
	return UninstallResult{Kind: "launchdaemon", Path: path, Scope: ServiceScopeSystemLaunchDaemon, Removed: removed}, nil
}

func (installer Installer) unloadLaunchdService(path string, domain string, labelTarget string) error {
	runner := installer.runner()
	exists, err := serviceFileExists(path)
	if err != nil {
		return err
	}
	if exists {
		err = runner.Run("launchctl", "bootout", domain, path)
	} else {
		err = runner.Run("launchctl", "bootout", labelTarget)
	}
	if err != nil && !isIgnorableLaunchctlUninstallError(err) {
		return fmt.Errorf("launchctl bootout %s: %w", labelTarget, err)
	}
	err = runner.Run("launchctl", "disable", labelTarget)
	if err != nil && !isIgnorableLaunchctlUninstallError(err) {
		return fmt.Errorf("launchctl disable %s: %w", labelTarget, err)
	}
	return nil
}

func (installer Installer) disableOppositeLaunchdService(scope ServiceScope, homeDir string) error {
	runner := installer.runner()
	switch scope {
	case ServiceScopeUserLaunchAgent:
		path := filepath.Join(installer.systemRoot(), "Library", "LaunchDaemons", launchdLabel+".plist")
		_ = runner.Run("launchctl", "bootout", "system", path)
		_ = runner.Run("launchctl", "disable", "system/"+launchdLabel)
		if err := removeOppositeLaunchdPlist(path); err != nil {
			return err
		}
	case ServiceScopeSystemLaunchDaemon:
		guiDomain, homeDir := installer.launchAgentTarget(homeDir)
		path := filepath.Join(homeDir, "Library", "LaunchAgents", launchdLabel+".plist")
		_ = runner.Run("launchctl", "bootout", guiDomain, path)
		_ = runner.Run("launchctl", "disable", guiDomain+"/"+launchdLabel)
		if err := removeOppositeLaunchdPlist(path); err != nil {
			return err
		}
	}
	return nil
}

func removeOppositeLaunchdPlist(path string) error {
	if _, err := removeServiceFile(path); err != nil {
		return fmt.Errorf("remove opposite launchd service: %w", err)
	}
	return nil
}

func removeServiceFile(path string) (bool, error) {
	if err := os.Remove(path); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("remove service file: %w", err)
	}
	return true, nil
}

func serviceFileExists(path string) (bool, error) {
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("stat service file: %w", err)
	}
	return true, nil
}

func isIgnorableLaunchctlUninstallError(err error) bool {
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "could not find service") ||
		strings.Contains(message, "no such process") ||
		strings.Contains(message, "service is not loaded") ||
		strings.Contains(message, "service could not be found")
}

func (installer Installer) launchAgentTarget(defaultHomeDir string) (string, string) {
	defaultDomain := fmt.Sprintf("gui/%d", os.Getuid())
	if strings.TrimSpace(installer.HomeDir) != "" {
		return defaultDomain, defaultHomeDir
	}
	sudoUser := strings.TrimSpace(os.Getenv("SUDO_USER"))
	if sudoUser == "" || sudoUser == "root" {
		return defaultDomain, defaultHomeDir
	}
	account, err := user.Lookup(sudoUser)
	if err != nil || strings.TrimSpace(account.HomeDir) == "" {
		return defaultDomain, defaultHomeDir
	}
	return "gui/" + strings.TrimSpace(account.Uid), account.HomeDir
}

func (installer Installer) installSystemdUser(homeDir string, executablePath string) (InstallResult, error) {
	path := filepath.Join(homeDir, ".config", "systemd", "user", serviceName+".service")
	unit := fmt.Sprintf(`[Unit]
Description=PersonaStack Connector

[Service]
%s
ExecStart=%s run --foreground
Restart=always
RestartSec=5

[Install]
WantedBy=default.target
`, systemdEnvironmentLine("HERMES_HOME", installer.HermesHome), systemdQuote(executablePath))
	if err := writeOwnerOnly(path, []byte(unit)); err != nil {
		return InstallResult{}, err
	}
	runner := installer.runner()
	if err := runner.Run("systemctl", "--user", "daemon-reload"); err != nil {
		return installer.installLinuxAutostart(homeDir, executablePath)
	}
	if err := runner.Run("systemctl", "--user", "enable", "--now", serviceName+".service"); err != nil {
		return installer.installLinuxAutostart(homeDir, executablePath)
	}
	return InstallResult{Kind: "systemd-user", Path: path, Scope: ServiceScopeUserLaunchAgent}, nil
}

func (installer Installer) installSystemdSystem(_ string, executablePath string) (InstallResult, error) {
	path := filepath.Join(installer.systemRoot(), "etc", "systemd", "system", serviceName+".service")
	target, err := installer.SystemServiceTarget()
	if err != nil {
		return InstallResult{}, err
	}
	unit := fmt.Sprintf(`[Unit]
Description=PersonaStack Connector
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=%s
Environment=%s
%s
WorkingDirectory=%s
ExecStart=%s run --foreground --service-scope %s
Restart=always
RestartSec=30

[Install]
WantedBy=multi-user.target
`, systemdValue(target.Username), systemdQuote("HOME="+target.HomeDir), systemdEnvironmentLine("HERMES_HOME", installer.HermesHome), systemdQuote(target.HomeDir), systemdQuote(executablePath), ServiceScopeLinuxSystemService)
	if err := writeSystemServiceFile(path, []byte(unit)); err != nil {
		return InstallResult{}, err
	}
	runner := installer.runner()
	if err := runner.Run("systemctl", "daemon-reload"); err != nil {
		return InstallResult{}, fmt.Errorf("systemctl daemon-reload: %w", err)
	}
	if err := runner.Run("systemctl", "enable", "--now", serviceName+".service"); err != nil {
		return InstallResult{}, fmt.Errorf("systemctl enable system service: %w", err)
	}
	return InstallResult{Kind: "systemd-system", Path: path, Scope: ServiceScopeLinuxSystemService}, nil
}

func (installer Installer) installLinuxAutostart(homeDir string, executablePath string) (InstallResult, error) {
	path := filepath.Join(homeDir, ".config", "autostart", serviceName+".desktop")
	entry := fmt.Sprintf(`[Desktop Entry]
Type=Application
Name=PersonaStack Connector
Exec=%s
X-GNOME-Autostart-enabled=true
`, desktopExecCommand(executablePath, installer.HermesHome, "run --foreground"))
	if err := writeOwnerOnly(path, []byte(entry)); err != nil {
		return InstallResult{}, err
	}
	return InstallResult{Kind: "no_user_service_manager", Path: path, Scope: ServiceScopeUserLaunchAgent}, nil
}

func (installer Installer) uninstallLinuxUser(homeDir string) ([]UninstallResult, error) {
	runner := installer.runner()
	systemdPath := filepath.Join(homeDir, ".config", "systemd", "user", serviceName+".service")
	systemdWantsPath := filepath.Join(homeDir, ".config", "systemd", "user", "default.target.wants", serviceName+".service")
	autostartPath := filepath.Join(homeDir, ".config", "autostart", serviceName+".desktop")

	systemdExists, err := serviceFileExists(systemdPath)
	if err != nil {
		return nil, err
	}
	systemdWantsExists, err := serviceFileExists(systemdWantsPath)
	if err != nil {
		return nil, err
	}
	err = runner.Run("systemctl", "--user", "disable", "--now", serviceName+".service")
	if err != nil && (systemdExists || systemdWantsExists) && !isIgnorableSystemctlUninstallError(err) {
		return nil, fmt.Errorf("systemctl disable user service: %w", err)
	}
	systemdRemoved, err := removeServiceFile(systemdPath)
	if err != nil {
		return nil, err
	}
	systemdWantsRemoved, err := removeServiceFile(systemdWantsPath)
	if err != nil {
		return nil, err
	}
	err = runner.Run("systemctl", "--user", "daemon-reload")
	if err != nil && (systemdExists || systemdWantsExists || systemdRemoved || systemdWantsRemoved) && !isIgnorableSystemctlUninstallError(err) {
		return nil, fmt.Errorf("systemctl daemon-reload: %w", err)
	}
	autostartRemoved, err := removeServiceFile(autostartPath)
	if err != nil {
		return nil, err
	}

	results := []UninstallResult{{Kind: "systemd-user", Path: systemdPath, Scope: ServiceScopeUserLaunchAgent, Removed: systemdRemoved}}
	if systemdWantsRemoved {
		results = append(results, UninstallResult{Kind: "systemd-user-wants", Path: systemdWantsPath, Scope: ServiceScopeUserLaunchAgent, Removed: true})
	}
	results = append(results, UninstallResult{Kind: "no_user_service_manager", Path: autostartPath, Scope: ServiceScopeUserLaunchAgent, Removed: autostartRemoved})
	return results, nil
}

func (installer Installer) uninstallLinuxSystem() (UninstallResult, error) {
	runner := installer.runner()
	path := filepath.Join(installer.systemRoot(), "etc", "systemd", "system", serviceName+".service")
	exists, err := serviceFileExists(path)
	if err != nil {
		return UninstallResult{}, err
	}
	err = runner.Run("systemctl", "disable", "--now", serviceName+".service")
	if err != nil && exists && !isIgnorableSystemctlUninstallError(err) {
		return UninstallResult{}, fmt.Errorf("systemctl disable system service: %w", err)
	}
	removed, err := removeServiceFile(path)
	if err != nil {
		return UninstallResult{}, err
	}
	err = runner.Run("systemctl", "daemon-reload")
	if err != nil && (exists || removed) && !isIgnorableSystemctlUninstallError(err) {
		return UninstallResult{}, fmt.Errorf("systemctl daemon-reload: %w", err)
	}
	return UninstallResult{Kind: "systemd-system", Path: path, Scope: ServiceScopeLinuxSystemService, Removed: removed}, nil
}

func isIgnorableSystemctlUninstallError(err error) bool {
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "not loaded") ||
		strings.Contains(message, "does not exist") ||
		strings.Contains(message, "executable file not found") ||
		strings.Contains(message, "failed to connect to bus") ||
		strings.Contains(message, "no medium found") ||
		strings.Contains(message, "no user bus") ||
		strings.Contains(message, "system has not been booted with systemd") ||
		strings.Contains(message, "transport endpoint is not connected") ||
		(strings.Contains(message, "unit ") && strings.Contains(message, " not found"))
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
	return InstallResult{Kind: "windows-scheduled-task", Path: path, Scope: ServiceScopeUserLaunchAgent}, nil
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

func (installer Installer) serviceScope(goos string) (ServiceScope, error) {
	scope := installer.Scope
	if scope == "" {
		scope = installer.ServiceScope
	}
	if scope == "" {
		return ServiceScopeUserLaunchAgent, nil
	}
	switch scope {
	case ServiceScopeUserLaunchAgent:
		return scope, nil
	case ServiceScopeSystemLaunchDaemon:
		if goos != "darwin" {
			return "", fmt.Errorf("system launch daemon scope requires darwin")
		}
		return scope, nil
	case ServiceScopeLinuxSystemService:
		if goos != "linux" {
			return "", fmt.Errorf("linux system service scope requires linux")
		}
		return scope, nil
	default:
		return "", fmt.Errorf("unsupported service scope: %s", scope)
	}
}

func (installer Installer) systemRoot() string {
	root := strings.TrimSpace(installer.SystemRoot)
	if root == "" {
		return launchdSystemRoot
	}
	return root
}

func (installer Installer) SystemServiceTarget() (SystemServiceTarget, error) {
	account, err := installer.systemServiceUser()
	if err != nil {
		return SystemServiceTarget{}, fmt.Errorf("resolve service user: %w", err)
	}
	username := strings.TrimSpace(account.Username)
	if username == "" {
		return SystemServiceTarget{}, fmt.Errorf("resolve service user: empty username")
	}
	uid, err := strconv.Atoi(strings.TrimSpace(account.Uid))
	if err != nil {
		return SystemServiceTarget{}, fmt.Errorf("resolve service uid: %w", err)
	}
	gid, err := strconv.Atoi(strings.TrimSpace(account.Gid))
	if err != nil {
		return SystemServiceTarget{}, fmt.Errorf("resolve service gid: %w", err)
	}
	homeDir := strings.TrimSpace(installer.HomeDir)
	if homeDir == "" {
		homeDir = strings.TrimSpace(account.HomeDir)
	}
	if homeDir == "" {
		return SystemServiceTarget{}, fmt.Errorf("resolve service home: empty home directory")
	}
	return SystemServiceTarget{Username: username, HomeDir: homeDir, UID: uid, GID: gid}, nil
}

func (installer Installer) systemServiceUser() (*user.User, error) {
	if strings.TrimSpace(installer.HomeDir) == "" {
		sudoUser := strings.TrimSpace(os.Getenv("SUDO_USER"))
		if sudoUser != "" && sudoUser != "root" {
			account, err := user.Lookup(sudoUser)
			if err == nil {
				return account, nil
			}
		}
	}
	return user.Current()
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

func writeLaunchDaemonPlist(path string, raw []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create launchdaemon dir: %w", err)
	}
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		return fmt.Errorf("write launchdaemon plist: %w", err)
	}
	return nil
}

func writeSystemServiceFile(path string, raw []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create system service dir: %w", err)
	}
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		return fmt.Errorf("write system service file: %w", err)
	}
	return nil
}

func writeOwnerExecutable(path string, raw []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create service dir: %w", err)
	}
	file, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create temp service file: %w", err)
	}
	tempPath := file.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tempPath)
		}
	}()
	if _, err := file.Write(raw); err != nil {
		_ = file.Close()
		return fmt.Errorf("write temp service file: %w", err)
	}
	if err := file.Chmod(0o700); err != nil {
		_ = file.Close()
		return fmt.Errorf("secure temp service file: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close temp service file: %w", err)
	}
	if err := os.Rename(tempPath, path); err != nil {
		return fmt.Errorf("replace service file: %w", err)
	}
	cleanup = false
	return nil
}

func xmlEscape(value string) string {
	return html.EscapeString(strings.TrimSpace(value))
}

func launchdEnvironmentVariables(hermesHome string) string {
	value := strings.TrimSpace(hermesHome)
	if value == "" {
		return ""
	}
	return fmt.Sprintf("  <key>EnvironmentVariables</key>\n  <dict>\n    <key>HERMES_HOME</key><string>%s</string>\n  </dict>\n", xmlEscape(value))
}

func systemdEnvironmentLine(name string, value string) string {
	trimmedName := strings.TrimSpace(name)
	trimmedValue := strings.TrimSpace(value)
	if trimmedName == "" || trimmedValue == "" {
		return ""
	}
	return "Environment=" + systemdQuote(trimmedName+"="+trimmedValue) + "\n"
}

func systemdQuote(value string) string {
	escaped := strings.ReplaceAll(strings.TrimSpace(value), `\`, `\\`)
	escaped = strings.ReplaceAll(escaped, `"`, `\"`)
	return `"` + escaped + `"`
}

func systemdValue(value string) string {
	return strings.ReplaceAll(strings.TrimSpace(value), " ", `\x20`)
}

func desktopExecQuote(value string) string {
	escaped := strings.ReplaceAll(strings.TrimSpace(value), `\`, `\\`)
	escaped = strings.ReplaceAll(escaped, `"`, `\"`)
	return `"` + escaped + `"`
}

func desktopExecCommand(executablePath string, hermesHome string, args string) string {
	trimmedHermesHome := strings.TrimSpace(hermesHome)
	if trimmedHermesHome == "" {
		return desktopExecQuote(executablePath) + " " + strings.TrimSpace(args)
	}
	return "env HERMES_HOME=" + desktopExecQuote(trimmedHermesHome) + " " + desktopExecQuote(executablePath) + " " + strings.TrimSpace(args)
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(strings.TrimSpace(value), "'", "'\\''") + "'"
}
