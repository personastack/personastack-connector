// Package installmetadata classifies the local Connector installation without
// network access. It only recognizes package ownership that it can verify.
package installmetadata

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/personastack/personastack-connector/internal/externalagentprotocol"
)

// Metadata is the bounded local install state reported on Connect and Heartbeat.
type Metadata struct {
	InstallChannel      externalagentprotocol.InstallChannel
	ExecutablePathClass externalagentprotocol.ExecutablePathClass
	UpdateCapability    externalagentprotocol.UpdateCapability
	UpdateState         externalagentprotocol.UpdateState
	UpdateReason        externalagentprotocol.UpdateReason
}

// CommandRunner runs a local package-query command. It exists so detector tests
// can exercise package ownership without requiring the host package manager.
type CommandRunner func(name string, args ...string) (string, error)

// DetectCurrent classifies the running executable with local filesystem and
// package-manager queries only.
func DetectCurrent() Metadata {
	path, err := os.Executable()
	if err != nil {
		return unsupportedUnknown()
	}
	return Detect(path, runtime.GOOS, runCommand)
}

// Detect classifies one executable path. Only verified Homebrew, dpkg, and RPM
// ownership are reported as manually updatable. Archive and ambiguous installs
// stay unsupported so the product never guesses a shell command.
func Detect(executablePath string, goos string, runner CommandRunner) Metadata {
	paths := executablePaths(executablePath)
	if isVerifiedHomebrew(goos, paths) {
		return manual(externalagentprotocol.InstallChannelHomebrew, externalagentprotocol.ExecutablePathClassHomebrewOpt)
	}
	if strings.TrimSpace(goos) == "linux" && runner != nil {
		for _, path := range paths {
			if packageOwnsDeb(path, runner) {
				return manual(externalagentprotocol.InstallChannelDeb, externalagentprotocol.ExecutablePathClassPackageManaged)
			}
			if packageOwnsRPM(path, runner) {
				return manual(externalagentprotocol.InstallChannelRPM, externalagentprotocol.ExecutablePathClassPackageManaged)
			}
		}
	}
	return unsupportedUnknown()
}

func manual(channel externalagentprotocol.InstallChannel, class externalagentprotocol.ExecutablePathClass) Metadata {
	return Metadata{
		InstallChannel:      channel,
		ExecutablePathClass: class,
		UpdateCapability:    externalagentprotocol.UpdateCapabilityManualRequired,
		UpdateState:         externalagentprotocol.UpdateStateIdle,
	}
}

func unsupportedUnknown() Metadata {
	return Metadata{
		InstallChannel:      externalagentprotocol.InstallChannelUnknown,
		ExecutablePathClass: externalagentprotocol.ExecutablePathClassUnknown,
		UpdateCapability:    externalagentprotocol.UpdateCapabilityUnsupported,
		UpdateState:         externalagentprotocol.UpdateStateIdle,
		UpdateReason:        externalagentprotocol.UpdateReasonUnknownInstallChannel,
	}
}

func executablePaths(executablePath string) []string {
	path := filepath.Clean(strings.TrimSpace(executablePath))
	if path == "" || path == "." {
		return nil
	}
	paths := []string{path}
	if resolved, err := filepath.EvalSymlinks(path); err == nil {
		resolved = filepath.Clean(resolved)
		if resolved != path {
			paths = append(paths, resolved)
		}
	}
	return paths
}

func isVerifiedHomebrew(goos string, paths []string) bool {
	if strings.TrimSpace(goos) != "darwin" {
		return false
	}
	for _, path := range paths {
		if strings.HasPrefix(path, "/opt/homebrew/opt/personastack-connector/") ||
			strings.HasPrefix(path, "/usr/local/opt/personastack-connector/") ||
			strings.HasPrefix(path, "/opt/homebrew/Cellar/personastack-connector/") ||
			strings.HasPrefix(path, "/usr/local/Cellar/personastack-connector/") {
			return true
		}
	}
	return false
}

func packageOwnsDeb(path string, runner CommandRunner) bool {
	output, err := runner("dpkg-query", "-S", path)
	if err != nil {
		return false
	}
	for _, line := range strings.Split(output, "\n") {
		owner, _, found := strings.Cut(strings.TrimSpace(line), ":")
		if found && isConnectorPackage(owner) {
			return true
		}
	}
	return false
}

func packageOwnsRPM(path string, runner CommandRunner) bool {
	output, err := runner("rpm", "-qf", "--qf", "%{NAME}", path)
	return err == nil && isConnectorPackage(strings.TrimSpace(output))
}

func isConnectorPackage(name string) bool {
	name = strings.TrimSpace(name)
	return name == "personastack-connector" || strings.HasPrefix(name, "personastack-connector:")
}

func runCommand(name string, args ...string) (string, error) {
	output, err := exec.Command(name, args...).Output()
	return string(output), err
}
