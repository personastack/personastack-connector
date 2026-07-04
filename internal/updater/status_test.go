package updater

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/personastack/personastack-connector/internal/externalagentprotocol"
)

func TestDetectHomebrewUserLaunchAgentAllowsOneClick(t *testing.T) {
	status := Detect(DetectionOptions{
		GOOS:           "darwin",
		ExecutablePath: "/opt/homebrew/Cellar/personastack-connector/1.2.3/bin/personastack-connector",
		ServiceScope:   externalagentprotocol.ServiceScopeUserLaunchAgent,
	})

	if status.InstallChannel != externalagentprotocol.InstallChannelHomebrew ||
		status.ExecutablePathClass != externalagentprotocol.ExecutablePathClassHomebrewOpt ||
		status.UpdateCapability != externalagentprotocol.UpdateCapabilityOneClickAvailable ||
		status.UpdateReason != "" {
		t.Fatalf("unexpected status: %+v", status)
	}
}

func TestDetectHomebrewSystemLaunchDaemonRequiresManualUpdate(t *testing.T) {
	status := Detect(DetectionOptions{
		GOOS:           "darwin",
		ExecutablePath: "/usr/local/opt/personastack-connector/bin/personastack-connector",
		ServiceScope:   externalagentprotocol.ServiceScopeSystemLaunchDaemon,
	})

	if status.InstallChannel != externalagentprotocol.InstallChannelHomebrew ||
		status.UpdateCapability != externalagentprotocol.UpdateCapabilityManualRequired ||
		status.UpdateReason != externalagentprotocol.UpdateReasonSystemLaunchDaemonHomebrew {
		t.Fatalf("unexpected status: %+v", status)
	}
}

func TestDetectLinuxDebRequiresManualWithoutRootSystemService(t *testing.T) {
	status := Detect(DetectionOptions{
		GOOS:           "linux",
		ServiceScope:   externalagentprotocol.ServiceScopeUserLaunchAgent,
		LookPath:       fakeLookPath("dpkg", "dpkg-query", "apt-get"),
		CommandOutput:  fakeCommandOutput(map[string][]byte{"dpkg-query\x00-W\x00-f=${Status}\x00personastack-connector": []byte("install ok installed")}),
		ExecutablePath: "/usr/bin/personastack-connector",
		CurrentEUID:    func() int { return 1000 },
	})

	if status.InstallChannel != externalagentprotocol.InstallChannelDeb ||
		status.ExecutablePathClass != externalagentprotocol.ExecutablePathClassPackageManaged ||
		status.UpdateCapability != externalagentprotocol.UpdateCapabilityManualRequired ||
		status.UpdateReason != externalagentprotocol.UpdateReasonRequiresSudo {
		t.Fatalf("unexpected status: %+v", status)
	}
}

func TestDetectLinuxRPMRequiresManualWithoutRootPrivilege(t *testing.T) {
	status := Detect(DetectionOptions{
		GOOS:           "linux",
		ServiceScope:   externalagentprotocol.ServiceScopeLinuxSystemService,
		LookPath:       fakeLookPath("rpm", "dnf"),
		CommandOutput:  fakeCommandOutput(map[string][]byte{"rpm\x00-q\x00personastack-connector": []byte("personastack-connector-1.2.3-1")}),
		ExecutablePath: "/usr/bin/personastack-connector",
		CurrentEUID:    func() int { return 1000 },
	})

	if status.InstallChannel != externalagentprotocol.InstallChannelRPM ||
		status.UpdateCapability != externalagentprotocol.UpdateCapabilityManualRequired ||
		status.UpdateReason != externalagentprotocol.UpdateReasonRequiresSudo {
		t.Fatalf("unexpected status: %+v", status)
	}
}

func TestDetectLinuxDebSystemServiceRootAllowsOneClick(t *testing.T) {
	status := Detect(DetectionOptions{
		GOOS:           "linux",
		ServiceScope:   externalagentprotocol.ServiceScopeLinuxSystemService,
		LookPath:       fakeLookPath("dpkg", "dpkg-query", "apt-get"),
		CommandOutput:  fakeCommandOutput(map[string][]byte{"dpkg-query\x00-W\x00-f=${Status}\x00personastack-connector": []byte("install ok installed")}),
		ExecutablePath: "/usr/bin/personastack-connector",
		CurrentEUID:    func() int { return 0 },
	})

	if status.InstallChannel != externalagentprotocol.InstallChannelDeb ||
		status.UpdateCapability != externalagentprotocol.UpdateCapabilityOneClickAvailable ||
		status.UpdateReason != "" {
		t.Fatalf("unexpected status: %+v", status)
	}
}

func TestDetectLinuxRPMSystemServiceRootAllowsOneClick(t *testing.T) {
	status := Detect(DetectionOptions{
		GOOS:           "linux",
		ServiceScope:   externalagentprotocol.ServiceScopeLinuxSystemService,
		LookPath:       fakeLookPath("rpm", "dnf"),
		CommandOutput:  fakeCommandOutput(map[string][]byte{"rpm\x00-q\x00personastack-connector": []byte("personastack-connector-1.2.3-1")}),
		ExecutablePath: "/usr/bin/personastack-connector",
		CurrentEUID:    func() int { return 0 },
	})

	if status.InstallChannel != externalagentprotocol.InstallChannelRPM ||
		status.UpdateCapability != externalagentprotocol.UpdateCapabilityOneClickAvailable ||
		status.UpdateReason != "" {
		t.Fatalf("unexpected status: %+v", status)
	}
}

func TestDetectLinuxRootMissingInstallerRequiresManual(t *testing.T) {
	status := Detect(DetectionOptions{
		GOOS:           "linux",
		ServiceScope:   externalagentprotocol.ServiceScopeLinuxSystemService,
		LookPath:       fakeLookPath("dpkg", "dpkg-query"),
		CommandOutput:  fakeCommandOutput(map[string][]byte{"dpkg-query\x00-W\x00-f=${Status}\x00personastack-connector": []byte("install ok installed")}),
		ExecutablePath: "/usr/bin/personastack-connector",
		CurrentEUID:    func() int { return 0 },
	})

	if status.InstallChannel != externalagentprotocol.InstallChannelDeb ||
		status.UpdateCapability != externalagentprotocol.UpdateCapabilityManualRequired ||
		status.UpdateReason != externalagentprotocol.UpdateReasonPackageManagerMissing {
		t.Fatalf("unexpected status: %+v", status)
	}
}

func TestDetectLinuxWSL2RequiresManual(t *testing.T) {
	status := Detect(DetectionOptions{
		GOOS:           "linux",
		ServiceScope:   externalagentprotocol.ServiceScopeLinuxSystemService,
		LookPath:       fakeLookPath("dpkg", "dpkg-query", "apt-get"),
		CommandOutput:  fakeCommandOutput(map[string][]byte{"dpkg-query\x00-W\x00-f=${Status}\x00personastack-connector": []byte("install ok installed")}),
		ExecutablePath: "/usr/bin/personastack-connector",
		CurrentEUID:    func() int { return 0 },
		WSL2:           true,
	})

	if status.InstallChannel != externalagentprotocol.InstallChannelDeb ||
		status.UpdateCapability != externalagentprotocol.UpdateCapabilityManualRequired ||
		status.UpdateReason != externalagentprotocol.UpdateReasonWSL2ManualRequired {
		t.Fatalf("unexpected status: %+v", status)
	}
}

func TestDetectLinuxWSL2FromProcRequiresManual(t *testing.T) {
	status := Detect(DetectionOptions{
		GOOS:           "linux",
		ServiceScope:   externalagentprotocol.ServiceScopeLinuxSystemService,
		LookPath:       fakeLookPath("dpkg", "dpkg-query", "apt-get"),
		CommandOutput:  fakeCommandOutput(map[string][]byte{"dpkg-query\x00-W\x00-f=${Status}\x00personastack-connector": []byte("install ok installed")}),
		ExecutablePath: "/usr/bin/personastack-connector",
		CurrentEUID:    func() int { return 0 },
		ReadFile: func(name string) ([]byte, error) {
			if name == "/proc/sys/kernel/osrelease" {
				return []byte("5.15.90.1-microsoft-standard-WSL2"), nil
			}
			return nil, errors.New("not found")
		},
	})

	if status.InstallChannel != externalagentprotocol.InstallChannelDeb ||
		status.UpdateCapability != externalagentprotocol.UpdateCapabilityManualRequired ||
		status.UpdateReason != externalagentprotocol.UpdateReasonWSL2ManualRequired {
		t.Fatalf("unexpected status: %+v", status)
	}
}

func TestDetectLinuxPackageManagerWithoutPackageDoesNotClaimInstallChannel(t *testing.T) {
	status := Detect(DetectionOptions{
		GOOS:           "linux",
		ServiceScope:   externalagentprotocol.ServiceScopeLinuxSystemService,
		LookPath:       fakeLookPath("dpkg", "dpkg-query", "rpm", "apt-get", "dnf"),
		CommandOutput:  fakeCommandOutput(map[string][]byte{}),
		ExecutablePath: "/usr/local/bin/personastack-connector",
		CurrentEUID:    func() int { return 0 },
	})

	if status.InstallChannel != externalagentprotocol.InstallChannelUnknown ||
		status.ExecutablePathClass != externalagentprotocol.ExecutablePathClassUnknown ||
		status.UpdateCapability != externalagentprotocol.UpdateCapabilityManualRequired ||
		status.UpdateReason != externalagentprotocol.UpdateReasonUnknownInstallChannel {
		t.Fatalf("unexpected status: %+v", status)
	}
}

func TestDetectLinuxMissingPackageManagerReportsManualRequired(t *testing.T) {
	status := Detect(DetectionOptions{
		GOOS:           "linux",
		ServiceScope:   externalagentprotocol.ServiceScopeLinuxSystemService,
		LookPath:       fakeLookPath(),
		ExecutablePath: "/usr/bin/personastack-connector",
	})

	if status.InstallChannel != externalagentprotocol.InstallChannelUnknown ||
		status.UpdateCapability != externalagentprotocol.UpdateCapabilityManualRequired ||
		status.UpdateReason != externalagentprotocol.UpdateReasonPackageManagerMissing {
		t.Fatalf("unexpected status: %+v", status)
	}
}

func TestStableHomebrewOptExecutablePath(t *testing.T) {
	tests := map[string]string{
		"/opt/homebrew/Cellar/personastack-connector/1.2.3/bin/personastack-connector": "/opt/homebrew/opt/personastack-connector/bin/personastack-connector",
		"/opt/homebrew/bin/personastack-connector":                                     "/opt/homebrew/opt/personastack-connector/bin/personastack-connector",
		"/usr/local/opt/personastack-connector/bin/personastack-connector":             "/usr/local/opt/personastack-connector/bin/personastack-connector",
	}

	for input, want := range tests {
		got, ok := StableHomebrewOptExecutablePath(input)
		if !ok || got != want {
			t.Fatalf("StableHomebrewOptExecutablePath(%q) = %q, %v; want %q, true", input, got, ok, want)
		}
	}
}

func TestHomebrewUpdateCommandsAreFixed(t *testing.T) {
	want := [][]string{
		{"brew", "update"},
		{"brew", "upgrade", "personastack/tap/personastack-connector"},
	}
	if got := HomebrewUpdateCommands(); !reflect.DeepEqual(got, want) {
		t.Fatalf("HomebrewUpdateCommands() = %+v, want %+v", got, want)
	}
}

func TestManualUpdateCommandUsesFixedHomebrewCommand(t *testing.T) {
	got := ManualUpdateCommand(Metadata{InstallChannel: externalagentprotocol.InstallChannelHomebrew})
	if got != "brew update && brew upgrade personastack/tap/personastack-connector" {
		t.Fatalf("ManualUpdateCommand() = %q", got)
	}
	if command := ManualUpdateCommand(Metadata{InstallChannel: externalagentprotocol.InstallChannelDeb}); command != "" {
		t.Fatalf("ManualUpdateCommand(deb) = %q, want empty", command)
	}
}

func TestDerivePlanUsesFixedHomebrewCommands(t *testing.T) {
	plan, failure := DerivePlan(PlanOptions{
		GOOS:         "darwin",
		ServiceScope: externalagentprotocol.ServiceScopeUserLaunchAgent,
		UID:          501,
		Metadata: Metadata{
			InstallChannel:   externalagentprotocol.InstallChannelHomebrew,
			UpdateCapability: externalagentprotocol.UpdateCapabilityOneClickAvailable,
		},
		Request: externalagentprotocol.UpdateRequestPayload{
			RequestID:      "update-1",
			TargetVersion:  "v1.2.4",
			InstallChannel: externalagentprotocol.InstallChannelHomebrew,
			PackageKind:    "homebrew",
		},
	})
	if failure != nil {
		t.Fatalf("DerivePlan() failure = %v", failure)
	}
	wantCommands := []Command{
		{Name: "brew", Args: []string{"update"}},
		{Name: "brew", Args: []string{"upgrade", "personastack/tap/personastack-connector"}},
	}
	if !reflect.DeepEqual(plan.Commands, wantCommands) {
		t.Fatalf("commands = %+v, want %+v", plan.Commands, wantCommands)
	}
	wantRestart := Command{Name: "launchctl", Args: []string{"kickstart", "-k", "gui/501/ai.personastack.connector"}}
	if !reflect.DeepEqual(plan.RestartCommand, wantRestart) {
		t.Fatalf("restart = %+v, want %+v", plan.RestartCommand, wantRestart)
	}
}

func TestDerivePlanRejectsManualCapability(t *testing.T) {
	_, failure := DerivePlan(PlanOptions{
		GOOS:         "darwin",
		ServiceScope: externalagentprotocol.ServiceScopeSystemLaunchDaemon,
		Metadata: Metadata{
			InstallChannel:    externalagentprotocol.InstallChannelHomebrew,
			UpdateCapability:  externalagentprotocol.UpdateCapabilityManualRequired,
			UpdateReason:      externalagentprotocol.UpdateReasonSystemLaunchDaemonHomebrew,
			LastUpdateSummary: "manual update required",
		},
		Request: externalagentprotocol.UpdateRequestPayload{
			RequestID:      "update-1",
			TargetVersion:  "v1.2.4",
			InstallChannel: externalagentprotocol.InstallChannelHomebrew,
			PackageKind:    "homebrew",
		},
	})
	if failure == nil || failure.Reason != externalagentprotocol.UpdateReasonSystemLaunchDaemonHomebrew {
		t.Fatalf("failure = %+v, want system launch daemon reason", failure)
	}
}

func TestDerivePlanUsesLinuxDebCommandsFromTypedReleaseMetadata(t *testing.T) {
	plan, failure := DerivePlan(PlanOptions{
		GOOS:           "linux",
		GOARCH:         "amd64",
		CurrentVersion: "v1.2.3",
		ServiceScope:   externalagentprotocol.ServiceScopeLinuxSystemService,
		Metadata: Metadata{
			InstallChannel:   externalagentprotocol.InstallChannelDeb,
			UpdateCapability: externalagentprotocol.UpdateCapabilityOneClickAvailable,
		},
		Request: externalagentprotocol.UpdateRequestPayload{
			RequestID:      "update-1",
			TargetVersion:  "v1.2.4",
			InstallChannel: externalagentprotocol.InstallChannelDeb,
			PackageKind:    "deb",
			AssetURL:       "https://github.com/personastack/personastack-connector/releases/download/v1.2.4/personastack-connector_1.2.4_linux_amd64.deb",
			ChecksumURL:    "https://github.com/personastack/personastack-connector/releases/download/v1.2.4/personastack-connector_1.2.4_checksums.txt",
		},
	})
	if failure != nil {
		t.Fatalf("DerivePlan() failure = %v", failure)
	}
	tempDir := "/var/lib/personastack-connector/update"
	wantCommands := []Command{
		{Name: "install", Args: []string{"-d", "-m", "0700", tempDir}},
		{Name: "curl", Args: []string{"-fsSL", "-o", "personastack-connector_1.2.4_linux_amd64.deb", "https://github.com/personastack/personastack-connector/releases/download/v1.2.4/personastack-connector_1.2.4_linux_amd64.deb"}, Dir: tempDir},
		{Name: "curl", Args: []string{"-fsSL", "-o", "personastack-connector_1.2.4_checksums.txt", "https://github.com/personastack/personastack-connector/releases/download/v1.2.4/personastack-connector_1.2.4_checksums.txt"}, Dir: tempDir},
		{Name: "grep", Args: []string{"-E", `^[[:xdigit:]]{64}[[:space:]][ *]?personastack-connector_1\.2\.4_linux_amd64\.deb$`, "personastack-connector_1.2.4_checksums.txt"}, Dir: tempDir},
		{Name: "sha256sum", Args: []string{"--check", "--ignore-missing", "personastack-connector_1.2.4_checksums.txt"}, Dir: tempDir},
		{Name: "apt-get", Args: []string{"install", "-y", "./personastack-connector_1.2.4_linux_amd64.deb"}, Dir: tempDir},
	}
	if !reflect.DeepEqual(plan.Commands, wantCommands) {
		t.Fatalf("commands = %+v, want %+v", plan.Commands, wantCommands)
	}
	wantRestart := Command{Name: "systemctl", Args: []string{"restart", "personastack-connector.service"}}
	if !reflect.DeepEqual(plan.RestartCommand, wantRestart) {
		t.Fatalf("restart = %+v, want %+v", plan.RestartCommand, wantRestart)
	}
}

func TestDerivePlanUsesLinuxRPMCommandsFromTypedReleaseMetadata(t *testing.T) {
	plan, failure := DerivePlan(PlanOptions{
		GOOS:           "linux",
		GOARCH:         "arm64",
		CurrentVersion: "v1.2.3",
		ServiceScope:   externalagentprotocol.ServiceScopeLinuxSystemService,
		Metadata: Metadata{
			InstallChannel:   externalagentprotocol.InstallChannelRPM,
			UpdateCapability: externalagentprotocol.UpdateCapabilityOneClickAvailable,
		},
		Request: externalagentprotocol.UpdateRequestPayload{
			RequestID:      "update-1",
			TargetVersion:  "v1.2.4",
			InstallChannel: externalagentprotocol.InstallChannelRPM,
			PackageKind:    "rpm",
			AssetURL:       "https://github.com/personastack/personastack-connector/releases/download/v1.2.4/personastack-connector_1.2.4_linux_arm64.rpm",
			ChecksumURL:    "https://github.com/personastack/personastack-connector/releases/download/v1.2.4/personastack-connector_1.2.4_checksums.txt",
		},
	})
	if failure != nil {
		t.Fatalf("DerivePlan() failure = %v", failure)
	}
	if got := plan.Commands[len(plan.Commands)-1]; !reflect.DeepEqual(got, Command{Name: "dnf", Args: []string{"install", "-y", "./personastack-connector_1.2.4_linux_arm64.rpm"}, Dir: "/var/lib/personastack-connector/update"}) {
		t.Fatalf("install command = %+v", got)
	}
}

func TestDerivePlanRejectsStaleReleaseMetadata(t *testing.T) {
	_, failure := DerivePlan(PlanOptions{
		GOOS:           "linux",
		GOARCH:         "amd64",
		CurrentVersion: "v1.2.4",
		ServiceScope:   externalagentprotocol.ServiceScopeLinuxSystemService,
		Metadata: Metadata{
			InstallChannel:   externalagentprotocol.InstallChannelDeb,
			UpdateCapability: externalagentprotocol.UpdateCapabilityOneClickAvailable,
		},
		Request: externalagentprotocol.UpdateRequestPayload{
			RequestID:      "update-1",
			TargetVersion:  "v1.2.4",
			InstallChannel: externalagentprotocol.InstallChannelDeb,
			PackageKind:    "deb",
			AssetURL:       "https://github.com/personastack/personastack-connector/releases/download/v1.2.4/personastack-connector_1.2.4_linux_amd64.deb",
			ChecksumURL:    "https://github.com/personastack/personastack-connector/releases/download/v1.2.4/personastack-connector_1.2.4_checksums.txt",
		},
	})
	if failure == nil || failure.Reason != externalagentprotocol.UpdateReasonReleaseMetadataUnavailable {
		t.Fatalf("failure = %+v, want release metadata failure", failure)
	}
}

func TestDerivePlanRejectsMismatchedChecksumMetadata(t *testing.T) {
	_, failure := DerivePlan(PlanOptions{
		GOOS:           "linux",
		GOARCH:         "amd64",
		CurrentVersion: "v1.2.3",
		ServiceScope:   externalagentprotocol.ServiceScopeLinuxSystemService,
		Metadata: Metadata{
			InstallChannel:   externalagentprotocol.InstallChannelDeb,
			UpdateCapability: externalagentprotocol.UpdateCapabilityOneClickAvailable,
		},
		Request: externalagentprotocol.UpdateRequestPayload{
			RequestID:      "update-1",
			TargetVersion:  "v1.2.4",
			InstallChannel: externalagentprotocol.InstallChannelDeb,
			PackageKind:    "deb",
			AssetURL:       "https://github.com/personastack/personastack-connector/releases/download/v1.2.4/personastack-connector_1.2.4_linux_amd64.deb",
			ChecksumURL:    "https://github.com/personastack/personastack-connector/releases/download/v1.2.4/personastack-connector_1.2.3_checksums.txt",
		},
	})
	if failure == nil || failure.Reason != externalagentprotocol.UpdateReasonReleaseMetadataUnavailable {
		t.Fatalf("failure = %+v, want release metadata failure", failure)
	}
}

func TestRunPlanStopsOnChecksumMismatch(t *testing.T) {
	tempDir := "/var/lib/personastack-connector/update"
	runner := &dirRecordingRunner{
		failures: map[string]error{
			tempDir + "\x00sha256sum\x00--check\x00--ignore-missing\x00personastack-connector_1.2.4_checksums.txt": errors.New("checksum mismatch"),
		},
	}
	err := RunPlan(runner, Plan{Commands: []Command{
		{Name: "curl", Args: []string{"-fsSL", "-o", "personastack-connector_1.2.4_linux_amd64.deb", "https://example.invalid/personastack-connector_1.2.4_linux_amd64.deb"}, Dir: tempDir},
		{Name: "sha256sum", Args: []string{"--check", "--ignore-missing", "personastack-connector_1.2.4_checksums.txt"}, Dir: tempDir},
		{Name: "apt-get", Args: []string{"install", "-y", "./personastack-connector_1.2.4_linux_amd64.deb"}, Dir: tempDir},
	}})
	if err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("RunPlan() error = %v, want checksum mismatch", err)
	}
	if len(runner.commands) != 2 {
		t.Fatalf("commands = %+v, want execution to stop before install", runner.commands)
	}
}

func TestRunPlanStopsWhenChecksumFileOmitsPackage(t *testing.T) {
	tempDir := "/var/lib/personastack-connector/update"
	runner := &dirRecordingRunner{
		failures: map[string]error{
			tempDir + "\x00grep\x00-E\x00^[[:xdigit:]]{64}[[:space:]][ *]?personastack-connector_1\\.2\\.4_linux_amd64\\.deb$\x00personastack-connector_1.2.4_checksums.txt": errors.New("checksum entry missing"),
		},
	}
	err := RunPlan(runner, Plan{Commands: []Command{
		{Name: "grep", Args: []string{"-E", checksumLinePattern("personastack-connector_1.2.4_linux_amd64.deb"), "personastack-connector_1.2.4_checksums.txt"}, Dir: tempDir},
		{Name: "sha256sum", Args: []string{"--check", "--ignore-missing", "personastack-connector_1.2.4_checksums.txt"}, Dir: tempDir},
		{Name: "apt-get", Args: []string{"install", "-y", "./personastack-connector_1.2.4_linux_amd64.deb"}, Dir: tempDir},
	}})
	if err == nil || !strings.Contains(err.Error(), "checksum entry missing") {
		t.Fatalf("RunPlan() error = %v, want missing checksum entry", err)
	}
	if len(runner.commands) != 1 {
		t.Fatalf("commands = %+v, want execution to stop before checksum verification", runner.commands)
	}
}

func fakeLookPath(available ...string) func(string) (string, error) {
	allowed := map[string]bool{}
	for _, name := range available {
		allowed[name] = true
	}
	return func(name string) (string, error) {
		if allowed[name] {
			return "/usr/bin/" + name, nil
		}
		return "", errors.New("not found")
	}
}

type dirRecordingRunner struct {
	commands []string
	failures map[string]error
}

func (runner *dirRecordingRunner) Run(name string, args ...string) error {
	return runner.RunInDir("", name, args...)
}

func (runner *dirRecordingRunner) RunInDir(dir string, name string, args ...string) error {
	parts := append([]string{dir, name}, args...)
	key := strings.Join(parts, "\x00")
	runner.commands = append(runner.commands, key)
	if err := runner.failures[key]; err != nil {
		return err
	}
	return nil
}

func fakeCommandOutput(outputs map[string][]byte) func(string, ...string) ([]byte, error) {
	return func(name string, args ...string) ([]byte, error) {
		keyParts := append([]string{name}, args...)
		key := strings.Join(keyParts, "\x00")
		output, ok := outputs[key]
		if !ok {
			return nil, errors.New("command failed")
		}
		return output, nil
	}
}
