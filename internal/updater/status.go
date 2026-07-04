package updater

import (
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"

	"github.com/personastack/personastack-connector/internal/buildinfo"
	"github.com/personastack/personastack-connector/internal/externalagentprotocol"
)

const (
	packageName    = "personastack-connector"
	homebrewTapRef = "personastack/tap/personastack-connector"
)

type Metadata struct {
	InstallChannel      externalagentprotocol.InstallChannel
	ExecutablePathClass externalagentprotocol.ExecutablePathClass
	UpdateCapability    externalagentprotocol.UpdateCapability
	UpdateState         externalagentprotocol.UpdateState
	UpdateReason        externalagentprotocol.UpdateReason
	LastUpdateRequestID string
	LastUpdateSummary   string
}

type DetectionOptions struct {
	GOOS           string
	ExecutablePath string
	ServiceScope   externalagentprotocol.ServiceScope
	LookPath       func(string) (string, error)
	CommandOutput  func(string, ...string) ([]byte, error)
	CurrentEUID    func() int
	ReadFile       func(string) ([]byte, error)
	WSL2           bool
}

type PlanOptions struct {
	GOOS           string
	GOARCH         string
	CurrentVersion string
	ServiceScope   externalagentprotocol.ServiceScope
	Metadata       Metadata
	Request        externalagentprotocol.UpdateRequestPayload
	UID            int
}

type Command struct {
	Name string
	Args []string
	Dir  string
}

type Plan struct {
	Commands       []Command
	RestartCommand Command
}

type PlanFailure struct {
	Reason  externalagentprotocol.UpdateReason
	Message string
}

func (failure PlanFailure) Error() string {
	return strings.TrimSpace(failure.Message)
}

func DerivePlan(options PlanOptions) (Plan, *PlanFailure) {
	goos := strings.TrimSpace(options.GOOS)
	if goos == "" {
		goos = runtime.GOOS
	}
	request := options.Request
	if strings.TrimSpace(request.RequestID) == "" {
		return Plan{}, &PlanFailure{Reason: externalagentprotocol.UpdateReasonReleaseMetadataUnavailable, Message: "update request id required"}
	}
	if strings.TrimSpace(request.TargetVersion) == "" {
		return Plan{}, &PlanFailure{Reason: externalagentprotocol.UpdateReasonReleaseMetadataUnavailable, Message: "target version required"}
	}
	currentVersion := strings.TrimSpace(options.CurrentVersion)
	if currentVersion == "" {
		currentVersion = buildinfo.VersionString()
	}
	if !targetVersionIsNewer(currentVersion, request.TargetVersion) {
		return Plan{}, &PlanFailure{Reason: externalagentprotocol.UpdateReasonReleaseMetadataUnavailable, Message: "target version is not newer than current version"}
	}
	metadata := options.Metadata
	if metadata.UpdateCapability != externalagentprotocol.UpdateCapabilityOneClickAvailable {
		reason := metadata.UpdateReason
		if reason == "" {
			reason = externalagentprotocol.UpdateReasonUnknownInstallChannel
		}
		message := strings.TrimSpace(metadata.LastUpdateSummary)
		if message == "" {
			message = "one-click update unavailable"
		}
		return Plan{}, &PlanFailure{Reason: reason, Message: message}
	}
	if goos == "darwin" &&
		options.ServiceScope == externalagentprotocol.ServiceScopeUserLaunchAgent &&
		metadata.InstallChannel == externalagentprotocol.InstallChannelHomebrew &&
		(request.InstallChannel == "" || request.InstallChannel == externalagentprotocol.InstallChannelHomebrew) &&
		(strings.TrimSpace(request.PackageKind) == "" || strings.TrimSpace(request.PackageKind) == "homebrew") {
		return Plan{
			Commands: []Command{
				{Name: "brew", Args: []string{"update"}},
				{Name: "brew", Args: []string{"upgrade", homebrewTapRef}},
			},
			RestartCommand: Command{Name: "launchctl", Args: []string{"kickstart", "-k", fmt.Sprintf("gui/%d/ai.personastack.connector", options.UID)}},
		}, nil
	}
	if goos == "linux" && options.ServiceScope == externalagentprotocol.ServiceScopeLinuxSystemService {
		return deriveLinuxPackagePlan(options)
	}
	return Plan{}, &PlanFailure{Reason: externalagentprotocol.UpdateReasonUnknownInstallChannel, Message: "one-click update unsupported for install channel"}
}

func deriveLinuxPackagePlan(options PlanOptions) (Plan, *PlanFailure) {
	request := options.Request
	arch := strings.TrimSpace(options.GOARCH)
	if arch == "" {
		arch = runtime.GOARCH
	}
	packageKind := strings.TrimSpace(request.PackageKind)
	if packageKind == "" {
		packageKind = string(request.InstallChannel)
	}
	var installCommand Command
	var extension string
	switch {
	case options.Metadata.InstallChannel == externalagentprotocol.InstallChannelDeb &&
		request.InstallChannel == externalagentprotocol.InstallChannelDeb &&
		packageKind == "deb":
		installCommand = Command{Name: "apt-get", Args: []string{"install", "-y"}}
		extension = ".deb"
	case options.Metadata.InstallChannel == externalagentprotocol.InstallChannelRPM &&
		request.InstallChannel == externalagentprotocol.InstallChannelRPM &&
		packageKind == "rpm":
		installCommand = Command{Name: "dnf", Args: []string{"install", "-y"}}
		extension = ".rpm"
	default:
		return Plan{}, &PlanFailure{Reason: externalagentprotocol.UpdateReasonUnknownInstallChannel, Message: "one-click update unsupported for install channel"}
	}
	assetName, failure := packageAssetName(request.AssetURL, request.TargetVersion, arch, extension)
	if failure != nil {
		return Plan{}, failure
	}
	checksumName, failure := checksumAssetName(request.ChecksumURL, request.TargetVersion)
	if failure != nil {
		return Plan{}, failure
	}
	tempDir := "/var/lib/personastack-connector/update"
	installCommand.Args = append(installCommand.Args, "./"+assetName)
	installCommand.Dir = tempDir
	return Plan{
		Commands: []Command{
			{Name: "install", Args: []string{"-d", "-m", "0700", tempDir}},
			{Name: "curl", Args: []string{"-fsSL", "-o", assetName, strings.TrimSpace(request.AssetURL)}, Dir: tempDir},
			{Name: "curl", Args: []string{"-fsSL", "-o", checksumName, strings.TrimSpace(request.ChecksumURL)}, Dir: tempDir},
			{Name: "grep", Args: []string{"-E", checksumLinePattern(assetName), checksumName}, Dir: tempDir},
			{Name: "sha256sum", Args: []string{"--check", "--ignore-missing", checksumName}, Dir: tempDir},
			installCommand,
		},
		RestartCommand: Command{Name: "systemctl", Args: []string{"restart", packageName + ".service"}},
	}, nil
}

func checksumLinePattern(assetName string) string {
	return `^[[:xdigit:]]{64}[[:space:]][ *]?` + regexp.QuoteMeta(assetName) + `$`
}

func packageAssetName(rawURL string, targetVersion string, arch string, extension string) (string, *PlanFailure) {
	name, failure := safeURLBase(rawURL, "asset URL required")
	if failure != nil {
		return "", failure
	}
	version := strings.TrimPrefix(strings.TrimSpace(targetVersion), "v")
	want := fmt.Sprintf("%s_%s_linux_%s%s", packageName, version, strings.TrimSpace(arch), extension)
	if name != want {
		return "", &PlanFailure{Reason: externalagentprotocol.UpdateReasonReleaseMetadataUnavailable, Message: "release asset does not match target platform"}
	}
	return name, nil
}

func checksumAssetName(rawURL string, targetVersion string) (string, *PlanFailure) {
	name, failure := safeURLBase(rawURL, "checksum URL required")
	if failure != nil {
		return "", failure
	}
	version := strings.TrimPrefix(strings.TrimSpace(targetVersion), "v")
	want := fmt.Sprintf("%s_%s_checksums.txt", packageName, version)
	if name != want {
		return "", &PlanFailure{Reason: externalagentprotocol.UpdateReasonReleaseMetadataUnavailable, Message: "checksum asset does not match target version"}
	}
	return name, nil
}

func safeURLBase(rawURL string, missingMessage string) (string, *PlanFailure) {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return "", &PlanFailure{Reason: externalagentprotocol.UpdateReasonReleaseMetadataUnavailable, Message: missingMessage}
	}
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Scheme != "https" || strings.TrimSpace(parsed.Host) == "" {
		return "", &PlanFailure{Reason: externalagentprotocol.UpdateReasonReleaseMetadataUnavailable, Message: "release URL must be https"}
	}
	name := path.Base(parsed.EscapedPath())
	if name == "." || name == "/" || strings.Contains(name, "%2F") || strings.Contains(name, "%2f") {
		return "", &PlanFailure{Reason: externalagentprotocol.UpdateReasonReleaseMetadataUnavailable, Message: "release URL filename invalid"}
	}
	return name, nil
}

func targetVersionIsNewer(currentVersion string, targetVersion string) bool {
	target := strings.TrimSpace(targetVersion)
	if strings.EqualFold(target, "latest") {
		return true
	}
	currentParsed, currentOK := parseVersion(currentVersion)
	targetParsed, targetOK := parseVersion(target)
	if !currentOK || !targetOK {
		return true
	}
	return compareVersion(targetParsed, currentParsed) > 0
}

func parseVersion(value string) ([3]int, bool) {
	var parsed [3]int
	value = strings.TrimPrefix(strings.TrimSpace(value), "v")
	if idx := strings.Index(value, "-"); idx >= 0 {
		value = value[:idx]
	}
	parts := strings.Split(value, ".")
	if len(parts) != 3 {
		return parsed, false
	}
	for index, part := range parts {
		if strings.TrimSpace(part) == "" {
			return parsed, false
		}
		for _, ch := range part {
			if ch < '0' || ch > '9' {
				return parsed, false
			}
			parsed[index] = parsed[index]*10 + int(ch-'0')
		}
	}
	return parsed, true
}

func compareVersion(left [3]int, right [3]int) int {
	for index := range left {
		if left[index] > right[index] {
			return 1
		}
		if left[index] < right[index] {
			return -1
		}
	}
	return 0
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

func RunPlan(runner CommandRunner, plan Plan) error {
	if runner == nil {
		runner = ExecRunner{}
	}
	for _, command := range plan.Commands {
		if runner, ok := runner.(interface {
			RunInDir(string, string, ...string) error
		}); ok {
			if err := runner.RunInDir(command.Dir, command.Name, command.Args...); err != nil {
				return err
			}
			continue
		}
		if err := runCommand(runner, command); err != nil {
			return err
		}
	}
	return nil
}

func runCommand(runner CommandRunner, command Command) error {
	if strings.TrimSpace(command.Dir) == "" {
		return runner.Run(command.Name, command.Args...)
	}
	return fmt.Errorf("command runner does not support working directory")
}

func RestartFromPlan(runner CommandRunner, plan Plan) error {
	if runner == nil {
		runner = ExecRunner{}
	}
	if strings.TrimSpace(plan.RestartCommand.Name) == "" {
		return nil
	}
	return runner.Run(plan.RestartCommand.Name, plan.RestartCommand.Args...)
}

func (ExecRunner) RunInDir(dir string, name string, args ...string) error {
	cmd := exec.Command(name, args...)
	if strings.TrimSpace(dir) != "" {
		cmd.Dir = dir
	}
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

func Detect(options DetectionOptions) Metadata {
	goos := strings.TrimSpace(options.GOOS)
	if goos == "" {
		goos = runtime.GOOS
	}
	executablePath := strings.TrimSpace(options.ExecutablePath)
	if executablePath == "" {
		executablePath, _ = os.Executable()
	}
	scope := options.ServiceScope
	if scope == "" {
		scope = externalagentprotocol.ServiceScopeUserLaunchAgent
	}
	status := Metadata{
		InstallChannel:      externalagentprotocol.InstallChannelUnknown,
		ExecutablePathClass: externalagentprotocol.ExecutablePathClassUnknown,
		UpdateCapability:    externalagentprotocol.UpdateCapabilityUnsupported,
		UpdateState:         externalagentprotocol.UpdateStateIdle,
		UpdateReason:        externalagentprotocol.UpdateReasonUnknownInstallChannel,
		LastUpdateSummary:   "install channel unknown",
	}
	switch goos {
	case "darwin":
		return detectDarwin(executablePath, scope, status)
	case "linux":
		return detectLinux(options, status)
	default:
		status.LastUpdateSummary = "host operating system unsupported"
		return status
	}
}

func detectDarwin(executablePath string, scope externalagentprotocol.ServiceScope, status Metadata) Metadata {
	if HomebrewPrefix(executablePath) != "" {
		status.InstallChannel = externalagentprotocol.InstallChannelHomebrew
		status.ExecutablePathClass = externalagentprotocol.ExecutablePathClassHomebrewOpt
		if scope == externalagentprotocol.ServiceScopeSystemLaunchDaemon {
			status.UpdateCapability = externalagentprotocol.UpdateCapabilityManualRequired
			status.UpdateReason = externalagentprotocol.UpdateReasonSystemLaunchDaemonHomebrew
			status.LastUpdateSummary = "Homebrew system LaunchDaemon updates require manual update"
			return status
		}
		status.UpdateCapability = externalagentprotocol.UpdateCapabilityOneClickAvailable
		status.UpdateReason = ""
		status.LastUpdateSummary = ""
		return status
	}
	if strings.TrimSpace(executablePath) != "" {
		status.InstallChannel = externalagentprotocol.InstallChannelArchive
		status.ExecutablePathClass = externalagentprotocol.ExecutablePathClassArchivePath
		status.UpdateCapability = externalagentprotocol.UpdateCapabilityManualRequired
		status.UpdateReason = externalagentprotocol.UpdateReasonUnknownInstallChannel
		status.LastUpdateSummary = "archive installs require manual update"
	}
	return status
}

func detectLinux(options DetectionOptions, status Metadata) Metadata {
	lookPath := options.LookPath
	if lookPath == nil {
		lookPath = exec.LookPath
	}
	commandOutput := options.CommandOutput
	if commandOutput == nil {
		commandOutput = func(name string, args ...string) ([]byte, error) {
			return exec.Command(name, args...).Output()
		}
	}
	packageManagerPresent := false
	if commandAvailable(lookPath, "dpkg-query") || commandAvailable(lookPath, "dpkg") {
		packageManagerPresent = true
	}
	if commandAvailable(lookPath, "dpkg-query") && debPackageInstalled(commandOutput) {
		status.InstallChannel = externalagentprotocol.InstallChannelDeb
		status.ExecutablePathClass = externalagentprotocol.ExecutablePathClassPackageManaged
		return linuxPackageStatus(options, status, "dpkg", commandAvailable(lookPath, "apt-get"))
	}
	if commandAvailable(lookPath, "rpm") {
		packageManagerPresent = true
	}
	if commandAvailable(lookPath, "rpm") && rpmPackageInstalled(commandOutput) {
		status.InstallChannel = externalagentprotocol.InstallChannelRPM
		status.ExecutablePathClass = externalagentprotocol.ExecutablePathClassPackageManaged
		return linuxPackageStatus(options, status, "rpm", commandAvailable(lookPath, "dnf"))
	}
	if packageManagerPresent {
		status.UpdateCapability = externalagentprotocol.UpdateCapabilityManualRequired
		status.UpdateReason = externalagentprotocol.UpdateReasonUnknownInstallChannel
		status.LastUpdateSummary = "package manager install not detected"
		return status
	}
	status.UpdateCapability = externalagentprotocol.UpdateCapabilityManualRequired
	status.UpdateReason = externalagentprotocol.UpdateReasonPackageManagerMissing
	status.LastUpdateSummary = "package manager missing"
	return status
}

func linuxPackageStatus(options DetectionOptions, status Metadata, manager string, installerAvailable bool) Metadata {
	if isWSL2(options) {
		status.UpdateCapability = externalagentprotocol.UpdateCapabilityManualRequired
		status.UpdateReason = externalagentprotocol.UpdateReasonWSL2ManualRequired
		status.LastUpdateSummary = "WSL2 package updates require manual update"
		return status
	}
	if options.ServiceScope == externalagentprotocol.ServiceScopeLinuxSystemService && effectiveUID(options) == 0 {
		if !installerAvailable {
			status.UpdateCapability = externalagentprotocol.UpdateCapabilityManualRequired
			status.UpdateReason = externalagentprotocol.UpdateReasonPackageManagerMissing
			status.LastUpdateSummary = manager + " package installer missing"
			return status
		}
		status.UpdateCapability = externalagentprotocol.UpdateCapabilityOneClickAvailable
		status.UpdateReason = ""
		status.LastUpdateSummary = ""
		return status
	}
	status.UpdateCapability = externalagentprotocol.UpdateCapabilityManualRequired
	status.UpdateReason = externalagentprotocol.UpdateReasonRequiresSudo
	status.LastUpdateSummary = manager + " updates require sudo"
	return status
}

func isWSL2(options DetectionOptions) bool {
	if options.WSL2 {
		return true
	}
	readFile := options.ReadFile
	if readFile == nil {
		readFile = os.ReadFile
	}
	for _, name := range []string{"/proc/sys/kernel/osrelease", "/proc/version"} {
		content, err := readFile(name)
		if err != nil {
			continue
		}
		lower := strings.ToLower(string(content))
		if strings.Contains(lower, "microsoft") || strings.Contains(lower, "wsl2") {
			return true
		}
	}
	return false
}

func effectiveUID(options DetectionOptions) int {
	if options.CurrentEUID != nil {
		return options.CurrentEUID()
	}
	return os.Geteuid()
}

func commandAvailable(lookPath func(string) (string, error), name string) bool {
	path, err := lookPath(name)
	return err == nil && strings.TrimSpace(path) != ""
}

func debPackageInstalled(commandOutput func(string, ...string) ([]byte, error)) bool {
	output, err := commandOutput("dpkg-query", "-W", "-f=${Status}", packageName)
	return err == nil && strings.Contains(strings.ToLower(string(output)), "install ok installed")
}

func rpmPackageInstalled(commandOutput func(string, ...string) ([]byte, error)) bool {
	_, err := commandOutput("rpm", "-q", packageName)
	return err == nil
}

func HomebrewUpdateCommands() [][]string {
	return [][]string{
		{"brew", "update"},
		{"brew", "upgrade", homebrewTapRef},
	}
}

func ManualUpdateCommand(metadata Metadata) string {
	if metadata.InstallChannel != externalagentprotocol.InstallChannelHomebrew {
		return ""
	}
	return "brew update && brew upgrade " + homebrewTapRef
}

func StableHomebrewOptExecutablePath(executablePath string) (string, bool) {
	prefix := HomebrewPrefix(executablePath)
	if prefix == "" {
		return "", false
	}
	return filepath.ToSlash(filepath.Join(prefix, "opt", packageName, "bin", packageName)), true
}

func HomebrewPrefix(executablePath string) string {
	path := filepath.ToSlash(filepath.Clean(strings.TrimSpace(executablePath)))
	if path == "." || path == "" {
		return ""
	}
	if strings.HasSuffix(path, "/opt/"+packageName+"/bin/"+packageName) {
		return strings.TrimSuffix(path, "/opt/"+packageName+"/bin/"+packageName)
	}
	if strings.HasSuffix(path, "/bin/"+packageName) {
		prefix := strings.TrimSuffix(path, "/bin/"+packageName)
		if prefix == "/opt/homebrew" || prefix == "/usr/local" || strings.HasSuffix(prefix, "/homebrew") {
			return prefix
		}
	}
	marker := "/Cellar/" + packageName + "/"
	if idx := strings.Index(path, marker); idx > 0 && strings.HasSuffix(path, "/bin/"+packageName) {
		return path[:idx]
	}
	return ""
}
