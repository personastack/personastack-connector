package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/integrii/flaggy"
	"github.com/personastack/personastack-connector/internal/buildinfo"
	"github.com/personastack/personastack-connector/internal/config"
	"github.com/personastack/personastack-connector/internal/daemon"
	"github.com/personastack/personastack-connector/internal/externalagentprotocol"
	"github.com/personastack/personastack-connector/internal/hermessetup"
	"github.com/personastack/personastack-connector/internal/mcp"
	"github.com/personastack/personastack-connector/internal/pairing"
	"github.com/personastack/personastack-connector/internal/runtime"
	"github.com/personastack/personastack-connector/internal/service"
	"github.com/zalando/go-keyring"
)

const usage = `Usage:
  personastack-connector pair <code> [--runtime auto|hermes|openclaw] [--openclaw-token <token>|--openclaw-password <password>|--openclaw-device-token <token>] [--openclaw-agent-id <id>]
  personastack-connector status [--repair]
  personastack-connector diagnostics
  personastack-connector runtime detect
  personastack-connector runtime repair
  personastack-connector runtime hermes configure [--enable-api] [--configure-mcp]
  personastack-connector runtime openclaw configure [--gateway ws://127.0.0.1:18789] [--configure-mcp]
  personastack-connector mcp install
  personastack-connector mcp repair
  personastack-connector mcp stdio --binding <connection_id>
  personastack-connector service install
  personastack-connector service plan
  personastack-connector run --foreground
  personastack-connector unpair
  personastack-connector version
`

type command struct {
	stdin  io.Reader
	stdout io.Writer
	stderr io.Writer
	store  config.Store
}

var installService = func() (service.InstallResult, error) {
	return (service.Installer{}).Install()
}

var flaggyParseMu sync.Mutex

func main() {
	if strings.TrimSpace(os.Getenv("PERSONASTACK_CONNECTOR_MOCK_KEYRING")) == "1" {
		keyring.MockInit()
	}
	cmd := command{
		stdin:  os.Stdin,
		stdout: os.Stdout,
		stderr: os.Stderr,
		store:  defaultStore(),
	}

	err := cmd.Run(context.Background(), os.Args[1:])
	if err == nil {
		return
	}

	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

func Run(ctx context.Context, args []string, stdin io.Reader, stdout io.Writer, stderr io.Writer) error {
	cmd := command{
		stdin:  stdin,
		stdout: stdout,
		stderr: stderr,
		store:  config.EmptyStore(),
	}
	return cmd.Run(ctx, args)
}

func (cmd command) Run(ctx context.Context, args []string) error {
	if len(args) == 0 {
		cmd.printUsage(cmd.stderr)
		return errors.New("help requested")
	}

	switch args[0] {
	case "-h", "--help", "help":
		cmd.printUsage(cmd.stdout)
		return nil
	case "pair":
		return cmd.runPair(args[1:])
	case "status":
		return cmd.runStatus(ctx, args[1:])
	case "diagnostics":
		return cmd.runDiagnostics(args[1:])
	case "runtime":
		return cmd.runRuntime(args[1:])
	case "mcp":
		return cmd.runMCP(ctx, args[1:])
	case "service":
		return cmd.runService(args[1:])
	case "run":
		return cmd.runDaemon(ctx, args[1:])
	case "unpair":
		return cmd.runUnpair(args[1:])
	case "version":
		return cmd.runVersion(args[1:])
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func (cmd command) printUsage(writer io.Writer) {
	fmt.Fprint(writer, usage)
}

func newFlaggyParser(name string) *flaggy.Parser {
	parser := flaggy.NewParser(name)
	parser.ShowCompletion = false
	parser.ShowHelpWithHFlag = false
	parser.ShowVersionWithVersionFlag = false
	return parser
}

func parseFlaggyArgs(parser *flaggy.Parser, args []string) (err error) {
	flaggyParseMu.Lock()
	defer flaggyParseMu.Unlock()

	panicInsteadOfExit := flaggy.PanicInsteadOfExit
	flaggy.PanicInsteadOfExit = true
	defer func() {
		flaggy.PanicInsteadOfExit = panicInsteadOfExit
	}()
	defer func() {
		recovered := recover()
		if recovered == nil {
			return
		}
		message := fmt.Sprint(recovered)
		if strings.HasPrefix(message, "Panic instead of exit with code: 0") {
			err = nil
			return
		}
		if strings.HasPrefix(message, "Panic instead of exit with code: ") {
			err = errors.New("invalid arguments")
			return
		}
		panic(recovered)
	}()

	return parser.ParseArgs(args)
}

func (cmd command) runPair(args []string) error {
	runtimeValue := "auto"
	configureMCP := true
	gateway := externalagentprotocol.DefaultGatewayBaseURL
	openClawToken := ""
	openClawPassword := ""
	openClawDeviceToken := ""
	openClawAgentID := ""
	pairingCode := ""

	parser := newFlaggyParser("pair")
	parser.String(&runtimeValue, "", "runtime", "runtime adapter")
	parser.Bool(&configureMCP, "", "configure-mcp", "configure native runtime MCP")
	parser.String(&gateway, "", "gateway", "PersonaStack gateway URL")
	parser.String(&openClawToken, "", "openclaw-token", "OpenClaw operator token")
	parser.String(&openClawPassword, "", "openclaw-password", "OpenClaw operator password")
	parser.String(&openClawDeviceToken, "", "openclaw-device-token", "OpenClaw operator device token")
	parser.String(&openClawAgentID, "", "openclaw-agent-id", "OpenClaw agent id")
	parser.AddPositionalValue(&pairingCode, "code", 1, true, "pairing code")

	err := parseFlaggyArgs(parser, args)
	if err != nil {
		return err
	}
	if strings.TrimSpace(pairingCode) == "" {
		return errors.New("pair requires one pairing code")
	}

	kind, err := runtime.ParseAdapterKind(runtimeValue)
	if err != nil {
		return err
	}
	if kind == runtime.AdapterKindAuto {
		detectedKind, err := detectSingleReadyRuntime()
		if err != nil {
			return err
		}
		kind = detectedKind
	}

	writable, ok := cmd.store.(config.WritableStore)
	if !ok {
		return errors.New("connector store is not writable")
	}
	pairOptions, err := cmd.resolveOpenClawPairOptions(kind, openClawPairOptions{
		token:       openClawToken,
		password:    openClawPassword,
		deviceToken: openClawDeviceToken,
		agentID:     openClawAgentID,
	})
	if err != nil {
		return err
	}
	result, err := pairing.Client{GatewayBaseURL: gateway}.Exchange(context.Background(), pairing.Request{
		Code:         pairingCode,
		RuntimeKind:  kind,
		ConfigureMCP: configureMCP,
	})
	if err != nil {
		return err
	}
	binding := result.Binding
	if err := applyOpenClawPairOptions(&binding, pairOptions); err != nil {
		return err
	}
	replaced := replacedBindingCount(cmd.store.ListBindings(), binding.ConnectionID)
	if err := writable.SaveBinding(binding); err != nil {
		return err
	}
	repairResults, err := cmd.repairSetup(configureMCP)
	if err != nil {
		return err
	}
	fmt.Fprintln(cmd.stdout, "Connector paired successfully.")
	fmt.Fprintf(cmd.stdout, "Persona: %s\n", binding.PersonaID)
	fmt.Fprintf(cmd.stdout, "Connection: %s\n", binding.ConnectionID)
	fmt.Fprintf(cmd.stdout, "Runtime: %s\n", binding.RuntimeKind)
	if replaced > 0 {
		fmt.Fprintf(cmd.stdout, "Local link: replaced %d previous binding\n", replaced)
	} else {
		fmt.Fprintln(cmd.stdout, "Local link: active")
	}
	if configureMCP {
		fmt.Fprintln(cmd.stdout, "MCP: configured")
	} else {
		fmt.Fprintln(cmd.stdout, "MCP: skipped")
	}
	fmt.Fprintln(cmd.stdout, "Status: waiting for bridge wake probe")
	fmt.Fprintln(cmd.stdout)
	fmt.Fprintln(cmd.stdout, "Details:")
	for _, repairResult := range repairResults {
		fmt.Fprintln(cmd.stdout, repairResult)
	}
	fmt.Fprintf(cmd.stdout, "paired persona=%s connection=%s runtime=%s configure_mcp=%t setup_state=pending_bridge_wake_probe\n", binding.PersonaID, binding.ConnectionID, binding.RuntimeKind, configureMCP)
	return nil
}

func replacedBindingCount(bindings []config.Binding, connectionID config.ConnectionID) int {
	count := 0
	for _, binding := range bindings {
		if binding.ConnectionID != connectionID {
			count++
		}
	}
	return count
}

type runtimeDetectionReport struct {
	detections []runtime.Detection
	readyKinds []runtime.AdapterKind
}

func collectRuntimeDetectionReport() runtimeDetectionReport {
	report := runtimeDetectionReport{
		detections: make([]runtime.Detection, 0, 2),
		readyKinds: make([]runtime.AdapterKind, 0, 2),
	}
	for _, kind := range []runtime.AdapterKind{runtime.AdapterKindHermes, runtime.AdapterKindOpenClaw} {
		detection := runtime.NewAdapter(kind).Detect()
		report.detections = append(report.detections, detection)
		if detection.State == runtime.AdapterStateReady {
			report.readyKinds = append(report.readyKinds, kind)
		}
	}
	return report
}

func (report runtimeDetectionReport) readyKindStrings() []string {
	values := make([]string, 0, len(report.readyKinds))
	for _, kind := range report.readyKinds {
		values = append(values, kind.String())
	}
	return values
}

func (report runtimeDetectionReport) summaryLine() string {
	switch len(report.readyKinds) {
	case 0:
		return "choice=repair action=runtime_repair ready=none"
	case 1:
		return "choice=auto runtime=" + report.readyKinds[0].String()
	default:
		return "choice=manual action=choose_runtime options=" + strings.Join(report.readyKindStrings(), ",")
	}
}

func (report runtimeDetectionReport) autoDetectError() error {
	switch len(report.readyKinds) {
	case 1:
		return nil
	case 0:
		return errors.New("runtime auto-detect found no ready Hermes or OpenClaw runtime; run personastack-connector runtime repair")
	default:
		return fmt.Errorf("runtime auto-detect found multiple ready runtimes (%s); rerun with --runtime hermes or --runtime openclaw", strings.Join(report.readyKindStrings(), ","))
	}
}

type openClawPairOptions struct {
	token       string
	password    string
	deviceToken string
	agentID     string
}

func (cmd command) resolveOpenClawPairOptions(kind runtime.AdapterKind, options openClawPairOptions) (openClawPairOptions, error) {
	if kind != runtime.AdapterKindOpenClaw {
		return options, nil
	}
	if openClawPairCredentialAvailable(options, config.Binding{}) {
		return options, nil
	}
	fmt.Fprint(cmd.stderr, "OpenClaw operator token: ")
	credential, err := readLine(cmd.stdin)
	if err != nil {
		return options, errors.New("OpenClaw operator credential required; enter a token, rerun with --openclaw-token, --openclaw-password, --openclaw-device-token, or set OPENCLAW_GATEWAY_TOKEN/OPENCLAW_GATEWAY_PASSWORD/OPENCLAW_GATEWAY_DEVICE_TOKEN")
	}
	options.token = strings.TrimSpace(credential)
	if openClawPairCredentialAvailable(options, config.Binding{}) {
		return options, nil
	}
	return options, errors.New("OpenClaw operator credential required; enter a token, rerun with --openclaw-token, --openclaw-password, --openclaw-device-token, or set OPENCLAW_GATEWAY_TOKEN/OPENCLAW_GATEWAY_PASSWORD/OPENCLAW_GATEWAY_DEVICE_TOKEN")
}

func applyOpenClawPairOptions(binding *config.Binding, options openClawPairOptions) error {
	if binding == nil || binding.RuntimeKind != runtime.AdapterKindOpenClaw {
		return nil
	}
	binding.OpenClawGatewayToken = firstNonEmpty(options.token, binding.OpenClawGatewayToken, os.Getenv("OPENCLAW_GATEWAY_TOKEN"))
	binding.OpenClawPassword = firstNonEmpty(options.password, binding.OpenClawPassword, os.Getenv("OPENCLAW_GATEWAY_PASSWORD"))
	binding.OpenClawDeviceToken = firstNonEmpty(options.deviceToken, binding.OpenClawDeviceToken, os.Getenv("OPENCLAW_GATEWAY_DEVICE_TOKEN"))
	binding.OpenClawAgentID = firstNonEmpty(options.agentID, binding.OpenClawAgentID)
	if !openClawPairCredentialAvailable(options, *binding) {
		return errors.New("OpenClaw operator credential required; rerun with --openclaw-token, --openclaw-password, --openclaw-device-token, or set OPENCLAW_GATEWAY_TOKEN/OPENCLAW_GATEWAY_PASSWORD/OPENCLAW_GATEWAY_DEVICE_TOKEN")
	}
	return nil
}

func openClawPairCredentialAvailable(options openClawPairOptions, binding config.Binding) bool {
	return firstNonEmpty(
		options.token,
		options.password,
		options.deviceToken,
		binding.OpenClawGatewayToken,
		binding.OpenClawPassword,
		binding.OpenClawDeviceToken,
		os.Getenv("OPENCLAW_GATEWAY_TOKEN"),
		os.Getenv("OPENCLAW_GATEWAY_PASSWORD"),
		os.Getenv("OPENCLAW_GATEWAY_DEVICE_TOKEN"),
	) != ""
}

func readLine(reader io.Reader) (string, error) {
	if reader == nil {
		return "", io.EOF
	}
	line, err := bufio.NewReader(reader).ReadString('\n')
	if err != nil && strings.TrimSpace(line) == "" {
		return "", err
	}
	return strings.TrimSpace(line), nil
}

func (cmd command) runStatus(ctx context.Context, args []string) error {
	repair := false
	parser := newFlaggyParser("status")
	parser.Bool(&repair, "", "repair", "repair local connector setup")

	err := parseFlaggyArgs(parser, args)
	if err != nil {
		return err
	}

	bindings := cmd.store.ListBindings()
	if len(bindings) == 0 {
		fmt.Fprintln(cmd.stdout, "no bindings")
		return nil
	}
	if repair {
		results, err := cmd.repairSetup(true)
		if err != nil {
			return err
		}
		for _, result := range results {
			fmt.Fprintln(cmd.stdout, result)
		}
	}

	homeDir, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("resolve home dir: %w", err)
	}
	for _, binding := range bindings {
		line, err := cmd.bindingStatusLine(ctx, homeDir, binding, false)
		if err != nil {
			return err
		}
		fmt.Fprintln(cmd.stdout, line)
	}
	return nil
}

func (cmd command) runVersion(args []string) error {
	if len(args) != 0 {
		return errors.New("version accepts no arguments")
	}
	fmt.Fprintf(cmd.stdout, "personastack-connector version=%s commit=%s channel=%s\n", buildinfo.VersionString(), buildinfo.GitCommitString(), buildinfo.ReleaseChannelString())
	return nil
}

func (cmd command) runRuntime(args []string) error {
	if len(args) == 0 {
		return errors.New("runtime requires a subcommand")
	}
	if args[0] == "repair" {
		if len(args) != 1 {
			return errors.New("runtime repair accepts no arguments")
		}
		for _, binding := range cmd.store.ListBindings() {
			detection := adapterForBinding(binding).Diagnose()
			if detection.State != runtime.AdapterStateReady {
				fmt.Fprintf(cmd.stdout, "runtime repair binding=%s runtime=%s state=%s note=%q action=%s\n", binding.ConnectionID, binding.RuntimeKind, detection.State, detection.Note, runtimeRepairAction(detection.State))
				continue
			}
			fmt.Fprintf(cmd.stdout, "runtime repair binding=%s runtime=%s state=%s note=%q\n", binding.ConnectionID, binding.RuntimeKind, detection.State, detection.Note)
		}
		results, err := cmd.repairSetup(true)
		if err != nil {
			return err
		}
		for _, result := range results {
			fmt.Fprintln(cmd.stdout, result)
		}
		return nil
	}
	if args[0] == "hermes" {
		return cmd.runRuntimeHermes(args[1:])
	}
	if args[0] == "openclaw" {
		return cmd.runRuntimeOpenClaw(args[1:])
	}
	if args[0] != "detect" {
		return fmt.Errorf("unknown runtime subcommand %q", args[0])
	}
	if len(args) != 1 {
		return errors.New("runtime detect accepts no arguments")
	}

	report := collectRuntimeDetectionReport()
	for _, detection := range report.detections {
		fmt.Fprintf(cmd.stdout, "%s %s %s\n", detection.Kind, detection.State, detection.Note)
	}
	fmt.Fprintf(cmd.stdout, "runtime detect %s\n", report.summaryLine())
	return nil
}

func (cmd command) runRuntimeHermes(args []string) error {
	if len(args) == 0 || args[0] != "configure" {
		return errors.New("runtime hermes requires configure")
	}
	enableAPI := true
	configureMCP := true
	parser := newFlaggyParser("runtime hermes configure")
	parser.Bool(&enableAPI, "", "enable-api", "enable Hermes API on loopback")
	parser.Bool(&configureMCP, "", "configure-mcp", "configure native runtime MCP")

	err := parseFlaggyArgs(parser, args[1:])
	if err != nil {
		return err
	}

	homeDir, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("resolve home dir: %w", err)
	}
	if enableAPI {
		report, err := hermessetup.EnsureAPISetup(homeDir)
		if err != nil {
			return err
		}
		fmt.Fprintf(cmd.stdout, "runtime hermes configure state=%s note=%q\n", report.State, report.Note)
	}
	if configureMCP {
		results, err := cmd.installMCPForKind(runtime.AdapterKindHermes)
		if err != nil {
			return err
		}
		for _, result := range results {
			fmt.Fprintln(cmd.stdout, result)
		}
	}
	return nil
}

func (cmd command) runRuntimeOpenClaw(args []string) error {
	if len(args) == 0 || args[0] != "configure" {
		return errors.New("runtime openclaw requires configure")
	}
	gateway := "ws://127.0.0.1:18789"
	configureMCP := true
	parser := newFlaggyParser("runtime openclaw configure")
	parser.String(&gateway, "", "gateway", "OpenClaw Gateway URL")
	parser.Bool(&configureMCP, "", "configure-mcp", "configure native runtime MCP")

	err := parseFlaggyArgs(parser, args[1:])
	if err != nil {
		return err
	}

	bindings := filterBindingsByRuntime(cmd.store.ListBindings(), runtime.AdapterKindOpenClaw)
	if len(bindings) == 0 {
		fmt.Fprintf(cmd.stdout, "runtime openclaw configure gateway=%s no_bindings\n", strings.TrimSpace(gateway))
		return nil
	}
	for _, binding := range bindings {
		detection := runtime.NewOpenClawAdapterWithAuth(gateway, runtime.OpenClawAuth{
			Token:       firstNonEmpty(binding.OpenClawGatewayToken, os.Getenv("OPENCLAW_GATEWAY_TOKEN")),
			Password:    firstNonEmpty(binding.OpenClawPassword, os.Getenv("OPENCLAW_GATEWAY_PASSWORD")),
			DeviceToken: firstNonEmpty(binding.OpenClawDeviceToken, os.Getenv("OPENCLAW_GATEWAY_DEVICE_TOKEN")),
		}, binding.OpenClawAgentID).Detect()
		fmt.Fprintf(cmd.stdout, "runtime openclaw configure binding=%s gateway=%s state=%s note=%q\n", binding.ConnectionID, strings.TrimSpace(gateway), detection.State, detection.Note)
	}
	if configureMCP {
		results, err := cmd.installMCPForKind(runtime.AdapterKindOpenClaw)
		if err != nil {
			return err
		}
		for _, result := range results {
			fmt.Fprintln(cmd.stdout, result)
		}
	}
	return nil
}

func detectSingleReadyRuntime() (runtime.AdapterKind, error) {
	report := collectRuntimeDetectionReport()
	switch len(report.readyKinds) {
	case 1:
		return report.readyKinds[0], nil
	case 0:
		return runtime.AdapterKindAuto, report.autoDetectError()
	default:
		return runtime.AdapterKindAuto, report.autoDetectError()
	}
}

func adapterForBinding(binding config.Binding) runtime.Adapter {
	if binding.RuntimeKind != runtime.AdapterKindOpenClaw {
		return runtime.NewAdapter(binding.RuntimeKind)
	}
	return runtime.NewOpenClawAdapterWithAuth(os.Getenv("PERSONASTACK_CONNECTOR_OPENCLAW_GATEWAY_URL"), runtime.OpenClawAuth{
		Token:       firstNonEmpty(binding.OpenClawGatewayToken, os.Getenv("OPENCLAW_GATEWAY_TOKEN")),
		Password:    firstNonEmpty(binding.OpenClawPassword, os.Getenv("OPENCLAW_GATEWAY_PASSWORD")),
		DeviceToken: firstNonEmpty(binding.OpenClawDeviceToken, os.Getenv("OPENCLAW_GATEWAY_DEVICE_TOKEN")),
	}, binding.OpenClawAgentID)
}

func filterBindingsByRuntime(bindings []config.Binding, kind runtime.AdapterKind) []config.Binding {
	filtered := make([]config.Binding, 0, len(bindings))
	for _, binding := range bindings {
		if binding.RuntimeKind == kind {
			filtered = append(filtered, binding)
		}
	}
	return filtered
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func emptyOrDash(value string) string {
	if trimmed := strings.TrimSpace(value); trimmed != "" {
		return trimmed
	}
	return "-"
}

func (cmd command) runMCP(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("mcp requires a subcommand")
	}

	switch args[0] {
	case "install":
		return cmd.runMCPInstall(args[1:])
	case "repair":
		return cmd.runMCPRepair(args[1:])
	case "stdio":
		return cmd.runMCPStdio(ctx, args[1:])
	default:
		return fmt.Errorf("unknown mcp subcommand %q", args[0])
	}
}

func (cmd command) runMCPInstall(args []string) error {
	if len(args) != 0 {
		return errors.New("mcp install accepts no arguments")
	}
	results, err := cmd.installMCP()
	if err != nil {
		return err
	}
	for _, result := range results {
		fmt.Fprintln(cmd.stdout, result)
	}
	return nil
}

func (cmd command) runMCPRepair(args []string) error {
	if len(args) != 0 {
		return errors.New("mcp repair accepts no arguments")
	}
	results, err := cmd.installMCP()
	if err != nil {
		return err
	}
	for _, result := range results {
		fmt.Fprintln(cmd.stdout, result)
	}
	return nil
}

func (cmd command) installMCPForKind(kind runtime.AdapterKind) ([]string, error) {
	bindings := filterBindingsByRuntime(cmd.store.ListBindings(), kind)
	if len(bindings) == 0 {
		return nil, nil
	}
	var results []mcp.InstallResult
	installer := mcp.Installer{Store: cmd.store}
	for _, binding := range bindings {
		result, err := installer.InstallBinding(binding)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	lines := make([]string, 0, len(results))
	for _, result := range results {
		line := fmt.Sprintf("installed mcp binding=%s runtime=%s server=%s path=%s", result.ConnectionID, result.Runtime, result.ServerName, result.Path)
		if strings.TrimSpace(result.Note) != "" {
			line = line + " note=" + strconv.Quote(result.Note)
		}
		lines = append(lines, line)
	}
	return lines, nil
}

func (cmd command) runMCPStdio(ctx context.Context, args []string) error {
	bindingID := ""
	parser := newFlaggyParser("mcp stdio")
	parser.String(&bindingID, "", "binding", "Connector binding id")

	err := parseFlaggyArgs(parser, args)
	if err != nil {
		return err
	}
	if bindingID == "" {
		return errors.New("mcp stdio requires --binding")
	}

	proxy := mcp.NewStdioProxy(cmd.store)
	return proxy.Serve(ctx, config.ConnectionID(bindingID), cmd.stdin, cmd.stdout, cmd.stderr)
}

func (cmd command) runService(args []string) error {
	if len(args) == 0 {
		return errors.New("service requires a subcommand")
	}
	if args[0] != "install" && args[0] != "plan" {
		return fmt.Errorf("unknown service subcommand %q", args[0])
	}
	if len(args) != 1 {
		return fmt.Errorf("service %s accepts no arguments", args[0])
	}
	if args[0] == "plan" {
		result, err := (service.Installer{}).Plan()
		if err != nil {
			return err
		}
		fmt.Fprintf(cmd.stdout, "service plan kind=%s path=%s\n", result.Kind, result.Path)
		return nil
	}
	result, err := (service.Installer{}).Install()
	if err != nil {
		return err
	}
	fmt.Fprintf(cmd.stdout, "service installed kind=%s path=%s\n", result.Kind, result.Path)
	return nil
}

func (cmd command) repairSetup(configureMCP bool) ([]string, error) {
	var results []string
	if configureMCP {
		mcpResults, err := cmd.installMCP()
		if err != nil {
			return nil, err
		}
		results = append(results, mcpResults...)
	}
	serviceResult, err := installService()
	if err != nil {
		return nil, err
	}
	results = append(results, fmt.Sprintf("service installed kind=%s path=%s", serviceResult.Kind, serviceResult.Path))
	return results, nil
}

func (cmd command) installMCP() ([]string, error) {
	results, err := (mcp.Installer{Store: cmd.store}).InstallAll()
	if err != nil {
		return nil, err
	}
	lines := make([]string, 0, len(results))
	for _, result := range results {
		line := fmt.Sprintf("installed mcp binding=%s runtime=%s server=%s path=%s", result.ConnectionID, result.Runtime, result.ServerName, result.Path)
		if strings.TrimSpace(result.Note) != "" {
			line = line + " note=" + strconv.Quote(result.Note)
		}
		lines = append(lines, line)
	}
	return lines, nil
}

func (cmd command) runDaemon(ctx context.Context, args []string) error {
	foreground := false
	parser := newFlaggyParser("run")
	parser.Bool(&foreground, "", "foreground", "run in foreground")

	err := parseFlaggyArgs(parser, args)
	if err != nil {
		return err
	}
	if !foreground {
		return errors.New("run requires --foreground")
	}

	if err := (daemon.Runner{Store: cmd.store}).RunForeground(ctx); err != nil {
		return err
	}
	fmt.Fprintln(cmd.stdout, "connector daemon stopped")
	return nil
}

func (cmd command) runUnpair(args []string) error {
	if len(args) != 0 {
		return errors.New("unpair accepts no arguments")
	}
	deleting, ok := cmd.store.(config.DeletingStore)
	if !ok {
		return errors.New("connector store does not support unpair")
	}
	bindings := cmd.store.ListBindings()
	if len(bindings) == 0 {
		fmt.Fprintln(cmd.stdout, "no bindings")
		return nil
	}
	for _, binding := range bindings {
		if err := deleting.DeleteBinding(binding.ConnectionID); err != nil {
			return err
		}
		fmt.Fprintf(cmd.stdout, "unpaired connection=%s persona=%s\n", binding.ConnectionID, binding.PersonaID)
	}
	return nil
}

func defaultStore() config.Store {
	store, err := config.DefaultFileStore()
	if err != nil {
		return config.EmptyStore()
	}
	return store
}
