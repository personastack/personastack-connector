package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	stdruntime "runtime"
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
	"github.com/personastack/personastack-connector/internal/openclawauth"
	"github.com/personastack/personastack-connector/internal/pairing"
	"github.com/personastack/personastack-connector/internal/runtime"
	"github.com/personastack/personastack-connector/internal/service"
	"github.com/zalando/go-keyring"
	"golang.org/x/sys/unix"
)

const usage = `Usage:
  personastack-connector pair <code> [--runtime auto|hermes|openclaw] [--service-scope user|system] [--hermes-home <path>] [--openclaw-token <token>|--openclaw-password <password>|--openclaw-device-token <token>] [--openclaw-agent-id <id>]
  personastack-connector status [--repair] [--service-scope user|system]
  personastack-connector diagnostics
  personastack-connector runtime detect
  personastack-connector runtime repair [--service-scope user|system]
  personastack-connector runtime hermes configure [--enable-api] [--configure-mcp] [--hermes-home <path>]
  personastack-connector runtime openclaw configure [--gateway ws://127.0.0.1:18789] [--configure-mcp]
  personastack-connector mcp install [--service-scope user|system]
  personastack-connector mcp repair [--service-scope user|system]
  personastack-connector mcp stdio --binding <connection_id> [--service-scope user|system]
  personastack-connector service install [--service-scope user|system]
  personastack-connector service plan [--service-scope user|system]
  personastack-connector service uninstall [--service-scope user|system]
  personastack-connector run --foreground [--service-scope user|system]
  personastack-connector unpair [--service-scope user|system]
  personastack-connector version
`

type command struct {
	stdin  io.Reader
	stdout io.Writer
	stderr io.Writer
	store  config.Store
}

var installService = func(scope service.ServiceScope) (service.InstallResult, error) {
	return (service.Installer{ServiceScope: scope, HermesHome: os.Getenv("HERMES_HOME")}).Install()
}

var newServiceInstaller = func(scope service.ServiceScope) service.Installer {
	return service.Installer{
		ServiceScope: scope,
		HermesHome:   os.Getenv("HERMES_HOME"),
		GOOS:         currentGOOS,
		SystemRoot:   os.Getenv("PERSONASTACK_CONNECTOR_SYSTEM_ROOT"),
	}
}

var currentGOOS = stdruntime.GOOS

func parseCommandServiceScope(value string) (service.ServiceScope, error) {
	if strings.TrimSpace(value) == "system" {
		if currentGOOS == "linux" {
			return service.ServiceScopeLinuxSystemService, nil
		}
		return service.ServiceScopeSystemLaunchDaemon, nil
	}
	return service.ParseServiceScope(value)
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
	hermesHome := ""
	serviceScopeValue := "user"
	pairingCode := ""

	parser := newFlaggyParser("pair")
	parser.String(&runtimeValue, "", "runtime", "runtime adapter")
	parser.Bool(&configureMCP, "", "configure-mcp", "configure native runtime MCP")
	parser.String(&gateway, "", "gateway", "PersonaStack gateway URL")
	parser.String(&openClawToken, "", "openclaw-token", "OpenClaw operator token")
	parser.String(&openClawPassword, "", "openclaw-password", "OpenClaw operator password")
	parser.String(&openClawDeviceToken, "", "openclaw-device-token", "OpenClaw operator device token")
	parser.String(&openClawAgentID, "", "openclaw-agent-id", "OpenClaw agent id")
	parser.String(&hermesHome, "", "hermes-home", "Hermes profile home")
	parser.String(&serviceScopeValue, "", "service-scope", "service scope user or system")
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
	normalizedHermesHome, err := normalizeHermesHome(hermesHome)
	if err != nil {
		return err
	}
	if kind == runtime.AdapterKindAuto {
		detectedKind, err := detectSingleReadyRuntimeForHermesHome(normalizedHermesHome)
		if err != nil {
			return err
		}
		kind = detectedKind
	}
	serviceScope, err := parseCommandServiceScope(serviceScopeValue)
	if err != nil {
		return err
	}
	if serviceScope == service.ServiceScopeSystemLaunchDaemon {
		if err := validateSystemServiceScopePlatform(); err != nil {
			return err
		}
		if err := validateSystemServiceScopeAccess("pairing"); err != nil {
			return err
		}
	}
	if serviceScope == service.ServiceScopeLinuxSystemService {
		if err := validateLinuxSystemServiceScopePlatform(); err != nil {
			return err
		}
		if err := validateLinuxSystemServiceScopeAccess("pairing"); err != nil {
			return err
		}
	}

	cmd.store = cmd.storeForServiceScope(serviceScope)
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
	if err := applyHermesPairOptions(&binding, hermesHome); err != nil {
		return err
	}
	if err := applyOpenClawPairOptions(&binding, pairOptions); err != nil {
		return err
	}
	replaced := replacedBindingCount(cmd.store.ListBindings(), binding.ConnectionID)
	if serviceScope == service.ServiceScopeLinuxSystemService {
		err = withLinuxSystemServiceConfigEnv(func() error {
			if err := writable.SaveBinding(binding); err != nil {
				return err
			}
			return chownLinuxSystemScopePaths(binding.HermesHome)
		})
		if err != nil {
			return err
		}
	} else {
		if err := writable.SaveBinding(binding); err != nil {
			return err
		}
	}
	repairResults, err := cmd.repairSetup(configureMCP, serviceScope)
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
	fmt.Fprintf(cmd.stdout, "paired persona=%s connection=%s runtime=%s configure_mcp=%t service_scope=%s setup_state=pending_bridge_wake_probe\n", binding.PersonaID, binding.ConnectionID, binding.RuntimeKind, configureMCP, serviceScope)
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
	return collectRuntimeDetectionReportForHermesHome("")
}

func collectRuntimeDetectionReportForHermesHome(hermesHome string) runtimeDetectionReport {
	report := runtimeDetectionReport{
		detections: make([]runtime.Detection, 0, 2),
		readyKinds: make([]runtime.AdapterKind, 0, 2),
	}
	for _, kind := range []runtime.AdapterKind{runtime.AdapterKindHermes, runtime.AdapterKindOpenClaw} {
		adapter := adapterForRuntimeKind(kind)
		if kind == runtime.AdapterKindHermes && strings.TrimSpace(hermesHome) != "" {
			adapter = runtime.NewHermesAdapterForHome(os.Getenv("PERSONASTACK_CONNECTOR_HERMES_URL"), hermesHome)
		}
		detection := adapter.Detect()
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
	resolved, err := openclawauth.Resolve(openclawauth.Options{
		Binding: firstOpenClawBinding(cmd.store.ListBindings()),
		Explicit: openclawauth.Explicit{
			Token:       options.token,
			Password:    options.password,
			DeviceToken: options.deviceToken,
		},
	})
	if err != nil {
		return options, err
	}
	if resolved.Found() {
		if err := validateResolvedOpenClawAuth(resolved.Auth, options.agentID); err != nil {
			return options, err
		}
		options.token = resolved.Auth.Token
		options.password = resolved.Auth.Password
		options.deviceToken = resolved.Auth.DeviceToken
		return options, nil
	}
	fmt.Fprint(cmd.stderr, "OpenClaw operator token: ")
	fmt.Fprint(cmd.stderr, "Run `openclaw config get gateway.auth.token` and paste the token. If that is empty, run `openclaw devices list`, then `openclaw devices rotate --device <id> --role operator --scope operator.read --scope operator.write --json` and paste the returned token.\n")
	fmt.Fprint(cmd.stderr, "Token: ")
	credential, err := readLine(cmd.stdin)
	if err != nil {
		return options, errors.New(openClawCredentialRequiredMessage())
	}
	options.token = strings.TrimSpace(credential)
	if openClawPairCredentialAvailable(options, config.Binding{}) {
		return options, nil
	}
	return options, errors.New(openClawCredentialRequiredMessage())
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
		return errors.New(openClawCredentialRequiredMessage())
	}
	return nil
}

func applyHermesPairOptions(binding *config.Binding, explicitHermesHome string) error {
	if binding == nil || binding.RuntimeKind != runtime.AdapterKindHermes {
		return nil
	}
	selected := firstNonEmpty(explicitHermesHome, binding.HermesHome, os.Getenv("HERMES_HOME"))
	cleaned, err := normalizeHermesHome(selected)
	if err != nil {
		return err
	}
	if cleaned == "" {
		return nil
	}
	binding.HermesHome = cleaned
	return nil
}

func normalizeHermesHome(value string) (string, error) {
	selected := strings.TrimSpace(value)
	if selected == "" {
		return "", nil
	}
	cleaned := filepath.Clean(selected)
	if !filepath.IsAbs(cleaned) {
		return "", fmt.Errorf("Hermes home must be an absolute path: %s", selected)
	}
	return cleaned, nil
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

func firstOpenClawBinding(bindings []config.Binding) config.Binding {
	for _, binding := range bindings {
		if binding.RuntimeKind == runtime.AdapterKindOpenClaw {
			return binding
		}
	}
	return config.Binding{}
}

func firstHermesHome(bindings []config.Binding) string {
	for _, binding := range bindings {
		if binding.RuntimeKind != runtime.AdapterKindHermes {
			continue
		}
		if strings.TrimSpace(binding.HermesHome) != "" {
			return strings.TrimSpace(binding.HermesHome)
		}
	}
	return ""
}

func (cmd command) persistHermesHome(hermesHome string) error {
	trimmed := strings.TrimSpace(hermesHome)
	if trimmed == "" {
		return nil
	}
	writable, ok := cmd.store.(config.WritableStore)
	if !ok {
		return nil
	}
	for _, binding := range cmd.store.ListBindings() {
		if binding.RuntimeKind != runtime.AdapterKindHermes {
			continue
		}
		binding.HermesHome = trimmed
		if err := writable.SaveBinding(binding); err != nil {
			return err
		}
	}
	return nil
}

func openClawCredentialRequiredMessage() string {
	return "OpenClaw operator credential required; run `openclaw config get gateway.auth.token`, rerun with --openclaw-token, --openclaw-password, --openclaw-device-token, or set OPENCLAW_GATEWAY_TOKEN/OPENCLAW_GATEWAY_PASSWORD/OPENCLAW_GATEWAY_DEVICE_TOKEN"
}

func validateResolvedOpenClawAuth(auth runtime.OpenClawAuth, agentID string) error {
	gatewayURL := os.Getenv("PERSONASTACK_CONNECTOR_OPENCLAW_GATEWAY_URL")
	if !openclawauth.GatewayIsLoopback(gatewayURL) || !openclawauth.GatewayReachable(gatewayURL) {
		return nil
	}
	detection := runtime.NewOpenClawAdapterWithAuth(gatewayURL, auth, agentID).Detect()
	if detection.State == runtime.AdapterStateAuthMissing {
		return fmt.Errorf("OpenClaw operator credential rejected by local Gateway: %s", detection.Note)
	}
	return nil
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
	scopeValue := "user"
	parser := newFlaggyParser("status")
	parser.Bool(&repair, "", "repair", "repair local connector setup")
	parser.String(&scopeValue, "", "service-scope", "service scope user or system")

	err := parseFlaggyArgs(parser, args)
	if err != nil {
		return err
	}
	scope, err := parseCommandServiceScope(scopeValue)
	if err != nil {
		return err
	}
	cmd.store = cmd.storeForServiceScope(scope)

	bindings := cmd.store.ListBindings()
	if len(bindings) == 0 {
		fmt.Fprintln(cmd.stdout, "no bindings")
		return nil
	}
	if repair {
		if err := validateServiceScopeForBindings(scope, bindings, true); err != nil {
			return err
		}
		results, err := cmd.repairSetup(true, scope)
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
		scopeValue := "user"
		parser := newFlaggyParser("runtime repair")
		parser.String(&scopeValue, "", "service-scope", "service scope user or system")
		err := parseFlaggyArgs(parser, args[1:])
		if err != nil {
			return err
		}
		scope, err := parseCommandServiceScope(scopeValue)
		if err != nil {
			return err
		}
		cmd.store = cmd.storeForServiceScope(scope)
		bindings := cmd.store.ListBindings()
		if err := validateServiceScopeForBindings(scope, bindings, true); err != nil {
			return err
		}
		for _, binding := range bindings {
			detection := adapterForBinding(binding).Diagnose()
			if detection.State != runtime.AdapterStateReady {
				fmt.Fprintf(cmd.stdout, "runtime repair binding=%s runtime=%s state=%s note=%q action=%s\n", binding.ConnectionID, binding.RuntimeKind, detection.State, detection.Note, runtimeRepairAction(detection.State))
				continue
			}
			fmt.Fprintf(cmd.stdout, "runtime repair binding=%s runtime=%s state=%s note=%q\n", binding.ConnectionID, binding.RuntimeKind, detection.State, detection.Note)
		}
		results, err := cmd.repairSetup(true, scope)
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
	hermesHome := ""
	parser := newFlaggyParser("runtime hermes configure")
	parser.Bool(&enableAPI, "", "enable-api", "enable Hermes API on loopback")
	parser.Bool(&configureMCP, "", "configure-mcp", "configure native runtime MCP")
	parser.String(&hermesHome, "", "hermes-home", "Hermes profile home")

	err := parseFlaggyArgs(parser, args[1:])
	if err != nil {
		return err
	}

	homeDir, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("resolve home dir: %w", err)
	}
	paths := hermessetup.ResolvePaths(homeDir, hermesHome)
	if strings.TrimSpace(hermesHome) != "" && !filepath.IsAbs(paths.HermesHome) {
		return fmt.Errorf("Hermes home must be an absolute path: %s", hermesHome)
	}
	if enableAPI {
		report, err := hermessetup.EnsureAPISetupForPaths(paths)
		if err != nil {
			return err
		}
		fmt.Fprintf(cmd.stdout, "runtime hermes configure state=%s note=%q\n", report.State, report.Note)
	}
	if strings.TrimSpace(hermesHome) != "" {
		if err := cmd.persistHermesHome(paths.HermesHome); err != nil {
			return err
		}
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
		resolved := openclawauth.Result{}
		if openclawauth.GatewayIsLoopback(gateway) {
			var err error
			resolved, err = openclawauth.Resolve(openclawauth.Options{Binding: binding})
			if err != nil {
				return err
			}
		}
		detection := runtime.NewOpenClawAdapterWithAuth(gateway, resolved.Auth, binding.OpenClawAgentID).Detect()
		if resolved.Found() {
			if writable, ok := cmd.store.(config.WritableStore); ok && openClawCredentialValidatedByDetection(detection) {
				binding = openclawauth.ApplyToBinding(binding, resolved)
				if err := writable.SaveBinding(binding); err != nil {
					return err
				}
			}
		}
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
	return detectSingleReadyRuntimeForHermesHome("")
}

func detectSingleReadyRuntimeForHermesHome(hermesHome string) (runtime.AdapterKind, error) {
	report := collectRuntimeDetectionReportForHermesHome(hermesHome)
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
	if binding.RuntimeKind == runtime.AdapterKindHermes && strings.TrimSpace(binding.HermesHome) != "" {
		return runtime.NewHermesAdapterForHome(os.Getenv("PERSONASTACK_CONNECTOR_HERMES_URL"), binding.HermesHome)
	}
	if binding.RuntimeKind != runtime.AdapterKindOpenClaw {
		return runtime.NewAdapter(binding.RuntimeKind)
	}
	if !openclawauth.GatewayIsLoopback(os.Getenv("PERSONASTACK_CONNECTOR_OPENCLAW_GATEWAY_URL")) {
		return runtime.NewOpenClawAdapterWithAuth(os.Getenv("PERSONASTACK_CONNECTOR_OPENCLAW_GATEWAY_URL"), runtime.OpenClawAuth{}, binding.OpenClawAgentID)
	}
	resolved, err := openclawauth.Resolve(openclawauth.Options{Binding: binding})
	if err != nil {
		return runtime.NewErrorAdapter(runtime.AdapterKindOpenClaw, runtime.AdapterStateAuthMissing, err.Error())
	}
	return runtime.NewOpenClawAdapterWithAuth(os.Getenv("PERSONASTACK_CONNECTOR_OPENCLAW_GATEWAY_URL"), resolved.Auth, binding.OpenClawAgentID)
}

func adapterForRuntimeKind(kind runtime.AdapterKind) runtime.Adapter {
	if kind != runtime.AdapterKindOpenClaw {
		return runtime.NewAdapter(kind)
	}
	if !openclawauth.GatewayIsLoopback(os.Getenv("PERSONASTACK_CONNECTOR_OPENCLAW_GATEWAY_URL")) {
		return runtime.NewOpenClawAdapterWithAuth(os.Getenv("PERSONASTACK_CONNECTOR_OPENCLAW_GATEWAY_URL"), runtime.OpenClawAuth{}, os.Getenv("OPENCLAW_AGENT_ID"))
	}
	resolved, err := openclawauth.Resolve(openclawauth.Options{})
	if err != nil {
		return runtime.NewErrorAdapter(runtime.AdapterKindOpenClaw, runtime.AdapterStateAuthMissing, err.Error())
	}
	return runtime.NewOpenClawAdapterWithAuth(os.Getenv("PERSONASTACK_CONNECTOR_OPENCLAW_GATEWAY_URL"), resolved.Auth, os.Getenv("OPENCLAW_AGENT_ID"))
}

func openClawCredentialValidatedByDetection(detection runtime.Detection) bool {
	switch detection.State {
	case runtime.AdapterStateReady, runtime.AdapterStateCapabilityMissing, runtime.AdapterStateRuntimeStopped:
		return true
	default:
		return false
	}
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
	scopeValue := "user"
	parser := newFlaggyParser("mcp install")
	parser.String(&scopeValue, "", "service-scope", "service scope user or system")
	err := parseFlaggyArgs(parser, args)
	if err != nil {
		return err
	}
	scope, err := parseCommandServiceScope(scopeValue)
	if err != nil {
		return err
	}
	cmd.store = cmd.storeForServiceScope(scope)
	if err := validateServiceScopeForBindings(scope, cmd.store.ListBindings(), true); err != nil {
		return err
	}
	results, err := cmd.installMCPForServiceScope(scope)
	if err != nil {
		return err
	}
	for _, result := range results {
		fmt.Fprintln(cmd.stdout, result)
	}
	return nil
}

func (cmd command) runMCPRepair(args []string) error {
	scopeValue := "user"
	parser := newFlaggyParser("mcp repair")
	parser.String(&scopeValue, "", "service-scope", "service scope user or system")
	err := parseFlaggyArgs(parser, args)
	if err != nil {
		return err
	}
	scope, err := parseCommandServiceScope(scopeValue)
	if err != nil {
		return err
	}
	cmd.store = cmd.storeForServiceScope(scope)
	if err := validateServiceScopeForBindings(scope, cmd.store.ListBindings(), true); err != nil {
		return err
	}
	results, err := cmd.installMCPForServiceScope(scope)
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

func (cmd command) installMCPForServiceScope(scope service.ServiceScope) ([]string, error) {
	if scope != service.ServiceScopeLinuxSystemService {
		return cmd.installMCP()
	}
	if err := validateLinuxSystemServiceScopePlatform(); err != nil {
		return nil, err
	}
	if err := validateLinuxSystemServiceScopeAccess("mcp install"); err != nil {
		return nil, err
	}
	var results []string
	err := withLinuxSystemServiceConfigEnv(func() error {
		var installErr error
		results, installErr = cmd.installMCP()
		if chownErr := chownLinuxSystemScopePaths(firstHermesHome(cmd.store.ListBindings())); chownErr != nil {
			return chownErr
		}
		return installErr
	})
	if err != nil {
		return nil, err
	}
	return results, nil
}

func (cmd command) runMCPStdio(ctx context.Context, args []string) error {
	bindingID := ""
	scopeValue := "user"
	parser := newFlaggyParser("mcp stdio")
	parser.String(&bindingID, "", "binding", "Connector binding id")
	parser.String(&scopeValue, "", "service-scope", "service scope user or system")

	err := parseFlaggyArgs(parser, args)
	if err != nil {
		return err
	}
	scope, err := parseCommandServiceScope(scopeValue)
	if err != nil {
		return err
	}
	cmd.store = cmd.storeForServiceScope(scope)
	if err := validateServiceScopeForBindings(scope, cmd.store.ListBindings(), true); err != nil {
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
	if args[0] != "install" && args[0] != "plan" && args[0] != "uninstall" {
		return fmt.Errorf("unknown service subcommand %q", args[0])
	}
	scopeValue := "user"
	parser := newFlaggyParser("service " + args[0])
	parser.String(&scopeValue, "", "service-scope", "service scope user or system")
	err := parseFlaggyArgs(parser, args[1:])
	if err != nil {
		return err
	}
	scope, err := parseCommandServiceScope(scopeValue)
	if err != nil {
		return err
	}
	cmd.store = cmd.storeForServiceScope(scope)
	installer := newServiceInstaller(scope)
	if hermesHome := firstHermesHome(cmd.store.ListBindings()); hermesHome != "" {
		installer.HermesHome = hermesHome
	}
	if args[0] == "plan" {
		result, err := installer.Plan()
		if err != nil {
			return err
		}
		fmt.Fprintf(cmd.stdout, "service plan kind=%s scope=%s path=%s\n", result.Kind, result.Scope, result.Path)
		return nil
	}
	if args[0] == "uninstall" {
		if scope == service.ServiceScopeSystemLaunchDaemon {
			if err := validateSystemServiceScopePlatform(); err != nil {
				return err
			}
			if err := validateSystemServiceScopeAccess("uninstall"); err != nil {
				return err
			}
		}
		if scope == service.ServiceScopeLinuxSystemService {
			if err := validateLinuxSystemServiceScopePlatform(); err != nil {
				return err
			}
			if err := validateLinuxSystemServiceScopeAccess("uninstall"); err != nil {
				return err
			}
		}
		results, err := installer.Uninstall()
		if err != nil {
			return err
		}
		for _, result := range results {
			fmt.Fprintf(cmd.stdout, "service uninstalled kind=%s scope=%s removed=%t path=%s\n", result.Kind, result.Scope, result.Removed, result.Path)
		}
		return nil
	}
	if scope == service.ServiceScopeLinuxSystemService {
		if err := validateLinuxSystemServiceScopePlatform(); err != nil {
			return err
		}
		if err := validateLinuxSystemServiceScopeAccess("install"); err != nil {
			return err
		}
	}
	result, err := installer.Install()
	if err != nil {
		return err
	}
	fmt.Fprintf(cmd.stdout, "service installed kind=%s scope=%s path=%s\n", result.Kind, result.Scope, result.Path)
	return nil
}

func (cmd command) repairSetup(configureMCP bool, scope service.ServiceScope) ([]string, error) {
	if scope == service.ServiceScopeLinuxSystemService {
		var results []string
		err := withLinuxSystemServiceConfigEnv(func() error {
			var setupErr error
			results, setupErr = cmd.repairSetupInCurrentEnv(configureMCP, scope)
			if chownErr := chownLinuxSystemScopePaths(firstHermesHome(cmd.store.ListBindings())); chownErr != nil {
				return chownErr
			}
			return setupErr
		})
		if err != nil {
			return nil, err
		}
		return results, nil
	}
	return cmd.repairSetupInCurrentEnv(configureMCP, scope)
}

func (cmd command) repairSetupInCurrentEnv(configureMCP bool, scope service.ServiceScope) ([]string, error) {
	var results []string
	if configureMCP {
		mcpResults, err := cmd.installMCP()
		if err != nil {
			return nil, err
		}
		results = append(results, mcpResults...)
	}
	serviceResult, err := cmd.installServiceForBindings(scope)
	if err != nil {
		return nil, err
	}
	results = append(results, fmt.Sprintf("service installed kind=%s scope=%s path=%s", serviceResult.Kind, serviceResult.Scope, serviceResult.Path))
	return results, nil
}

func (cmd command) installServiceForBindings(scope service.ServiceScope) (service.InstallResult, error) {
	hermesHome := firstHermesHome(cmd.store.ListBindings())
	if hermesHome == "" {
		return installService(scope)
	}
	return withHermesHome(hermesHome, func() (service.InstallResult, error) {
		return installService(scope)
	})
}

func withHermesHome(hermesHome string, fn func() (service.InstallResult, error)) (service.InstallResult, error) {
	trimmed := strings.TrimSpace(hermesHome)
	if trimmed == "" {
		return fn()
	}
	oldValue, hadValue := os.LookupEnv("HERMES_HOME")
	if err := os.Setenv("HERMES_HOME", trimmed); err != nil {
		return service.InstallResult{}, err
	}
	defer func() {
		if hadValue {
			_ = os.Setenv("HERMES_HOME", oldValue)
		} else {
			_ = os.Unsetenv("HERMES_HOME")
		}
	}()
	return fn()
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
	scopeValue := "user"
	parser := newFlaggyParser("run")
	parser.Bool(&foreground, "", "foreground", "run in foreground")
	parser.String(&scopeValue, "", "service-scope", "service scope user or system")

	err := parseFlaggyArgs(parser, args)
	if err != nil {
		return err
	}
	scope, err := parseCommandServiceScope(scopeValue)
	if err != nil {
		return err
	}
	if !foreground {
		return errors.New("run requires --foreground")
	}
	store := cmd.storeForServiceScope(scope)
	bindings := store.ListBindings()
	if err := validateServiceScopeForBindings(scope, bindings, true); err != nil {
		return err
	}

	if err := (daemon.Runner{Store: store, ServiceScope: protocolServiceScope(scope)}).RunForeground(ctx); err != nil {
		return err
	}
	fmt.Fprintln(cmd.stdout, "connector daemon stopped")
	return nil
}

func (cmd command) storeForServiceScope(scope service.ServiceScope) config.Store {
	switch scope {
	case service.ServiceScopeSystemLaunchDaemon:
		return config.SystemFileStore(os.Getenv("PERSONASTACK_CONNECTOR_SYSTEM_ROOT"))
	case service.ServiceScopeLinuxSystemService:
		target, err := (service.Installer{GOOS: currentGOOS}).SystemServiceTarget()
		if err != nil {
			return cmd.store
		}
		return config.NewFileStore(filepath.Join(target.HomeDir, ".config", "personastack", "connector", "state.json"))
	default:
		return cmd.store
	}
}

func (cmd command) runUnpair(args []string) error {
	scopeValue := "user"
	parser := newFlaggyParser("unpair")
	parser.String(&scopeValue, "", "service-scope", "service scope user or system")
	err := parseFlaggyArgs(parser, args)
	if err != nil {
		return err
	}
	scope, err := parseCommandServiceScope(scopeValue)
	if err != nil {
		return err
	}
	cmd.store = cmd.storeForServiceScope(scope)
	if scope == service.ServiceScopeLinuxSystemService {
		if err := validateLinuxSystemServiceScopePlatform(); err != nil {
			return err
		}
		if err := validateLinuxSystemServiceScopeAccess("unpair"); err != nil {
			return err
		}
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
		if scope == service.ServiceScopeLinuxSystemService {
			err := withLinuxSystemServiceConfigEnv(func() error {
				return deleting.DeleteBinding(binding.ConnectionID)
			})
			if err != nil {
				return err
			}
			if err := chownLinuxSystemScopePaths(binding.HermesHome); err != nil && !os.IsNotExist(err) {
				return err
			}
		} else {
			if err := deleting.DeleteBinding(binding.ConnectionID); err != nil {
				return err
			}
		}
		fmt.Fprintf(cmd.stdout, "unpaired connection=%s persona=%s\n", binding.ConnectionID, binding.PersonaID)
	}
	return nil
}

func validateServiceScopeForBindings(scope service.ServiceScope, bindings []config.Binding, allowEmpty bool) error {
	if scope != service.ServiceScopeSystemLaunchDaemon {
		return nil
	}
	if err := validateSystemServiceScopePlatform(); err != nil {
		return err
	}
	if len(bindings) == 0 && allowEmpty {
		return nil
	}
	for _, binding := range bindings {
		if binding.RuntimeKind != runtime.AdapterKindOpenClaw && binding.RuntimeKind != runtime.AdapterKindHermes {
			return errors.New("system service scope requires Hermes or OpenClaw runtime")
		}
	}
	return nil
}

func validateSystemServiceScopePlatform() error {
	if currentGOOS != "darwin" {
		return errors.New("system service scope requires macOS")
	}
	return nil
}

func validateLinuxSystemServiceScopePlatform() error {
	if currentGOOS != "linux" {
		return errors.New("linux system service scope requires Linux")
	}
	return nil
}

func validateSystemServiceScopeAccess(action string) error {
	if strings.TrimSpace(os.Getenv("PERSONASTACK_CONNECTOR_SYSTEM_ROOT")) != "" {
		return nil
	}
	if os.Geteuid() != 0 {
		action = strings.TrimSpace(action)
		if action == "" {
			return errors.New("system service scope requires sudo")
		}
		return fmt.Errorf("system service scope requires sudo before %s", action)
	}
	return nil
}

func validateLinuxSystemServiceScopeAccess(action string) error {
	if strings.TrimSpace(os.Getenv("PERSONASTACK_CONNECTOR_SYSTEM_ROOT")) != "" {
		return nil
	}
	if os.Geteuid() != 0 {
		action = strings.TrimSpace(action)
		if action == "" {
			return errors.New("linux system service scope requires sudo")
		}
		return fmt.Errorf("linux system service scope requires sudo before %s", action)
	}
	return nil
}

func withLinuxSystemServiceConfigEnv(fn func() error) error {
	target, err := (service.Installer{GOOS: currentGOOS}).SystemServiceTarget()
	if err != nil {
		return err
	}
	configDir := filepath.Join(target.HomeDir, ".config")
	oldHome, hadHome := os.LookupEnv("HOME")
	oldXDG, hadXDG := os.LookupEnv("XDG_CONFIG_HOME")
	if err := os.Setenv("HOME", target.HomeDir); err != nil {
		return err
	}
	if err := os.Setenv("XDG_CONFIG_HOME", configDir); err != nil {
		return err
	}
	defer func() {
		if hadHome {
			_ = os.Setenv("HOME", oldHome)
		} else {
			_ = os.Unsetenv("HOME")
		}
		if hadXDG {
			_ = os.Setenv("XDG_CONFIG_HOME", oldXDG)
		} else {
			_ = os.Unsetenv("XDG_CONFIG_HOME")
		}
	}()
	return fn()
}

func chownLinuxSystemScopePaths(hermesHome string) error {
	if os.Geteuid() != 0 {
		return nil
	}
	target, err := (service.Installer{GOOS: currentGOOS}).SystemServiceTarget()
	if err != nil {
		return err
	}
	paths := []string{
		filepath.Join(target.HomeDir, ".config", "personastack"),
		filepath.Join(target.HomeDir, ".config", "personastack", "connector"),
		filepath.Join(target.HomeDir, ".config", "personastack", "connector", "state.json"),
		filepath.Join(target.HomeDir, ".config", "personastack", "connector", "secrets.enc"),
		filepath.Join(target.HomeDir, ".config", "personastack", "connector", "secrets.key"),
		filepath.Join(target.HomeDir, ".openclaw"),
		filepath.Join(target.HomeDir, ".openclaw", "openclaw.json"),
		filepath.Join(target.HomeDir, ".openclaw", "openclaw.json.personastack.bak"),
	}
	hermesPaths := hermessetup.ResolvePaths(target.HomeDir, hermesHome)
	paths = append(paths,
		hermesPaths.HermesHome,
		hermesPaths.EnvPath,
		hermesPaths.EnvPath+".personastack.bak",
		hermesPaths.ConfigPath,
		hermesPaths.ConfigPath+".personastack.bak",
	)
	for _, path := range paths {
		if err := lchownPathNoSymlinkAncestors(path, target.UID, target.GID); err != nil {
			return fmt.Errorf("chown linux system scope path: %w", err)
		}
	}
	return nil
}

func lchownPathNoSymlinkAncestors(path string, uid int, gid int) error {
	cleaned := filepath.Clean(path)
	if !filepath.IsAbs(cleaned) {
		return fmt.Errorf("path must be absolute: %s", path)
	}
	components := strings.Split(strings.TrimPrefix(cleaned, string(os.PathSeparator)), string(os.PathSeparator))
	if len(components) == 0 || components[0] == "" {
		return nil
	}
	dirFd, err := unix.Open(string(os.PathSeparator), unix.O_RDONLY|unix.O_CLOEXEC|unix.O_DIRECTORY, 0)
	if err != nil {
		return err
	}
	defer func() {
		_ = unix.Close(dirFd)
	}()
	for _, component := range components[:len(components)-1] {
		nextFd, err := unix.Openat(dirFd, component, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_DIRECTORY|unix.O_NOFOLLOW, 0)
		if err != nil {
			if errors.Is(err, unix.ENOENT) {
				return nil
			}
			return err
		}
		if err := unix.Close(dirFd); err != nil {
			_ = unix.Close(nextFd)
			return err
		}
		dirFd = nextFd
	}
	leaf := components[len(components)-1]
	if err := unix.Fchownat(dirFd, leaf, uid, gid, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		if errors.Is(err, unix.ENOENT) {
			return nil
		}
		return err
	}
	return nil
}

func protocolServiceScope(scope service.ServiceScope) externalagentprotocol.ServiceScope {
	if scope == service.ServiceScopeSystemLaunchDaemon {
		return externalagentprotocol.ServiceScopeSystemLaunchDaemon
	}
	if scope == service.ServiceScopeLinuxSystemService {
		return externalagentprotocol.ServiceScopeLinuxSystemService
	}
	return externalagentprotocol.ServiceScopeUserLaunchAgent
}

func defaultStore() config.Store {
	store, err := config.DefaultFileStore()
	if err != nil {
		return config.EmptyStore()
	}
	return store
}
