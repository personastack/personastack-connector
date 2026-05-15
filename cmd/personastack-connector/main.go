package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/personastack/agent-gateway/pkg/externalagentprotocol"
	"github.com/personastack/personastack-connector/internal/buildinfo"
	"github.com/personastack/personastack-connector/internal/config"
	"github.com/personastack/personastack-connector/internal/daemon"
	"github.com/personastack/personastack-connector/internal/hermessetup"
	"github.com/personastack/personastack-connector/internal/mcp"
	"github.com/personastack/personastack-connector/internal/pairing"
	"github.com/personastack/personastack-connector/internal/runtime"
	"github.com/personastack/personastack-connector/internal/service"
	"github.com/zalando/go-keyring"
)

const usage = `Usage:
  personastack-connector pair <code> [--runtime auto|hermes|openclaw] [--configure-mcp] [--openclaw-token <token>|--openclaw-password <password>|--openclaw-device-token <token>] [--openclaw-agent-id <id>]
  personastack-connector status [--repair]
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
		return flag.ErrHelp
	}

	switch args[0] {
	case "-h", "--help", "help":
		cmd.printUsage(cmd.stdout)
		return nil
	case "pair":
		return cmd.runPair(args[1:])
	case "status":
		return cmd.runStatus(ctx, args[1:])
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

func (cmd command) runPair(args []string) error {
	flags := flag.NewFlagSet("pair", flag.ContinueOnError)
	flags.SetOutput(cmd.stderr)

	runtimeValue := flags.String("runtime", "auto", "runtime adapter")
	configureMCP := flags.Bool("configure-mcp", true, "configure native runtime MCP")
	gateway := flags.String("gateway", externalagentprotocol.DefaultGatewayBaseURL, "PersonaStack gateway URL")
	openClawToken := flags.String("openclaw-token", "", "OpenClaw operator token")
	openClawPassword := flags.String("openclaw-password", "", "OpenClaw operator password")
	openClawDeviceToken := flags.String("openclaw-device-token", "", "OpenClaw operator device token")
	openClawAgentID := flags.String("openclaw-agent-id", "", "OpenClaw agent id")

	err := flags.Parse(args)
	if err != nil {
		return err
	}
	if flags.NArg() != 1 {
		return errors.New("pair requires one pairing code")
	}

	kind, err := runtime.ParseAdapterKind(*runtimeValue)
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
	result, err := pairing.Client{GatewayBaseURL: *gateway}.Exchange(context.Background(), pairing.Request{
		Code:         flags.Arg(0),
		RuntimeKind:  kind,
		ConfigureMCP: *configureMCP,
	})
	if err != nil {
		return err
	}
	binding := result.Binding
	if err := applyOpenClawPairOptions(&binding, openClawPairOptions{
		token:       *openClawToken,
		password:    *openClawPassword,
		deviceToken: *openClawDeviceToken,
		agentID:     *openClawAgentID,
	}); err != nil {
		return err
	}
	if err := writable.SaveBinding(binding); err != nil {
		return err
	}
	repairResults, err := cmd.repairSetup(*configureMCP)
	if err != nil {
		return err
	}
	for _, repairResult := range repairResults {
		fmt.Fprintln(cmd.stdout, repairResult)
	}
	fmt.Fprintf(cmd.stdout, "paired persona=%s connection=%s runtime=%s configure_mcp=%t setup_state=pending_bridge_wake_probe\n", binding.PersonaID, binding.ConnectionID, binding.RuntimeKind, *configureMCP)
	return nil
}

type openClawPairOptions struct {
	token       string
	password    string
	deviceToken string
	agentID     string
}

func applyOpenClawPairOptions(binding *config.Binding, options openClawPairOptions) error {
	if binding == nil || binding.RuntimeKind != runtime.AdapterKindOpenClaw {
		return nil
	}
	binding.OpenClawGatewayToken = firstNonEmpty(options.token, binding.OpenClawGatewayToken)
	binding.OpenClawPassword = firstNonEmpty(options.password, binding.OpenClawPassword)
	binding.OpenClawDeviceToken = firstNonEmpty(options.deviceToken, binding.OpenClawDeviceToken)
	binding.OpenClawAgentID = firstNonEmpty(options.agentID, binding.OpenClawAgentID)
	if firstNonEmpty(binding.OpenClawGatewayToken, binding.OpenClawPassword, binding.OpenClawDeviceToken, os.Getenv("OPENCLAW_GATEWAY_TOKEN"), os.Getenv("OPENCLAW_GATEWAY_PASSWORD"), os.Getenv("OPENCLAW_GATEWAY_DEVICE_TOKEN")) == "" {
		return errors.New("OpenClaw operator credential required; rerun with --openclaw-token, --openclaw-password, --openclaw-device-token, or set OPENCLAW_GATEWAY_TOKEN/OPENCLAW_GATEWAY_PASSWORD/OPENCLAW_GATEWAY_DEVICE_TOKEN")
	}
	return nil
}

func (cmd command) runStatus(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("status", flag.ContinueOnError)
	flags.SetOutput(cmd.stderr)

	repair := flags.Bool("repair", false, "repair local connector setup")

	err := flags.Parse(args)
	if err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("status accepts flags only")
	}

	bindings := cmd.store.ListBindings()
	if len(bindings) == 0 {
		fmt.Fprintln(cmd.stdout, "no bindings")
		return nil
	}
	if *repair {
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
		verifyCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		verify := mcp.VerifyBindingWithLive(verifyCtx, homeDir, binding, nil)
		cancel()
		detection := adapterForBinding(binding).Detect()
		fmt.Fprintf(cmd.stdout, "%s persona=%s runtime=%s runtime_state=%s mcp=%s mcp_note=%q active_run=%s active_assignment=%s active_native_run=%s active_run_mcp=%t\n",
			binding.ConnectionID,
			binding.PersonaID,
			binding.RuntimeKind,
			detection.State,
			verify.State,
			verify.Note,
			emptyOrDash(binding.ActiveRunID),
			emptyOrDash(binding.ActiveAssignmentID),
			emptyOrDash(binding.ActiveNativeRunID),
			binding.HasActiveRunMCPToken,
		)
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
				fmt.Fprintf(cmd.stdout, "runtime repair binding=%s runtime=%s state=%s note=%q action=manual_runtime_setup_required\n", binding.ConnectionID, binding.RuntimeKind, detection.State, detection.Note)
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

	for _, kind := range []runtime.AdapterKind{runtime.AdapterKindHermes, runtime.AdapterKindOpenClaw} {
		detection := runtime.NewAdapter(kind).Detect()
		fmt.Fprintf(cmd.stdout, "%s %s %s\n", detection.Kind, detection.State, detection.Note)
	}
	return nil
}

func (cmd command) runRuntimeHermes(args []string) error {
	if len(args) == 0 || args[0] != "configure" {
		return errors.New("runtime hermes requires configure")
	}
	flags := flag.NewFlagSet("runtime hermes configure", flag.ContinueOnError)
	flags.SetOutput(cmd.stderr)

	enableAPI := flags.Bool("enable-api", true, "enable Hermes API on loopback")
	configureMCP := flags.Bool("configure-mcp", true, "configure native runtime MCP")

	err := flags.Parse(args[1:])
	if err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("runtime hermes configure accepts flags only")
	}

	homeDir, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("resolve home dir: %w", err)
	}
	if *enableAPI {
		report, err := hermessetup.EnsureAPISetup(homeDir)
		if err != nil {
			return err
		}
		fmt.Fprintf(cmd.stdout, "runtime hermes configure state=%s note=%q\n", report.State, report.Note)
	}
	if *configureMCP {
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
	flags := flag.NewFlagSet("runtime openclaw configure", flag.ContinueOnError)
	flags.SetOutput(cmd.stderr)

	gateway := flags.String("gateway", "ws://127.0.0.1:18789", "OpenClaw Gateway URL")
	configureMCP := flags.Bool("configure-mcp", true, "configure native runtime MCP")

	err := flags.Parse(args[1:])
	if err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("runtime openclaw configure accepts flags only")
	}

	bindings := filterBindingsByRuntime(cmd.store.ListBindings(), runtime.AdapterKindOpenClaw)
	if len(bindings) == 0 {
		fmt.Fprintf(cmd.stdout, "runtime openclaw configure gateway=%s no_bindings\n", strings.TrimSpace(*gateway))
		return nil
	}
	for _, binding := range bindings {
		detection := runtime.NewOpenClawAdapterWithAuth(*gateway, runtime.OpenClawAuth{
			Token:       firstNonEmpty(binding.OpenClawGatewayToken, os.Getenv("OPENCLAW_GATEWAY_TOKEN")),
			Password:    firstNonEmpty(binding.OpenClawPassword, os.Getenv("OPENCLAW_GATEWAY_PASSWORD")),
			DeviceToken: firstNonEmpty(binding.OpenClawDeviceToken, os.Getenv("OPENCLAW_GATEWAY_DEVICE_TOKEN")),
		}, binding.OpenClawAgentID).Detect()
		fmt.Fprintf(cmd.stdout, "runtime openclaw configure binding=%s gateway=%s state=%s note=%q\n", binding.ConnectionID, strings.TrimSpace(*gateway), detection.State, detection.Note)
	}
	if *configureMCP {
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
	var ready []runtime.Detection
	for _, kind := range []runtime.AdapterKind{runtime.AdapterKindHermes, runtime.AdapterKindOpenClaw} {
		detection := runtime.NewAdapter(kind).Detect()
		if detection.State == runtime.AdapterStateReady {
			ready = append(ready, detection)
		}
	}
	switch len(ready) {
	case 1:
		return ready[0].Kind, nil
	case 0:
		return runtime.AdapterKindAuto, errors.New("runtime auto-detect found no ready Hermes or OpenClaw runtime")
	default:
		return runtime.AdapterKindAuto, errors.New("runtime auto-detect found multiple ready runtimes; rerun with --runtime hermes or --runtime openclaw")
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
	results, err := (mcp.Installer{Store: config.NewMemoryStore(config.State{Bindings: bindings})}).InstallAll()
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

func (cmd command) runMCPStdio(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("mcp stdio", flag.ContinueOnError)
	flags.SetOutput(cmd.stderr)

	bindingID := flags.String("binding", "", "Connector binding id")

	err := flags.Parse(args)
	if err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("mcp stdio accepts flags only")
	}
	if *bindingID == "" {
		return errors.New("mcp stdio requires --binding")
	}

	proxy := mcp.NewStdioProxy(cmd.store)
	return proxy.Serve(ctx, config.ConnectionID(*bindingID), cmd.stdin, cmd.stdout, cmd.stderr)
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
	serviceResult, err := (service.Installer{}).Install()
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
	flags := flag.NewFlagSet("run", flag.ContinueOnError)
	flags.SetOutput(cmd.stderr)

	foreground := flags.Bool("foreground", false, "run in foreground")

	err := flags.Parse(args)
	if err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("run accepts flags only")
	}
	if !*foreground {
		return errors.New("run requires --foreground in this scaffold")
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
	fmt.Fprintln(cmd.stdout, "unpair scaffold: no persisted bindings")
	return nil
}

func defaultStore() config.Store {
	store, err := config.DefaultFileStore()
	if err != nil {
		return config.EmptyStore()
	}
	return store
}
