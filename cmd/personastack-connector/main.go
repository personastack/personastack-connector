package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/personastack/agent-gateway/pkg/externalagentprotocol"
	"github.com/personastack/personastack-connector/internal/config"
	"github.com/personastack/personastack-connector/internal/daemon"
	"github.com/personastack/personastack-connector/internal/mcp"
	"github.com/personastack/personastack-connector/internal/pairing"
	"github.com/personastack/personastack-connector/internal/runtime"
	"github.com/personastack/personastack-connector/internal/service"
)

const usage = `Usage:
  personastack-connector pair <code> [--runtime auto|hermes|openclaw] [--configure-mcp]
  personastack-connector status
  personastack-connector runtime detect
  personastack-connector mcp install
  personastack-connector mcp stdio --binding <connection_id>
  personastack-connector service install
  personastack-connector run --foreground
  personastack-connector unpair
`

type command struct {
	stdin  io.Reader
	stdout io.Writer
	stderr io.Writer
	store  config.Store
}

func main() {
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
		return cmd.runStatus(args[1:])
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
	configureMCP := flags.Bool("configure-mcp", false, "configure native runtime MCP")
	gateway := flags.String("gateway", externalagentprotocol.DefaultGatewayBaseURL, "PersonaStack gateway URL")

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
		kind = runtime.AdapterKindHermes
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
	if err := writable.SaveBinding(result.Binding); err != nil {
		return err
	}
	if *configureMCP {
		if _, err := (mcp.Installer{Store: cmd.store}).InstallAll(); err != nil {
			return err
		}
	}
	serviceResult, err := (service.Installer{}).Install()
	if err != nil {
		return err
	}
	fmt.Fprintf(cmd.stdout, "service installed kind=%s path=%s\n", serviceResult.Kind, serviceResult.Path)
	fmt.Fprintf(cmd.stdout, "paired persona=%s connection=%s runtime=%s configure_mcp=%t\n", result.Binding.PersonaID, result.Binding.ConnectionID, result.Binding.RuntimeKind, *configureMCP)
	return nil
}

func (cmd command) runStatus(args []string) error {
	if len(args) != 0 {
		return errors.New("status accepts no arguments")
	}

	bindings := cmd.store.ListBindings()
	if len(bindings) == 0 {
		fmt.Fprintln(cmd.stdout, "no bindings")
		return nil
	}

	for _, binding := range bindings {
		fmt.Fprintf(cmd.stdout, "%s persona=%s runtime=%s state=%s\n", binding.ConnectionID, binding.PersonaID, binding.RuntimeKind, binding.ReadinessState)
	}
	return nil
}

func (cmd command) runRuntime(args []string) error {
	if len(args) == 0 {
		return errors.New("runtime requires a subcommand")
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

func (cmd command) runMCP(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("mcp requires a subcommand")
	}

	switch args[0] {
	case "install":
		return cmd.runMCPInstall(args[1:])
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
	results, err := (mcp.Installer{Store: cmd.store}).InstallAll()
	if err != nil {
		return err
	}
	for _, result := range results {
		fmt.Fprintf(cmd.stdout, "installed mcp binding=%s runtime=%s server=%s path=%s\n", result.ConnectionID, result.Runtime, result.ServerName, result.Path)
	}
	return nil
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
	if args[0] != "install" {
		return fmt.Errorf("unknown service subcommand %q", args[0])
	}
	if len(args) != 1 {
		return errors.New("service install accepts no arguments")
	}
	result, err := (service.Installer{}).Install()
	if err != nil {
		return err
	}
	fmt.Fprintf(cmd.stdout, "service installed kind=%s path=%s\n", result.Kind, result.Path)
	return nil
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
