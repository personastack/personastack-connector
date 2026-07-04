//go:build darwin

package menubar

import (
	"context"
	_ "embed"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"

	"github.com/getlantern/systray"
	"github.com/personastack/personastack-connector/internal/externalagentprotocol"
)

//go:embed icon/personastack-template.png
var personaStackIcon []byte

type darwinController struct {
	cancel context.CancelFunc
	mu     sync.Mutex
	ready  chan struct{}
	state  State

	connection *systray.MenuItem
	runtime    *systray.MenuItem
	persona    *systray.MenuItem
	heartbeat  *systray.MenuItem
	wake       *systray.MenuItem
	activeRun  *systray.MenuItem
	version    *systray.MenuItem
	latest     *systray.MenuItem
	update     *systray.MenuItem
	copyUpdate *systray.MenuItem
	install    *systray.MenuItem
}

func Start(ctx context.Context, options Options) Controller {
	if !Enabled(options) || strings.HasSuffix(os.Args[0], ".test") {
		return Noop()
	}
	menuCtx, cancel := context.WithCancel(ctx)
	controller := &darwinController{
		cancel: cancel,
		ready:  make(chan struct{}),
	}
	go systray.Run(controller.onReady(menuCtx), controller.onExit)
	return controller
}

func RunWithController(ctx context.Context, options Options, run RunFunc) error {
	if !Enabled(options) || strings.HasSuffix(os.Args[0], ".test") {
		return run(ctx, Noop())
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	controller := &darwinController{
		cancel: cancel,
		ready:  make(chan struct{}),
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- run(runCtx, controller)
		systray.Quit()
	}()
	systray.Run(controller.onReady(runCtx), controller.onExit)
	cancel()
	return <-errCh
}

func (controller *darwinController) Stop() {
	controller.cancel()
	systray.Quit()
}

func (controller *darwinController) Update(state State) {
	state = NormalizeState(state)
	controller.mu.Lock()
	controller.state = state
	controller.mu.Unlock()
	select {
	case <-controller.ready:
		controller.render(state)
	default:
	}
}

func (controller *darwinController) onReady(ctx context.Context) func() {
	return func() {
		systray.SetTemplateIcon(personaStackIcon, personaStackIcon)
		systray.SetTitle("")
		systray.SetTooltip("PersonaStack Connector")

		controller.connection = disabledItem("Connection: unknown")
		controller.runtime = disabledItem("Runtime: unknown")
		controller.persona = disabledItem("Persona: unknown")
		controller.heartbeat = disabledItem("Last heartbeat: unknown")
		controller.wake = disabledItem("Wake: unknown")
		controller.activeRun = disabledItem("Active run: none")
		controller.version = disabledItem("Version: unknown")
		controller.latest = disabledItem("Latest: unknown")
		controller.update = disabledItem("Update: idle")
		systray.AddSeparator()
		openItem := systray.AddMenuItem("Open PersonaStack", "Open PersonaStack in the browser")
		copyStatus := systray.AddMenuItem("Copy Status", "Copy redacted Connector status")
		repair := systray.AddMenuItem("Repair Connector", "Run local Connector repair")
		diagnostics := systray.AddMenuItem("View Diagnostics", "Copy Connector diagnostics")
		check := systray.AddMenuItem("Check for Updates", "Refresh update status")
		controller.install = systray.AddMenuItem("Install Update", "Install the available Connector update")
		controller.copyUpdate = systray.AddMenuItem("Copy Update Command", "Copy the manual update command")
		quit := systray.AddMenuItem("Quit Connector", "Quit PersonaStack Connector")

		controller.install.Disable()
		controller.copyUpdate.Disable()
		close(controller.ready)
		controller.Update(controller.currentState())

		go controller.handleClicks(ctx, openItem, copyStatus, repair, diagnostics, check, quit)
	}
}

func (controller *darwinController) onExit() {
	controller.cancel()
}

func (controller *darwinController) currentState() State {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	return controller.state
}

func (controller *darwinController) render(state State) {
	if controller.connection == nil {
		return
	}
	controller.connection.SetTitle("Connection: " + emptyAsUnknown(string(state.ConnectionStatus)))
	controller.runtime.SetTitle("Runtime: " + emptyAsUnknown(runtimeLabel(state)))
	controller.persona.SetTitle("Persona: " + emptyAsUnknown(state.PersonaID))
	if state.LastHeartbeatAt.IsZero() {
		controller.heartbeat.SetTitle("Last heartbeat: unknown")
	} else {
		controller.heartbeat.SetTitle("Last heartbeat: " + state.LastHeartbeatAt.Local().Format("Jan 2 15:04:05"))
	}
	controller.wake.SetTitle("Wake: " + emptyAsUnknown(string(state.WakeReadiness)))
	controller.activeRun.SetTitle("Active run: " + emptyAsNone(state.ActiveRunID))
	controller.version.SetTitle("Version: " + emptyAsUnknown(state.CurrentVersion))
	controller.latest.SetTitle("Latest: " + emptyAsUnknown(state.LatestVersion))
	controller.update.SetTitle("Update: " + emptyAsUnknown(string(state.UpdateState)))
	if state.UpdateCapability == externalagentprotocol.UpdateCapabilityOneClickAvailable {
		controller.install.Enable()
	} else {
		controller.install.Disable()
	}
	if strings.TrimSpace(state.ManualUpdateCommand) != "" {
		controller.copyUpdate.Enable()
	} else {
		controller.copyUpdate.Disable()
	}
}

func (controller *darwinController) handleClicks(ctx context.Context, openItem, copyStatus, repair, diagnostics, check, quit *systray.MenuItem) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-openItem.ClickedCh:
			startCommand("open", "https://my.personastack.ai/user/personas")
		case <-copyStatus.ClickedCh:
			copyText(StatusText(controller.currentState()))
		case <-repair.ClickedCh:
			runSelf("mcp", "repair")
		case <-diagnostics.ClickedCh:
			copyCommandOutput("diagnostics")
		case <-check.ClickedCh:
			controller.Update(controller.currentState())
		case <-controller.install.ClickedCh:
			runSelf("update", "install")
		case <-controller.copyUpdate.ClickedCh:
			copyText(controller.currentState().ManualUpdateCommand)
		case <-quit.ClickedCh:
			controller.Stop()
			return
		}
	}
}

func disabledItem(title string) *systray.MenuItem {
	item := systray.AddMenuItem(title, title)
	item.Disable()
	return item
}

func runtimeLabel(state State) string {
	if strings.TrimSpace(state.RuntimeLabel) != "" {
		return state.RuntimeLabel
	}
	return string(state.RuntimeKind)
}

func startCommand(name string, args ...string) {
	if err := exec.Command(name, args...).Start(); err != nil {
		log.Printf("menubar command failed name=%s err=%v", name, err)
	}
}

func runSelf(args ...string) {
	executable, err := os.Executable()
	if err != nil {
		log.Printf("menubar self command failed err=%v", err)
		return
	}
	startCommand(executable, args...)
}

func copyCommandOutput(args ...string) {
	executable, err := os.Executable()
	if err != nil {
		log.Printf("menubar diagnostics command failed err=%v", err)
		return
	}
	output, err := exec.Command(executable, args...).CombinedOutput()
	if err != nil {
		log.Printf("menubar diagnostics command failed err=%v", err)
	}
	copyText(string(output))
}

func copyText(text string) {
	text = strings.TrimSpace(text)
	if text == "" {
		return
	}
	cmd := exec.Command("pbcopy")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		log.Printf("menubar copy failed err=%v", err)
		return
	}
	if err := cmd.Start(); err != nil {
		log.Printf("menubar copy failed err=%v", err)
		return
	}
	_, _ = stdin.Write([]byte(text))
	_ = stdin.Close()
	if err := cmd.Wait(); err != nil {
		log.Printf("menubar copy failed err=%v", err)
	}
}
