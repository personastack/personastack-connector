package menubar

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/personastack/personastack-connector/internal/buildinfo"
	"github.com/personastack/personastack-connector/internal/externalagentprotocol"
)

type Options struct {
	GOOS         string
	ServiceScope externalagentprotocol.ServiceScope
}

type State struct {
	ActiveRunID         string
	ConnectionStatus    externalagentprotocol.ConnectionStatus
	CurrentVersion      string
	LatestVersion       string
	LastHeartbeatAt     time.Time
	ManualUpdateCommand string
	PersonaID           string
	RuntimeKind         externalagentprotocol.RuntimeKind
	RuntimeLabel        string
	UpdateCapability    externalagentprotocol.UpdateCapability
	UpdateReason        externalagentprotocol.UpdateReason
	UpdateState         externalagentprotocol.UpdateState
	WakeReadiness       externalagentprotocol.ReadinessStatus
}

type Controller interface {
	Stop()
	Update(State)
}

type RunFunc func(context.Context, Controller) error

type noopController struct{}

func (noopController) Stop()        {}
func (noopController) Update(State) {}

func Noop() Controller {
	return noopController{}
}

func Enabled(options Options) bool {
	return strings.TrimSpace(options.GOOS) == "darwin" &&
		options.ServiceScope == externalagentprotocol.ServiceScopeUserLaunchAgent
}

func NormalizeState(state State) State {
	if strings.TrimSpace(state.CurrentVersion) == "" {
		state.CurrentVersion = buildinfo.VersionString()
	}
	if state.UpdateState == "" {
		state.UpdateState = externalagentprotocol.UpdateStateIdle
	}
	if state.UpdateCapability == "" {
		state.UpdateCapability = externalagentprotocol.UpdateCapabilityUnknown
	}
	return state
}

func StatusText(state State) string {
	state = NormalizeState(state)
	lines := []string{
		fmt.Sprintf("Connection: %s", emptyAsUnknown(string(state.ConnectionStatus))),
		fmt.Sprintf("Runtime: %s", emptyAsUnknown(string(state.RuntimeKind))),
		fmt.Sprintf("Persona: %s", emptyAsUnknown(state.PersonaID)),
		fmt.Sprintf("Wake: %s", emptyAsUnknown(string(state.WakeReadiness))),
		fmt.Sprintf("Active run: %s", emptyAsNone(state.ActiveRunID)),
		fmt.Sprintf("Current version: %s", emptyAsUnknown(state.CurrentVersion)),
		fmt.Sprintf("Latest version: %s", emptyAsUnknown(state.LatestVersion)),
		fmt.Sprintf("Update state: %s", emptyAsUnknown(string(state.UpdateState))),
	}
	if !state.LastHeartbeatAt.IsZero() {
		lines = append(lines, "Last heartbeat: "+state.LastHeartbeatAt.UTC().Format(time.RFC3339))
	}
	if state.UpdateReason != "" {
		lines = append(lines, "Update reason: "+string(state.UpdateReason))
	}
	return strings.Join(lines, "\n")
}

func emptyAsUnknown(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "unknown"
	}
	return value
}

func emptyAsNone(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "none"
	}
	return value
}
