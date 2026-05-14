package config

import "github.com/personastack/personastack-connector/internal/runtime"

type ConnectionID string
type PersonaID string

type ExternalAgentKind int

const (
	ExternalAgentKindHermes ExternalAgentKind = iota
	ExternalAgentKindOpenClaw
)

func (kind ExternalAgentKind) String() string {
	switch kind {
	case ExternalAgentKindHermes:
		return "hermes"
	case ExternalAgentKindOpenClaw:
		return "openclaw"
	default:
		return "unknown"
	}
}

type Binding struct {
	ConnectionID         ConnectionID
	PersonaID            PersonaID
	ExternalAgentKind    ExternalAgentKind
	ConnectionGeneration int64
	NativeMCPServer      string
	RuntimeKind          runtime.AdapterKind
	ReadinessState       runtime.AdapterState
	HasBridgeSecret      bool
	HasPersonaMCPToken   bool
}

type State struct {
	Bindings []Binding
}

type Store interface {
	Binding(connectionID ConnectionID) (Binding, bool)
	ListBindings() []Binding
}

type MemoryStore struct {
	state State
}

func NewMemoryStore(state State) MemoryStore {
	return MemoryStore{state: state}
}

func (store MemoryStore) Binding(connectionID ConnectionID) (Binding, bool) {
	for _, binding := range store.state.Bindings {
		if binding.ConnectionID == connectionID {
			return binding, true
		}
	}
	return Binding{}, false
}

func (store MemoryStore) ListBindings() []Binding {
	bindings := make([]Binding, len(store.state.Bindings))
	copy(bindings, store.state.Bindings)
	return bindings
}

func EmptyStore() MemoryStore {
	return NewMemoryStore(State{})
}
