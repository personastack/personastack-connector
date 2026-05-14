package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/personastack/personastack-connector/internal/runtime"
)

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
	GatewayWebsocketURL  string
	BridgeCredentialID   string
	BridgePrivateKey     string
	BridgePublicKey      string
	NativeMCPServer      string
	NativeMCPNamespace   string
	PersonaMCPURL        string
	PersonaMCPToken      string
	ActiveRunID          string
	ActiveRunMCPToken    string
	RuntimeKind          runtime.AdapterKind
	ReadinessState       runtime.AdapterState
	HasBridgeSecret      bool
	HasPersonaMCPToken   bool
	HasActiveRunMCPToken bool
}

type State struct {
	Bindings []Binding
}

type Store interface {
	Binding(connectionID ConnectionID) (Binding, bool)
	ListBindings() []Binding
}

type WritableStore interface {
	Store
	SaveBinding(Binding) error
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

func (store *MemoryStore) SaveBinding(binding Binding) error {
	if store == nil {
		return fmt.Errorf("memory store required")
	}
	for i, existing := range store.state.Bindings {
		if existing.ConnectionID == binding.ConnectionID {
			store.state.Bindings[i] = binding
			return nil
		}
	}
	store.state.Bindings = append(store.state.Bindings, binding)
	return nil
}

type FileStore struct {
	path string
}

func DefaultFileStore() (FileStore, error) {
	dir, err := os.UserConfigDir()
	if err != nil {
		return FileStore{}, fmt.Errorf("resolve user config dir: %w", err)
	}
	return NewFileStore(filepath.Join(dir, "personastack", "connector", "state.json")), nil
}

func NewFileStore(path string) FileStore {
	return FileStore{path: path}
}

func (store FileStore) Binding(connectionID ConnectionID) (Binding, bool) {
	return NewMemoryStore(store.load()).Binding(connectionID)
}

func (store FileStore) ListBindings() []Binding {
	return NewMemoryStore(store.load()).ListBindings()
}

func (store FileStore) SaveBinding(binding Binding) error {
	storedBinding, err := storeBindingSecrets(binding)
	if err != nil {
		return err
	}
	state := store.load()
	memory := NewMemoryStore(state)
	if err := (&memory).SaveBinding(storedBinding); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(store.path), 0o700); err != nil {
		return fmt.Errorf("create connector config dir: %w", err)
	}
	raw, err := json.MarshalIndent(memory.state, "", "  ")
	if err != nil {
		return fmt.Errorf("encode connector state: %w", err)
	}
	if err := os.WriteFile(store.path, raw, 0o600); err != nil {
		return fmt.Errorf("write connector state: %w", err)
	}
	return nil
}

func (store FileStore) load() State {
	raw, err := os.ReadFile(store.path)
	if err != nil {
		return State{}
	}
	var state State
	if err := json.Unmarshal(raw, &state); err != nil {
		return State{}
	}
	for i, binding := range state.Bindings {
		state.Bindings[i] = loadBindingSecrets(binding)
	}
	return state
}
