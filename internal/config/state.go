package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

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
	ConnectionID            ConnectionID
	PersonaID               PersonaID
	ExternalAgentKind       ExternalAgentKind
	ConnectionGeneration    int64
	GatewayWebsocketURL     string
	BridgeCredentialID      string
	BridgePrivateKey        string
	BridgePublicKey         string
	NativeMCPServer         string
	NativeMCPNamespace      string
	HermesHome              string
	OpenClawAgentID         string
	OpenClawGatewayToken    string
	OpenClawPassword        string
	OpenClawDeviceToken     string
	PersonaMCPURL           string
	PersonaMCPToken         string
	LocalMCPProxyURL        string
	LocalMCPProxyToken      string
	ActiveRunID             string
	ActiveAssignmentID      string
	ActiveNativeRunID       string
	ActiveRunDeadlineAt     time.Time
	LastHeartbeatAt         time.Time
	LastWakeProbeAt         time.Time
	LastWakeProbeGeneration int64
	RuntimeKind             runtime.AdapterKind
	ReadinessState          runtime.AdapterState
	HasBridgeSecret         bool
	HasOpenClawToken        bool
	HasOpenClawPassword     bool
	HasOpenClawDevice       bool
	HasPersonaMCPToken      bool
	HasLocalMCPProxyToken   bool
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

type DeletingStore interface {
	Store
	DeleteBinding(ConnectionID) error
}

type MemoryStore struct {
	mu    *sync.RWMutex
	state *State
}

func NewMemoryStore(state State) MemoryStore {
	copied := state
	return MemoryStore{mu: &sync.RWMutex{}, state: &copied}
}

func (store MemoryStore) Binding(connectionID ConnectionID) (Binding, bool) {
	if store.mu != nil {
		store.mu.RLock()
		defer store.mu.RUnlock()
	}
	if store.state == nil {
		return Binding{}, false
	}
	for _, binding := range store.state.Bindings {
		if binding.ConnectionID == connectionID {
			return binding, true
		}
	}
	return Binding{}, false
}

func (store MemoryStore) ListBindings() []Binding {
	if store.mu != nil {
		store.mu.RLock()
		defer store.mu.RUnlock()
	}
	if store.state == nil {
		return nil
	}
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
	if store.mu != nil {
		store.mu.Lock()
		defer store.mu.Unlock()
	}
	if store.state == nil {
		store.state = &State{}
	}
	store.state.Bindings = []Binding{binding}
	return nil
}

func (store *MemoryStore) DeleteBinding(connectionID ConnectionID) error {
	if store == nil {
		return fmt.Errorf("memory store required")
	}
	if store.mu != nil {
		store.mu.Lock()
		defer store.mu.Unlock()
	}
	if store.state == nil {
		return nil
	}
	for i, binding := range store.state.Bindings {
		if binding.ConnectionID == connectionID {
			store.state.Bindings = append(store.state.Bindings[:i], store.state.Bindings[i+1:]...)
			return nil
		}
	}
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

func SystemFileStore(systemRoot string) FileStore {
	root := strings.TrimSpace(systemRoot)
	if root == "" {
		root = string(filepath.Separator)
	}
	return NewFileStore(filepath.Join(root, "Library", "Application Support", "PersonaStack", "Connector", "state.json"))
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
	replacedBindings := make([]Binding, 0, len(state.Bindings))
	replacingExistingConnection := false
	for _, existing := range state.Bindings {
		if existing.ConnectionID != storedBinding.ConnectionID {
			replacedBindings = append(replacedBindings, existing)
			continue
		}
		replacingExistingConnection = true
	}
	state.Bindings = []Binding{storedBinding}
	if err := os.MkdirAll(filepath.Dir(store.path), 0o700); err != nil {
		if !replacingExistingConnection {
			deleteBindingSecrets(storedBinding)
		}
		return fmt.Errorf("create connector config dir: %w", err)
	}
	raw, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		if !replacingExistingConnection {
			deleteBindingSecrets(storedBinding)
		}
		return fmt.Errorf("encode connector state: %w", err)
	}
	if err := os.WriteFile(store.path, raw, 0o600); err != nil {
		if !replacingExistingConnection {
			deleteBindingSecrets(storedBinding)
		}
		return fmt.Errorf("write connector state: %w", err)
	}
	for _, replaced := range replacedBindings {
		deleteBindingSecrets(replaced)
	}
	return nil
}

func (store FileStore) DeleteBinding(connectionID ConnectionID) error {
	trimmedID := ConnectionID(strings.TrimSpace(string(connectionID)))
	if trimmedID == "" {
		return nil
	}
	state := store.load()
	bindings := state.Bindings[:0]
	var deleted *Binding
	for _, binding := range state.Bindings {
		if binding.ConnectionID == trimmedID {
			copyBinding := binding
			deleted = &copyBinding
			continue
		}
		bindings = append(bindings, binding)
	}
	state.Bindings = bindings
	if deleted != nil {
		deleteBindingSecrets(*deleted)
	}
	if err := os.MkdirAll(filepath.Dir(store.path), 0o700); err != nil {
		return fmt.Errorf("create connector config dir: %w", err)
	}
	raw, err := json.MarshalIndent(state, "", "  ")
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
