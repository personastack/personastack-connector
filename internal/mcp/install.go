package mcp

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/personastack/personastack-connector/internal/config"
	"github.com/personastack/personastack-connector/internal/runtime"
	"gopkg.in/yaml.v3"
)

type InstallResult struct {
	ConnectionID config.ConnectionID
	Runtime      runtime.AdapterKind
	Path         string
	ServerName   string
}

type Installer struct {
	Store          config.Store
	HomeDir        string
	ExecutablePath string
}

func (installer Installer) InstallAll() ([]InstallResult, error) {
	if installer.Store == nil {
		return nil, fmt.Errorf("store required")
	}
	executablePath, err := installer.executablePath()
	if err != nil {
		return nil, err
	}
	homeDir, err := installer.homeDir()
	if err != nil {
		return nil, err
	}
	bindings := installer.Store.ListBindings()
	if len(bindings) == 0 {
		return nil, ErrMissingBinding
	}
	var results []InstallResult
	for _, binding := range bindings {
		result, err := installBinding(homeDir, executablePath, binding)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	return results, nil
}

func installBinding(homeDir string, executablePath string, binding config.Binding) (InstallResult, error) {
	server := stdioServer(binding, executablePath)
	switch binding.RuntimeKind {
	case runtime.AdapterKindHermes:
		path := filepath.Join(homeDir, ".hermes", "config.yaml")
		err := upsertHermesServer(path, server)
		if err != nil {
			return InstallResult{}, err
		}
		return InstallResult{ConnectionID: binding.ConnectionID, Runtime: binding.RuntimeKind, Path: path, ServerName: server.Name}, nil
	case runtime.AdapterKindOpenClaw:
		path := filepath.Join(homeDir, ".openclaw", "config.json")
		err := upsertOpenClawServer(path, server)
		if err != nil {
			return InstallResult{}, err
		}
		return InstallResult{ConnectionID: binding.ConnectionID, Runtime: binding.RuntimeKind, Path: path, ServerName: server.Name}, nil
	default:
		return InstallResult{}, fmt.Errorf("unsupported runtime for mcp install: %s", binding.RuntimeKind)
	}
}

type stdioServerConfig struct {
	Name    string   `json:"-"`
	Command string   `json:"command" yaml:"command"`
	Args    []string `json:"args" yaml:"args"`
}

func stdioServer(binding config.Binding, executablePath string) stdioServerConfig {
	name := strings.TrimSpace(binding.NativeMCPServer)
	if name == "" {
		name = "personastack-" + strings.TrimSpace(string(binding.ConnectionID))
	}
	return stdioServerConfig{
		Name:    name,
		Command: executablePath,
		Args: []string{
			"mcp",
			"stdio",
			"--binding",
			strings.TrimSpace(string(binding.ConnectionID)),
		},
	}
}

func upsertHermesServer(path string, server stdioServerConfig) error {
	root := map[string]any{}
	raw, err := os.ReadFile(path)
	if err == nil && len(raw) > 0 {
		if err := yaml.Unmarshal(raw, &root); err != nil {
			return fmt.Errorf("parse Hermes config: %w", err)
		}
	}
	mcpNode := ensureMap(root, "mcp")
	servers := ensureMap(mcpNode, "servers")
	servers[server.Name] = map[string]any{
		"command": server.Command,
		"args":    server.Args,
	}
	output, err := yaml.Marshal(root)
	if err != nil {
		return fmt.Errorf("encode Hermes config: %w", err)
	}
	return writeOwnerOnly(path, output)
}

func upsertOpenClawServer(path string, server stdioServerConfig) error {
	root := map[string]any{}
	raw, err := os.ReadFile(path)
	if err == nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, &root); err != nil {
			return fmt.Errorf("parse OpenClaw config: %w", err)
		}
	}
	mcpNode := ensureMap(root, "mcp")
	servers := ensureMap(mcpNode, "servers")
	servers[server.Name] = map[string]any{
		"command": server.Command,
		"args":    server.Args,
	}
	output, err := json.MarshalIndent(root, "", "  ")
	if err != nil {
		return fmt.Errorf("encode OpenClaw config: %w", err)
	}
	output = append(output, '\n')
	return writeOwnerOnly(path, output)
}

func ensureMap(parent map[string]any, key string) map[string]any {
	existing, ok := parent[key].(map[string]any)
	if ok {
		return existing
	}
	created := map[string]any{}
	parent[key] = created
	return created
}

func writeOwnerOnly(path string, raw []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create config dir: %w", err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		return fmt.Errorf("write config: %w", err)
	}
	return nil
}

func (installer Installer) executablePath() (string, error) {
	value := strings.TrimSpace(installer.ExecutablePath)
	if value != "" {
		return value, nil
	}
	path, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("resolve connector executable: %w", err)
	}
	return path, nil
}

func (installer Installer) homeDir() (string, error) {
	value := strings.TrimSpace(installer.HomeDir)
	if value != "" {
		return value, nil
	}
	dir, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home dir: %w", err)
	}
	return dir, nil
}
