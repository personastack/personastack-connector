package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	osruntime "runtime"
	"strings"

	"github.com/personastack/personastack-connector/internal/config"
	"github.com/personastack/personastack-connector/internal/runtime"
	"github.com/personastack/personastack-connector/internal/service"
	"gopkg.in/yaml.v3"
)

type InstallResult struct {
	ConnectionID config.ConnectionID
	Runtime      runtime.AdapterKind
	Path         string
	ServerName   string
}

type VerifyResult struct {
	ConnectionID config.ConnectionID
	Runtime      runtime.AdapterKind
	Path         string
	ServerName   string
	State        runtime.AdapterState
	Note         string
}

type Installer struct {
	Store          config.Store
	HomeDir        string
	ExecutablePath string
	GOOS           string
}

func (installer Installer) InstallAll() ([]InstallResult, error) {
	if installer.Store == nil {
		return nil, fmt.Errorf("store required")
	}
	homeDir, err := installer.homeDir()
	if err != nil {
		return nil, err
	}
	executablePath, err := installer.executablePath(homeDir)
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

func VerifyBinding(homeDir string, binding config.Binding) VerifyResult {
	serverName := strings.TrimSpace(binding.NativeMCPServer)
	if serverName == "" {
		serverName = "personastack-" + strings.TrimSpace(string(binding.ConnectionID))
	}
	result := VerifyResult{
		ConnectionID: binding.ConnectionID,
		Runtime:      binding.RuntimeKind,
		ServerName:   serverName,
		State:        runtime.AdapterStateMCPConfigMissing,
	}
	if strings.TrimSpace(binding.PersonaMCPURL) == "" || strings.TrimSpace(binding.PersonaMCPToken) == "" {
		result.Note = "PersonaStack MCP credential missing"
		return result
	}
	switch binding.RuntimeKind {
	case runtime.AdapterKindHermes:
		result.Path = filepath.Join(homeDir, ".hermes", "config.yaml")
		result.State, result.Note = verifyHermesServer(result.Path, serverName, binding.ConnectionID)
	case runtime.AdapterKindOpenClaw:
		result.Path = filepath.Join(homeDir, ".openclaw", "config.json")
		result.State, result.Note = verifyOpenClawServer(result.Path, serverName, binding.ConnectionID)
	default:
		result.Note = "unsupported runtime for mcp verification"
	}
	return result
}

func VerifyBindingWithLive(ctx context.Context, homeDir string, binding config.Binding, client *http.Client) VerifyResult {
	result := VerifyBinding(homeDir, binding)
	if result.State != runtime.AdapterStateMCPRestartRequired {
		return result
	}
	live := VerifyBindingLive(ctx, binding, client)
	if !live.OK {
		result.Note = live.Note
		return result
	}
	result.Note = live.Note + "; native runtime restart may be required"
	return result
}

func VerifyBindingInUserHome(binding config.Binding) VerifyResult {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return VerifyResult{
			ConnectionID: binding.ConnectionID,
			Runtime:      binding.RuntimeKind,
			State:        runtime.AdapterStateMCPConfigMissing,
			Note:         "resolve home dir: " + err.Error(),
		}
	}
	return VerifyBinding(homeDir, binding)
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
	servers := ensureMap(root, "mcp_servers")
	servers[server.Name] = map[string]any{
		"command": server.Command,
		"args":    server.Args,
	}
	removeLegacyNestedServer(root, server.Name)
	output, err := yaml.Marshal(root)
	if err != nil {
		return fmt.Errorf("encode Hermes config: %w", err)
	}
	return writeOwnerOnlyAtomic(path, output)
}

func verifyHermesServer(path string, serverName string, bindingID config.ConnectionID) (runtime.AdapterState, string) {
	root := map[string]any{}
	raw, err := os.ReadFile(path)
	if err != nil {
		return runtime.AdapterStateMCPConfigMissing, err.Error()
	}
	if err := yaml.Unmarshal(raw, &root); err != nil {
		return runtime.AdapterStateMCPConfigMissing, "parse Hermes config: " + err.Error()
	}
	servers, ok := root["mcp_servers"].(map[string]any)
	if !ok {
		return runtime.AdapterStateMCPConfigMissing, "mcp_servers section missing"
	}
	return verifyNamedServerMap(servers, serverName, bindingID)
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
	return writeOwnerOnlyAtomic(path, output)
}

func verifyOpenClawServer(path string, serverName string, bindingID config.ConnectionID) (runtime.AdapterState, string) {
	root := map[string]any{}
	raw, err := os.ReadFile(path)
	if err != nil {
		return runtime.AdapterStateMCPConfigMissing, err.Error()
	}
	if err := json.Unmarshal(raw, &root); err != nil {
		return runtime.AdapterStateMCPConfigMissing, "parse OpenClaw config: " + err.Error()
	}
	return verifyServerMap(root, serverName, bindingID)
}

func verifyServerMap(root map[string]any, serverName string, bindingID config.ConnectionID) (runtime.AdapterState, string) {
	mcpNode, ok := root["mcp"].(map[string]any)
	if !ok {
		return runtime.AdapterStateMCPConfigMissing, "mcp section missing"
	}
	servers, ok := mcpNode["servers"].(map[string]any)
	if !ok {
		return runtime.AdapterStateMCPConfigMissing, "mcp servers section missing"
	}
	return verifyNamedServerMap(servers, serverName, bindingID)
}

func verifyNamedServerMap(servers map[string]any, serverName string, bindingID config.ConnectionID) (runtime.AdapterState, string) {
	server, ok := servers[serverName].(map[string]any)
	if !ok {
		return runtime.AdapterStateMCPConfigMissing, "PersonaStack MCP server missing"
	}
	command, ok := server["command"].(string)
	if !ok || strings.TrimSpace(command) == "" {
		return runtime.AdapterStateMCPConfigMissing, "PersonaStack MCP command missing"
	}
	if !serverArgsMatchBinding(server["args"], bindingID) {
		return runtime.AdapterStateMCPConfigMissing, "PersonaStack MCP binding argument missing"
	}
	return runtime.AdapterStateMCPRestartRequired, "PersonaStack MCP config present; live verification required"
}

func serverArgsMatchBinding(value any, bindingID config.ConnectionID) bool {
	target := strings.TrimSpace(string(bindingID))
	if target == "" {
		return false
	}
	values, ok := value.([]any)
	if !ok {
		return false
	}
	args := make([]string, 0, len(values))
	for _, item := range values {
		args = append(args, strings.TrimSpace(fmt.Sprint(item)))
	}
	if len(args) < 4 || args[0] != "mcp" || args[1] != "stdio" {
		return false
	}
	for index := 2; index < len(args)-1; index++ {
		if args[index] == "--binding" && args[index+1] == target {
			return true
		}
	}
	return false
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

func removeLegacyNestedServer(root map[string]any, serverName string) {
	mcpNode, ok := root["mcp"].(map[string]any)
	if !ok {
		return
	}
	servers, ok := mcpNode["servers"].(map[string]any)
	if !ok {
		return
	}
	delete(servers, serverName)
	if len(servers) == 0 {
		delete(mcpNode, "servers")
	}
	if len(mcpNode) == 0 {
		delete(root, "mcp")
	}
}

func writeOwnerOnlyAtomic(path string, raw []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create config dir: %w", err)
	}
	existing, readErr := os.ReadFile(path)
	if readErr != nil && !os.IsNotExist(readErr) {
		return fmt.Errorf("read config for backup: %w", readErr)
	}
	if readErr == nil {
		backupPath := path + ".personastack.bak"
		if _, err := os.Stat(backupPath); os.IsNotExist(err) {
			if err := writeFileAtomic(backupPath, existing, 0o600); err != nil {
				return fmt.Errorf("write config backup: %w", err)
			}
		} else if err != nil {
			return fmt.Errorf("stat config backup: %w", err)
		}
	}
	return writeFileAtomic(path, raw, 0o600)
}

func writeFileAtomic(path string, raw []byte, mode os.FileMode) error {
	temp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create temp config: %w", err)
	}
	tempPath := temp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tempPath)
		}
	}()
	if err := temp.Chmod(mode); err != nil {
		_ = temp.Close()
		return fmt.Errorf("secure temp config: %w", err)
	}
	if _, err := temp.Write(raw); err != nil {
		_ = temp.Close()
		return fmt.Errorf("write temp config: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close temp config: %w", err)
	}
	if err := replaceFile(tempPath, path); err != nil {
		return fmt.Errorf("replace config: %w", err)
	}
	cleanup = false
	return nil
}

func replaceFile(tempPath string, path string) error {
	if osruntime.GOOS != "windows" {
		return os.Rename(tempPath, path)
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return os.Rename(tempPath, path)
}

func (installer Installer) executablePath(homeDir string) (string, error) {
	value := strings.TrimSpace(installer.ExecutablePath)
	if value != "" {
		shim, err := service.EnsureShim(homeDir, value, installer.GOOS)
		if err != nil {
			return "", err
		}
		return shim.Path, nil
	}
	path, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("resolve connector executable: %w", err)
	}
	goos := strings.TrimSpace(installer.GOOS)
	if goos == "" {
		goos = osruntime.GOOS
	}
	shim, err := service.EnsureShim(homeDir, path, goos)
	if err != nil {
		return "", err
	}
	return shim.Path, nil
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
