package mcp

import (
	"context"
	cryptoRand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	osruntime "runtime"
	"strings"

	"github.com/personastack/personastack-connector/internal/config"
	"github.com/personastack/personastack-connector/internal/hermessetup"
	"github.com/personastack/personastack-connector/internal/openclawauth"
	"github.com/personastack/personastack-connector/internal/runtime"
	"github.com/personastack/personastack-connector/internal/service"
	"gopkg.in/yaml.v3"
)

type InstallResult struct {
	ConnectionID config.ConnectionID
	Runtime      runtime.AdapterKind
	Path         string
	ServerName   string
	Note         string
}

type VerifyResult struct {
	ConnectionID   config.ConnectionID
	Runtime        runtime.AdapterKind
	Path           string
	ServerName     string
	State          runtime.AdapterState
	Note           string
	DiagnosticCode string
}

type Installer struct {
	Store          config.Store
	HomeDir        string
	ExecutablePath string
	GOOS           string
	Transport      MCPProxyTransport
}

type MCPProxyTransport int

const (
	MCPProxyTransportAuto MCPProxyTransport = iota
	MCPProxyTransportStdio
	MCPProxyTransportLoopbackHTTP
)

const credentialStorageWarning = "credential warning: local MCP auth stays in owner-only connector config"

var errOpenClawCLIMissing = errors.New("openclaw cli missing")
var verifyHermesRuntimeMCPRegistry = runtime.VerifyHermesMCPServerLoaded

func openClawConfigPath(homeDir string) string {
	return filepath.Join(homeDir, ".openclaw", "openclaw.json")
}

func (installer Installer) InstallAll() ([]InstallResult, error) {
	if installer.Store == nil {
		return nil, fmt.Errorf("store required")
	}
	bindings := installer.Store.ListBindings()
	if len(bindings) == 0 {
		return nil, ErrMissingBinding
	}
	var results []InstallResult
	for _, binding := range bindings {
		result, err := installer.InstallBinding(binding)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	return results, nil
}

func (installer Installer) InstallBinding(binding config.Binding) (InstallResult, error) {
	homeDir, err := installer.homeDir()
	if err != nil {
		return InstallResult{}, err
	}
	executablePath, err := installer.executablePath(homeDir)
	if err != nil {
		return InstallResult{}, err
	}
	return installer.installBinding(homeDir, executablePath, binding)
}

func (installer Installer) installBinding(homeDir string, executablePath string, binding config.Binding) (InstallResult, error) {
	server := stdioServer(binding, executablePath)
	hermesPaths := hermessetup.ResolvePaths(homeDir, binding.HermesHome)
	transport := installer.Transport
	if transport == MCPProxyTransportAuto {
		return installer.installBindingNativeHTTP(homeDir, binding, server.Name)
	}
	if transport == MCPProxyTransportLoopbackHTTP {
		return installer.installBindingLoopbackHTTP(homeDir, binding, server)
	}
	switch binding.RuntimeKind {
	case runtime.AdapterKindHermes:
		setupReport, err := hermessetup.EnsureAPISetupForPaths(hermesPaths)
		if err != nil {
			return InstallResult{}, err
		}
		path := hermesPaths.ConfigPath
		if err := upsertHermesServer(path, server); err == nil {
			result := InstallResult{ConnectionID: binding.ConnectionID, Runtime: binding.RuntimeKind, Path: path, ServerName: server.Name, Note: setupReport.Note}
			if started, err := hermessetup.TryStartGatewayForPaths(hermesPaths); err != nil {
				return InstallResult{}, err
			} else if started {
				result.Note = appendNote(result.Note, "Hermes gateway start attempted")
			}
			diagnostic := hermessetup.DiagnoseForPaths(hermesPaths)
			if strings.TrimSpace(diagnostic.Note) != "" {
				result.Note = appendNote(result.Note, diagnostic.Note)
			}
			return result, nil
		} else if transport == MCPProxyTransportStdio {
			return InstallResult{}, err
		}
		return installer.installBindingLoopbackHTTP(homeDir, binding, server)
	case runtime.AdapterKindOpenClaw:
		if transport == MCPProxyTransportStdio {
			if result, err := installOpenClawServerWithCLI(homeDir, binding, server); err == nil {
				return result, nil
			} else if !errors.Is(err, errOpenClawCLIMissing) {
				return InstallResult{}, err
			}
			path := openClawConfigPath(homeDir)
			if err := upsertOpenClawServer(path, server); err != nil {
				return InstallResult{}, err
			}
			return InstallResult{ConnectionID: binding.ConnectionID, Runtime: binding.RuntimeKind, Path: path, ServerName: server.Name}, nil
		}
		if result, err := installOpenClawServerWithCLI(homeDir, binding, server); err == nil {
			return result, nil
		}
		path := openClawConfigPath(homeDir)
		if err := upsertOpenClawServer(path, server); err == nil {
			return InstallResult{ConnectionID: binding.ConnectionID, Runtime: binding.RuntimeKind, Path: path, ServerName: server.Name}, nil
		}
		return installer.installBindingLoopbackHTTP(homeDir, binding, server)
	default:
		return InstallResult{}, fmt.Errorf("unsupported runtime for mcp install: %s", binding.RuntimeKind)
	}
}

func (installer Installer) installBindingNativeHTTP(homeDir string, binding config.Binding, serverName string) (InstallResult, error) {
	if strings.TrimSpace(binding.PersonaMCPURL) == "" || strings.TrimSpace(binding.PersonaMCPToken) == "" {
		return InstallResult{}, fmt.Errorf("PersonaStack MCP credential missing")
	}
	server := nativeHTTPMCPServer{
		Name:  serverName,
		URL:   strings.TrimSpace(binding.PersonaMCPURL),
		Token: strings.TrimSpace(binding.PersonaMCPToken),
	}
	switch binding.RuntimeKind {
	case runtime.AdapterKindHermes:
		hermesPaths := hermessetup.ResolvePaths(homeDir, binding.HermesHome)
		setupReport, err := hermessetup.EnsureAPISetupForPaths(hermesPaths)
		if err != nil {
			return InstallResult{}, err
		}
		path := hermesPaths.ConfigPath
		if err := upsertHermesNativeHTTPServer(path, server, binding, installer.preservedNativeServerNames()); err != nil {
			return InstallResult{}, err
		}
		result := InstallResult{
			ConnectionID: binding.ConnectionID,
			Runtime:      binding.RuntimeKind,
			Path:         path,
			ServerName:   server.Name,
			Note:         setupReport.Note,
		}
		if started, err := hermessetup.TryStartGatewayForPaths(hermesPaths); err != nil {
			return InstallResult{}, err
		} else if started {
			result.Note = appendNote(result.Note, "Hermes gateway start attempted")
		}
		diagnostic := hermessetup.DiagnoseForPaths(hermesPaths)
		if strings.TrimSpace(diagnostic.Note) != "" {
			result.Note = appendNote(result.Note, diagnostic.Note)
		}
		return result, nil
	case runtime.AdapterKindOpenClaw:
		path := openClawConfigPath(homeDir)
		if err := upsertOpenClawNativeHTTPServer(path, server, binding); err != nil {
			return InstallResult{}, err
		}
		return InstallResult{ConnectionID: binding.ConnectionID, Runtime: binding.RuntimeKind, Path: path, ServerName: server.Name}, nil
	default:
		return InstallResult{}, fmt.Errorf("unsupported runtime for mcp install: %s", binding.RuntimeKind)
	}
}

func installOpenClawServerWithCLI(homeDir string, binding config.Binding, server stdioServerConfig) (InstallResult, error) {
	cliPath, err := exec.LookPath("openclaw")
	if err != nil {
		return InstallResult{}, fmt.Errorf("%w: %v", errOpenClawCLIMissing, err)
	}
	configPath := openClawConfigPath(homeDir)
	if err := os.MkdirAll(filepath.Dir(configPath), 0o700); err != nil {
		return InstallResult{}, fmt.Errorf("create OpenClaw config dir: %w", err)
	}
	raw, err := json.Marshal(map[string]any{
		"command": server.Command,
		"args":    server.Args,
	})
	if err != nil {
		return InstallResult{}, err
	}
	cmd := exec.Command(cliPath, "mcp", "set", server.Name, string(raw))
	cmd.Env = append(os.Environ(), "OPENCLAW_CONFIG_PATH="+configPath)
	if output, err := cmd.CombinedOutput(); err != nil {
		return InstallResult{}, fmt.Errorf("openclaw mcp set: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return InstallResult{ConnectionID: binding.ConnectionID, Runtime: binding.RuntimeKind, Path: configPath, ServerName: server.Name}, nil
}

func (installer Installer) preservedNativeServerNames() map[string]bool {
	out := map[string]bool{}
	if installer.Store == nil {
		return out
	}
	for _, binding := range installer.Store.ListBindings() {
		name := strings.TrimSpace(binding.NativeMCPServer)
		if name == "" {
			name = "personastack-" + strings.TrimSpace(string(binding.ConnectionID))
		}
		if name != "" {
			out[name] = true
		}
	}
	return out
}

func (installer Installer) installBindingLoopbackHTTP(homeDir string, binding config.Binding, server stdioServerConfig) (InstallResult, error) {
	loopback, err := loopbackHTTPMCPServerForBinding(binding)
	if err != nil {
		return InstallResult{}, err
	}
	binding.LocalMCPProxyURL = loopback.URL
	binding.LocalMCPProxyToken = loopback.Token
	binding.HasLocalMCPProxyToken = true
	writable, ok := installer.Store.(config.WritableStore)
	if ok {
		if err := writable.SaveBinding(binding); err != nil {
			return InstallResult{}, err
		}
	}
	switch binding.RuntimeKind {
	case runtime.AdapterKindHermes:
		hermesPaths := hermessetup.ResolvePaths(homeDir, binding.HermesHome)
		setupReport, err := hermessetup.EnsureAPISetupForPaths(hermesPaths)
		if err != nil {
			return InstallResult{}, err
		}
		if err := upsertHermesLoopbackHTTPEnv(hermesPaths.EnvPath, loopback.EnvironmentVariable, loopback.Token); err != nil {
			return InstallResult{}, err
		}
		path := hermesPaths.ConfigPath
		if err := upsertHermesLoopbackHTTPServer(path, server, loopback); err != nil {
			return InstallResult{}, err
		}
		result := InstallResult{
			ConnectionID: binding.ConnectionID,
			Runtime:      binding.RuntimeKind,
			Path:         path,
			ServerName:   server.Name,
			Note:         appendNote(setupReport.Note, "Hermes loopback HTTP MCP proxy configured", credentialStorageWarning),
		}
		return result, nil
	case runtime.AdapterKindOpenClaw:
		path := openClawConfigPath(homeDir)
		if err := upsertOpenClawLoopbackHTTPServer(path, server, loopback); err != nil {
			return InstallResult{}, err
		}
		return InstallResult{
			ConnectionID: binding.ConnectionID,
			Runtime:      binding.RuntimeKind,
			Path:         path,
			ServerName:   server.Name,
			Note:         appendNote("OpenClaw loopback HTTP MCP proxy configured", credentialStorageWarning),
		}, nil
	default:
		return InstallResult{}, fmt.Errorf("unsupported runtime for loopback HTTP install: %s", binding.RuntimeKind)
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
		result.DiagnosticCode = "mcp_token_missing"
		return result
	}
	switch binding.RuntimeKind {
	case runtime.AdapterKindHermes:
		result.Path = hermessetup.ResolvePaths(homeDir, binding.HermesHome).ConfigPath
		result.State, result.Note = verifyHermesServer(result.Path, serverName, binding)
	case runtime.AdapterKindOpenClaw:
		result.Path = openClawConfigPath(homeDir)
		result.State, result.Note = verifyOpenClawServer(result.Path, serverName, binding)
	default:
		result.Note = "unsupported runtime for mcp verification"
	}
	result.DiagnosticCode = diagnosticCodeForMCPVerify(result.State, result.Note)
	return result
}

func VerifyBindingWithLive(ctx context.Context, homeDir string, binding config.Binding, client *http.Client) VerifyResult {
	result := VerifyBinding(homeDir, binding)
	if result.State != runtime.AdapterStateMCPRestartRequired {
		return result
	}
	if result.DiagnosticCode == "mcp_token_rejected" {
		return result
	}
	if binding.RuntimeKind == runtime.AdapterKindOpenClaw {
		resolved := openclawauth.Result{}
		if openclawauth.GatewayIsLoopback(os.Getenv("PERSONASTACK_CONNECTOR_OPENCLAW_GATEWAY_URL")) {
			var err error
			resolved, err = openclawauth.Resolve(openclawauth.Options{
				Binding: binding,
				HomeDir: homeDir,
			})
			if err != nil {
				result.Note = appendNote(result.Note, err.Error())
				return result
			}
		}
		adapter := runtime.NewOpenClawAdapterWithAuth(os.Getenv("PERSONASTACK_CONNECTOR_OPENCLAW_GATEWAY_URL"), resolved.Auth, binding.OpenClawAgentID)
		openClawLive := adapter.VerifyMCPCatalog(ctx, result.ServerName)
		if openClawLive.OK {
			result.Note = openClawLive.Note
		} else {
			result.Note = appendNote(result.Note, openClawLive.Note)
		}
	}
	live := VerifyBindingLive(ctx, binding, client)
	if isLoopbackHTTPConfig(result.Note) {
		live = VerifyLoopbackHTTPProxy(ctx, binding, client)
	}
	if !live.OK {
		result.Note = live.Note
		result.DiagnosticCode = live.DiagnosticCode
		return result
	}
	if binding.RuntimeKind == runtime.AdapterKindHermes {
		hermesLive := verifyHermesRuntimeMCPRegistryForHome(ctx, result.ServerName, binding.HermesHome)
		if !hermesLive.OK {
			result.Note = appendNote(result.Note, live.Note, hermesLive.Note)
			result.DiagnosticCode = "native_mcp_unreachable"
			return result
		}
		result.Note = appendNote(result.Note, live.Note, hermesLive.Note)
		result.State = runtime.AdapterStateMCPVerified
		result.DiagnosticCode = ""
		return result
	}
	result.State = runtime.AdapterStateMCPVerified
	result.Note = result.Note + "; " + live.Note + "; native runtime restart may be required"
	result.DiagnosticCode = ""
	return result
}

func verifyHermesRuntimeMCPRegistryForHome(ctx context.Context, serverName string, hermesHome string) runtime.HermesMCPRegistryCheck {
	if strings.TrimSpace(hermesHome) == "" {
		return verifyHermesRuntimeMCPRegistry(ctx, serverName)
	}
	return runtime.VerifyHermesMCPServerLoadedWithHome(ctx, serverName, hermesHome)
}

func isLoopbackHTTPConfig(note string) bool {
	return strings.Contains(strings.ToLower(note), "credential warning")
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

type loopbackHTTPMCPServer struct {
	URL                 string
	Token               string
	EnvironmentVariable string
}

type nativeHTTPMCPServer struct {
	Name  string
	URL   string
	Token string
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

func newLoopbackHTTPMCPServer(binding config.Binding) (loopbackHTTPMCPServer, error) {
	port, err := acquireLoopbackPort()
	if err != nil {
		return loopbackHTTPMCPServer{}, err
	}
	token, err := generateLoopbackHTTPToken()
	if err != nil {
		return loopbackHTTPMCPServer{}, err
	}
	return loopbackHTTPMCPServer{
		URL:                 fmt.Sprintf("http://127.0.0.1:%d/mcp/%s", port, strings.TrimSpace(string(binding.ConnectionID))),
		Token:               token,
		EnvironmentVariable: loopbackHTTPEnvironmentVariable(binding.ConnectionID),
	}, nil
}

func loopbackHTTPMCPServerForBinding(binding config.Binding) (loopbackHTTPMCPServer, error) {
	localURL := strings.TrimSpace(binding.LocalMCPProxyURL)
	localToken := strings.TrimSpace(binding.LocalMCPProxyToken)
	if localURL != "" && localToken != "" {
		parsed, err := url.Parse(localURL)
		if err != nil {
			return loopbackHTTPMCPServer{}, fmt.Errorf("parse existing loopback mcp url: %w", err)
		}
		if !loopbackMCPURL(parsed, binding.ConnectionID) {
			return loopbackHTTPMCPServer{}, fmt.Errorf("existing loopback mcp url is invalid")
		}
		return loopbackHTTPMCPServer{
			URL:                 localURL,
			Token:               localToken,
			EnvironmentVariable: loopbackHTTPEnvironmentVariable(binding.ConnectionID),
		}, nil
	}
	return newLoopbackHTTPMCPServer(binding)
}

func acquireLoopbackPort() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("acquire loopback port: %w", err)
	}
	defer listener.Close()
	addr, ok := listener.Addr().(*net.TCPAddr)
	if !ok {
		return 0, fmt.Errorf("acquire loopback port: unexpected address")
	}
	return addr.Port, nil
}

func generateLoopbackHTTPToken() (string, error) {
	var raw [32]byte
	if _, err := cryptoRand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate loopback http token: %w", err)
	}
	return hex.EncodeToString(raw[:]), nil
}

func loopbackHTTPEnvironmentVariable(bindingID config.ConnectionID) string {
	trimmed := strings.TrimSpace(string(bindingID))
	if trimmed == "" {
		return "PERSONASTACK_CONNECTOR_LOCAL_MCP"
	}
	var builder strings.Builder
	builder.WriteString("PERSONASTACK_CONNECTOR_LOCAL_MCP_")
	for _, r := range strings.ToUpper(trimmed) {
		switch {
		case r >= 'A' && r <= 'Z':
			builder.WriteRune(r)
		case r >= '0' && r <= '9':
			builder.WriteRune(r)
		default:
			builder.WriteRune('_')
		}
	}
	return builder.String()
}

func upsertHermesLoopbackHTTPEnv(path string, envVar string, token string) error {
	state := map[string]string{}
	raw, err := os.ReadFile(path)
	if err == nil && len(raw) > 0 {
		scanner := strings.Split(strings.ReplaceAll(string(raw), "\r\n", "\n"), "\n")
		for _, line := range scanner {
			trimmed := strings.TrimSpace(line)
			if trimmed == "" || strings.HasPrefix(trimmed, "#") {
				continue
			}
			if strings.HasPrefix(trimmed, "export ") {
				trimmed = strings.TrimSpace(strings.TrimPrefix(trimmed, "export "))
			}
			key, value, ok := strings.Cut(trimmed, "=")
			if !ok || strings.TrimSpace(key) == "" {
				continue
			}
			state[strings.TrimSpace(key)] = strings.TrimSpace(value)
		}
	}
	state[envVar] = token
	var builder strings.Builder
	for key, value := range state {
		builder.WriteString(key)
		builder.WriteByte('=')
		builder.WriteString(value)
		builder.WriteByte('\n')
	}
	return writeOwnerOnlyAtomic(path, []byte(builder.String()))
}

func upsertHermesLoopbackHTTPServer(path string, server stdioServerConfig, loopback loopbackHTTPMCPServer) error {
	root := map[string]any{}
	raw, err := os.ReadFile(path)
	if err == nil && len(raw) > 0 {
		_ = yaml.Unmarshal(raw, &root)
	}
	servers := ensureMap(root, "mcp_servers")
	servers[server.Name] = map[string]any{
		"url": loopback.URL,
		"headers": map[string]any{
			"Authorization": "Bearer ${" + loopback.EnvironmentVariable + "}",
		},
		"timeout":         120,
		"connect_timeout": 10,
		"enabled":         true,
	}
	removeLegacyNestedServer(root, server.Name)
	output, err := yaml.Marshal(root)
	if err != nil {
		return fmt.Errorf("encode Hermes loopback config: %w", err)
	}
	return writeOwnerOnlyAtomic(path, output)
}

func upsertOpenClawLoopbackHTTPServer(path string, server stdioServerConfig, loopback loopbackHTTPMCPServer) error {
	root := map[string]any{}
	raw, err := os.ReadFile(path)
	if err == nil && len(raw) > 0 {
		_ = json.Unmarshal(raw, &root)
	}
	mcpNode := ensureMap(root, "mcp")
	servers := ensureMap(mcpNode, "servers")
	servers[server.Name] = map[string]any{
		"transport":           "streamable-http",
		"url":                 loopback.URL,
		"connectionTimeoutMs": 10000,
		"headers": map[string]any{
			"Authorization": "Bearer " + loopback.Token,
		},
	}
	output, err := json.MarshalIndent(root, "", "  ")
	if err != nil {
		return fmt.Errorf("encode OpenClaw loopback config: %w", err)
	}
	output = append(output, '\n')
	return writeOwnerOnlyAtomic(path, output)
}

func upsertHermesNativeHTTPServer(path string, server nativeHTTPMCPServer, binding config.Binding, preserveNames map[string]bool) error {
	root := map[string]any{}
	raw, err := os.ReadFile(path)
	if err == nil && len(raw) > 0 {
		if err := yaml.Unmarshal(raw, &root); err != nil {
			return fmt.Errorf("parse Hermes config: %w", err)
		}
	}
	servers := ensureMap(root, "mcp_servers")
	if existing, ok := servers[server.Name].(map[string]any); ok {
		if err := requireRecognizedServer(existing, binding); err != nil {
			return err
		}
	}
	removeDuplicateHermesPersonaStackServers(servers, server.Name, binding, preserveNames)
	servers[server.Name] = nativeHTTPServerMap(server)
	enableHermesAPIServerMCP(root)
	removeLegacyNestedServer(root, server.Name)
	output, err := yaml.Marshal(root)
	if err != nil {
		return fmt.Errorf("encode Hermes config: %w", err)
	}
	return writeOwnerOnlyAtomic(path, output)
}

func removeDuplicateHermesPersonaStackServers(servers map[string]any, keepName string, binding config.Binding, preserveNames map[string]bool) {
	for name, value := range servers {
		if name == keepName || preserveNames[name] || !strings.HasPrefix(strings.ToLower(strings.TrimSpace(name)), "personastack") {
			continue
		}
		server, ok := value.(map[string]any)
		if !ok {
			continue
		}
		if hermesServerOwnedByBinding(server, binding) {
			delete(servers, name)
		}
	}
}

func hermesServerOwnedByBinding(server map[string]any, binding config.Binding) bool {
	if serverArgsMatchBinding(server["args"], binding.ConnectionID) {
		return true
	}
	transport := normalizedMCPTransport(server)
	if transport != "streamable-http" && transport != "sse" {
		return false
	}
	rawURL, _ := server["url"].(string)
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || !directMCPURL(parsed, binding) {
		return false
	}
	return bearerAuthorizationHeaderMatches(server, binding.PersonaMCPToken)
}

func upsertOpenClawNativeHTTPServer(path string, server nativeHTTPMCPServer, binding config.Binding) error {
	root := map[string]any{}
	raw, err := os.ReadFile(path)
	if err == nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, &root); err != nil {
			return fmt.Errorf("parse OpenClaw config: %w", err)
		}
	}
	mcpNode := ensureMap(root, "mcp")
	servers := ensureMap(mcpNode, "servers")
	if existing, ok := servers[server.Name].(map[string]any); ok {
		if err := requireRecognizedServer(existing, binding); err != nil {
			return err
		}
	}
	servers[server.Name] = nativeHTTPServerMap(server)
	output, err := json.MarshalIndent(root, "", "  ")
	if err != nil {
		return fmt.Errorf("encode OpenClaw config: %w", err)
	}
	output = append(output, '\n')
	return writeOwnerOnlyAtomic(path, output)
}

func nativeHTTPServerMap(server nativeHTTPMCPServer) map[string]any {
	return map[string]any{
		"transport":           "streamable-http",
		"url":                 server.URL,
		"connectionTimeoutMs": 10000,
		"headers": map[string]any{
			"Authorization": "Bearer " + server.Token,
		},
		"timeout":         120,
		"connect_timeout": 10,
		"enabled":         true,
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
		"command":         server.Command,
		"args":            server.Args,
		"timeout":         120,
		"connect_timeout": 60,
		"enabled":         true,
	}
	enableHermesAPIServerMCP(root)
	removeLegacyNestedServer(root, server.Name)
	output, err := yaml.Marshal(root)
	if err != nil {
		return fmt.Errorf("encode Hermes config: %w", err)
	}
	return writeOwnerOnlyAtomic(path, output)
}

func verifyHermesServer(path string, serverName string, binding config.Binding) (runtime.AdapterState, string) {
	root := map[string]any{}
	raw, err := os.ReadFile(path)
	if err != nil {
		return runtime.AdapterStateMCPConfigMissing, err.Error()
	}
	if err := yaml.Unmarshal(raw, &root); err != nil {
		return runtime.AdapterStateMCPConfigMissing, "parse Hermes config: " + err.Error()
	}
	if hermesAPIServerMCPDisabled(root) {
		return runtime.AdapterStateMCPConfigMissing, "Hermes api_server toolsets disable MCP; repair required"
	}
	servers, ok := root["mcp_servers"].(map[string]any)
	if !ok {
		return runtime.AdapterStateMCPConfigMissing, "mcp_servers section missing"
	}
	return verifyNamedServerMap(servers, serverName, binding)
}

func enableHermesAPIServerMCP(root map[string]any) {
	platforms, ok := root["platform_toolsets"].(map[string]any)
	if !ok {
		return
	}
	rawToolsets, ok := platforms["api_server"]
	if !ok {
		return
	}
	toolsets, ok := rawToolsets.([]any)
	if !ok {
		return
	}
	filtered := make([]any, 0, len(toolsets))
	for _, raw := range toolsets {
		name, ok := raw.(string)
		if ok && strings.EqualFold(strings.TrimSpace(name), "no_mcp") {
			continue
		}
		filtered = append(filtered, raw)
	}
	platforms["api_server"] = filtered
}

func hermesAPIServerMCPDisabled(root map[string]any) bool {
	platforms, ok := root["platform_toolsets"].(map[string]any)
	if !ok {
		return false
	}
	toolsets, ok := platforms["api_server"].([]any)
	if !ok {
		return false
	}
	for _, raw := range toolsets {
		name, ok := raw.(string)
		if ok && strings.EqualFold(strings.TrimSpace(name), "no_mcp") {
			return true
		}
	}
	return false
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

func verifyOpenClawServer(path string, serverName string, binding config.Binding) (runtime.AdapterState, string) {
	root := map[string]any{}
	raw, err := os.ReadFile(path)
	if err != nil {
		return runtime.AdapterStateMCPConfigMissing, err.Error()
	}
	if err := json.Unmarshal(raw, &root); err != nil {
		return runtime.AdapterStateMCPConfigMissing, "parse OpenClaw config: " + err.Error()
	}
	return verifyServerMap(root, serverName, binding)
}

func verifyServerMap(root map[string]any, serverName string, binding config.Binding) (runtime.AdapterState, string) {
	mcpNode, ok := root["mcp"].(map[string]any)
	if !ok {
		return runtime.AdapterStateMCPConfigMissing, "mcp section missing"
	}
	servers, ok := mcpNode["servers"].(map[string]any)
	if !ok {
		return runtime.AdapterStateMCPConfigMissing, "mcp servers section missing"
	}
	return verifyNamedServerMap(servers, serverName, binding)
}

func verifyNamedServerMap(servers map[string]any, serverName string, binding config.Binding) (runtime.AdapterState, string) {
	server, ok := servers[serverName].(map[string]any)
	if !ok {
		return runtime.AdapterStateMCPConfigMissing, "PersonaStack MCP server missing"
	}
	if transport := normalizedMCPTransport(server); transport == "streamable-http" || transport == "sse" {
		return verifyStreamableHTTPServer(server, binding)
	}
	command, ok := server["command"].(string)
	if !ok || strings.TrimSpace(command) == "" {
		return runtime.AdapterStateMCPConfigMissing, "PersonaStack MCP command missing"
	}
	if !serverArgsMatchBinding(server["args"], binding.ConnectionID) {
		return runtime.AdapterStateMCPConfigMissing, "PersonaStack MCP server config conflict"
	}
	return runtime.AdapterStateMCPRestartRequired, "PersonaStack MCP config present; live verification required"
}

func verifyStreamableHTTPServer(server map[string]any, binding config.Binding) (runtime.AdapterState, string) {
	rawURL, ok := server["url"].(string)
	if !ok || strings.TrimSpace(rawURL) == "" {
		return runtime.AdapterStateMCPConfigMissing, "PersonaStack MCP url missing"
	}
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return runtime.AdapterStateMCPConfigMissing, "parse PersonaStack MCP url: " + err.Error()
	}
	loopback := loopbackMCPURL(parsed, binding.ConnectionID)
	if !directMCPURL(parsed, binding) && !loopback {
		return runtime.AdapterStateMCPConfigMissing, "PersonaStack MCP url does not match binding"
	}
	if !hasBearerAuthorizationHeader(server) {
		return runtime.AdapterStateMCPConfigMissing, "PersonaStack MCP Authorization header missing"
	}
	if !loopback && !bearerAuthorizationHeaderMatches(server, binding.PersonaMCPToken) {
		return runtime.AdapterStateMCPRestartRequired, "PersonaStack MCP Authorization token differs from binding; repair required"
	}
	if loopback {
		return runtime.AdapterStateMCPRestartRequired, appendNote("PersonaStack MCP streamable-http config present; live verification required", credentialStorageWarning)
	}
	return runtime.AdapterStateMCPRestartRequired, "PersonaStack MCP streamable-http config present; live verification required"
}

func normalizedMCPTransport(server map[string]any) string {
	if rawURL, ok := server["url"].(string); ok && strings.TrimSpace(rawURL) != "" {
		return "streamable-http"
	}
	for _, key := range []string{"transport", "type"} {
		raw, ok := server[key].(string)
		if !ok {
			continue
		}
		switch strings.ToLower(strings.TrimSpace(raw)) {
		case "http":
			return "streamable-http"
		default:
			return strings.ToLower(strings.TrimSpace(raw))
		}
	}
	return ""
}

func requireRecognizedServer(server map[string]any, binding config.Binding) error {
	transport := normalizedMCPTransport(server)
	if transport == "streamable-http" || transport == "sse" {
		rawURL, _ := server["url"].(string)
		parsed, err := url.Parse(strings.TrimSpace(rawURL))
		if err == nil && (directMCPURL(parsed, binding) || loopbackMCPURL(parsed, binding.ConnectionID)) {
			return nil
		}
	}
	if serverArgsMatchBinding(server["args"], binding.ConnectionID) {
		return nil
	}
	return fmt.Errorf("native MCP server %q already exists with an unrecognized config", strings.TrimSpace(binding.NativeMCPServer))
}

func directMCPURL(parsed *url.URL, binding config.Binding) bool {
	if parsed == nil {
		return false
	}
	expected, err := url.Parse(strings.TrimSpace(binding.PersonaMCPURL))
	if err != nil || expected == nil {
		return false
	}
	return parsed.Scheme == expected.Scheme && parsed.Host == expected.Host && parsed.EscapedPath() == expected.EscapedPath()
}

func hasBearerAuthorizationHeader(server map[string]any) bool {
	headers, ok := server["headers"].(map[string]any)
	if !ok {
		return false
	}
	raw, ok := headers["Authorization"].(string)
	if !ok {
		return false
	}
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(raw)), "bearer ")
}

func bearerAuthorizationHeaderMatches(server map[string]any, token string) bool {
	headers, ok := server["headers"].(map[string]any)
	if !ok {
		return false
	}
	raw, ok := headers["Authorization"].(string)
	if !ok {
		return false
	}
	value := strings.TrimSpace(raw)
	if !strings.HasPrefix(strings.ToLower(value), "bearer ") {
		return false
	}
	return strings.TrimSpace(value[len("bearer "):]) == strings.TrimSpace(token)
}

func diagnosticCodeForMCPVerify(state runtime.AdapterState, note string) string {
	lowerNote := strings.ToLower(note)
	switch {
	case strings.Contains(lowerNote, "parse "):
		return "mcp_config_parse_error"
	case strings.Contains(lowerNote, "unrecognized config"), strings.Contains(lowerNote, "config conflict"):
		return "mcp_config_conflict"
	case strings.Contains(lowerNote, "credential missing"), strings.Contains(lowerNote, "authorization header missing"):
		return "mcp_token_missing"
	case strings.Contains(lowerNote, "token differs"):
		return "mcp_token_rejected"
	case state == runtime.AdapterStateMCPRestartRequired:
		return "mcp_restart_required"
	case state == runtime.AdapterStateMCPConfigMissing:
		return "mcp_config_missing"
	default:
		return state.String()
	}
}

func loopbackMCPURL(parsed *url.URL, bindingID config.ConnectionID) bool {
	if parsed == nil {
		return false
	}
	if parsed.Scheme != "http" {
		return false
	}
	host := strings.TrimSpace(parsed.Hostname())
	if host != "127.0.0.1" {
		return false
	}
	connectionID := strings.TrimSpace(string(bindingID))
	if connectionID == "" {
		return false
	}
	return strings.Contains(strings.TrimSpace(parsed.Path), "/mcp/"+connectionID)
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

func appendNote(parts ...string) string {
	notes := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		notes = append(notes, trimmed)
	}
	return strings.Join(notes, "; ")
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
