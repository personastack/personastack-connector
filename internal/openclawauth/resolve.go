package openclawauth

import (
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/personastack/personastack-connector/internal/config"
	"github.com/personastack/personastack-connector/internal/runtime"
)

const DefaultGatewayURL = "ws://127.0.0.1:18789"

type Source string

const (
	SourceNone             Source = ""
	SourceConnectorBinding Source = "connector_binding"
	SourceExplicit         Source = "explicit"
	SourceEnvironment      Source = "environment"
	SourceOpenClawConfig   Source = "openclaw_config"
	SourceOpenClawEnvFile  Source = "openclaw_env_file"
	SourceDeviceAuth       Source = "openclaw_device_auth"
	SourceServiceEnv       Source = "openclaw_service_env"
)

type Explicit struct {
	Token       string
	Password    string
	DeviceToken string
}

type Options struct {
	Binding  config.Binding
	Explicit Explicit
	HomeDir  string
	Env      func(string) string
}

type Result struct {
	Auth   runtime.OpenClawAuth
	Source Source
}

func (result Result) Found() bool {
	return result.Auth.Token != "" || result.Auth.Password != "" || result.Auth.DeviceToken != ""
}

func ApplyToBinding(binding config.Binding, result Result) config.Binding {
	if !result.Found() {
		return binding
	}
	binding.OpenClawGatewayToken = firstNonEmpty(result.Auth.Token, binding.OpenClawGatewayToken)
	binding.OpenClawPassword = firstNonEmpty(result.Auth.Password, binding.OpenClawPassword)
	binding.OpenClawDeviceToken = firstNonEmpty(result.Auth.DeviceToken, binding.OpenClawDeviceToken)
	return binding
}

func Resolve(options Options) (Result, error) {
	env := options.Env
	if env == nil {
		env = os.Getenv
	}
	for _, candidate := range candidateResolvers(options, env) {
		result, err := candidate()
		if err != nil {
			return Result{}, err
		}
		if result.Found() {
			return result, nil
		}
	}
	return Result{}, nil
}

func GatewayIsLoopback(gatewayURL string) bool {
	host, ok := gatewayHost(gatewayURL)
	if !ok {
		return false
	}
	hostOnly, _, err := net.SplitHostPort(host)
	if err != nil {
		return false
	}
	if strings.EqualFold(hostOnly, "localhost") {
		return true
	}
	ip := net.ParseIP(strings.Trim(hostOnly, "[]"))
	return ip != nil && ip.IsLoopback()
}

func GatewayReachable(gatewayURL string) bool {
	host, ok := gatewayHost(gatewayURL)
	if !ok {
		return false
	}
	conn, err := net.DialTimeout("tcp", host, 500*time.Millisecond)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

func gatewayHost(gatewayURL string) (string, bool) {
	if strings.TrimSpace(gatewayURL) == "" {
		gatewayURL = DefaultGatewayURL
	}
	parsed, err := url.Parse(gatewayURL)
	if err != nil {
		return "", false
	}
	host := parsed.Host
	if strings.TrimSpace(host) == "" {
		return "", false
	}
	if !strings.Contains(host, ":") {
		switch parsed.Scheme {
		case "wss", "https":
			host = net.JoinHostPort(host, "443")
		default:
			host = net.JoinHostPort(host, "80")
		}
	}
	return host, true
}

func candidateResolvers(options Options, env func(string) string) []func() (Result, error) {
	return []func() (Result, error){
		func() (Result, error) {
			return resultFromAuth(SourceConnectorBinding, runtime.OpenClawAuth{
				Token:       options.Binding.OpenClawGatewayToken,
				Password:    options.Binding.OpenClawPassword,
				DeviceToken: options.Binding.OpenClawDeviceToken,
			}), nil
		},
		func() (Result, error) {
			return resultFromAuth(SourceExplicit, runtime.OpenClawAuth{
				Token:       options.Explicit.Token,
				Password:    options.Explicit.Password,
				DeviceToken: options.Explicit.DeviceToken,
			}), nil
		},
		func() (Result, error) {
			return resultFromAuth(SourceEnvironment, authFromEnv(env)), nil
		},
		func() (Result, error) {
			return resultFromConfig(options, env)
		},
		func() (Result, error) {
			dir := stateDir(options, env)
			if dir == "" {
				return Result{}, nil
			}
			return resultFromEnvFile(SourceOpenClawEnvFile, filepath.Join(dir, ".env"))
		},
		func() (Result, error) {
			dir := homeDir(options, env)
			if dir == "" {
				return Result{}, nil
			}
			return resultFromEnvFile(SourceOpenClawEnvFile, filepath.Join(dir, ".config", "openclaw", "gateway.env"))
		},
		func() (Result, error) {
			dir := stateDir(options, env)
			if dir == "" {
				return Result{}, nil
			}
			return resultFromEnvFile(SourceServiceEnv, filepath.Join(dir, "service-env", "ai.openclaw.gateway.env"))
		},
		func() (Result, error) {
			dir := stateDir(options, env)
			if dir == "" {
				return Result{}, nil
			}
			return resultFromDeviceAuth(filepath.Join(dir, "identity", "device-auth.json"))
		},
	}
}

func resultFromAuth(source Source, auth runtime.OpenClawAuth) Result {
	return Result{
		Auth: runtime.OpenClawAuth{
			Token:       strings.TrimSpace(auth.Token),
			Password:    strings.TrimSpace(auth.Password),
			DeviceToken: strings.TrimSpace(auth.DeviceToken),
		},
		Source: source,
	}
}

func authFromEnv(env func(string) string) runtime.OpenClawAuth {
	return runtime.OpenClawAuth{
		Token:       env("OPENCLAW_GATEWAY_TOKEN"),
		Password:    env("OPENCLAW_GATEWAY_PASSWORD"),
		DeviceToken: env("OPENCLAW_GATEWAY_DEVICE_TOKEN"),
	}
}

func resultFromConfig(options Options, env func(string) string) (Result, error) {
	path := strings.TrimSpace(env("OPENCLAW_CONFIG_PATH"))
	if path == "" {
		dir := stateDir(options, env)
		if dir == "" {
			return Result{}, nil
		}
		path = filepath.Join(dir, "openclaw.json")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return Result{}, nil
	}
	var doc struct {
		Gateway struct {
			Auth struct {
				Token    string `json:"token"`
				Password string `json:"password"`
			} `json:"auth"`
		} `json:"gateway"`
		Env map[string]string `json:"env"`
	}
	err = json.Unmarshal(raw, &doc)
	if err != nil {
		return Result{}, fmt.Errorf("parse OpenClaw config auth: %w", err)
	}
	auth := runtime.OpenClawAuth{
		Token:       firstNonEmpty(doc.Gateway.Auth.Token, doc.Env["OPENCLAW_GATEWAY_TOKEN"]),
		Password:    firstNonEmpty(doc.Gateway.Auth.Password, doc.Env["OPENCLAW_GATEWAY_PASSWORD"]),
		DeviceToken: doc.Env["OPENCLAW_GATEWAY_DEVICE_TOKEN"],
	}
	return resultFromAuth(SourceOpenClawConfig, auth), nil
}

func resultFromEnvFile(source Source, path string) (Result, error) {
	values, err := readEnvFile(path)
	if err != nil {
		return Result{}, err
	}
	auth := runtime.OpenClawAuth{
		Token:       values["OPENCLAW_GATEWAY_TOKEN"],
		Password:    values["OPENCLAW_GATEWAY_PASSWORD"],
		DeviceToken: values["OPENCLAW_GATEWAY_DEVICE_TOKEN"],
	}
	return resultFromAuth(source, auth), nil
}

func resultFromDeviceAuth(path string) (Result, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Result{}, nil
	}
	var doc struct {
		Tokens map[string]struct {
			Token  string   `json:"token"`
			Role   string   `json:"role"`
			Scopes []string `json:"scopes"`
		} `json:"tokens"`
	}
	err = json.Unmarshal(raw, &doc)
	if err != nil {
		return Result{}, fmt.Errorf("parse OpenClaw device auth: %w", err)
	}
	operator := doc.Tokens["operator"]
	if strings.TrimSpace(operator.Role) != "operator" {
		return Result{}, nil
	}
	if !hasScope(operator.Scopes, "operator.read") || !hasScope(operator.Scopes, "operator.write") {
		return Result{}, nil
	}
	return resultFromAuth(SourceDeviceAuth, runtime.OpenClawAuth{DeviceToken: operator.Token}), nil
}

func readEnvFile(path string) (map[string]string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return map[string]string{}, nil
	}
	values := map[string]string{}
	for _, line := range strings.Split(string(raw), "\n") {
		key, value, ok := parseEnvLine(line)
		if ok {
			values[key] = value
		}
	}
	return values, nil
}

func parseEnvLine(line string) (string, string, bool) {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" || strings.HasPrefix(trimmed, "#") {
		return "", "", false
	}
	trimmed = strings.TrimPrefix(trimmed, "export ")
	key, value, ok := strings.Cut(trimmed, "=")
	if !ok {
		return "", "", false
	}
	key = strings.TrimSpace(key)
	if !strings.HasPrefix(key, "OPENCLAW_GATEWAY_") {
		return "", "", false
	}
	return key, trimEnvValue(value), true
}

func trimEnvValue(value string) string {
	trimmed := strings.TrimSpace(value)
	if len(trimmed) >= 2 {
		first := trimmed[0]
		last := trimmed[len(trimmed)-1]
		if (first == '\'' && last == '\'') || (first == '"' && last == '"') {
			return strings.TrimSpace(trimmed[1 : len(trimmed)-1])
		}
	}
	return trimmed
}

func stateDir(options Options, env func(string) string) string {
	if value := strings.TrimSpace(env("OPENCLAW_STATE_DIR")); value != "" {
		return value
	}
	if value := strings.TrimSpace(env("OPENCLAW_HOME")); value != "" {
		return filepath.Join(value, ".openclaw")
	}
	dir := homeDir(options, env)
	if dir == "" {
		return ""
	}
	return filepath.Join(dir, ".openclaw")
}

func homeDir(options Options, env func(string) string) string {
	if strings.TrimSpace(options.HomeDir) != "" {
		return strings.TrimSpace(options.HomeDir)
	}
	if value := strings.TrimSpace(env("HOME")); value != "" {
		return value
	}
	home, err := os.UserHomeDir()
	if err == nil {
		return home
	}
	return ""
}

func hasScope(scopes []string, want string) bool {
	for _, scope := range scopes {
		if strings.TrimSpace(scope) == want {
			return true
		}
	}
	return false
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
