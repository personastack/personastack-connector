package hermessetup

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/zalando/go-keyring"
	"gopkg.in/yaml.v3"
)

const (
	keyringServiceName = "personastack-connector"
	keyringHermesKey   = "hermes-api-server-key"

	defaultHermesHost = "127.0.0.1"
	defaultHermesPort = "8642"
	defaultHermesBase = "http://127.0.0.1:8642"
)

var (
	keyringGet = keyring.Get
	keyringSet = keyring.Set

	lookPath     = exec.LookPath
	startGateway = func(homeDir string, binary string) error {
		cmd := exec.Command(binary, "gateway")
		cmd.Env = append(os.Environ(), "HERMES_HOME="+strings.TrimSpace(homeDir))
		cmd.Dir = strings.TrimSpace(homeDir)
		cmd.Stdout = ioDiscard{}
		cmd.Stderr = ioDiscard{}
		return cmd.Start()
	}
)

func init() {
	if os.Getenv("PERSONASTACK_CONNECTOR_KEYRING_MOCK") == "1" {
		keyring.MockInit()
	}
}

type ioDiscard struct{}

func (ioDiscard) Write(p []byte) (int, error) { return len(p), nil }

type SetupState int

const (
	SetupStateReady SetupState = iota
	SetupStateNeedsGateway
	SetupStateNeedsEnv
	SetupStateNeedsConfig
)

func (state SetupState) String() string {
	switch state {
	case SetupStateReady:
		return "ready"
	case SetupStateNeedsGateway:
		return "needs_gateway"
	case SetupStateNeedsEnv:
		return "needs_env"
	case SetupStateNeedsConfig:
		return "needs_config"
	default:
		return "unknown"
	}
}

type SetupReport struct {
	State          SetupState
	Note           string
	EnvPath        string
	ConfigPath     string
	APIKey         string
	GatewayStarted bool
}

type Paths struct {
	HomeDir    string
	HermesHome string
	EnvPath    string
	ConfigPath string
}

func ResolvePaths(homeDir string, explicitHermesHome string) Paths {
	trimmedHome := strings.TrimSpace(homeDir)
	hermesHome := strings.TrimSpace(explicitHermesHome)
	if hermesHome == "" {
		hermesHome = strings.TrimSpace(os.Getenv("HERMES_HOME"))
	}
	if hermesHome == "" {
		hermesHome = filepath.Join(trimmedHome, ".hermes")
	}
	hermesHome = filepath.Clean(hermesHome)
	return Paths{
		HomeDir:    trimmedHome,
		HermesHome: hermesHome,
		EnvPath:    filepath.Join(hermesHome, ".env"),
		ConfigPath: filepath.Join(hermesHome, "config.yaml"),
	}
}

func EnsureAPISetup(homeDir string) (SetupReport, error) {
	return EnsureAPISetupForPaths(ResolvePaths(homeDir, ""))
}

func EnsureAPISetupForPaths(paths Paths) (SetupReport, error) {
	apiKey, err := resolveAPIKey(paths.EnvPath)
	if err != nil {
		return SetupReport{}, err
	}
	if err := StoreAPIKey(apiKey); err != nil {
		return SetupReport{}, err
	}
	envChanged, err := ensureEnvFile(paths.EnvPath, map[string]string{
		"API_SERVER_ENABLED": "true",
		"API_SERVER_HOST":    defaultHermesHost,
		"API_SERVER_PORT":    defaultHermesPort,
		"API_SERVER_KEY":     apiKey,
	})
	if err != nil {
		return SetupReport{}, err
	}
	report := SetupReport{
		State:      SetupStateReady,
		EnvPath:    paths.EnvPath,
		ConfigPath: paths.ConfigPath,
		APIKey:     apiKey,
	}
	if !envChanged {
		report.Note = "Hermes API env already configured"
	} else {
		report.Note = "Hermes API env configured"
	}
	return report, nil
}

func Diagnose(homeDir string) SetupReport {
	return DiagnoseForPaths(ResolvePaths(homeDir, ""))
}

func DiagnoseForPaths(paths Paths) SetupReport {
	report := SetupReport{
		State:      SetupStateNeedsGateway,
		EnvPath:    paths.EnvPath,
		ConfigPath: paths.ConfigPath,
	}
	env, err := loadEnvState(paths.EnvPath)
	if err != nil {
		report.State = SetupStateNeedsEnv
		report.Note = err.Error()
		return report
	}
	envProblems := missingHermesEnv(env)
	if len(envProblems) > 0 {
		report.State = SetupStateNeedsEnv
		report.Note = strings.Join(envProblems, "; ")
		return report
	}
	if !hasHermesConfig(paths.ConfigPath) {
		report.State = SetupStateNeedsConfig
		report.Note = "Hermes config missing mcp_servers entry"
		return report
	}
	if err := probeHermesHealth(defaultHermesBase); err != nil {
		report.State = SetupStateNeedsGateway
		report.Note = "Hermes API not listening on " + defaultHermesBase + "; run hermes gateway"
		return report
	}
	report.State = SetupStateReady
	report.Note = "Hermes API listening"
	return report
}

func LoadAPIKey() string {
	if key := loadStoredAPIKey(); key != "" {
		return key
	}
	return loadAPIKeyFromDefaultEnvFile()
}

func loadStoredAPIKey() string {
	if shouldUseEnvAPIKeyOnly() {
		return strings.TrimSpace(os.Getenv("HERMES_API_SERVER_KEY"))
	}
	if secret, err := keyringGet(keyringServiceName, keyringHermesKey); err == nil && strings.TrimSpace(secret) != "" {
		return strings.TrimSpace(secret)
	}
	if key := strings.TrimSpace(os.Getenv("HERMES_API_SERVER_KEY")); key != "" {
		return key
	}
	return ""
}

func StoreAPIKey(apiKey string) error {
	key := strings.TrimSpace(apiKey)
	if key == "" {
		return fmt.Errorf("Hermes API key required")
	}
	if shouldUseEnvAPIKeyOnly() {
		return nil
	}
	_ = keyringSet(keyringServiceName, keyringHermesKey, key)
	return nil
}

func shouldUseEnvAPIKeyOnly() bool {
	return os.Getenv("PERSONASTACK_CONNECTOR_FORCE_SECRET_FALLBACK") == "1"
}

func loadAPIKeyFromDefaultEnvFile() string {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	state, err := loadEnvState(ResolvePaths(homeDir, "").EnvPath)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(findEnvValue(state, "API_SERVER_KEY"))
}

func TryStartGateway(homeDir string) (bool, error) {
	return TryStartGatewayForPaths(ResolvePaths(homeDir, ""))
}

func TryStartGatewayForPaths(paths Paths) (bool, error) {
	if os.Getenv("PERSONASTACK_CONNECTOR_DISABLE_HERMES_GATEWAY_START") == "1" {
		return false, nil
	}
	if err := probeHermesHealth(defaultHermesBase); err == nil {
		return false, nil
	}
	binary, err := lookPath("hermes")
	if err != nil {
		return false, nil
	}
	if err := startGateway(paths.HermesHome, binary); err != nil {
		return false, fmt.Errorf("start Hermes gateway: %w", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := probeHermesHealth(defaultHermesBase); err == nil {
			return true, nil
		}
		time.Sleep(250 * time.Millisecond)
	}
	return true, nil
}

func ensureEnvFile(path string, values map[string]string) (bool, error) {
	state, err := loadEnvState(path)
	if err != nil {
		return false, err
	}
	if state.matches(values) {
		return false, nil
	}
	state.upsert(values)
	if err := ensureOwnerOnlyFile(path, state.write()); err != nil {
		return false, err
	}
	return true, nil
}

func resolveAPIKey(envPath string) (string, error) {
	state, err := loadEnvState(envPath)
	if err != nil {
		return "", err
	}
	if key := strings.TrimSpace(findEnvValue(state, "API_SERVER_KEY")); key != "" {
		return key, nil
	}
	if key := loadStoredAPIKey(); key != "" {
		return key, nil
	}
	return generateAPIKey()
}

func missingHermesEnv(state envState) []string {
	required := []string{
		"API_SERVER_ENABLED=true",
		"API_SERVER_HOST=127.0.0.1",
		"API_SERVER_PORT=8642",
		"API_SERVER_KEY",
	}
	missing := []string{}
	for _, item := range required {
		key, value, hasValue := strings.Cut(item, "=")
		current := strings.TrimSpace(findEnvValue(state, key))
		if !hasValue {
			if current == "" {
				missing = append(missing, key)
			}
			continue
		}
		if current == "" || current != value {
			missing = append(missing, key)
		}
	}
	if !shouldUseEnvAPIKeyOnly() {
		if secret, err := keyringGet(keyringServiceName, keyringHermesKey); err != nil || strings.TrimSpace(secret) == "" {
			missing = append(missing, "keyring")
		}
	}
	return missing
}

func findEnvValue(state envState, key string) string {
	index, ok := state.index[key]
	if !ok || index >= len(state.lines) {
		return ""
	}
	return strings.TrimSpace(state.lines[index].value)
}

func hasHermesConfig(path string) bool {
	raw, err := os.ReadFile(path)
	if err != nil || len(raw) == 0 {
		return false
	}
	var root map[string]any
	if err := yaml.Unmarshal(raw, &root); err != nil {
		return false
	}
	servers, ok := root["mcp_servers"].(map[string]any)
	return ok && len(servers) > 0
}

func probeHermesHealth(baseURL string) error {
	client := &http.Client{Timeout: 2 * time.Second}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, baseURL+"/health", nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode >= 300 {
		_ = resp.Body.Close()
		return fmt.Errorf("health status %d", resp.StatusCode)
	}
	_ = resp.Body.Close()
	return nil
}

func generateAPIKey() (string, error) {
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate Hermes API key: %w", err)
	}
	return hex.EncodeToString(raw[:]), nil
}
