package hermessetup

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestEnsureAPISetupMergesEnvAndStoresKey(t *testing.T) {
	t.Setenv("PERSONASTACK_CONNECTOR_DISABLE_HERMES_GATEWAY_START", "1")
	secrets := map[string]string{}
	originalGet := keyringGet
	originalSet := keyringSet
	t.Cleanup(func() {
		keyringGet = originalGet
		keyringSet = originalSet
	})
	keyringGet = func(service string, user string) (string, error) {
		if value, ok := secrets[service+":"+user]; ok {
			return value, nil
		}
		return "", os.ErrNotExist
	}
	keyringSet = func(service string, user string, password string) error {
		secrets[service+":"+user] = password
		return nil
	}

	homeDir := t.TempDir()
	envPath := filepath.Join(homeDir, ".hermes", ".env")
	if err := os.MkdirAll(filepath.Dir(envPath), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(envPath, []byte("OTHER=keep\nAPI_SERVER_PORT=9999\n"), 0o600); err != nil {
		t.Fatalf("write env: %v", err)
	}

	report, err := EnsureAPISetup(homeDir)
	if err != nil {
		t.Fatalf("EnsureAPISetup() error = %v", err)
	}
	if report.State != SetupStateReady {
		t.Fatalf("report.State = %s note=%s", report.State, report.Note)
	}
	raw, err := os.ReadFile(envPath)
	if err != nil {
		t.Fatalf("read env: %v", err)
	}
	text := string(raw)
	for _, want := range []string{
		"OTHER=keep",
		"API_SERVER_ENABLED=true",
		"API_SERVER_HOST=127.0.0.1",
		"API_SERVER_PORT=8642",
		"API_SERVER_KEY=",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("env missing %q:\n%s", want, text)
		}
	}
	if len(secrets) != 1 {
		t.Fatalf("expected keyring secret, got %+v", secrets)
	}
	if report.APIKey == "" {
		t.Fatalf("expected API key in report")
	}
}

func TestEnsureAPISetupSkipsKeyringWhenFallbackForced(t *testing.T) {
	t.Setenv("PERSONASTACK_CONNECTOR_DISABLE_HERMES_GATEWAY_START", "1")
	t.Setenv("PERSONASTACK_CONNECTOR_FORCE_SECRET_FALLBACK", "1")
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	originalSet := keyringSet
	t.Cleanup(func() {
		keyringSet = originalSet
	})
	keyringSet = func(service string, user string, password string) error {
		return os.ErrPermission
	}

	homeDir := t.TempDir()
	envPath := filepath.Join(homeDir, ".hermes", ".env")
	if err := os.MkdirAll(filepath.Dir(envPath), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(envPath, []byte("API_SERVER_KEY=env-key\n"), 0o600); err != nil {
		t.Fatalf("write env: %v", err)
	}

	report, err := EnsureAPISetup(homeDir)
	if err != nil {
		t.Fatalf("EnsureAPISetup() error = %v", err)
	}
	if report.APIKey != "env-key" {
		t.Fatalf("report.APIKey = %q", report.APIKey)
	}
	raw, err := os.ReadFile(envPath)
	if err != nil {
		t.Fatalf("read env: %v", err)
	}
	if !strings.Contains(string(raw), "API_SERVER_KEY=env-key") {
		t.Fatalf("env key not preserved:\n%s", string(raw))
	}
}

func TestEnsureAPISetupContinuesWhenKeyringUnavailable(t *testing.T) {
	t.Setenv("PERSONASTACK_CONNECTOR_DISABLE_HERMES_GATEWAY_START", "1")
	originalSet := keyringSet
	t.Cleanup(func() {
		keyringSet = originalSet
	})
	keyringSet = func(service string, user string, password string) error {
		return os.ErrPermission
	}

	homeDir := t.TempDir()
	report, err := EnsureAPISetup(homeDir)
	if err != nil {
		t.Fatalf("EnsureAPISetup() error = %v", err)
	}
	if report.APIKey == "" {
		t.Fatalf("expected API key in report")
	}
	raw, err := os.ReadFile(filepath.Join(homeDir, ".hermes", ".env"))
	if err != nil {
		t.Fatalf("read env: %v", err)
	}
	if !strings.Contains(string(raw), "API_SERVER_KEY=") {
		t.Fatalf("env key missing:\n%s", string(raw))
	}
}

func TestEnsureAPISetupFallbackDoesNotReadWrongHomeEnv(t *testing.T) {
	t.Setenv("PERSONASTACK_CONNECTOR_DISABLE_HERMES_GATEWAY_START", "1")
	t.Setenv("PERSONASTACK_CONNECTOR_FORCE_SECRET_FALLBACK", "1")
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	wrongHome := t.TempDir()
	t.Setenv("HOME", wrongHome)
	if err := os.MkdirAll(filepath.Join(wrongHome, ".hermes"), 0o700); err != nil {
		t.Fatalf("mkdir wrong home env: %v", err)
	}
	if err := os.WriteFile(filepath.Join(wrongHome, ".hermes", ".env"), []byte("API_SERVER_KEY=wrong-home-key\n"), 0o600); err != nil {
		t.Fatalf("write wrong home env: %v", err)
	}

	homeDir := t.TempDir()
	report, err := EnsureAPISetup(homeDir)
	if err != nil {
		t.Fatalf("EnsureAPISetup() error = %v", err)
	}
	if report.APIKey == "" || report.APIKey == "wrong-home-key" {
		t.Fatalf("report.APIKey = %q", report.APIKey)
	}
}

func TestEnsureAPISetupUsesResolvedHermesHome(t *testing.T) {
	t.Setenv("PERSONASTACK_CONNECTOR_DISABLE_HERMES_GATEWAY_START", "1")
	t.Setenv("PERSONASTACK_CONNECTOR_FORCE_SECRET_FALLBACK", "1")
	homeDir := t.TempDir()
	hermesHome := filepath.Join(homeDir, ".hermes", "profiles", "homeschool")
	paths := ResolvePaths(filepath.Join(hermesHome, "home"), hermesHome)

	report, err := EnsureAPISetupForPaths(paths)
	if err != nil {
		t.Fatalf("EnsureAPISetupForPaths() error = %v", err)
	}
	if report.EnvPath != filepath.Join(hermesHome, ".env") {
		t.Fatalf("EnvPath = %q", report.EnvPath)
	}
	if _, err := os.Stat(filepath.Join(hermesHome, ".env")); err != nil {
		t.Fatalf("Hermes profile env not written: %v", err)
	}
	if _, err := os.Stat(filepath.Join(hermesHome, "home", ".hermes", ".env")); !os.IsNotExist(err) {
		t.Fatalf("nested HOME Hermes env exists or stat failed: %v", err)
	}
}

func TestDiagnoseReportsMissingHermesSetup(t *testing.T) {
	t.Setenv("PERSONASTACK_CONNECTOR_DISABLE_HERMES_GATEWAY_START", "1")
	homeDir := t.TempDir()
	report := Diagnose(homeDir)
	if report.State != SetupStateNeedsEnv {
		t.Fatalf("report.State = %s note=%s", report.State, report.Note)
	}
}
