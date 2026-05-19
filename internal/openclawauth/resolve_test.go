package openclawauth

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/personastack/personastack-connector/internal/config"
	"github.com/personastack/personastack-connector/internal/runtime"
)

func TestResolvePrefersBindingExplicitAndEnvironment(t *testing.T) {
	home := t.TempDir()
	result, err := Resolve(Options{
		HomeDir: home,
		Binding: config.Binding{
			OpenClawGatewayToken: "binding-token",
		},
		Explicit: Explicit{Token: "explicit-token"},
		Env: envMap(map[string]string{
			"OPENCLAW_GATEWAY_TOKEN": "env-token",
		}),
	})
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if result.Source != SourceConnectorBinding || result.Auth.Token != "binding-token" {
		t.Fatalf("Resolve() = %+v", result)
	}
}

func TestResolveReadsOpenClawConfigPath(t *testing.T) {
	home := t.TempDir()
	configPath := filepath.Join(home, "openclaw-test.json")
	writeFile(t, configPath, `{"gateway":{"auth":{"token":"config-token"}}}`)
	result, err := Resolve(Options{
		HomeDir: home,
		Env: envMap(map[string]string{
			"OPENCLAW_CONFIG_PATH": configPath,
		}),
	})
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if result.Source != SourceOpenClawConfig || result.Auth.Token != "config-token" {
		t.Fatalf("Resolve() = %+v", result)
	}
}

func TestResolveReadsStateEnvAndUbuntuFallback(t *testing.T) {
	for _, test := range []struct {
		name string
		path func(string) string
		want Source
	}{
		{
			name: "state env",
			path: func(home string) string {
				return filepath.Join(home, ".openclaw", ".env")
			},
			want: SourceOpenClawEnvFile,
		},
		{
			name: "ubuntu fallback",
			path: func(home string) string {
				return filepath.Join(home, ".config", "openclaw", "gateway.env")
			},
			want: SourceOpenClawEnvFile,
		},
		{
			name: "service env",
			path: func(home string) string {
				return filepath.Join(home, ".openclaw", "service-env", "ai.openclaw.gateway.env")
			},
			want: SourceServiceEnv,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			home := t.TempDir()
			writeFile(t, test.path(home), "export OPENCLAW_GATEWAY_TOKEN='file-token'\n")
			result, err := Resolve(Options{HomeDir: home, Env: envMap(nil)})
			if err != nil {
				t.Fatalf("Resolve() error = %v", err)
			}
			if result.Source != test.want || result.Auth.Token != "file-token" {
				t.Fatalf("Resolve() = %+v", result)
			}
		})
	}
}

func TestResolveReadsDeviceAuthAsDeviceToken(t *testing.T) {
	home := t.TempDir()
	writeFile(t, filepath.Join(home, ".openclaw", "identity", "device-auth.json"), `{
		"tokens": {
			"operator": {
				"token": "device-token",
				"role": "operator",
				"scopes": ["operator.read", "operator.write"]
			}
		}
	}`)
	result, err := Resolve(Options{HomeDir: home, Env: envMap(nil)})
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if result.Source != SourceDeviceAuth || result.Auth.DeviceToken != "device-token" {
		t.Fatalf("Resolve() = %+v", result)
	}
}

func TestResolveHonorsStateDirAndOpenClawHome(t *testing.T) {
	for _, test := range []struct {
		name string
		env  map[string]string
		path func(string) string
	}{
		{
			name: "state dir",
			env:  map[string]string{"OPENCLAW_STATE_DIR": "state"},
			path: func(home string) string {
				return filepath.Join(home, "state", ".env")
			},
		},
		{
			name: "openclaw home",
			env:  map[string]string{"OPENCLAW_HOME": "openclaw-home"},
			path: func(home string) string {
				return filepath.Join(home, "openclaw-home", ".openclaw", ".env")
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			home := t.TempDir()
			env := map[string]string{}
			for key, value := range test.env {
				env[key] = filepath.Join(home, value)
			}
			writeFile(t, test.path(home), "OPENCLAW_GATEWAY_PASSWORD=password-1\n")
			result, err := Resolve(Options{HomeDir: home, Env: envMap(env)})
			if err != nil {
				t.Fatalf("Resolve() error = %v", err)
			}
			if result.Auth.Password != "password-1" {
				t.Fatalf("Resolve() = %+v", result)
			}
		})
	}
}

func TestResolveDoesNotReadCurrentDirectoryWhenHomeMissing(t *testing.T) {
	t.Setenv("HOME", "")
	dir := t.TempDir()
	t.Chdir(dir)
	writeFile(t, filepath.Join(dir, ".openclaw", ".env"), "OPENCLAW_GATEWAY_TOKEN=cwd-token\n")

	result, err := Resolve(Options{Env: envMap(map[string]string{})})
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if result.Found() {
		t.Fatalf("Resolve() read cwd credential: %+v", result)
	}
}

func TestResultFound(t *testing.T) {
	if (Result{Auth: runtime.OpenClawAuth{}}).Found() {
		t.Fatal("empty auth reported found")
	}
	if !(Result{Auth: runtime.OpenClawAuth{DeviceToken: "token"}}).Found() {
		t.Fatal("device token auth reported missing")
	}
}

func envMap(values map[string]string) func(string) string {
	return func(key string) string {
		return values[key]
	}
}

func writeFile(t *testing.T, path string, content string) {
	t.Helper()
	err := os.MkdirAll(filepath.Dir(path), 0o700)
	if err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	err = os.WriteFile(path, []byte(content), 0o600)
	if err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
}
