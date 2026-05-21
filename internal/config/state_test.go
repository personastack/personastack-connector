package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestExternalAgentKindString(t *testing.T) {
	if ExternalAgentKindHermes.String() != "hermes" {
		t.Fatalf("ExternalAgentKindHermes.String = %q", ExternalAgentKindHermes.String())
	}
	if ExternalAgentKindOpenClaw.String() != "openclaw" {
		t.Fatalf("ExternalAgentKindOpenClaw.String = %q", ExternalAgentKindOpenClaw.String())
	}
}

func TestFileStoreSavesBinding(t *testing.T) {
	path := t.TempDir() + "/state.json"
	store := NewFileStore(path)
	binding := Binding{
		ConnectionID:       "conn-1",
		PersonaID:          "persona-1",
		BridgeCredentialID: "cred-1",
	}
	if err := store.SaveBinding(binding); err != nil {
		t.Fatalf("save binding: %v", err)
	}
	loaded, ok := store.Binding("conn-1")
	if !ok {
		t.Fatalf("expected binding")
	}
	if loaded.PersonaID != "persona-1" || loaded.BridgeCredentialID != "cred-1" {
		t.Fatalf("unexpected binding: %+v", loaded)
	}
}

func TestFileStoreMovesSecretsToKeyring(t *testing.T) {
	secrets := map[string]string{}
	originalGet := keyringGet
	originalSet := keyringSet
	originalDelete := keyringDelete
	keyringGet = func(service string, user string) (string, error) {
		value, ok := secrets[service+":"+user]
		if !ok {
			return "", os.ErrNotExist
		}
		return value, nil
	}
	keyringSet = func(service string, user string, password string) error {
		secrets[service+":"+user] = password
		return nil
	}
	keyringDelete = func(service string, user string) error {
		delete(secrets, service+":"+user)
		return nil
	}
	t.Cleanup(func() {
		keyringGet = originalGet
		keyringSet = originalSet
		keyringDelete = originalDelete
	})

	path := t.TempDir() + "/state.json"
	store := NewFileStore(path)
	binding := Binding{
		ConnectionID:         "conn-1",
		PersonaID:            "persona-1",
		BridgePrivateKey:     "bridge-secret",
		OpenClawGatewayToken: "openclaw-token",
		OpenClawPassword:     "openclaw-password",
		OpenClawDeviceToken:  "openclaw-device",
		PersonaMCPToken:      "mcp-token",
		ActiveRunID:          "run-1",
		ActiveAssignmentID:   "assignment-1",
		HasBridgeSecret:      true,
		HasOpenClawToken:     true,
		HasOpenClawPassword:  true,
		HasOpenClawDevice:    true,
		HasPersonaMCPToken:   true,
	}
	if err := store.SaveBinding(binding); err != nil {
		t.Fatalf("save binding: %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read state: %v", err)
	}
	if strings.Contains(string(raw), "bridge-secret") || strings.Contains(string(raw), "mcp-token") || strings.Contains(string(raw), "openclaw-token") || strings.Contains(string(raw), "openclaw-password") || strings.Contains(string(raw), "openclaw-device") {
		t.Fatalf("state file leaked secret: %s", string(raw))
	}
	loaded, ok := store.Binding("conn-1")
	if !ok {
		t.Fatalf("expected binding")
	}
	if loaded.BridgePrivateKey != "bridge-secret" || loaded.PersonaMCPToken != "mcp-token" || loaded.ActiveAssignmentID != "assignment-1" || loaded.OpenClawGatewayToken != "openclaw-token" || loaded.OpenClawPassword != "openclaw-password" || loaded.OpenClawDeviceToken != "openclaw-device" {
		t.Fatalf("expected keyring-backed secrets, got %+v", loaded)
	}
	loaded.ActiveRunID = ""
	loaded.ActiveAssignmentID = ""
	if err := store.SaveBinding(loaded); err != nil {
		t.Fatalf("clear active run: %v", err)
	}
	if _, ok := secrets[keyringService+":conn-1:active-run-mcp-token"]; ok {
		t.Fatalf("expected active run token to be deleted")
	}
}

func TestFileStoreUsesEncryptedFallbackWhenKeyringUnavailable(t *testing.T) {
	t.Setenv("HOME", t.TempDir())

	originalGet := keyringGet
	originalSet := keyringSet
	originalDelete := keyringDelete
	keyringGet = func(service string, user string) (string, error) {
		return "", os.ErrNotExist
	}
	keyringSet = func(service string, user string, password string) error {
		return os.ErrPermission
	}
	keyringDelete = func(service string, user string) error {
		return os.ErrNotExist
	}
	t.Cleanup(func() {
		keyringGet = originalGet
		keyringSet = originalSet
		keyringDelete = originalDelete
	})

	path := t.TempDir() + "/state.json"
	store := NewFileStore(path)
	binding := Binding{
		ConnectionID:       "conn-1",
		PersonaID:          "persona-1",
		BridgePrivateKey:   "fallback-bridge-secret",
		PersonaMCPToken:    "fallback-mcp-token",
		HasBridgeSecret:    true,
		HasPersonaMCPToken: true,
	}
	if err := store.SaveBinding(binding); err != nil {
		t.Fatalf("save binding: %v", err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read state: %v", err)
	}
	if strings.Contains(string(raw), "fallback-bridge-secret") || strings.Contains(string(raw), "fallback-mcp-token") {
		t.Fatalf("state file leaked secret: %s", string(raw))
	}

	configDir, err := os.UserConfigDir()
	if err != nil {
		t.Fatalf("user config dir: %v", err)
	}
	secretPath := filepath.Join(configDir, "personastack", "connector", "secrets.enc")
	secretRaw, err := os.ReadFile(secretPath)
	if err != nil {
		t.Fatalf("read fallback secret store: %v", err)
	}
	if strings.Contains(string(secretRaw), "fallback-bridge-secret") || strings.Contains(string(secretRaw), "fallback-mcp-token") {
		t.Fatalf("fallback secret store leaked plaintext: %s", string(secretRaw))
	}

	loaded, ok := store.Binding("conn-1")
	if !ok {
		t.Fatalf("expected binding")
	}
	if loaded.BridgePrivateKey != "fallback-bridge-secret" || loaded.PersonaMCPToken != "fallback-mcp-token" {
		t.Fatalf("expected fallback secrets, got %+v", loaded)
	}

	if err := store.DeleteBinding("conn-1"); err != nil {
		t.Fatalf("delete binding: %v", err)
	}
	if _, ok := store.Binding("conn-1"); ok {
		t.Fatalf("expected binding deleted")
	}
}

func TestFileStoreUsesEncryptedFallbackOnDarwinKeyringFailure(t *testing.T) {
	t.Setenv("HOME", t.TempDir())

	originalGet := keyringGet
	originalSet := keyringSet
	originalDelete := keyringDelete
	keyringGet = func(service string, user string) (string, error) {
		return "", os.ErrPermission
	}
	keyringSet = func(service string, user string, password string) error {
		return os.ErrPermission
	}
	keyringDelete = func(service string, user string) error {
		return os.ErrPermission
	}
	t.Cleanup(func() {
		keyringGet = originalGet
		keyringSet = originalSet
		keyringDelete = originalDelete
	})

	path := filepath.Join(t.TempDir(), "state.json")
	store := NewFileStore(path)
	binding := Binding{
		ConnectionID:       "conn-1",
		PersonaID:          "persona-1",
		BridgePrivateKey:   "fallback-bridge-secret",
		PersonaMCPToken:    "fallback-mcp-token",
		HasBridgeSecret:    true,
		HasPersonaMCPToken: true,
	}
	if err := store.SaveBinding(binding); err != nil {
		t.Fatalf("save binding: %v", err)
	}

	loaded, ok := store.Binding("conn-1")
	if !ok {
		t.Fatalf("expected binding")
	}
	if loaded.BridgePrivateKey != "fallback-bridge-secret" || loaded.PersonaMCPToken != "fallback-mcp-token" {
		t.Fatalf("expected fallback secrets, got %+v", loaded)
	}
}

func TestFileStoreKeepsEncryptedFallbackWhenKeyringWriteSucceeds(t *testing.T) {
	t.Setenv("HOME", t.TempDir())

	secrets := map[string]string{}
	allowKeyringRead := true
	originalGet := keyringGet
	originalSet := keyringSet
	originalDelete := keyringDelete
	keyringGet = func(service string, user string) (string, error) {
		if !allowKeyringRead {
			return "", os.ErrPermission
		}
		value, ok := secrets[service+":"+user]
		if !ok {
			return "", os.ErrNotExist
		}
		return value, nil
	}
	keyringSet = func(service string, user string, password string) error {
		secrets[service+":"+user] = password
		return nil
	}
	keyringDelete = func(service string, user string) error {
		delete(secrets, service+":"+user)
		return nil
	}
	t.Cleanup(func() {
		keyringGet = originalGet
		keyringSet = originalSet
		keyringDelete = originalDelete
	})

	path := filepath.Join(t.TempDir(), "state.json")
	store := NewFileStore(path)
	binding := Binding{
		ConnectionID:       "conn-1",
		PersonaID:          "persona-1",
		BridgePrivateKey:   "fallback-bridge-secret",
		PersonaMCPToken:    "fallback-mcp-token",
		HasBridgeSecret:    true,
		HasPersonaMCPToken: true,
	}
	if err := store.SaveBinding(binding); err != nil {
		t.Fatalf("save binding: %v", err)
	}

	allowKeyringRead = false
	loaded, ok := store.Binding("conn-1")
	if !ok {
		t.Fatalf("expected binding")
	}
	if loaded.BridgePrivateKey != "fallback-bridge-secret" || loaded.PersonaMCPToken != "fallback-mcp-token" {
		t.Fatalf("expected fallback secrets when keyring read fails, got %+v", loaded)
	}
}

func TestFileStoreForcedFallbackDoesNotDependOnReadableKeyring(t *testing.T) {
	t.Setenv("PERSONASTACK_CONNECTOR_FORCE_SECRET_FALLBACK", "1")
	t.Setenv("HOME", t.TempDir())

	secrets := map[string]string{}
	originalGet := keyringGet
	originalSet := keyringSet
	originalDelete := keyringDelete
	keyringGet = func(service string, user string) (string, error) {
		return "", os.ErrPermission
	}
	keyringSet = func(service string, user string, password string) error {
		secrets[service+":"+user] = password
		return nil
	}
	keyringDelete = func(service string, user string) error {
		delete(secrets, service+":"+user)
		return nil
	}
	t.Cleanup(func() {
		keyringGet = originalGet
		keyringSet = originalSet
		keyringDelete = originalDelete
	})

	path := filepath.Join(t.TempDir(), "state.json")
	store := NewFileStore(path)
	binding := Binding{
		ConnectionID:       "conn-1",
		PersonaID:          "persona-1",
		BridgePrivateKey:   "fallback-bridge-secret",
		PersonaMCPToken:    "fallback-mcp-token",
		HasBridgeSecret:    true,
		HasPersonaMCPToken: true,
	}
	if err := store.SaveBinding(binding); err != nil {
		t.Fatalf("save binding: %v", err)
	}
	if len(secrets) != 0 {
		t.Fatalf("forced fallback should not write keyring secrets: %+v", secrets)
	}

	loaded, ok := store.Binding("conn-1")
	if !ok {
		t.Fatalf("expected binding")
	}
	if loaded.BridgePrivateKey != "fallback-bridge-secret" || loaded.PersonaMCPToken != "fallback-mcp-token" {
		t.Fatalf("expected fallback secrets, got %+v", loaded)
	}
}

func TestFileStoreForcedFallbackDeletesFallbackAndBestEffortKeyringSecrets(t *testing.T) {
	t.Setenv("PERSONASTACK_CONNECTOR_FORCE_SECRET_FALLBACK", "1")
	t.Setenv("HOME", t.TempDir())

	secrets := map[string]string{
		keyringService + ":conn-1:persona-mcp-token": "stale-keyring-token",
	}
	originalGet := keyringGet
	originalSet := keyringSet
	originalDelete := keyringDelete
	keyringGet = func(service string, user string) (string, error) {
		value, ok := secrets[service+":"+user]
		if !ok {
			return "", os.ErrNotExist
		}
		return value, nil
	}
	keyringSet = func(service string, user string, password string) error {
		secrets[service+":"+user] = password
		return nil
	}
	keyringDelete = func(service string, user string) error {
		delete(secrets, service+":"+user)
		return nil
	}
	t.Cleanup(func() {
		keyringGet = originalGet
		keyringSet = originalSet
		keyringDelete = originalDelete
	})

	path := filepath.Join(t.TempDir(), "state.json")
	store := NewFileStore(path)
	binding := Binding{
		ConnectionID:       "conn-1",
		PersonaID:          "persona-1",
		PersonaMCPToken:    "fallback-mcp-token",
		HasPersonaMCPToken: true,
	}
	if err := store.SaveBinding(binding); err != nil {
		t.Fatalf("save binding: %v", err)
	}
	if err := store.DeleteBinding("conn-1"); err != nil {
		t.Fatalf("delete binding: %v", err)
	}
	if _, ok := secrets[keyringService+":conn-1:persona-mcp-token"]; ok {
		t.Fatalf("forced fallback delete left stale keyring secret: %+v", secrets)
	}
}

func TestFileStoreKeyringSecretWinsOverExistingFallbackWhenFallbackNotForced(t *testing.T) {
	t.Setenv("PERSONASTACK_CONNECTOR_FORCE_SECRET_FALLBACK", "1")
	t.Setenv("HOME", t.TempDir())

	secrets := map[string]string{}
	originalGet := keyringGet
	originalSet := keyringSet
	originalDelete := keyringDelete
	keyringGet = func(service string, user string) (string, error) {
		value, ok := secrets[service+":"+user]
		if !ok {
			return "", os.ErrNotExist
		}
		return value, nil
	}
	keyringSet = func(service string, user string, password string) error {
		secrets[service+":"+user] = password
		return nil
	}
	keyringDelete = func(service string, user string) error {
		delete(secrets, service+":"+user)
		return nil
	}
	t.Cleanup(func() {
		keyringGet = originalGet
		keyringSet = originalSet
		keyringDelete = originalDelete
	})

	path := filepath.Join(t.TempDir(), "state.json")
	store := NewFileStore(path)
	binding := Binding{
		ConnectionID:       "conn-1",
		PersonaID:          "persona-1",
		PersonaMCPToken:    "fallback-mcp-token",
		HasPersonaMCPToken: true,
	}
	if err := store.SaveBinding(binding); err != nil {
		t.Fatalf("save binding: %v", err)
	}
	secrets[keyringService+":conn-1:persona-mcp-token"] = "keyring-token"
	t.Setenv("PERSONASTACK_CONNECTOR_FORCE_SECRET_FALLBACK", "0")

	loaded, ok := store.Binding("conn-1")
	if !ok {
		t.Fatalf("expected binding")
	}
	if loaded.PersonaMCPToken != "keyring-token" {
		t.Fatalf("expected keyring secret after fallback disabled, got %+v", loaded)
	}
}

func TestFileStoreDeleteBindingDeletesSecrets(t *testing.T) {
	secrets := map[string]string{}
	originalGet := keyringGet
	originalSet := keyringSet
	originalDelete := keyringDelete
	keyringGet = func(service string, user string) (string, error) {
		value, ok := secrets[service+":"+user]
		if !ok {
			return "", os.ErrNotExist
		}
		return value, nil
	}
	keyringSet = func(service string, user string, password string) error {
		secrets[service+":"+user] = password
		return nil
	}
	keyringDelete = func(service string, user string) error {
		delete(secrets, service+":"+user)
		return nil
	}
	t.Cleanup(func() {
		keyringGet = originalGet
		keyringSet = originalSet
		keyringDelete = originalDelete
	})

	path := t.TempDir() + "/state.json"
	store := NewFileStore(path)
	binding := Binding{
		ConnectionID:         "conn-1",
		PersonaID:            "persona-1",
		BridgePrivateKey:     "bridge-secret",
		OpenClawGatewayToken: "openclaw-token",
		OpenClawPassword:     "openclaw-password",
		OpenClawDeviceToken:  "openclaw-device",
		PersonaMCPToken:      "mcp-token",
		HasBridgeSecret:      true,
		HasOpenClawToken:     true,
		HasOpenClawPassword:  true,
		HasOpenClawDevice:    true,
		HasPersonaMCPToken:   true,
	}
	if err := store.SaveBinding(binding); err != nil {
		t.Fatalf("save binding: %v", err)
	}
	if len(secrets) != 5 {
		t.Fatalf("expected secrets before delete: %+v", secrets)
	}
	if err := store.DeleteBinding("conn-1"); err != nil {
		t.Fatalf("delete binding: %v", err)
	}
	if _, ok := store.Binding("conn-1"); ok {
		t.Fatalf("expected binding deleted")
	}
	if len(secrets) != 0 {
		t.Fatalf("expected secrets deleted: %+v", secrets)
	}
}

func TestFileStoreReplacesExistingBindingAndDeletesOldSecrets(t *testing.T) {
	secrets := map[string]string{}
	originalGet := keyringGet
	originalSet := keyringSet
	originalDelete := keyringDelete
	keyringGet = func(service string, user string) (string, error) {
		value, ok := secrets[service+":"+user]
		if !ok {
			return "", os.ErrNotExist
		}
		return value, nil
	}
	keyringSet = func(service string, user string, password string) error {
		secrets[service+":"+user] = password
		return nil
	}
	keyringDelete = func(service string, user string) error {
		delete(secrets, service+":"+user)
		return nil
	}
	t.Cleanup(func() {
		keyringGet = originalGet
		keyringSet = originalSet
		keyringDelete = originalDelete
	})

	path := t.TempDir() + "/state.json"
	store := NewFileStore(path)
	first := Binding{
		ConnectionID:       "conn-1",
		PersonaID:          "persona-1",
		PersonaMCPToken:    "mcp-token-1",
		HasPersonaMCPToken: true,
	}
	second := Binding{
		ConnectionID:       "conn-2",
		PersonaID:          "persona-2",
		PersonaMCPToken:    "mcp-token-2",
		HasPersonaMCPToken: true,
	}
	if err := store.SaveBinding(first); err != nil {
		t.Fatalf("save first: %v", err)
	}
	if err := store.SaveBinding(second); err != nil {
		t.Fatalf("save second: %v", err)
	}
	if _, ok := store.Binding("conn-1"); ok {
		t.Fatalf("expected first binding to be replaced")
	}
	loadedSecond, ok := store.Binding("conn-2")
	if !ok {
		t.Fatalf("expected second binding")
	}
	if loadedSecond.PersonaMCPToken != "mcp-token-2" {
		t.Fatalf("second binding secrets wrong: %+v", loadedSecond)
	}
	if _, ok := secrets[keyringService+":conn-1:persona-mcp-token"]; ok {
		t.Fatalf("expected first persona MCP token deleted")
	}
	if _, ok := secrets[keyringService+":conn-1:active-run-mcp-token"]; ok {
		t.Fatalf("expected first active run MCP token deleted")
	}
	bindings := store.ListBindings()
	if len(bindings) != 1 || bindings[0].ConnectionID != "conn-2" {
		t.Fatalf("expected one active binding, got %+v", bindings)
	}
}

func TestFileStorePreservesOldBindingSecretsWhenReplacementWriteFails(t *testing.T) {
	secrets := map[string]string{}
	originalGet := keyringGet
	originalSet := keyringSet
	originalDelete := keyringDelete
	keyringGet = func(service string, user string) (string, error) {
		value, ok := secrets[service+":"+user]
		if !ok {
			return "", os.ErrNotExist
		}
		return value, nil
	}
	keyringSet = func(service string, user string, password string) error {
		secrets[service+":"+user] = password
		return nil
	}
	keyringDelete = func(service string, user string) error {
		delete(secrets, service+":"+user)
		return nil
	}
	t.Cleanup(func() {
		keyringGet = originalGet
		keyringSet = originalSet
		keyringDelete = originalDelete
	})

	path := filepath.Join(t.TempDir(), "state.json")
	store := NewFileStore(path)
	first := Binding{
		ConnectionID:       "conn-1",
		PersonaID:          "persona-1",
		PersonaMCPToken:    "mcp-token-1",
		HasPersonaMCPToken: true,
	}
	second := Binding{
		ConnectionID:       "conn-2",
		PersonaID:          "persona-2",
		PersonaMCPToken:    "mcp-token-2",
		HasPersonaMCPToken: true,
	}
	if err := store.SaveBinding(first); err != nil {
		t.Fatalf("save first: %v", err)
	}
	if err := os.Chmod(path, 0o400); err != nil {
		t.Fatalf("chmod state file: %v", err)
	}
	t.Cleanup(func() {
		_ = os.Chmod(path, 0o600)
	})
	if err := store.SaveBinding(second); err == nil {
		t.Fatalf("expected replacement write failure")
	}
	if secrets[keyringService+":conn-1:persona-mcp-token"] != "mcp-token-1" {
		t.Fatalf("expected first binding secret preserved: %+v", secrets)
	}
	if _, ok := secrets[keyringService+":conn-2:persona-mcp-token"]; ok {
		t.Fatalf("expected failed replacement secret deleted: %+v", secrets)
	}
	loaded, ok := store.Binding("conn-1")
	if !ok || loaded.PersonaMCPToken != "mcp-token-1" {
		t.Fatalf("expected first binding to remain readable, got ok=%t binding=%+v", ok, loaded)
	}
}
