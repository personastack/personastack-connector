package config

import (
	"os"
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
		ActiveRunMCPToken:    "run-mcp-token",
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
	if strings.Contains(string(raw), "bridge-secret") || strings.Contains(string(raw), "mcp-token") || strings.Contains(string(raw), "run-mcp-token") || strings.Contains(string(raw), "openclaw-token") || strings.Contains(string(raw), "openclaw-password") || strings.Contains(string(raw), "openclaw-device") {
		t.Fatalf("state file leaked secret: %s", string(raw))
	}
	loaded, ok := store.Binding("conn-1")
	if !ok {
		t.Fatalf("expected binding")
	}
	if loaded.BridgePrivateKey != "bridge-secret" || loaded.PersonaMCPToken != "mcp-token" || loaded.ActiveAssignmentID != "assignment-1" || loaded.ActiveRunMCPToken != "run-mcp-token" || loaded.OpenClawGatewayToken != "openclaw-token" || loaded.OpenClawPassword != "openclaw-password" || loaded.OpenClawDeviceToken != "openclaw-device" {
		t.Fatalf("expected keyring-backed secrets, got %+v", loaded)
	}
	loaded.ActiveRunID = ""
	loaded.ActiveAssignmentID = ""
	loaded.ActiveRunMCPToken = ""
	loaded.HasActiveRunMCPToken = false
	if err := store.SaveBinding(loaded); err != nil {
		t.Fatalf("clear active token: %v", err)
	}
	if _, ok := secrets[keyringService+":conn-1:active-run-mcp-token"]; ok {
		t.Fatalf("expected active run token to be deleted")
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
		ActiveRunMCPToken:    "run-mcp-token",
		HasBridgeSecret:      true,
		HasOpenClawToken:     true,
		HasOpenClawPassword:  true,
		HasOpenClawDevice:    true,
		HasPersonaMCPToken:   true,
		HasActiveRunMCPToken: true,
	}
	if err := store.SaveBinding(binding); err != nil {
		t.Fatalf("save binding: %v", err)
	}
	if len(secrets) != 6 {
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
