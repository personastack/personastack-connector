package config

import "testing"

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
