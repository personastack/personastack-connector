package config

import (
	"fmt"
	"strings"

	"github.com/zalando/go-keyring"
)

const keyringService = "personastack-connector"

var (
	keyringGet    = keyring.Get
	keyringSet    = keyring.Set
	keyringDelete = keyring.Delete
)

func storeBindingSecrets(binding Binding) (Binding, error) {
	connectionID := strings.TrimSpace(string(binding.ConnectionID))
	if connectionID == "" {
		return binding, nil
	}
	if strings.TrimSpace(binding.BridgePrivateKey) != "" {
		err := keyringSet(keyringService, bindingSecretKey(connectionID, "bridge-private-key"), binding.BridgePrivateKey)
		if err != nil {
			return Binding{}, fmt.Errorf("store bridge private key: %w", err)
		}
		binding.BridgePrivateKey = ""
		binding.HasBridgeSecret = true
	}
	if strings.TrimSpace(binding.PersonaMCPToken) != "" {
		err := keyringSet(keyringService, bindingSecretKey(connectionID, "persona-mcp-token"), binding.PersonaMCPToken)
		if err != nil {
			return Binding{}, fmt.Errorf("store persona mcp token: %w", err)
		}
		binding.PersonaMCPToken = ""
		binding.HasPersonaMCPToken = true
	}
	if strings.TrimSpace(binding.ActiveRunMCPToken) != "" {
		err := keyringSet(keyringService, bindingSecretKey(connectionID, "active-run-mcp-token"), binding.ActiveRunMCPToken)
		if err != nil {
			return Binding{}, fmt.Errorf("store active run mcp token: %w", err)
		}
		binding.ActiveRunMCPToken = ""
		binding.HasActiveRunMCPToken = true
	}
	if !binding.HasActiveRunMCPToken {
		_ = keyringDelete(keyringService, bindingSecretKey(connectionID, "active-run-mcp-token"))
	}
	return binding, nil
}

func loadBindingSecrets(binding Binding) Binding {
	connectionID := strings.TrimSpace(string(binding.ConnectionID))
	if connectionID == "" {
		return binding
	}
	if binding.HasBridgeSecret && strings.TrimSpace(binding.BridgePrivateKey) == "" {
		secret, err := keyringGet(keyringService, bindingSecretKey(connectionID, "bridge-private-key"))
		if err == nil {
			binding.BridgePrivateKey = secret
		}
	}
	if binding.HasPersonaMCPToken && strings.TrimSpace(binding.PersonaMCPToken) == "" {
		secret, err := keyringGet(keyringService, bindingSecretKey(connectionID, "persona-mcp-token"))
		if err == nil {
			binding.PersonaMCPToken = secret
		}
	}
	if binding.HasActiveRunMCPToken && strings.TrimSpace(binding.ActiveRunMCPToken) == "" {
		secret, err := keyringGet(keyringService, bindingSecretKey(connectionID, "active-run-mcp-token"))
		if err == nil {
			binding.ActiveRunMCPToken = secret
		}
	}
	return binding
}

func deleteBindingSecrets(binding Binding) {
	connectionID := strings.TrimSpace(string(binding.ConnectionID))
	if connectionID == "" {
		return
	}
	_ = keyringDelete(keyringService, bindingSecretKey(connectionID, "bridge-private-key"))
	_ = keyringDelete(keyringService, bindingSecretKey(connectionID, "persona-mcp-token"))
	_ = keyringDelete(keyringService, bindingSecretKey(connectionID, "active-run-mcp-token"))
}

func bindingSecretKey(connectionID string, name string) string {
	return strings.TrimSpace(connectionID) + ":" + strings.TrimSpace(name)
}
