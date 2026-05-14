package config

import (
	"fmt"
	"strings"

	"github.com/zalando/go-keyring"
)

const keyringService = "personastack-connector"

var (
	keyringGet = keyring.Get
	keyringSet = keyring.Set
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
	return binding
}

func bindingSecretKey(connectionID string, name string) string {
	return strings.TrimSpace(connectionID) + ":" + strings.TrimSpace(name)
}
