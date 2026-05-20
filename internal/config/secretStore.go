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
		if err := storeSecret(bindingSecretKey(connectionID, "bridge-private-key"), binding.BridgePrivateKey); err != nil {
			return Binding{}, fmt.Errorf("store bridge private key: %w", err)
		}
		binding.BridgePrivateKey = ""
		binding.HasBridgeSecret = true
	}
	if strings.TrimSpace(binding.PersonaMCPToken) != "" {
		if err := storeSecret(bindingSecretKey(connectionID, "persona-mcp-token"), binding.PersonaMCPToken); err != nil {
			return Binding{}, fmt.Errorf("store persona mcp token: %w", err)
		}
		binding.PersonaMCPToken = ""
		binding.HasPersonaMCPToken = true
	}
	if strings.TrimSpace(binding.LocalMCPProxyToken) != "" {
		if err := storeSecret(bindingSecretKey(connectionID, "local-mcp-proxy-token"), binding.LocalMCPProxyToken); err != nil {
			return Binding{}, fmt.Errorf("store local mcp proxy token: %w", err)
		}
		binding.LocalMCPProxyToken = ""
		binding.HasLocalMCPProxyToken = true
	}
	if strings.TrimSpace(binding.OpenClawGatewayToken) != "" {
		if err := storeSecret(bindingSecretKey(connectionID, "openclaw-gateway-token"), binding.OpenClawGatewayToken); err != nil {
			return Binding{}, fmt.Errorf("store OpenClaw gateway token: %w", err)
		}
		binding.OpenClawGatewayToken = ""
		binding.HasOpenClawToken = true
	}
	if strings.TrimSpace(binding.OpenClawPassword) != "" {
		if err := storeSecret(bindingSecretKey(connectionID, "openclaw-password"), binding.OpenClawPassword); err != nil {
			return Binding{}, fmt.Errorf("store OpenClaw password: %w", err)
		}
		binding.OpenClawPassword = ""
		binding.HasOpenClawPassword = true
	}
	if strings.TrimSpace(binding.OpenClawDeviceToken) != "" {
		if err := storeSecret(bindingSecretKey(connectionID, "openclaw-device-token"), binding.OpenClawDeviceToken); err != nil {
			return Binding{}, fmt.Errorf("store OpenClaw device token: %w", err)
		}
		binding.OpenClawDeviceToken = ""
		binding.HasOpenClawDevice = true
	}
	if strings.TrimSpace(binding.ActiveRunMCPToken) != "" {
		if err := storeSecret(bindingSecretKey(connectionID, "active-run-mcp-token"), binding.ActiveRunMCPToken); err != nil {
			return Binding{}, fmt.Errorf("store active run mcp token: %w", err)
		}
		binding.ActiveRunMCPToken = ""
		binding.HasActiveRunMCPToken = true
	}
	if !binding.HasActiveRunMCPToken {
		_ = deleteSecret(bindingSecretKey(connectionID, "active-run-mcp-token"))
	}
	return binding, nil
}

func loadBindingSecrets(binding Binding) Binding {
	connectionID := strings.TrimSpace(string(binding.ConnectionID))
	if connectionID == "" {
		return binding
	}
	if binding.HasBridgeSecret && strings.TrimSpace(binding.BridgePrivateKey) == "" {
		binding.BridgePrivateKey = loadSecret(bindingSecretKey(connectionID, "bridge-private-key"))
	}
	if binding.HasPersonaMCPToken && strings.TrimSpace(binding.PersonaMCPToken) == "" {
		binding.PersonaMCPToken = loadSecret(bindingSecretKey(connectionID, "persona-mcp-token"))
	}
	if binding.HasLocalMCPProxyToken && strings.TrimSpace(binding.LocalMCPProxyToken) == "" {
		binding.LocalMCPProxyToken = loadSecret(bindingSecretKey(connectionID, "local-mcp-proxy-token"))
	}
	if binding.HasOpenClawToken && strings.TrimSpace(binding.OpenClawGatewayToken) == "" {
		binding.OpenClawGatewayToken = loadSecret(bindingSecretKey(connectionID, "openclaw-gateway-token"))
	}
	if binding.HasOpenClawPassword && strings.TrimSpace(binding.OpenClawPassword) == "" {
		binding.OpenClawPassword = loadSecret(bindingSecretKey(connectionID, "openclaw-password"))
	}
	if binding.HasOpenClawDevice && strings.TrimSpace(binding.OpenClawDeviceToken) == "" {
		binding.OpenClawDeviceToken = loadSecret(bindingSecretKey(connectionID, "openclaw-device-token"))
	}
	if binding.HasActiveRunMCPToken && strings.TrimSpace(binding.ActiveRunMCPToken) == "" {
		binding.ActiveRunMCPToken = loadSecret(bindingSecretKey(connectionID, "active-run-mcp-token"))
	}
	return binding
}

func deleteBindingSecrets(binding Binding) {
	connectionID := strings.TrimSpace(string(binding.ConnectionID))
	if connectionID == "" {
		return
	}
	_ = deleteSecret(bindingSecretKey(connectionID, "bridge-private-key"))
	_ = deleteSecret(bindingSecretKey(connectionID, "persona-mcp-token"))
	_ = deleteSecret(bindingSecretKey(connectionID, "local-mcp-proxy-token"))
	_ = deleteSecret(bindingSecretKey(connectionID, "openclaw-gateway-token"))
	_ = deleteSecret(bindingSecretKey(connectionID, "openclaw-password"))
	_ = deleteSecret(bindingSecretKey(connectionID, "openclaw-device-token"))
	_ = deleteSecret(bindingSecretKey(connectionID, "active-run-mcp-token"))
}

func bindingSecretKey(connectionID string, name string) string {
	return strings.TrimSpace(connectionID) + ":" + strings.TrimSpace(name)
}

func storeSecret(secretKey string, value string) error {
	if shouldForceFallbackSecretStore() {
		_ = keyringDelete(keyringService, secretKey)
		return fallbackSecretSet(secretKey, value)
	}
	if err := keyringSet(keyringService, secretKey, value); err == nil {
		_ = fallbackSecretDelete(secretKey)
		return nil
	} else if shouldUseFallbackSecretStore() {
		if fallbackErr := fallbackSecretSet(secretKey, value); fallbackErr == nil {
			return nil
		} else {
			return fallbackErr
		}
	} else {
		return err
	}
}

func loadSecret(secretKey string) string {
	if shouldForceFallbackSecretStore() {
		if secret, err := fallbackSecretGet(secretKey); err == nil && strings.TrimSpace(secret) != "" {
			return strings.TrimSpace(secret)
		}
		return ""
	}
	if secret, err := keyringGet(keyringService, secretKey); err == nil && strings.TrimSpace(secret) != "" {
		return strings.TrimSpace(secret)
	}
	if secret, err := fallbackSecretGet(secretKey); err == nil && strings.TrimSpace(secret) != "" {
		return strings.TrimSpace(secret)
	}
	return ""
}

func deleteSecret(secretKey string) error {
	if shouldForceFallbackSecretStore() {
		_ = keyringDelete(keyringService, secretKey)
		return fallbackSecretDelete(secretKey)
	}
	keyringErr := keyringDelete(keyringService, secretKey)
	if fallbackErr := fallbackSecretDelete(secretKey); fallbackErr != nil {
		return fallbackErr
	}
	return keyringErr
}
