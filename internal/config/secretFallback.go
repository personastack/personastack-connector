package config

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	osruntime "runtime"
)

var fallbackSecretMu sync.Mutex

type fallbackSecretEnvelope struct {
	Nonce      string `json:"nonce"`
	Ciphertext string `json:"ciphertext"`
}

func fallbackSecretGet(secretKey string) (string, error) {
	fallbackSecretMu.Lock()
	defer fallbackSecretMu.Unlock()

	secrets, err := loadFallbackSecrets()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(secrets[strings.TrimSpace(secretKey)]), nil
}

func fallbackSecretSet(secretKey string, value string) error {
	fallbackSecretMu.Lock()
	defer fallbackSecretMu.Unlock()

	secrets, err := loadFallbackSecrets()
	if err != nil {
		return err
	}
	secrets[strings.TrimSpace(secretKey)] = strings.TrimSpace(value)
	return saveFallbackSecrets(secrets)
}

func fallbackSecretDelete(secretKey string) error {
	fallbackSecretMu.Lock()
	defer fallbackSecretMu.Unlock()

	secrets, err := loadFallbackSecrets()
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	delete(secrets, strings.TrimSpace(secretKey))
	return saveFallbackSecrets(secrets)
}

func loadFallbackSecrets() (map[string]string, error) {
	dataPath, err := fallbackSecretsPath()
	if err != nil {
		return nil, err
	}
	raw, err := os.ReadFile(dataPath)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]string{}, nil
		}
		return nil, fmt.Errorf("read encrypted secret store: %w", err)
	}
	if len(raw) == 0 {
		return map[string]string{}, nil
	}
	var envelope fallbackSecretEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return nil, fmt.Errorf("decode encrypted secret store: %w", err)
	}
	plaintext, err := decryptFallbackSecrets(envelope)
	if err != nil {
		return nil, err
	}
	if len(plaintext) == 0 {
		return map[string]string{}, nil
	}
	secrets := map[string]string{}
	if err := json.Unmarshal(plaintext, &secrets); err != nil {
		return nil, fmt.Errorf("decode secret map: %w", err)
	}
	return secrets, nil
}

func saveFallbackSecrets(secrets map[string]string) error {
	dataPath, err := fallbackSecretsPath()
	if err != nil {
		return err
	}
	payload, err := json.Marshal(secrets)
	if err != nil {
		return fmt.Errorf("encode secret map: %w", err)
	}
	key, err := fallbackSecretsKey()
	if err != nil {
		return err
	}
	envelope, err := encryptFallbackSecrets(key, payload)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("encode encrypted secret store: %w", err)
	}
	if err := ensureOwnerOnlyDir(filepath.Dir(dataPath)); err != nil {
		return err
	}
	return writeOwnerOnlyFile(dataPath, raw)
}

func fallbackSecretsPath() (string, error) {
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("resolve user config dir: %w", err)
	}
	return filepath.Join(dir, "personastack", "connector", "secrets.enc"), nil
}

func fallbackSecretsKeyPath() (string, error) {
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("resolve user config dir: %w", err)
	}
	return filepath.Join(dir, "personastack", "connector", "secrets.key"), nil
}

func fallbackSecretsKey() ([]byte, error) {
	keyPath, err := fallbackSecretsKeyPath()
	if err != nil {
		return nil, err
	}
	raw, err := os.ReadFile(keyPath)
	if err == nil && len(raw) > 0 {
		key, decodeErr := hex.DecodeString(strings.TrimSpace(string(raw)))
		if decodeErr != nil {
			return nil, fmt.Errorf("decode fallback secret key: %w", decodeErr)
		}
		return key, nil
	}
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("read fallback secret key: %w", err)
	}
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return nil, fmt.Errorf("generate fallback secret key: %w", err)
	}
	if err := ensureOwnerOnlyDir(filepath.Dir(keyPath)); err != nil {
		return nil, err
	}
	if err := writeOwnerOnlyFile(keyPath, []byte(hex.EncodeToString(key))); err != nil {
		return nil, err
	}
	return key, nil
}

func fallbackSecretsKeyExisting() ([]byte, error) {
	keyPath, err := fallbackSecretsKeyPath()
	if err != nil {
		return nil, err
	}
	raw, err := os.ReadFile(keyPath)
	if err == nil && len(raw) > 0 {
		key, decodeErr := hex.DecodeString(strings.TrimSpace(string(raw)))
		if decodeErr != nil {
			return nil, fmt.Errorf("decode fallback secret key: %w", decodeErr)
		}
		return key, nil
	}
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("read fallback secret key: %w", err)
	}
	return nil, os.ErrNotExist
}

func encryptFallbackSecrets(key []byte, plaintext []byte) (fallbackSecretEnvelope, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return fallbackSecretEnvelope{}, fmt.Errorf("create fallback secret cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return fallbackSecretEnvelope{}, fmt.Errorf("create fallback secret gcm: %w", err)
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return fallbackSecretEnvelope{}, fmt.Errorf("generate fallback secret nonce: %w", err)
	}
	ciphertext := gcm.Seal(nil, nonce, plaintext, nil)
	return fallbackSecretEnvelope{
		Nonce:      hex.EncodeToString(nonce),
		Ciphertext: hex.EncodeToString(ciphertext),
	}, nil
}

func decryptFallbackSecrets(envelope fallbackSecretEnvelope) ([]byte, error) {
	key, err := fallbackSecretsKeyExisting()
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("create fallback secret cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create fallback secret gcm: %w", err)
	}
	nonce, err := hex.DecodeString(strings.TrimSpace(envelope.Nonce))
	if err != nil {
		return nil, fmt.Errorf("decode fallback secret nonce: %w", err)
	}
	ciphertext, err := hex.DecodeString(strings.TrimSpace(envelope.Ciphertext))
	if err != nil {
		return nil, fmt.Errorf("decode fallback secret ciphertext: %w", err)
	}
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("decrypt fallback secret store: %w", err)
	}
	return plaintext, nil
}

func ensureOwnerOnlyDir(path string) error {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return fmt.Errorf("create secret store dir: %w", err)
	}
	return nil
}

func writeOwnerOnlyFile(path string, raw []byte) error {
	if err := ensureOwnerOnlyDir(filepath.Dir(path)); err != nil {
		return err
	}
	temp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create temp secret store: %w", err)
	}
	tempPath := temp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tempPath)
		}
	}()
	if err := temp.Chmod(0o600); err != nil {
		_ = temp.Close()
		return fmt.Errorf("secure temp secret store: %w", err)
	}
	if _, err := temp.Write(raw); err != nil {
		_ = temp.Close()
		return fmt.Errorf("write temp secret store: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close temp secret store: %w", err)
	}
	if err := replaceFile(tempPath, path); err != nil {
		return fmt.Errorf("replace secret store: %w", err)
	}
	cleanup = false
	return nil
}

func replaceFile(tempPath string, path string) error {
	if osruntime.GOOS != "windows" {
		return os.Rename(tempPath, path)
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return os.Rename(tempPath, path)
}

func shouldUseFallbackSecretStore() bool {
	return shouldForceFallbackSecretStore() || osruntime.GOOS == "linux"
}

func shouldForceFallbackSecretStore() bool {
	return os.Getenv("PERSONASTACK_CONNECTOR_FORCE_SECRET_FALLBACK") == "1"
}
