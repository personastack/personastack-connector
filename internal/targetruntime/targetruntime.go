// Package targetruntime derives target-scoped local runtime endpoints.
package targetruntime

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"

	"github.com/personastack/personastack-connector/internal/externalagentprotocol"
)

const (
	firstTargetPort = 24000
	targetPortSpan  = 12000
)

// LoopbackURL returns a deterministic, Connector-session-only endpoint for a
// selected target. Separate selected profiles must not share a default native
// runtime port, because an existing listener cannot be safely attributed.
func LoopbackURL(target *externalagentprotocol.RuntimeTarget, installationSecret string) (string, error) {
	if target == nil || strings.TrimSpace(target.AccountCandidateID) == "" || strings.TrimSpace(target.ProfileCandidateID) == "" {
		return "", fmt.Errorf("runtime target required")
	}
	secret := strings.TrimSpace(installationSecret)
	if secret == "" {
		return "", fmt.Errorf("Connector installation secret required")
	}
	var scheme string
	switch target.RuntimeKind {
	case externalagentprotocol.RuntimeKindHermes:
		scheme = "http"
	case externalagentprotocol.RuntimeKindOpenClaw:
		scheme = "ws"
	default:
		return "", fmt.Errorf("unsupported runtime target")
	}
	port, err := Port(target, secret)
	if err != nil {
		return "", err
	}
	return scheme + "://127.0.0.1:" + strconv.Itoa(port), nil
}

// Port returns the deterministic loopback port for a selected target.
func Port(target *externalagentprotocol.RuntimeTarget, installationSecret string) (int, error) {
	if target == nil || strings.TrimSpace(target.AccountCandidateID) == "" || strings.TrimSpace(target.ProfileCandidateID) == "" {
		return 0, fmt.Errorf("runtime target required")
	}
	secret := strings.TrimSpace(installationSecret)
	if secret == "" {
		return 0, fmt.Errorf("Connector installation secret required")
	}
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(string(target.RuntimeKind)))
	_, _ = mac.Write([]byte{0})
	_, _ = mac.Write([]byte(strings.TrimSpace(target.AccountCandidateID)))
	_, _ = mac.Write([]byte{0})
	_, _ = mac.Write([]byte(strings.TrimSpace(target.ProfileCandidateID)))
	sum := mac.Sum(nil)
	return firstTargetPort + int(binary.BigEndian.Uint16(sum[:2]))%targetPortSpan, nil
}
