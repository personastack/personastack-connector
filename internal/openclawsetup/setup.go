// Package openclawsetup starts a selected account's local OpenClaw gateway.
package openclawsetup

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/personastack/personastack-connector/internal/hermessetup"
)

var lookPath = exec.LookPath

var startGateway = func(homeDir string, identity hermessetup.ProcessIdentity, binary string, port int) error {
	cmd := exec.Command(binary, "gateway", "run", "--port", strconv.Itoa(port))
	cmd.Env = processEnv(homeDir, identity)
	cmd.Dir = strings.TrimSpace(homeDir)
	cmd.Stdout = ioDiscard{}
	cmd.Stderr = ioDiscard{}
	if err := hermessetup.ApplyProcessIdentity(cmd, identity); err != nil {
		return err
	}
	return cmd.Start()
}

type ioDiscard struct{}

func (ioDiscard) Write(p []byte) (int, error) { return len(p), nil }

// TryStartGatewayForHome starts the default selected-account OpenClaw gateway.
// An existing listener is never replaced because the Connector cannot safely
// attribute it to a different account.
func TryStartGatewayForHome(homeDir string, identity hermessetup.ProcessIdentity, gatewayReachable func() bool) (bool, error) {
	return TryStartGatewayForHomeAt(homeDir, identity, 18789, gatewayReachable)
}

// TryStartGatewayForHomeAt starts OpenClaw on a target-specific loopback port.
func TryStartGatewayForHomeAt(homeDir string, identity hermessetup.ProcessIdentity, port int, gatewayReachable func() bool) (bool, error) {
	if port < 1024 || port > 65535 {
		return false, fmt.Errorf("OpenClaw gateway port invalid")
	}
	if os.Getenv("PERSONASTACK_CONNECTOR_DISABLE_OPENCLAW_GATEWAY_START") == "1" || gatewayReachable == nil || gatewayReachable() {
		return false, nil
	}
	binary, err := openClawBinaryForHome(homeDir)
	if err != nil {
		return false, nil
	}
	if err := startGateway(homeDir, identity, binary, port); err != nil {
		return false, fmt.Errorf("start OpenClaw gateway: %w", err)
	}
	return true, nil
}

func openClawBinaryForHome(homeDir string) (string, error) {
	if binary, err := lookPath("openclaw"); err == nil {
		return binary, nil
	}
	for _, candidate := range []string{
		filepath.Join(homeDir, ".local", "bin", "openclaw"),
		filepath.Join(homeDir, ".npm-global", "bin", "openclaw"),
		filepath.Join(homeDir, ".openclaw", "bin", "openclaw"),
	} {
		if info, err := os.Stat(candidate); err == nil && info.Mode().IsRegular() && info.Mode().Perm()&0o111 != 0 {
			return candidate, nil
		}
	}
	return "", exec.ErrNotFound
}

func processEnv(homeDir string, identity hermessetup.ProcessIdentity) []string {
	homeDir = strings.TrimSpace(homeDir)
	username := strings.TrimSpace(identity.Username)
	env := make([]string, 0, len(os.Environ())+6)
	for _, item := range os.Environ() {
		if strings.HasPrefix(item, "HOME=") || strings.HasPrefix(item, "USER=") || strings.HasPrefix(item, "LOGNAME=") || strings.HasPrefix(item, "OPENCLAW_HOME=") || strings.HasPrefix(item, "OPENCLAW_CONFIG_PATH=") {
			continue
		}
		env = append(env, item)
	}
	env = append(env,
		"HOME="+homeDir,
		"OPENCLAW_HOME="+homeDir,
		"OPENCLAW_CONFIG_PATH="+filepath.Join(homeDir, ".openclaw", "openclaw.json"),
	)
	if username != "" {
		env = append(env, "USER="+username, "LOGNAME="+username)
	}
	return env
}
