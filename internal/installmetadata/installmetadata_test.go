package installmetadata

import (
	"errors"
	"testing"

	"github.com/personastack/personastack-connector/internal/externalagentprotocol"
)

func TestDetectClassifiesOnlyVerifiedManualInstallOrigins(t *testing.T) {
	tests := []struct {
		name       string
		path       string
		goos       string
		runner     CommandRunner
		channel    externalagentprotocol.InstallChannel
		class      externalagentprotocol.ExecutablePathClass
		capability externalagentprotocol.UpdateCapability
	}{
		{name: "macOS Homebrew", path: "/opt/homebrew/opt/personastack-connector/bin/personastack-connector", goos: "darwin", channel: externalagentprotocol.InstallChannelHomebrew, class: externalagentprotocol.ExecutablePathClassHomebrewOpt, capability: externalagentprotocol.UpdateCapabilityManualRequired},
		{name: "Debian package owner", path: "/usr/bin/personastack-connector", goos: "linux", runner: func(name string, args ...string) (string, error) {
			if name == "dpkg-query" {
				return "personastack-connector: /usr/bin/personastack-connector", nil
			}
			return "", errors.New("not installed")
		}, channel: externalagentprotocol.InstallChannelDeb, class: externalagentprotocol.ExecutablePathClassPackageManaged, capability: externalagentprotocol.UpdateCapabilityManualRequired},
		{name: "RPM package owner", path: "/usr/bin/personastack-connector", goos: "linux", runner: func(name string, args ...string) (string, error) {
			if name == "rpm" {
				return "personastack-connector", nil
			}
			return "", errors.New("not installed")
		}, channel: externalagentprotocol.InstallChannelRPM, class: externalagentprotocol.ExecutablePathClassPackageManaged, capability: externalagentprotocol.UpdateCapabilityManualRequired},
		{name: "archive", path: "/tmp/personastack-connector", goos: "linux", runner: func(string, ...string) (string, error) { return "", errors.New("not installed") }, channel: externalagentprotocol.InstallChannelUnknown, class: externalagentprotocol.ExecutablePathClassUnknown, capability: externalagentprotocol.UpdateCapabilityUnsupported},
		{name: "missing package manager", path: "/usr/bin/personastack-connector", goos: "linux", runner: func(string, ...string) (string, error) { return "", errors.New("not found") }, channel: externalagentprotocol.InstallChannelUnknown, class: externalagentprotocol.ExecutablePathClassUnknown, capability: externalagentprotocol.UpdateCapabilityUnsupported},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			metadata := Detect(test.path, test.goos, test.runner)
			if metadata.InstallChannel != test.channel || metadata.ExecutablePathClass != test.class || metadata.UpdateCapability != test.capability || metadata.UpdateState != externalagentprotocol.UpdateStateIdle {
				t.Fatalf("Detect() = %+v", metadata)
			}
		})
	}
}
