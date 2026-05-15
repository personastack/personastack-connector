package main

import (
	"os"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

type goReleaserConfig struct {
	Before struct {
		Hooks []string `yaml:"hooks"`
	} `yaml:"before"`
	Builds []struct {
		ID     string   `yaml:"id"`
		Main   string   `yaml:"main"`
		Binary string   `yaml:"binary"`
		Env    []string `yaml:"env"`
		GoOS   []string `yaml:"goos"`
		GoArch []string `yaml:"goarch"`
		Ignore []struct {
			GoOS   string `yaml:"goos"`
			GoArch string `yaml:"goarch"`
		} `yaml:"ignore"`
		LdFlags []string `yaml:"ldflags"`
	} `yaml:"builds"`
	Archives []struct {
		ID              string `yaml:"id"`
		NameTemplate    string `yaml:"name_template"`
		FormatOverrides []struct {
			GoOS    string   `yaml:"goos"`
			Formats []string `yaml:"formats"`
		} `yaml:"format_overrides"`
	} `yaml:"archives"`
	NFPMs []struct {
		ID          string   `yaml:"id"`
		PackageName string   `yaml:"package_name"`
		Formats     []string `yaml:"formats"`
		BinDir      string   `yaml:"bindir"`
	} `yaml:"nfpms"`
	Checksum struct {
		NameTemplate string `yaml:"name_template"`
		Algorithm    string `yaml:"algorithm"`
	} `yaml:"checksum"`
	SBOMs []struct {
		Artifacts string   `yaml:"artifacts"`
		Documents []string `yaml:"documents"`
	} `yaml:"sboms"`
	Release struct {
		Draft bool   `yaml:"draft"`
		Mode  string `yaml:"mode"`
	} `yaml:"release"`
}

func TestGoReleaserConfigCoversReleaseMatrix(t *testing.T) {
	raw, err := os.ReadFile("../../.goreleaser.yaml")
	if err != nil {
		t.Fatalf("read goreleaser config: %v", err)
	}

	var cfg goReleaserConfig
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		t.Fatalf("decode goreleaser config: %v", err)
	}

	if len(cfg.Builds) != 1 {
		t.Fatalf("build count = %d, want 1", len(cfg.Builds))
	}
	build := cfg.Builds[0]
	if build.ID != "personastack-connector" || build.Main != "./cmd/personastack-connector" || build.Binary != "personastack-connector" {
		t.Fatalf("unexpected build identity: %+v", build)
	}
	if !containsAll(build.GoOS, "darwin", "linux", "windows") {
		t.Fatalf("unexpected goos matrix: %v", build.GoOS)
	}
	if !containsAll(build.GoArch, "amd64", "arm64") {
		t.Fatalf("unexpected goarch matrix: %v", build.GoArch)
	}
	if !containsString(build.Env, "CGO_ENABLED=0") {
		t.Fatalf("missing CGO disablement: %v", build.Env)
	}
	if !containsString(build.LdFlags, "-X github.com/personastack/personastack-connector/internal/buildinfo.Version={{ .Version }}") {
		t.Fatalf("missing buildinfo ldflags: %v", build.LdFlags)
	}
	if !hasIgnoreTarget(build.Ignore, "windows", "arm64") {
		t.Fatalf("expected windows/arm64 ignore entry: %+v", build.Ignore)
	}

	if len(cfg.Archives) != 1 {
		t.Fatalf("archive count = %d, want 1", len(cfg.Archives))
	}
	archive := cfg.Archives[0]
	if !strings.Contains(archive.NameTemplate, "personastack-connector_{{ .Version }}_") {
		t.Fatalf("unexpected archive name template: %q", archive.NameTemplate)
	}
	if !hasArchiveOverride(archive.FormatOverrides, "windows", "zip") {
		t.Fatalf("expected windows zip override: %+v", archive.FormatOverrides)
	}

	if len(cfg.NFPMs) != 1 {
		t.Fatalf("nfpm count = %d, want 1", len(cfg.NFPMs))
	}
	nfpm := cfg.NFPMs[0]
	if nfpm.PackageName != "personastack-connector" || nfpm.BinDir != "/usr/local/bin" {
		t.Fatalf("unexpected nfpm metadata: %+v", nfpm)
	}
	if !containsAll(nfpm.Formats, "deb", "rpm") {
		t.Fatalf("unexpected nfpm formats: %v", nfpm.Formats)
	}

	if cfg.Checksum.Algorithm != "sha256" || !strings.Contains(cfg.Checksum.NameTemplate, "_checksums.txt") {
		t.Fatalf("unexpected checksum config: %+v", cfg.Checksum)
	}
	if len(cfg.SBOMs) != 1 || cfg.SBOMs[0].Artifacts != "archive" {
		t.Fatalf("unexpected sbom config: %+v", cfg.SBOMs)
	}
	if !containsString(cfg.SBOMs[0].Documents, "{{ .ProjectName }}_{{ .Version }}_sbom.spdx.json") {
		t.Fatalf("unexpected sbom documents: %+v", cfg.SBOMs[0].Documents)
	}
	if !cfg.Release.Draft || cfg.Release.Mode != "replace" {
		t.Fatalf("unexpected release config: %+v", cfg.Release)
	}
	if !containsString(cfg.Before.Hooks, "go mod tidy") {
		t.Fatalf("missing go mod tidy hook: %v", cfg.Before.Hooks)
	}
}

func TestReleaseWorkflowContainsDryRunValidationSteps(t *testing.T) {
	ci, err := os.ReadFile("../../.github/workflows/ci.yml")
	if err != nil {
		t.Fatalf("read ci workflow: %v", err)
	}
	release, err := os.ReadFile("../../.github/workflows/release.yml")
	if err != nil {
		t.Fatalf("read release workflow: %v", err)
	}

	for _, want := range []string{
		"goreleaser/goreleaser-action@e435ccd777264be153ace6237001ef4d979d3a7a",
		"args: check",
		"args: release --snapshot --clean --skip=publish",
		"./scripts/smoke-release-artifacts.sh dist auto",
		"attest-build-provenance@96b4a1ef7235a096b17240c259729fdd70c83d45",
		"./scripts/check-release-policy.sh",
		"gh release upload",
		"personastack-connector_${GITHUB_REF_NAME#v}_release_manifest.json",
		"personastack-connector_${GITHUB_REF_NAME#v}_release_manifest.json.sha256",
		"./scripts/verify-release-manifest.sh dist \"${GITHUB_REF_NAME#v}\"",
	} {
		if !strings.Contains(string(ci)+"\n"+string(release), want) {
			t.Fatalf("missing release validation step %q", want)
		}
	}
}

func containsAll(values []string, want ...string) bool {
	have := make(map[string]struct{}, len(values))
	for _, value := range values {
		have[strings.TrimSpace(value)] = struct{}{}
	}
	for _, target := range want {
		if _, ok := have[target]; !ok {
			return false
		}
	}
	return true
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if strings.TrimSpace(value) == want {
			return true
		}
	}
	return false
}

func hasIgnoreTarget(entries []struct {
	GoOS   string `yaml:"goos"`
	GoArch string `yaml:"goarch"`
}, goOS string, goArch string) bool {
	for _, entry := range entries {
		if entry.GoOS == goOS && entry.GoArch == goArch {
			return true
		}
	}
	return false
}

func hasArchiveOverride(entries []struct {
	GoOS    string   `yaml:"goos"`
	Formats []string `yaml:"formats"`
}, goOS string, format string) bool {
	for _, entry := range entries {
		if entry.GoOS != goOS {
			continue
		}
		for _, candidate := range entry.Formats {
			if candidate == format {
				return true
			}
		}
	}
	return false
}
