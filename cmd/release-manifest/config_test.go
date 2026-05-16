package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
		Hooks   struct {
			Post []struct {
				Cmd    string `yaml:"cmd"`
				Output bool   `yaml:"output"`
			} `yaml:"post"`
		} `yaml:"hooks"`
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
	Notarize struct {
		MacOS []struct {
			Enabled string   `yaml:"enabled"`
			Ids     []string `yaml:"ids"`
			Sign    struct {
				Certificate string `yaml:"certificate"`
				Password    string `yaml:"password"`
			} `yaml:"sign"`
			Notarize struct {
				IssuerID string `yaml:"issuer_id"`
				KeyID    string `yaml:"key_id"`
				Key      string `yaml:"key"`
				Wait     bool   `yaml:"wait"`
			} `yaml:"notarize"`
		} `yaml:"macos"`
	} `yaml:"notarize"`
	Signs []struct {
		ID        string   `yaml:"id"`
		Cmd       string   `yaml:"cmd"`
		Args      []string `yaml:"args"`
		Signature string   `yaml:"signature"`
		Artifacts string   `yaml:"artifacts"`
		If        string   `yaml:"if"`
	} `yaml:"signs"`
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
	if len(build.Hooks.Post) != 1 || build.Hooks.Post[0].Cmd != "./scripts/sign-windows-binary.sh \"{{ .Path }}\"" || !build.Hooks.Post[0].Output {
		t.Fatalf("unexpected build hooks: %+v", build.Hooks.Post)
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
	if len(cfg.Notarize.MacOS) != 1 {
		t.Fatalf("macos notarize count = %d, want 1", len(cfg.Notarize.MacOS))
	}
	macos := cfg.Notarize.MacOS[0]
	if macos.Enabled != "{{ and (isEnvSet \"MACOS_SIGN_P12\") (isEnvSet \"MACOS_SIGN_PASSWORD\") (isEnvSet \"MACOS_NOTARY_ISSUER_ID\") (isEnvSet \"MACOS_NOTARY_KEY_ID\") (isEnvSet \"MACOS_NOTARY_KEY\") }}" {
		t.Fatalf("unexpected macos enable gate: %q", macos.Enabled)
	}
	if !containsAll(macos.Ids, "personastack-connector") {
		t.Fatalf("unexpected macos ids: %+v", macos.Ids)
	}
	if macos.Sign.Certificate != "{{ .Env.MACOS_SIGN_P12 }}" || macos.Sign.Password != "{{ .Env.MACOS_SIGN_PASSWORD }}" {
		t.Fatalf("unexpected macos signing config: %+v", macos.Sign)
	}
	if macos.Notarize.IssuerID != "{{ .Env.MACOS_NOTARY_ISSUER_ID }}" || macos.Notarize.KeyID != "{{ .Env.MACOS_NOTARY_KEY_ID }}" || macos.Notarize.Key != "{{ .Env.MACOS_NOTARY_KEY }}" || !macos.Notarize.Wait {
		t.Fatalf("unexpected macos notarize config: %+v", macos.Notarize)
	}
	if len(cfg.Signs) != 1 {
		t.Fatalf("sign count = %d, want 1", len(cfg.Signs))
	}
	sign := cfg.Signs[0]
	if sign.Cmd != "./scripts/sign-release-checksum-bundle.sh" || sign.Signature != "${artifact}.sigstore.json" || sign.Artifacts != "checksum" {
		t.Fatalf("unexpected signing config: %+v", sign)
	}
	if !containsAll(sign.Args, "${artifact}", "${signature}") {
		t.Fatalf("unexpected signing args: %+v", sign.Args)
	}
	if !cfg.Release.Draft || cfg.Release.Mode != "replace" {
		t.Fatalf("unexpected release config: %+v", cfg.Release)
	}
	if len(cfg.Before.Hooks) != 0 {
		t.Fatalf("unexpected release before hooks: %v", cfg.Before.Hooks)
	}
}

func TestBuildManifestGatesDefaultInstallEligibilityUntilReleaseSigningIsEnabled(t *testing.T) {
	distDir := t.TempDir()
	files := map[string]string{
		"personastack-connector_0.2.0_darwin_arm64.tar.gz": "artifact",
		"personastack-connector_0.2.0_linux_amd64.deb":     "artifact",
		"personastack-connector_0.2.0_linux_amd64.rpm":     "artifact",
		"personastack-connector_0.2.0_windows_amd64.zip":   "artifact",
		"personastack-connector_0.2.0_checksums.txt": "aaa111  personastack-connector_0.2.0_darwin_arm64.tar.gz\n" +
			"bbb222  personastack-connector_0.2.0_linux_amd64.deb\n" +
			"ccc333  personastack-connector_0.2.0_linux_amd64.rpm\n" +
			"ddd444  personastack-connector_0.2.0_windows_amd64.zip\n",
	}
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(distDir, name), []byte(body), 0o644); err != nil {
			t.Fatalf("write fixture: %v", err)
		}
	}

	manifest, err := buildManifest(distDir, "0.2.0", "commit-1", time.Unix(0, 0).UTC())
	if err != nil {
		t.Fatalf("build manifest: %v", err)
	}
	assetsByName := map[string]releaseAsset{}
	for _, asset := range manifest.Assets {
		assetsByName[asset.Name] = asset
	}
	if assetsByName["personastack-connector_0.2.0_linux_amd64.deb"].DefaultInstallEligible {
		t.Fatalf("linux deb should not be default-install eligible without release signing inputs: %+v", assetsByName["personastack-connector_0.2.0_linux_amd64.deb"])
	}
	if assetsByName["personastack-connector_0.2.0_linux_amd64.rpm"].DefaultInstallEligible {
		t.Fatalf("linux rpm should not be default-install eligible without release signing inputs: %+v", assetsByName["personastack-connector_0.2.0_linux_amd64.rpm"])
	}
	if assetsByName["personastack-connector_0.2.0_windows_amd64.zip"].DefaultInstallEligible {
		t.Fatalf("windows archive should not be default-install eligible without release signing inputs: %+v", assetsByName["personastack-connector_0.2.0_windows_amd64.zip"])
	}
	if assetsByName["personastack-connector_0.2.0_darwin_arm64.tar.gz"].DefaultInstallEligible {
		t.Fatalf("darwin archive should not be default-install eligible without macOS signing inputs: %+v", assetsByName["personastack-connector_0.2.0_darwin_arm64.tar.gz"])
	}
}

func TestBuildManifestMarksInstallerDefaultsWhenReleaseSigningConfigured(t *testing.T) {
	t.Setenv("PERSONASTACK_RELEASE_SIGNING", "1")
	t.Setenv("MACOS_SIGN_P12", "p12")
	t.Setenv("MACOS_SIGN_PASSWORD", "password")
	t.Setenv("MACOS_NOTARY_ISSUER_ID", "issuer")
	t.Setenv("MACOS_NOTARY_KEY_ID", "key-id")
	t.Setenv("MACOS_NOTARY_KEY", "p8")

	distDir := t.TempDir()
	files := map[string]string{
		"personastack-connector_0.2.0_darwin_arm64.tar.gz": "artifact",
		"personastack-connector_0.2.0_linux_amd64.deb":     "artifact",
		"personastack-connector_0.2.0_linux_amd64.rpm":     "artifact",
		"personastack-connector_0.2.0_windows_amd64.zip":   "artifact",
		"personastack-connector_0.2.0_checksums.txt": "aaa111  personastack-connector_0.2.0_darwin_arm64.tar.gz\n" +
			"bbb222  personastack-connector_0.2.0_linux_amd64.deb\n" +
			"ccc333  personastack-connector_0.2.0_linux_amd64.rpm\n" +
			"ddd444  personastack-connector_0.2.0_windows_amd64.zip\n",
	}
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(distDir, name), []byte(body), 0o644); err != nil {
			t.Fatalf("write fixture: %v", err)
		}
	}

	manifest, err := buildManifest(distDir, "0.2.0", "commit-1", time.Unix(0, 0).UTC())
	if err != nil {
		t.Fatalf("build manifest: %v", err)
	}
	assetsByName := map[string]releaseAsset{}
	for _, asset := range manifest.Assets {
		assetsByName[asset.Name] = asset
	}
	for _, name := range []string{
		"personastack-connector_0.2.0_darwin_arm64.tar.gz",
		"personastack-connector_0.2.0_linux_amd64.deb",
		"personastack-connector_0.2.0_linux_amd64.rpm",
		"personastack-connector_0.2.0_windows_amd64.zip",
	} {
		if !assetsByName[name].DefaultInstallEligible {
			t.Fatalf("expected default-install eligibility when release signing inputs are present: %+v", assetsByName[name])
		}
	}
}

func TestGoReleaserConfigStillExcludesWindowsArm64(t *testing.T) {
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
	if !hasIgnoreTarget(cfg.Builds[0].Ignore, "windows", "arm64") {
		t.Fatalf("expected windows/arm64 ignore entry: %+v", cfg.Builds[0].Ignore)
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
		"go install github.com/sigstore/cosign/v2/cmd/cosign@v2.4.1",
		"goreleaser/goreleaser-action@e435ccd777264be153ace6237001ef4d979d3a7a",
		"args: check",
		"args: release --snapshot --clean --skip=publish",
		"runtime smoke (${{ matrix.os }})",
		"./scripts/fake-runtime-smoke.sh",
		"ubuntu-latest",
		"macos-latest",
		"windows-latest",
		"Install osslsigncode",
		"Prepare Windows signing certificate",
		"Check Windows signing gate",
		"WINDOWS_CODE_SIGN_REQUIRED: \"1\"",
		"./scripts/smoke-release-artifacts.sh dist auto",
		"attest-build-provenance@96b4a1ef7235a096b17240c259729fdd70c83d45",
		"./scripts/check-release-policy.sh",
		"environment:",
		"name: release",
		"PERSONASTACK_RELEASE_SIGNING: \"1\"",
		"MACOS_SIGN_P12: ${{ secrets.MACOS_SIGN_P12 }}",
		"MACOS_NOTARY_KEY: ${{ secrets.MACOS_NOTARY_KEY }}",
		"Sign release manifest",
		"cosign sign-blob --bundle \"dist/personastack-connector_${GITHUB_REF_NAME#v}_release_manifest.json.sigstore.json\"",
		"(cd dist && sha256sum \"personastack-connector_${GITHUB_REF_NAME#v}_release_manifest.json\" > \"personastack-connector_${GITHUB_REF_NAME#v}_release_manifest.json.sha256\")",
		"gh release upload",
		"personastack-connector_${GITHUB_REF_NAME#v}_release_manifest.json",
		"personastack-connector_${GITHUB_REF_NAME#v}_release_manifest.json.sha256",
		"personastack-connector_${GITHUB_REF_NAME#v}_release_manifest.json.sigstore.json",
		"personastack-connector_${GITHUB_REF_NAME#v}_checksums.txt.sigstore.json",
		"./scripts/verify-release-manifest.sh dist \"${GITHUB_REF_NAME#v}\"",
		"./scripts/render-package-manager-metadata.sh dist \"${GITHUB_REF_NAME#v}\"",
		"./scripts/publish-release-metadata-to-api.sh dist \"${GITHUB_REF_NAME#v}\"",
		"PERSONASTACK_API_URL: ${{ secrets.PERSONASTACK_API_URL }}",
		"PERSONASTACK_ADMIN_BEARER_TOKEN: ${{ secrets.PERSONASTACK_ADMIN_BEARER_TOKEN }}",
		"dist/package-manager/homebrew/personastack-connector.rb",
		"dist/package-manager/winget/PersonaStack.Connector/${GITHUB_REF_NAME#v}/*.yaml",
		"COSIGN_CERTIFICATE_IDENTITY: https://github.com/personastack/personastack-connector/.github/workflows/release.yml@refs/tags/${{ github.ref_name }}",
	} {
		if !strings.Contains(string(ci)+"\n"+string(release), want) {
			t.Fatalf("missing release validation step %q", want)
		}
	}
}

func TestRenderPackageManagerMetadata(t *testing.T) {
	dist := t.TempDir()
	checksums := strings.Join([]string{
		"1111111111111111111111111111111111111111111111111111111111111111  personastack-connector_0.2.0_darwin_arm64.tar.gz",
		"2222222222222222222222222222222222222222222222222222222222222222  personastack-connector_0.2.0_darwin_amd64.tar.gz",
		"3333333333333333333333333333333333333333333333333333333333333333  personastack-connector_0.2.0_windows_amd64.zip",
		"",
	}, "\n")
	if err := os.WriteFile(filepath.Join(dist, "personastack-connector_0.2.0_checksums.txt"), []byte(checksums), 0o600); err != nil {
		t.Fatalf("write checksums: %v", err)
	}

	cmd := exec.Command("../../scripts/render-package-manager-metadata.sh", dist, "0.2.0")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("render package metadata: %v output=%s", err, string(out))
	}
	homebrew, err := os.ReadFile(filepath.Join(dist, "package-manager", "homebrew", "personastack-connector.rb"))
	if err != nil {
		t.Fatalf("read homebrew metadata: %v", err)
	}
	for _, want := range []string{
		`version "0.2.0"`,
		"personastack-connector_0.2.0_darwin_arm64.tar.gz",
		"1111111111111111111111111111111111111111111111111111111111111111",
	} {
		if !strings.Contains(string(homebrew), want) {
			t.Fatalf("homebrew metadata missing %q: %s", want, string(homebrew))
		}
	}
	winget, err := os.ReadFile(filepath.Join(dist, "package-manager", "winget", "PersonaStack.Connector", "0.2.0", "PersonaStack.Connector.installer.yaml"))
	if err != nil {
		t.Fatalf("read winget metadata: %v", err)
	}
	for _, want := range []string{
		"PackageIdentifier: PersonaStack.Connector",
		"InstallerType: zip",
		"personastack-connector_0.2.0_windows_amd64.zip",
		"3333333333333333333333333333333333333333333333333333333333333333",
	} {
		if !strings.Contains(string(winget), want) {
			t.Fatalf("winget metadata missing %q: %s", want, string(winget))
		}
	}
}

func TestPublishReleaseMetadataToAPI(t *testing.T) {
	dist := t.TempDir()
	manifest := `{
  "assets": [
    {"arch":"amd64","checksum":"aaa","default_install_eligible":true,"name":"personastack-connector_0.2.0_linux_amd64.deb","os":"linux","package_kind":"deb","path":"personastack-connector_0.2.0_linux_amd64.deb"},
    {"arch":"amd64","checksum":"bbb","default_install_eligible":true,"name":"personastack-connector_0.2.0_windows_amd64.zip","os":"windows","package_kind":"archive","path":"personastack-connector_0.2.0_windows_amd64.zip"},
    {"arch":"arm64","checksum":"ccc","default_install_eligible":true,"name":"personastack-connector_0.2.0_darwin_arm64.tar.gz","os":"darwin","package_kind":"archive","path":"personastack-connector_0.2.0_darwin_arm64.tar.gz"},
    {"name":"personastack-connector_0.2.0_checksums.txt","package_kind":"checksum","path":"personastack-connector_0.2.0_checksums.txt"}
  ],
  "commit":"commit-1",
  "minimum_protocol":"external-agent-v1",
  "project":"personastack-connector",
  "runtime_kinds":["hermes","openclaw"],
  "version":"0.2.0"
}`
	if err := os.WriteFile(filepath.Join(dist, "personastack-connector_0.2.0_release_manifest.json"), []byte(manifest), 0o600); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	binDir := t.TempDir()
	logPath := filepath.Join(t.TempDir(), "curl.log")
	curlPath := filepath.Join(binDir, "curl")
	curlScript := "#!/usr/bin/env bash\nprintf '%s\\n' \"$@\" >> \"$PERSONASTACK_CURL_LOG\"\nprintf '\\n' >> \"$PERSONASTACK_CURL_LOG\"\n"
	if err := os.WriteFile(curlPath, []byte(curlScript), 0o755); err != nil {
		t.Fatalf("write fake curl: %v", err)
	}
	cmd := exec.Command("../../scripts/publish-release-metadata-to-api.sh", dist, "0.2.0")
	cmd.Env = append(os.Environ(),
		"PATH="+binDir+string(os.PathListSeparator)+os.Getenv("PATH"),
		"PERSONASTACK_CURL_LOG="+logPath,
		"PERSONASTACK_API_URL=https://api.personastack.test",
		"PERSONASTACK_ADMIN_BEARER_TOKEN=admin-token",
		"GITHUB_REPOSITORY=personastack/personastack-connector",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("publish release metadata: %v output=%s", err, string(out))
	}
	raw, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read curl log: %v", err)
	}
	got := string(raw)
	for _, want := range []string{
		"Authorization: Bearer admin-token",
		"https://api.personastack.test/v1/admin/external-agent-connector/releases",
		`"version":"v0.2.0"`,
		`"package_kind":"deb"`,
		"cosign verify-blob",
		"Wrong platform: this Connector asset requires linux/amd64.",
		"personastack-connector_0.2.0_linux_amd64.deb",
		"personastack-connector_0.2.0_release_manifest.json.sha256",
		"personastack-connector_0.2.0_release_manifest.json.sigstore.json",
		"cosign verify-blob --bundle personastack-connector_0.2.0_release_manifest.json.sigstore.json --certificate-identity https://github.com/personastack/personastack-connector/.github/workflows/release.yml@refs/tags/v0.2.0 --certificate-oidc-issuer https://token.actions.githubusercontent.com personastack-connector_0.2.0_release_manifest.json",
		"cosign verify-blob --bundle personastack-connector_0.2.0_checksums.txt.sigstore.json --certificate-identity https://github.com/personastack/personastack-connector/.github/workflows/release.yml@refs/tags/v0.2.0 --certificate-oidc-issuer https://token.actions.githubusercontent.com personastack-connector_0.2.0_checksums.txt",
		"Wrong platform: this Connector asset requires windows/amd64.",
		"personastack-connector_0.2.0_windows_amd64.zip",
		"$env:OS -ne 'Windows_NT'",
		"SetEnvironmentVariable('Path'",
		"personastack-connector.exe') version",
		"Wrong platform: this Connector asset requires darwin/arm64.",
		`test \"$(uname -m)\" = \"arm64\"`,
		"mkdir -p ~/.local/bin",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("curl payload missing %q:\n%s", want, got)
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
