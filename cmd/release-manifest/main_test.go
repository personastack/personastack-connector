package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestBuildManifestScansReleaseAssets(t *testing.T) {
	distDir := t.TempDir()
	files := map[string]string{
		"personastack-connector_0.2.0_darwin_arm64.tar.gz": "artifact",
		"personastack-connector_0.2.0_linux_amd64.deb":     "artifact",
		"personastack-connector_0.2.0_linux_amd64.rpm":     "artifact",
		"personastack-connector_0.2.0_sbom.spdx.json":      "{}",
		"personastack-connector_0.2.0_checksums.txt": "aaa111  personastack-connector_0.2.0_darwin_arm64.tar.gz\n" +
			"bbb222  personastack-connector_0.2.0_linux_amd64.deb\n" +
			"ccc333  personastack-connector_0.2.0_linux_amd64.rpm\n" +
			"eee555  personastack-connector_0.2.0_sbom.spdx.json\n",
		"personastack-connector_0.2.0_checksums.txt.sigstore.json": "signature",
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
	if manifest.Project != "personastack-connector" || manifest.Version != "0.2.0" {
		t.Fatalf("unexpected manifest identity: %+v", manifest)
	}
	if manifest.MinimumProtocol != "external-agent-v1" {
		t.Fatalf("minimum protocol = %q", manifest.MinimumProtocol)
	}
	if len(manifest.Assets) != 5 {
		t.Fatalf("assets len = %d", len(manifest.Assets))
	}
	var checksumAsset releaseAsset
	for _, asset := range manifest.Assets {
		if asset.Name == "personastack-connector_0.2.0_checksums.txt" {
			checksumAsset = asset
			break
		}
	}
	if checksumAsset.Name != "personastack-connector_0.2.0_checksums.txt" {
		t.Fatalf("missing checksum asset: %+v", manifest.Assets)
	}
	if checksumAsset.PackageKind != "checksum" || checksumAsset.Signature != "personastack-connector_0.2.0_checksums.txt.sigstore.json" {
		t.Fatalf("unexpected checksum metadata: %+v", checksumAsset)
	}
	archive := manifest.Assets[1]
	if archive.Name != "personastack-connector_0.2.0_darwin_arm64.tar.gz" {
		t.Fatalf("unexpected archive asset: %+v", archive)
	}
	if archive.OS != "darwin" || archive.Arch != "arm64" || archive.PackageKind != "archive" {
		t.Fatalf("unexpected archive metadata: %+v", archive)
	}
	if archive.Checksum != "aaa111" {
		t.Fatalf("missing checksum: %+v", archive)
	}
	if archive.DefaultInstallEligible {
		t.Fatalf("darwin archive should not be default-install eligible without macOS signing inputs: %+v", archive)
	}
}

func TestBuildManifestMarksMacOSArchivesDefaultEligibleWhenSigningConfigured(t *testing.T) {
	t.Setenv("PERSONASTACK_RELEASE_SIGNING", "1")
	t.Setenv("MACOS_SIGN_P12", "p12")
	t.Setenv("MACOS_SIGN_PASSWORD", "password")
	t.Setenv("MACOS_NOTARY_ISSUER_ID", "issuer")
	t.Setenv("MACOS_NOTARY_KEY_ID", "key-id")
	t.Setenv("MACOS_NOTARY_KEY", "p8")

	distDir := t.TempDir()
	files := map[string]string{
		"personastack-connector_0.2.0_darwin_arm64.tar.gz": "artifact",
		"personastack-connector_0.2.0_linux_amd64.tar.gz":  "artifact",
		"personastack-connector_0.2.0_checksums.txt": "aaa111  personastack-connector_0.2.0_darwin_arm64.tar.gz\n" +
			"bbb222  personastack-connector_0.2.0_linux_amd64.tar.gz\n",
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
	if len(manifest.Assets) != 3 {
		t.Fatalf("assets len = %d, want 3", len(manifest.Assets))
	}
	var darwinArchive releaseAsset
	var linuxArchive releaseAsset
	for _, asset := range manifest.Assets {
		if asset.Name == "personastack-connector_0.2.0_darwin_arm64.tar.gz" {
			darwinArchive = asset
		}
		if asset.Name == "personastack-connector_0.2.0_linux_amd64.tar.gz" {
			linuxArchive = asset
		}
	}
	if darwinArchive.Name != "personastack-connector_0.2.0_darwin_arm64.tar.gz" || !darwinArchive.DefaultInstallEligible {
		t.Fatalf("expected macOS archive default-install eligibility when signing inputs are present: %+v", darwinArchive)
	}
	if linuxArchive.Name != "personastack-connector_0.2.0_linux_amd64.tar.gz" || linuxArchive.DefaultInstallEligible {
		t.Fatalf("linux archive must not be default-install eligible: %+v", linuxArchive)
	}
}
