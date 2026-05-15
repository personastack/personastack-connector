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
		"personastack-connector_0.2.0_windows_amd64.zip":   "artifact",
		"personastack-connector_0.2.0_sbom.spdx.json":      "{}",
		"personastack-connector_0.2.0_checksums.txt": "aaa111  personastack-connector_0.2.0_darwin_arm64.tar.gz\n" +
			"bbb222  personastack-connector_0.2.0_linux_amd64.deb\n" +
			"ccc333  personastack-connector_0.2.0_linux_amd64.rpm\n" +
			"ddd444  personastack-connector_0.2.0_windows_amd64.zip\n" +
			"eee555  personastack-connector_0.2.0_sbom.spdx.json\n",
		"personastack-connector_0.2.0_darwin_arm64.tar.gz.sig": "signature",
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
	if len(manifest.Assets) != 6 {
		t.Fatalf("assets len = %d", len(manifest.Assets))
	}
	archive := manifest.Assets[1]
	if archive.Name != "personastack-connector_0.2.0_darwin_arm64.tar.gz" {
		t.Fatalf("unexpected archive asset: %+v", archive)
	}
	if archive.OS != "darwin" || archive.Arch != "arm64" || archive.PackageKind != "archive" {
		t.Fatalf("unexpected archive metadata: %+v", archive)
	}
	if archive.Checksum != "aaa111" || archive.Signature == "" {
		t.Fatalf("missing checksum/signature: %+v", archive)
	}
}
