package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/personastack/agent-gateway/pkg/externalagentprotocol"
)

type releaseManifest struct {
	Assets          []releaseAsset `json:"assets"`
	Commit          string         `json:"commit"`
	GeneratedAt     string         `json:"generated_at"`
	MinimumProtocol string         `json:"minimum_protocol"`
	Project         string         `json:"project"`
	RuntimeKinds    []string       `json:"runtime_kinds"`
	Version         string         `json:"version"`
}

type releaseAsset struct {
	Arch        string `json:"arch,omitempty"`
	Checksum    string `json:"checksum,omitempty"`
	Name        string `json:"name"`
	OS          string `json:"os,omitempty"`
	PackageKind string `json:"package_kind"`
	Path        string `json:"path"`
	Signature   string `json:"signature,omitempty"`
}

func main() {
	distDir := flag.String("dist", "dist", "GoReleaser dist directory")
	version := flag.String("version", strings.TrimPrefix(os.Getenv("GITHUB_REF_NAME"), "v"), "release version")
	commit := flag.String("commit", os.Getenv("GITHUB_SHA"), "git commit")
	output := flag.String("output", "", "manifest output path")
	flag.Parse()

	manifest, err := buildManifest(*distDir, strings.TrimSpace(*version), strings.TrimSpace(*commit), time.Now().UTC())
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	raw, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	raw = append(raw, '\n')
	if strings.TrimSpace(*output) == "" {
		_, _ = os.Stdout.Write(raw)
		return
	}
	if err := os.WriteFile(*output, raw, 0o644); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func buildManifest(distDir string, version string, commit string, generatedAt time.Time) (releaseManifest, error) {
	if strings.TrimSpace(version) == "" {
		return releaseManifest{}, fmt.Errorf("version required")
	}
	if strings.TrimSpace(commit) == "" {
		return releaseManifest{}, fmt.Errorf("commit required")
	}
	checksums, err := readChecksums(distDir)
	if err != nil {
		return releaseManifest{}, err
	}
	entries, err := os.ReadDir(distDir)
	if err != nil {
		return releaseManifest{}, fmt.Errorf("read dist dir: %w", err)
	}
	signatures := map[string]string{}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".sig") {
			continue
		}
		signatures[strings.TrimSuffix(name, ".sig")] = name
	}
	assets := make([]releaseAsset, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || name == "artifacts.json" || name == "metadata.json" || strings.HasSuffix(name, ".sig") {
			continue
		}
		kind := packageKind(name)
		if kind == "" {
			continue
		}
		osName, arch := osArch(name)
		assets = append(assets, releaseAsset{
			Arch:        arch,
			Checksum:    checksums[name],
			Name:        name,
			OS:          osName,
			PackageKind: kind,
			Path:        name,
			Signature:   signatures[name],
		})
	}
	sort.Slice(assets, func(i int, j int) bool {
		return assets[i].Name < assets[j].Name
	})
	return releaseManifest{
		Assets:          assets,
		Commit:          commit,
		GeneratedAt:     generatedAt.Format(time.RFC3339),
		MinimumProtocol: externalagentprotocol.ProtocolVersionV1,
		Project:         "personastack-connector",
		RuntimeKinds:    []string{"hermes", "openclaw"},
		Version:         version,
	}, nil
}

func readChecksums(distDir string) (map[string]string, error) {
	matches, err := filepath.Glob(filepath.Join(distDir, "*_checksums.txt"))
	if err != nil {
		return nil, fmt.Errorf("find checksum file: %w", err)
	}
	if len(matches) == 0 {
		return nil, fmt.Errorf("checksum file missing")
	}
	file, err := os.Open(matches[0])
	if err != nil {
		return nil, fmt.Errorf("open checksum file: %w", err)
	}
	defer file.Close()
	checksums := map[string]string{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) >= 2 {
			checksums[fields[len(fields)-1]] = fields[0]
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read checksum file: %w", err)
	}
	return checksums, nil
}

func packageKind(name string) string {
	switch {
	case strings.HasSuffix(name, ".tar.gz"), strings.HasSuffix(name, ".zip"):
		return "archive"
	case strings.HasSuffix(name, ".deb"):
		return "deb"
	case strings.HasSuffix(name, ".rpm"):
		return "rpm"
	case strings.HasSuffix(name, "_checksums.txt"):
		return "checksum"
	case strings.HasSuffix(name, "_sbom.spdx.json"):
		return "sbom"
	default:
		return ""
	}
}

func osArch(name string) (string, string) {
	for _, osName := range []string{"darwin", "linux", "windows"} {
		for _, arch := range []string{"amd64", "arm64"} {
			if strings.Contains(name, "_"+osName+"_"+arch) {
				return osName, arch
			}
		}
	}
	return "", ""
}
