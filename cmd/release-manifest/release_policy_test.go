package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestCheckReleasePolicyScript(t *testing.T) {
	t.Run("accepts active tag ruleset with reviewers", func(t *testing.T) {
		out := runReleasePolicyScript(t, `#!/usr/bin/env bash
set -euo pipefail

case "$2" in
  repos/personastack/personastack-connector/rulesets)
    cat <<'JSON'
[{"id":1,"target":"tag","enforcement":"active"}]
JSON
    ;;
  repos/personastack/personastack-connector/rulesets/1)
    cat <<'JSON'
{"target":"tag","enforcement":"active","conditions":{"ref_name":{"include":["v*"]}}}
JSON
    ;;
  repos/personastack/personastack-connector/environments/release)
    cat <<'JSON'
{"protection_rules":[{"type":"required_reviewers","reviewers":[{"login":"eg"}]}]}
JSON
    ;;
  *)
    echo "unexpected gh api endpoint: $2" >&2
    exit 1
    ;;
esac
`)
		if out != "" {
			t.Fatalf("expected no output, got %q", out)
		}
	})

	t.Run("rejects missing tag coverage", func(t *testing.T) {
		out, err := runReleasePolicyScriptErr(t, `#!/usr/bin/env bash
set -euo pipefail

case "$2" in
  repos/personastack/personastack-connector/rulesets)
    cat <<'JSON'
[{"id":1,"target":"tag","enforcement":"active"}]
JSON
    ;;
  repos/personastack/personastack-connector/rulesets/1)
    cat <<'JSON'
{"target":"tag","enforcement":"active","conditions":{"ref_name":{"include":["release-*"]}}}
JSON
    ;;
  repos/personastack/personastack-connector/environments/release)
    cat <<'JSON'
{"protection_rules":[{"type":"required_reviewers","reviewers":[{"login":"eg"}]}]}
JSON
    ;;
  *)
    echo "unexpected gh api endpoint: $2" >&2
    exit 1
    ;;
esac
`)
		if err == nil {
			t.Fatalf("expected failure")
		}
		if !strings.Contains(out, "release tag ruleset does not cover v* tags") {
			t.Fatalf("unexpected output: %q", out)
		}
	})

	t.Run("rejects missing reviewers", func(t *testing.T) {
		out, err := runReleasePolicyScriptErr(t, `#!/usr/bin/env bash
set -euo pipefail

case "$2" in
  repos/personastack/personastack-connector/rulesets)
    cat <<'JSON'
[{"id":1,"target":"tag","enforcement":"active"}]
JSON
    ;;
  repos/personastack/personastack-connector/rulesets/1)
    cat <<'JSON'
{"target":"tag","enforcement":"active","conditions":{"ref_name":{"include":["v*"]}}}
JSON
    ;;
  repos/personastack/personastack-connector/environments/release)
    cat <<'JSON'
{"protection_rules":[{"type":"required_reviewers","reviewers":[]}]}
JSON
    ;;
  *)
    echo "unexpected gh api endpoint: $2" >&2
    exit 1
    ;;
esac
`)
		if err == nil {
			t.Fatalf("expected failure")
		}
		if !strings.Contains(out, "release environment missing required reviewers") {
			t.Fatalf("unexpected output: %q", out)
		}
	})
}

func runReleasePolicyScript(t *testing.T, ghScript string) string {
	t.Helper()

	out, err := runReleasePolicyScriptErr(t, ghScript)
	if err != nil {
		t.Fatalf("release policy script failed: %v\n%s", err, out)
	}
	return out
}

func runReleasePolicyScriptErr(t *testing.T, ghScript string) (string, error) {
	t.Helper()

	tempDir := t.TempDir()
	writeExecutable(t, tempDir, "gh", ghScript)

	cmd := exec.Command("bash", "../../scripts/check-release-policy.sh")
	cmd.Env = append(
		envWithoutKeys(os.Environ(), "PATH", "GITHUB_REPOSITORY"),
		"PATH="+tempDir+string(os.PathListSeparator)+os.Getenv("PATH"),
		"GITHUB_REPOSITORY=personastack/personastack-connector",
	)
	output, err := cmd.CombinedOutput()
	return string(output), err
}

func writeExecutable(t *testing.T, dir string, name string, content string) {
	t.Helper()

	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o755); err != nil {
		t.Fatalf("write executable %s: %v", name, err)
	}
}

func envWithoutKeys(values []string, keys ...string) []string {
	blocked := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		blocked[key+"="] = struct{}{}
	}
	filtered := make([]string, 0, len(values))
	for _, value := range values {
		keep := true
		for prefix := range blocked {
			if strings.HasPrefix(value, prefix) {
				keep = false
				break
			}
		}
		if keep {
			filtered = append(filtered, value)
		}
	}
	return filtered
}
