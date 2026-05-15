#!/usr/bin/env bash
set -euo pipefail

tag="${GITHUB_REF_NAME:-${1:-}}"
if [[ -z "${tag}" ]]; then
  echo "release tag required" >&2
  exit 1
fi

version="${tag#v}"
notes_path="docs/release-notes/${tag}.md"
if [[ ! -s "${notes_path}" ]]; then
  notes_path="docs/release-notes/v${version}.md"
fi
if [[ ! -s "${notes_path}" ]]; then
  echo "release notes missing for ${tag}; expected docs/release-notes/${tag}.md" >&2
  exit 1
fi

required_sections=(
  "Supported OS/arch"
  "Runtime caveats"
  "Upgrade"
  "Rollback"
)

for section in "${required_sections[@]}"; do
  if ! rg -qi "^#+[[:space:]]+${section//\//\\/}\\b" "${notes_path}"; then
    echo "release notes ${notes_path} missing section: ${section}" >&2
    exit 1
  fi
done
