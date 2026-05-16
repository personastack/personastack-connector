#!/usr/bin/env bash
set -euo pipefail

dist_dir="${1:-dist}"
version="${2:-}"

asset_pattern() {
  local suffix="$1"
  if [[ -n "$version" && "$version" != "auto" ]]; then
    printf 'personastack-connector_%s_%s' "$version" "$suffix"
    return
  fi
  printf 'personastack-connector_*_%s' "$suffix"
}

require_one() {
  local pattern="$1"
  local matches=()
  shopt -s nullglob
  matches=("${dist_dir}"/$pattern)
  shopt -u nullglob
  if [[ "${#matches[@]}" -ne 1 || ! -s "${matches[0]}" ]]; then
    echo "expected exactly one release asset matching ${pattern}" >&2
    exit 1
  fi
  printf '%s' "${matches[0]}"
}

require_one "$(asset_pattern darwin_amd64.tar.gz)" >/dev/null
require_one "$(asset_pattern darwin_arm64.tar.gz)" >/dev/null
linux_amd64_archive="$(require_one "$(asset_pattern linux_amd64.tar.gz)")"
require_one "$(asset_pattern linux_arm64.tar.gz)" >/dev/null
windows_amd64_archive="$(require_one "$(asset_pattern windows_amd64.zip)")"

require_one "$(asset_pattern checksums.txt)" >/dev/null
if [[ -n "$version" && "$version" != "auto" ]]; then
  require_one "$(asset_pattern checksums.txt.sigstore.json)" >/dev/null
fi
find "$dist_dir" -maxdepth 1 -name '*.deb' -print -quit | grep -q .
find "$dist_dir" -maxdepth 1 -name '*.rpm' -print -quit | grep -q .

if [[ -n "$version" && "$version" != "auto" ]]; then
  manifest="${dist_dir}/personastack-connector_${version}_release_manifest.json"
  manifest_sha="${manifest}.sha256"
  if [[ -s "$manifest" && -s "$manifest_sha" ]]; then
    (cd "$dist_dir" && sha256sum -c "$(basename "$manifest_sha")")
  fi
fi

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT
unzip -q "$windows_amd64_archive" -d "$tmp_dir/windows"
windows_binary="$tmp_dir/windows/personastack-connector.exe"
if [[ -n "$version" && "$version" != "auto" ]]; then
  if ! command -v osslsigncode >/dev/null 2>&1; then
    echo "osslsigncode is required to verify Windows Authenticode signatures" >&2
    exit 1
  fi
  if [[ ! -s "$windows_binary" ]]; then
    echo "windows binary missing from release archive: $windows_binary" >&2
    exit 1
  fi
  osslsigncode verify -in "$windows_binary" >/tmp/personastack-connector-windows-signature.out 2>/tmp/personastack-connector-windows-signature.err
  grep -q 'Signature verification: ok' /tmp/personastack-connector-windows-signature.out /tmp/personastack-connector-windows-signature.err
fi
tar -xzf "$linux_amd64_archive" -C "$tmp_dir"
binary="$tmp_dir/personastack-connector"

"$binary" version | grep -q 'personastack-connector version='
"$binary" --help | grep -q 'pair <code>'
"$binary" service plan | grep -q 'service plan kind='
if "$binary" mcp stdio --binding fake </dev/null >/tmp/personastack-connector-smoke.out 2>/tmp/personastack-connector-smoke.err; then
  echo "mcp stdio should fail for a missing fake binding" >&2
  exit 1
fi
grep -q 'binding not found' /tmp/personastack-connector-smoke.err
