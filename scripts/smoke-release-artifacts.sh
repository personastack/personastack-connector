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

darwin_amd64_archive="$(require_one "$(asset_pattern darwin_amd64.tar.gz)")"
darwin_arm64_archive="$(require_one "$(asset_pattern darwin_arm64.tar.gz)")"
linux_amd64_archive="$(require_one "$(asset_pattern linux_amd64.tar.gz)")"
require_one "$(asset_pattern linux_arm64.tar.gz)" >/dev/null

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
host_os="$(uname -s)"
host_arch="$(uname -m)"
case "${host_os}/${host_arch}" in
  Darwin/arm64)
    smoke_archive="$darwin_arm64_archive"
    ;;
  Darwin/x86_64)
    smoke_archive="$darwin_amd64_archive"
    ;;
  Linux/x86_64)
    smoke_archive="$linux_amd64_archive"
    ;;
  *)
    echo "unsupported smoke host ${host_os}/${host_arch}" >&2
    exit 1
    ;;
esac
tar -xzf "$smoke_archive" -C "$tmp_dir"
binary="$tmp_dir/personastack-connector"

"$binary" version | grep -q 'personastack-connector version='
echo "verified version command"
"$binary" --help | grep -q 'pair <code>'
echo "verified help command"
"$binary" service plan | grep -q 'service plan kind='
echo "verified service plan command"
if "$binary" mcp stdio --binding fake </dev/null >/tmp/personastack-connector-smoke.out 2>/tmp/personastack-connector-smoke.err; then
  echo "mcp stdio should fail for a missing fake binding" >&2
  exit 1
fi
grep -Eq 'binding not found|missing binding' /tmp/personastack-connector-smoke.err
echo "verified missing binding failure"
