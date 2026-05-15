#!/usr/bin/env bash
set -euo pipefail

dist_dir="${1:-dist}"
version="${2:-}"

if [[ -z "${version}" ]]; then
  echo "release version required" >&2
  exit 1
fi

manifest="${dist_dir}/personastack-connector_${version}_release_manifest.json"
manifest_sha="${manifest}.sha256"

if [[ ! -s "${manifest}" ]]; then
  echo "release manifest missing: ${manifest}" >&2
  exit 1
fi

if [[ ! -s "${manifest_sha}" ]]; then
  echo "release manifest checksum missing: ${manifest_sha}" >&2
  exit 1
fi

(cd "${dist_dir}" && sha256sum -c "$(basename "${manifest_sha}")")

manifest_sig="${manifest}.sig"
if [[ -s "${manifest_sig}" ]]; then
  echo "release manifest signature present: ${manifest_sig}"
fi
