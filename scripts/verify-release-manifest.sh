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
manifest_sig="${manifest}.sigstore.json"
checksums_sig="${dist_dir}/personastack-connector_${version}_checksums.txt.sigstore.json"

if [[ ! -s "${manifest}" ]]; then
  echo "release manifest missing: ${manifest}" >&2
  exit 1
fi

if [[ ! -s "${manifest_sha}" ]]; then
  echo "release manifest checksum missing: ${manifest_sha}" >&2
  exit 1
fi

if [[ ! -s "${manifest_sig}" ]]; then
  echo "release manifest signature bundle missing: ${manifest_sig}" >&2
  exit 1
fi

if [[ ! -s "${checksums_sig}" ]]; then
  echo "release checksum signature bundle missing: ${checksums_sig}" >&2
  exit 1
fi

(cd "${dist_dir}" && sha256sum -c "$(basename "${manifest_sha}")")

if command -v cosign >/dev/null 2>&1; then
  : "${COSIGN_CERTIFICATE_IDENTITY:?COSIGN_CERTIFICATE_IDENTITY is required when cosign is available}"
  : "${COSIGN_CERTIFICATE_OIDC_ISSUER:=https://token.actions.githubusercontent.com}"
  cosign verify-blob \
    --bundle "${manifest_sig}" \
    --certificate-identity "${COSIGN_CERTIFICATE_IDENTITY}" \
    --certificate-oidc-issuer "${COSIGN_CERTIFICATE_OIDC_ISSUER}" \
    "${manifest}"
  cosign verify-blob \
    --bundle "${checksums_sig}" \
    --certificate-identity "${COSIGN_CERTIFICATE_IDENTITY}" \
    --certificate-oidc-issuer "${COSIGN_CERTIFICATE_OIDC_ISSUER}" \
    "${dist_dir}/personastack-connector_${version}_checksums.txt"
fi
