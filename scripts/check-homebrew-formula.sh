#!/usr/bin/env bash
set -euo pipefail

generated_formula="${1:?generated formula path required}"
tap_formula="${2:?tap formula path required}"

if [[ ! -s "${generated_formula}" ]]; then
  echo "generated formula missing: ${generated_formula}" >&2
  exit 1
fi
if [[ ! -s "${tap_formula}" ]]; then
  echo "tap formula missing: ${tap_formula}" >&2
  exit 1
fi

generated_version="$(awk -F'"' '/^[[:space:]]*version "/ { print $2; exit }' "${generated_formula}")"
tap_version="$(awk -F'"' '/^[[:space:]]*version "/ { print $2; exit }' "${tap_formula}")"
if [[ -z "${generated_version}" || -z "${tap_version}" ]]; then
  echo "formula version missing" >&2
  exit 1
fi
if [[ "${generated_version}" != "${tap_version}" ]]; then
  echo "formula version mismatch: generated=${generated_version} tap=${tap_version}" >&2
  exit 1
fi

url_count="$(grep -Ec '^[[:space:]]*url "https://github.com/personastack/personastack-connector/releases/download/v[0-9]+\.[0-9]+\.[0-9]+/personastack-connector_[0-9]+\.[0-9]+\.[0-9]+_(darwin|linux)_(arm64|amd64)\.tar\.gz"$' "${tap_formula}")"
if [[ "${url_count}" != "4" ]]; then
  echo "expected four pinned platform URLs in tap formula, got ${url_count}" >&2
  exit 1
fi

sha_count="$(grep -Ec '^[[:space:]]*sha256 "[0-9a-f]{64}"$' "${tap_formula}")"
if [[ "${sha_count}" != "4" ]]; then
  echo "expected four sha256 values in tap formula, got ${sha_count}" >&2
  exit 1
fi

if ! diff -u "${generated_formula}" "${tap_formula}" >/dev/null; then
  echo "tap formula differs from generated formula" >&2
  diff -u "${generated_formula}" "${tap_formula}" >&2 || true
  exit 1
fi

echo "homebrew formula matches generated metadata version=${tap_version}"
