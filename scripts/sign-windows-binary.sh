#!/usr/bin/env bash
set -euo pipefail

binary="${1:-}"
if [[ -z "${binary}" ]]; then
  echo "binary path required" >&2
  exit 1
fi

case "${binary}" in
  *.exe) ;;
  *) exit 0 ;;
esac

required="${WINDOWS_CODE_SIGN_REQUIRED:-}"
if [[ "${required}" != "1" ]]; then
  exit 0
fi

: "${WINDOWS_CODE_SIGN_PFX_FILE:?WINDOWS_CODE_SIGN_PFX_FILE is required}"
: "${WINDOWS_CODE_SIGN_PFX_PASSWORD:?WINDOWS_CODE_SIGN_PFX_PASSWORD is required}"

timestamp_url="${WINDOWS_CODE_SIGN_TIMESTAMP_URL:-https://timestamp.digicert.com}"
description="${WINDOWS_CODE_SIGN_DESCRIPTION:-PersonaStack Connector}"
product_url="${WINDOWS_CODE_SIGN_URL:-https://personastack.ai}"

if ! command -v osslsigncode >/dev/null 2>&1; then
  echo "osslsigncode is required to sign windows binaries" >&2
  exit 1
fi

signed_binary="${binary}.signed"
osslsigncode sign \
  -pkcs12 "${WINDOWS_CODE_SIGN_PFX_FILE}" \
  -pass "${WINDOWS_CODE_SIGN_PFX_PASSWORD}" \
  -n "${description}" \
  -i "${product_url}" \
  -h sha256 \
  -t "${timestamp_url}" \
  -in "${binary}" \
  -out "${signed_binary}"
mv "${signed_binary}" "${binary}"
osslsigncode verify -in "${binary}"
