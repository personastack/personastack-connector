#!/usr/bin/env bash
set -euo pipefail

required="${WINDOWS_CODE_SIGN_REQUIRED:-}"
if [[ "${required}" != "1" ]]; then
  exit 0
fi

: "${WINDOWS_CODE_SIGN_PFX_FILE:?WINDOWS_CODE_SIGN_PFX_FILE is required}"
: "${WINDOWS_CODE_SIGN_PFX_PASSWORD:?WINDOWS_CODE_SIGN_PFX_PASSWORD is required}"
: "${WINDOWS_CODE_SIGN_TIMESTAMP_URL:?WINDOWS_CODE_SIGN_TIMESTAMP_URL is required}"

if [[ ! -s "${WINDOWS_CODE_SIGN_PFX_FILE}" ]]; then
  echo "windows code signing certificate file missing: ${WINDOWS_CODE_SIGN_PFX_FILE}" >&2
  exit 1
fi

openssl pkcs12 -in "${WINDOWS_CODE_SIGN_PFX_FILE}" -passin "pass:${WINDOWS_CODE_SIGN_PFX_PASSWORD}" -noout
