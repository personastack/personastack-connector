#!/usr/bin/env bash
set -euo pipefail

artifact="${1:?artifact required}"
signature="${2:?signature required}"

if [[ "${PERSONASTACK_RELEASE_SIGNING:-}" != "1" ]]; then
  exit 0
fi

if ! command -v cosign >/dev/null 2>&1; then
  echo "cosign is required for release signing" >&2
  exit 1
fi

cosign sign-blob --bundle "${signature}" "${artifact}" --yes
