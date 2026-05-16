#!/usr/bin/env bash
set -euo pipefail

dist_dir="${1:-dist}"
version="${2:-${GITHUB_REF_NAME:-}}"
api_url="${PERSONASTACK_API_URL:-}"
admin_token="${PERSONASTACK_ADMIN_BEARER_TOKEN:-}"

if [[ -z "${version}" ]]; then
  echo "version required" >&2
  exit 1
fi
if [[ -z "${api_url}" || -z "${admin_token}" ]]; then
  echo "PERSONASTACK_API_URL and PERSONASTACK_ADMIN_BEARER_TOKEN required" >&2
  exit 1
fi

version="${version#v}"
tag="v${version}"
manifest="${dist_dir}/personastack-connector_${version}_release_manifest.json"

if [[ ! -s "${manifest}" ]]; then
  echo "release manifest missing: ${manifest}" >&2
  exit 1
fi

payload="$(jq -cn \
  --arg version "${tag}" \
  --arg git_commit "$(jq -r '.commit' "${manifest}")" \
  --arg minimum_protocol "$(jq -r '.minimum_protocol' "${manifest}")" \
  '{version:$version, git_commit:$git_commit, minimum_protocol:$minimum_protocol}')"

curl -fsS \
  -H "Authorization: Bearer ${admin_token}" \
  -H "Content-Type: application/json" \
  -X POST \
  --data "${payload}" \
  "${api_url%/}/v1/admin/external-agent-connector/releases" >/dev/null
