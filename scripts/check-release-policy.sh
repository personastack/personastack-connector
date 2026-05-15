#!/usr/bin/env bash
set -euo pipefail

if ! command -v gh >/dev/null 2>&1; then
  echo "gh CLI is required for release policy checks" >&2
  exit 1
fi

repo="${GITHUB_REPOSITORY:-personastack/personastack-connector}"

rulesets_json="$(gh api "repos/${repo}/rulesets" 2>/dev/null || true)"
if [[ -z "${rulesets_json}" ]]; then
  echo "release tag rulesets unavailable" >&2
  exit 1
fi

tag_rule_count="$(jq '[.[]? | select(.target == "tag")] | length' <<<"${rulesets_json}")"
if [[ "${tag_rule_count}" -lt 1 ]]; then
  echo "release tag ruleset missing" >&2
  exit 1
fi

if ! jq -e '.[]? | select(.target == "tag") | tostring | contains("v*")' >/dev/null <<<"${rulesets_json}"; then
  echo "release tag ruleset does not cover v* tags" >&2
  exit 1
fi

environment_json="$(gh api "repos/${repo}/environments/release" 2>/dev/null || true)"
if [[ -z "${environment_json}" ]]; then
  echo "release environment unavailable" >&2
  exit 1
fi

reviewer_count="$(jq '[.protection_rules[]? | select(.type == "required_reviewers")] | length' <<<"${environment_json}")"
if [[ "${reviewer_count}" -lt 1 ]]; then
  echo "release environment missing required reviewers" >&2
  exit 1
fi
