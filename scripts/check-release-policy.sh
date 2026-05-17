#!/usr/bin/env bash
set -euo pipefail

if ! command -v gh >/dev/null 2>&1; then
  echo "gh CLI is required for release policy checks" >&2
  exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required for release policy checks" >&2
  exit 1
fi

repo="${GITHUB_REPOSITORY:-personastack/personastack-connector}"

rulesets_json="$(gh api "repos/${repo}/rulesets" 2>/dev/null || true)"
if [[ -z "${rulesets_json}" ]]; then
  if [[ "${GITHUB_ACTIONS:-}" != "true" || "${GITHUB_REF_TYPE:-}" != "tag" || "${GITHUB_REF_NAME:-}" != v* ]]; then
    echo "release tag rulesets unavailable" >&2
    exit 1
  fi
  echo "release tag rulesets unavailable in GitHub Actions; continuing with release environment reviewer gate" >&2
else
  tag_rule_count="$(jq '[.[]? | select(.target == "tag" and (.enforcement // "") == "active")] | length' <<<"${rulesets_json}")"
  if [[ "${tag_rule_count}" -lt 1 ]]; then
    echo "active release tag ruleset missing" >&2
    exit 1
  fi

  tag_ruleset_ids="$(jq -r '.[]? | select(.target == "tag" and (.enforcement // "") == "active") | .id' <<<"${rulesets_json}")"
  tag_ruleset_covers_release_tags=0
  while IFS= read -r ruleset_id; do
    if [[ -z "${ruleset_id}" ]]; then
      continue
    fi
    ruleset_json="$(gh api "repos/${repo}/rulesets/${ruleset_id}" 2>/dev/null || true)"
    if jq -e '((.conditions.ref_name.include // []) | (index("v*") != null or index("refs/tags/v*") != null))' >/dev/null <<<"${ruleset_json}"; then
      tag_ruleset_covers_release_tags=1
      break
    fi
  done <<<"${tag_ruleset_ids}"

  if [[ "${tag_ruleset_covers_release_tags}" -ne 1 ]]; then
    echo "release tag ruleset does not cover v* tags" >&2
    exit 1
  fi
fi

environment_json="$(gh api "repos/${repo}/environments/release" 2>/dev/null || true)"
if [[ -z "${environment_json}" ]]; then
  echo "release environment unavailable" >&2
  exit 1
fi

reviewer_count="$(jq '[.protection_rules[]? | select(.type == "required_reviewers" and ((.reviewers // []) | length > 0))] | length' <<<"${environment_json}")"
if [[ "${reviewer_count}" -lt 1 ]]; then
  echo "release environment missing required reviewers" >&2
  exit 1
fi
