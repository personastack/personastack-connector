#!/usr/bin/env bash
set -euo pipefail

tap_formula="${1:?tap formula path required}"
api_url="${PERSONASTACK_API_URL:-}"
admin_token="${PERSONASTACK_ADMIN_BEARER_TOKEN:-}"

if [[ ! -s "${tap_formula}" ]]; then
  echo "tap formula missing: ${tap_formula}" >&2
  exit 1
fi
if [[ -z "${api_url}" || -z "${admin_token}" ]]; then
  echo "PERSONASTACK_API_URL and PERSONASTACK_ADMIN_BEARER_TOKEN required" >&2
  exit 1
fi
if ! command -v curl >/dev/null 2>&1; then
  echo "curl is required for connector release metadata checks" >&2
  exit 1
fi
if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required for connector release metadata checks" >&2
  exit 1
fi

tap_version="$(awk -F'"' '/^[[:space:]]*version "/ { print $2; exit }' "${tap_formula}")"
if [[ -z "${tap_version}" ]]; then
  echo "tap formula version missing" >&2
  exit 1
fi

for archive_target in darwin_arm64 darwin_amd64 linux_arm64 linux_amd64; do
  archive_url="      url \"https://github.com/personastack/personastack-connector/releases/download/v${tap_version}/personastack-connector_${tap_version}_${archive_target}.tar.gz\""
  if ! grep -Fqx "${archive_url}" "${tap_formula}"; then
    echo "tap formula missing pinned archive URL: ${archive_url}" >&2
    exit 1
  fi
done

tap_sha_count="$(grep -Ec '^[[:space:]]*sha256 "[0-9a-f]{64}"$' "${tap_formula}")"
if [[ "${tap_sha_count}" != "4" ]]; then
  echo "tap formula does not contain four sha256 values" >&2
  exit 1
fi

expected_version="v${tap_version}"
metadata_json="$(curl -fsS \
  -H "Authorization: Bearer ${admin_token}" \
  -H "Cookie: personastack_token=${admin_token}" \
  -H "Accept: application/json" \
  "${api_url%/}/v1/admin/external-agent-connector/releases")"

errors="$(jq -r --arg version "${expected_version}" '
  def expected_targets: [
    {os:"darwin", arch:"amd64", package_kind:"homebrew", suffix:"_darwin_amd64.tar.gz", install:"brew install personastack/tap/personastack-connector"},
    {os:"darwin", arch:"arm64", package_kind:"homebrew", suffix:"_darwin_arm64.tar.gz", install:"brew install personastack/tap/personastack-connector"},
    {os:"linux", arch:"amd64", package_kind:"deb", suffix:"_linux_amd64.deb", install:"curl -fsSLO ASSET_URL && sudo apt install ./ASSET_NAME"},
    {os:"linux", arch:"arm64", package_kind:"deb", suffix:"_linux_arm64.deb", install:"curl -fsSLO ASSET_URL && sudo apt install ./ASSET_NAME"},
    {os:"linux", arch:"amd64", package_kind:"rpm", suffix:"_linux_amd64.rpm", install:"curl -fsSLO ASSET_URL && sudo dnf install ./ASSET_NAME"},
    {os:"linux", arch:"arm64", package_kind:"rpm", suffix:"_linux_arm64.rpm", install:"curl -fsSLO ASSET_URL && sudo dnf install ./ASSET_NAME"}
  ];
  def artifact_version: ($version | sub("^v"; ""));
  def expected_asset_url($target):
    "https://github.com/personastack/personastack-connector/releases/download/" + $version + "/personastack-connector_" + artifact_version + $target.suffix;
  def expected_checksum_url:
    "https://github.com/personastack/personastack-connector/releases/download/" + $version + "/personastack-connector_" + artifact_version + "_checksums.txt";
  def expected_manifest_url:
    "https://github.com/personastack/personastack-connector/releases/download/" + $version + "/personastack-connector_" + artifact_version + "_release_manifest.json";
  def expected_install_command($target; $asset_url):
    if $target.package_kind == "homebrew" then
      $target.install
    else
      ($target.install | sub("ASSET_URL"; $asset_url) | sub("ASSET_NAME"; ($asset_url | split("/")[-1])))
    end;
  def target_errors($target):
    (.releases // [] | map(select(
      (.recommended == true) and
      (.os == $target.os) and
      (.arch == $target.arch) and
      (.package_kind == $target.package_kind)
    ))) as $matches |
    if ($matches | length) != 1 then
      ["expected exactly one recommended release for " + $target.os + "/" + $target.arch + "/" + $target.package_kind + ", got " + (($matches | length) | tostring)]
    else
      ($matches[0]) as $release |
      (expected_asset_url($target)) as $asset_url |
      [
        if $release.version != $version then "version mismatch for " + $target.os + "/" + $target.arch + "/" + $target.package_kind + ": " + ($release.version // "<missing>") + " != " + $version else empty end,
        if $release.asset_url != $asset_url then "asset_url mismatch for " + $target.os + "/" + $target.arch + "/" + $target.package_kind else empty end,
        if $release.checksum_url != expected_checksum_url then "checksum_url mismatch for " + $target.os + "/" + $target.arch + "/" + $target.package_kind else empty end,
        if ($release.manifest_url // "") != expected_manifest_url then "manifest_url mismatch for " + $target.os + "/" + $target.arch + "/" + $target.package_kind else empty end,
        if ($release.install_command // "") != expected_install_command($target; $asset_url) then "install_command mismatch for " + $target.os + "/" + $target.arch + "/" + $target.package_kind else empty end
      ]
    end;
  if (.releases | type) != "array" then
    ["API response missing releases array"]
  else
    [expected_targets[] as $target | target_errors($target)[]]
  end | .[]
' <<<"${metadata_json}")"

if [[ -n "${errors}" ]]; then
  echo "${errors}" >&2
  exit 1
fi

echo "api connector release metadata matches tap formula version=${tap_version}"
