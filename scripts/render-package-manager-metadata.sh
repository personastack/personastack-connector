#!/usr/bin/env bash
set -euo pipefail

dist_dir="${1:?dist dir required}"
version="${2:?version required}"

tag="v${version}"
base_url="https://github.com/personastack/personastack-connector/releases/download/${tag}"
checksums="${dist_dir}/personastack-connector_${version}_checksums.txt"

if [[ ! -s "${checksums}" ]]; then
  echo "checksums file missing: ${checksums}" >&2
  exit 1
fi

checksum_for() {
  local file="$1"
  awk -v file="${file}" '$2 == file { print $1 }' "${checksums}"
}

require_checksum() {
  local file="$1"
  local checksum
  checksum="$(checksum_for "${file}")"
  if [[ -z "${checksum}" ]]; then
    echo "checksum missing for ${file}" >&2
    exit 1
  fi
  printf '%s' "${checksum}"
}

darwin_arm="personastack-connector_${version}_darwin_arm64.tar.gz"
darwin_amd="personastack-connector_${version}_darwin_amd64.tar.gz"
linux_arm="personastack-connector_${version}_linux_arm64.tar.gz"
linux_amd="personastack-connector_${version}_linux_amd64.tar.gz"

darwin_arm_sha="$(require_checksum "${darwin_arm}")"
darwin_amd_sha="$(require_checksum "${darwin_amd}")"
linux_arm_sha="$(require_checksum "${linux_arm}")"
linux_amd_sha="$(require_checksum "${linux_amd}")"

homebrew_dir="${dist_dir}/package-manager/homebrew"
mkdir -p "${homebrew_dir}"

cat >"${homebrew_dir}/personastack-connector.rb" <<EOF
class PersonastackConnector < Formula
  desc "Local Connector for PersonaStack external personas"
  homepage "https://personastack.ai"
  version "${version}"
  license :cannot_represent

  on_macos do
    if Hardware::CPU.arm?
      url "${base_url}/${darwin_arm}"
      sha256 "${darwin_arm_sha}"
    else
      url "${base_url}/${darwin_amd}"
      sha256 "${darwin_amd_sha}"
    end
  end

  on_linux do
    if Hardware::CPU.arm?
      url "${base_url}/${linux_arm}"
      sha256 "${linux_arm_sha}"
    else
      url "${base_url}/${linux_amd}"
      sha256 "${linux_amd_sha}"
    end
  end

  def install
    bin.install "personastack-connector"
  end

  test do
    assert_match "personastack-connector version=", shell_output("#{bin}/personastack-connector version")
  end
end
EOF

echo "wrote package-manager metadata under ${dist_dir}/package-manager"
