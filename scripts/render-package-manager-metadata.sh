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
windows_amd="personastack-connector_${version}_windows_amd64.zip"

darwin_arm_sha="$(require_checksum "${darwin_arm}")"
darwin_amd_sha="$(require_checksum "${darwin_amd}")"
windows_amd_sha="$(require_checksum "${windows_amd}")"

homebrew_dir="${dist_dir}/package-manager/homebrew"
winget_dir="${dist_dir}/package-manager/winget/PersonaStack.Connector/${version}"
mkdir -p "${homebrew_dir}" "${winget_dir}"

cat >"${homebrew_dir}/personastack-connector.rb" <<EOF
cask "personastack-connector" do
  version "${version}"

  on_arm do
    sha256 "${darwin_arm_sha}"
    url "${base_url}/${darwin_arm}"
  end

  on_intel do
    sha256 "${darwin_amd_sha}"
    url "${base_url}/${darwin_amd}"
  end

  name "PersonaStack Connector"
  desc "Local Connector for PersonaStack external personas"
  homepage "https://personastack.ai"

  binary "personastack-connector"
end
EOF

cat >"${winget_dir}/PersonaStack.Connector.yaml" <<EOF
PackageIdentifier: PersonaStack.Connector
PackageVersion: ${version}
DefaultLocale: en-US
ManifestType: version
ManifestVersion: 1.9.0
EOF

cat >"${winget_dir}/PersonaStack.Connector.locale.en-US.yaml" <<EOF
PackageIdentifier: PersonaStack.Connector
PackageVersion: ${version}
PackageLocale: en-US
Publisher: PersonaStack
PackageName: PersonaStack Connector
License: Proprietary
ShortDescription: Local Connector for PersonaStack external personas
ManifestType: defaultLocale
ManifestVersion: 1.9.0
EOF

cat >"${winget_dir}/PersonaStack.Connector.installer.yaml" <<EOF
PackageIdentifier: PersonaStack.Connector
PackageVersion: ${version}
InstallerType: zip
NestedInstallerType: portable
NestedInstallerFiles:
  - RelativeFilePath: personastack-connector.exe
    PortableCommandAlias: personastack-connector
Installers:
  - Architecture: x64
    InstallerUrl: ${base_url}/${windows_amd}
    InstallerSha256: ${windows_amd_sha}
ManifestType: installer
ManifestVersion: 1.9.0
EOF

echo "wrote package-manager metadata under ${dist_dir}/package-manager"
