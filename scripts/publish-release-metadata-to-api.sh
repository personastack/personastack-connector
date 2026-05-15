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
repo="${GITHUB_REPOSITORY:-personastack/personastack-connector}"
release_base="https://github.com/${repo}/releases/download/${tag}"
cosign_identity="https://github.com/${repo}/.github/workflows/release.yml@refs/tags/${tag}"
cosign_issuer="https://token.actions.githubusercontent.com"
manifest="${dist_dir}/personastack-connector_${version}_release_manifest.json"
checksums="personastack-connector_${version}_checksums.txt"
manifest_name="personastack-connector_${version}_release_manifest.json"
manifest_sha="${manifest_name}.sha256"
manifest_sig="${manifest_name}.sigstore.json"
checksum_sig="${checksums}.sigstore.json"

if [[ ! -s "${manifest}" ]]; then
  echo "release manifest missing: ${manifest}" >&2
  exit 1
fi

jq -c '.assets[] | select((.default_install_eligible == true) and (.os // "") != "" and (.arch // "") != "" and (.package_kind == "archive" or .package_kind == "deb" or .package_kind == "rpm"))' "${manifest}" |
while IFS= read -r asset; do
  os_name="$(jq -r '.os' <<<"${asset}")"
  arch="$(jq -r '.arch' <<<"${asset}")"
  package_kind="$(jq -r '.package_kind' <<<"${asset}")"
  name="$(jq -r '.name' <<<"${asset}")"
  asset_url="${release_base}/${name}"
  checksum_url="${release_base}/${checksums}"
  manifest_url="${release_base}/${manifest_name}"
  manifest_checksum_url="${release_base}/${manifest_sha}"
  manifest_signature_url="${release_base}/${manifest_sig}"
  signature_url="${release_base}/${checksum_sig}"
  unix_verify="curl -fsSL ${manifest_url} -o ${manifest_name} && curl -fsSL ${manifest_checksum_url} -o ${manifest_sha} && curl -fsSL ${manifest_signature_url} -o ${manifest_sig} && cosign verify-blob --bundle ${manifest_sig} --certificate-identity ${cosign_identity} --certificate-oidc-issuer ${cosign_issuer} ${manifest_name} && (sha256sum -c ${manifest_sha} || shasum -a 256 -c ${manifest_sha}) && curl -fsSL ${checksum_url} -o ${checksums} && curl -fsSL ${signature_url} -o ${checksum_sig} && cosign verify-blob --bundle ${checksum_sig} --certificate-identity ${cosign_identity} --certificate-oidc-issuer ${cosign_issuer} ${checksums} && grep '  ${name}$' ${checksums} > ${name}.sha256"
  unix_check="(sha256sum -c ${name}.sha256 || shasum -a 256 -c ${name}.sha256)"
  case "${arch}" in
    amd64)
      unix_arch="x86_64"
      windows_arches="'AMD64','x86_64'"
      ;;
    arm64)
      unix_arch="aarch64"
      windows_arches="'ARM64','aarch64'"
      if [[ "${os_name}" == "darwin" ]]; then
        unix_arch="arm64"
      fi
      ;;
    *)
      unix_arch="${arch}"
      windows_arches="'${arch}'"
      ;;
  esac
  case "${os_name}:${package_kind}" in
    windows:archive)
      install_command="powershell -NoProfile -ExecutionPolicy Bypass -Command \"if (\$env:OS -ne 'Windows_NT' -or \$env:PROCESSOR_ARCHITECTURE -notin @(${windows_arches})) { throw 'Wrong platform: this Connector asset requires windows/${arch}.' }; iwr ${manifest_url} -OutFile ${manifest_name}; iwr ${manifest_checksum_url} -OutFile ${manifest_sha}; iwr ${manifest_signature_url} -OutFile ${manifest_sig}; cosign verify-blob --bundle ${manifest_sig} --certificate-identity ${cosign_identity} --certificate-oidc-issuer ${cosign_issuer} ${manifest_name}; \$manifestExpected=(Select-String -Path ${manifest_sha} -Pattern '  ${manifest_name}\$').Line.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries)[0].ToLower(); \$manifestActual=(Get-FileHash ${manifest_name} -Algorithm SHA256).Hash.ToLower(); if (\$manifestActual -ne \$manifestExpected) { throw 'Checksum mismatch for ${manifest_name}' }; iwr ${checksum_url} -OutFile ${checksums}; iwr ${signature_url} -OutFile ${checksum_sig}; cosign verify-blob --bundle ${checksum_sig} --certificate-identity ${cosign_identity} --certificate-oidc-issuer ${cosign_issuer} ${checksums}; iwr ${asset_url} -OutFile personastack-connector.zip; \$expected=(Select-String -Path ${checksums} -Pattern '  ${name}\$').Line.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries)[0].ToLower(); \$actual=(Get-FileHash personastack-connector.zip -Algorithm SHA256).Hash.ToLower(); if (\$actual -ne \$expected) { throw 'Checksum mismatch for ${name}' }; \$installDir=Join-Path \$env:LOCALAPPDATA 'PersonaStack\\Connector'; Expand-Archive personastack-connector.zip -DestinationPath \$installDir -Force; \$userPath=[Environment]::GetEnvironmentVariable('Path','User'); if ((\$userPath -split ';') -notcontains \$installDir) { [Environment]::SetEnvironmentVariable('Path', ((\$userPath.TrimEnd(';') + ';' + \$installDir).TrimStart(';')), 'User') }; \$env:Path=\$installDir + ';' + \$env:Path; & (Join-Path \$installDir 'personastack-connector.exe') version\""
      ;;
    linux:deb)
      install_command="test \"\$(uname -s)\" = Linux && test \"\$(uname -m)\" = \"${unix_arch}\" || { echo 'Wrong platform: this Connector asset requires linux/${arch}.' >&2; exit 1; }; ${unix_verify} && curl -fsSL ${asset_url} -o ${name} && ${unix_check} && sudo dpkg -i ./${name}"
      ;;
    linux:rpm)
      install_command="test \"\$(uname -s)\" = Linux && test \"\$(uname -m)\" = \"${unix_arch}\" || { echo 'Wrong platform: this Connector asset requires linux/${arch}.' >&2; exit 1; }; ${unix_verify} && curl -fsSL ${asset_url} -o ${name} && ${unix_check} && sudo rpm -i ./${name}"
      ;;
    *)
      install_command="test \"\$(uname -s | tr '[:upper:]' '[:lower:]')\" = \"${os_name}\" && test \"\$(uname -m)\" = \"${unix_arch}\" || { echo 'Wrong platform: this Connector asset requires ${os_name}/${arch}.' >&2; exit 1; }; ${unix_verify} && curl -fsSL ${asset_url} -o ${name} && ${unix_check} && tar -xzf ${name} && mkdir -p ~/.local/bin && install -m 0755 personastack-connector ~/.local/bin/personastack-connector"
      ;;
  esac
  payload="$(jq -cn \
    --arg version "${tag}" \
    --arg git_commit "$(jq -r '.commit' "${manifest}")" \
    --arg os "${os_name}" \
    --arg arch "${arch}" \
    --arg package_kind "${package_kind}" \
    --arg asset_url "${asset_url}" \
    --arg checksum_url "${checksum_url}" \
    --arg manifest_url "${manifest_url}" \
    --arg manifest_checksum_url "${manifest_checksum_url}" \
    --arg signature_url "${signature_url}" \
    --arg install_command "${install_command}" \
    --arg minimum_protocol "$(jq -r '.minimum_protocol' "${manifest}")" \
    '{version:$version, git_commit:$git_commit, os:$os, arch:$arch, package_kind:$package_kind, asset_url:$asset_url, checksum_url:$checksum_url, manifest_url:$manifest_url, manifest_checksum_url:$manifest_checksum_url, signature_url:$signature_url, install_command:$install_command, minimum_protocol:$minimum_protocol}')"
  curl -fsS \
    -H "Authorization: Bearer ${admin_token}" \
    -H "Content-Type: application/json" \
    -X POST \
    --data "${payload}" \
    "${api_url%/}/v1/admin/external-agent-connector/releases" >/dev/null
done
