# PersonaStack Connector

PersonaStack Connector is the local bridge for external personas.

It runs on a user's macOS, Linux, or Windows machine, pairs with PersonaStack,
configures a supported local agent runtime, and keeps that runtime wakeable as a
PersonaStack stack member.

Hermes Termux installs are not Connector hosts. OpenClaw mobile nodes are also
not Connector hosts because the Gateway still has to run on macOS, Linux,
Windows, or Windows/WSL2.

Initial supported runtime targets:

- Hermes Agent
- OpenClaw

The Connector is not a PersonaStack integration and does not expose local
Hermes/OpenClaw tools as PersonaStack tools. It owns local runtime detection,
local MCP proxying, local credential storage, daemon startup, and local dispatch
into the selected runtime.

Shared product state, pairing state, run admission, PromptStack rendering, and
browser-visible runtime state remain owned by `personastack-api`.

Default pairing does not claim full wakeable success until the authenticated
bridge, local runtime health, native MCP verification, and wake probe have all
reported through PersonaStack. OpenClaw users must provide an approved operator
credential during pairing with `--openclaw-token`, `--openclaw-password`, or
`--openclaw-device-token`, or through the matching `OPENCLAW_GATEWAY_*`
environment variable.

The Connector has no public or LAN-facing local control listener in V1. CLI and
stdio MCP control are local process interfaces; any future loopback control
surface must bind `127.0.0.1` only. Any tray/menu UX is optional and limited to
status, repair, logs, and pairing; headless Linux must stay CLI-first.

Hermes fallback through responses or chat completions is degraded-only. OpenClaw
CLI fallback is also degraded-only and does not imply full Gateway streaming or
cancel support.

## Build and Release

- Local validation: `go test ./...`
- Race-sensitive validation: `go test -race ./internal/bridge ./internal/daemon ./internal/mcp`
- Security validation: `go run golang.org/x/vuln/cmd/govulncheck@latest ./...`
- Release config validation: `go run github.com/goreleaser/goreleaser/v2@latest check`
- Release artifact smoke: `./scripts/smoke-release-artifacts.sh dist <version>` or
  `./scripts/smoke-release-artifacts.sh dist auto` for snapshot builds.
- CI runs unit tests, formatting, targeted race tests, `go vet`,
  `govulncheck`, license-file validation, and a GoReleaser snapshot dry-run
  with Syft installed for SBOM generation. Snapshot artifacts are validation
  only and are not a distribution channel.
- Tagged `v*` releases publish draft GitHub Releases under
  `personastack/personastack-connector` with archives, Linux packages,
  checksums, SBOMs, a machine-readable release manifest plus manifest
  checksum, signed checksum bundles, a signed release manifest bundle, and
  provenance attestations.
- Each shippable upload is identified by one shipping-agent-selected semver
  tag. Admin release activation sends only that semver to PersonaStack API,
  which derives every canonical GitHub Release asset URL from the tag and
  supported OS/arch/package matrix.
- macOS binaries are signed and notarized through GoReleaser only when the
  release environment provides the Apple signing and App Store Connect inputs;
  default install metadata must stay gated off until that pipeline is enabled.
- macOS release artifacts stay split by `darwin/amd64` and `darwin/arm64`;
  Connector setup still uses LaunchAgent registration after installation.
- V1 signed distribution channels are GitHub Release archives for macOS,
  Linux, and Windows plus Linux `.deb` and `.rpm` packages; tagged releases
  also publish Homebrew cask and winget metadata generated from the signed
  checksum file.
- Release metadata only promotes installer defaults when the release signing
  gate is active; Linux package defaults stay off until that gate is enabled.
- Default install guidance must verify the signed release manifest and
  checksum bundle before recommending install commands.
- Release notes must enumerate supported OS/arch, known runtime caveats,
  upgrade notes, and a rollback command.
- V1 binary distribution stays GitHub Releases only; mirrored binary hosts are
  a later decision, not part of the initial public install path.
- Update prompts stay package-manager/manual only until signed auto-update
  launch scope is explicitly decided and shipped.
- The tagged release workflow declares the protected `release` environment and
  fails closed unless GitHub CLI metadata shows active `v*` tag rules and at
  least one required reviewer on that environment.
- Public stable release activation is gated separately by
  `personastack-ship`; routine implementation work does not make a version the
  recommended release.
- The repository is public at `https://github.com/personastack/personastack-connector`;
  release notes and install docs may link to public release artifacts.
