# PersonaStack Connector

PersonaStack Connector is the local bridge for external personas.

It runs on a user's macOS, Linux, or Windows machine, pairs with PersonaStack,
configures a supported local agent runtime, and keeps that runtime wakeable as a
PersonaStack stack member.

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
surface must bind `127.0.0.1` only.

## Build and Release

- Local validation: `go test ./...`
- Race-sensitive validation: `go test -race ./internal/bridge ./internal/daemon ./internal/mcp`
- Security validation: `go run golang.org/x/vuln/cmd/govulncheck@latest ./...`
- Release config validation: `go run github.com/goreleaser/goreleaser/v2@latest check`
- Release artifact smoke: `./scripts/smoke-release-artifacts.sh dist <version>` or
  `./scripts/smoke-release-artifacts.sh dist auto` for snapshot builds.
- CI runs unit tests, formatting, targeted race tests, `go vet`,
  `govulncheck`, license-file validation, and a GoReleaser snapshot dry-run
  with Syft installed for SBOM generation.
- Tagged `v*` releases publish draft GitHub Releases under
  `personastack/personastack-connector` with archives, Linux packages,
  checksums, SBOMs, a machine-readable release manifest plus manifest
  checksum, and provenance attestations.
- V1 signed distribution channels are GitHub Release archives for macOS,
  Linux, and Windows plus Linux `.deb` and `.rpm` packages; package-manager
  metadata for Homebrew and winget is deferred until signed metadata exists.
- Public stable release activation is gated separately by
  `personastack-ship`; routine implementation work does not make a version the
  recommended release.
- The repository is public at `https://github.com/personastack/personastack-connector`;
  release notes and install docs may link to public release artifacts.
