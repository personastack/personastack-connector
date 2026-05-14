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
  checksums, SBOMs, a machine-readable release manifest, and provenance
  attestations.
