# Development and Audit

This repository is source-available for public audit. It is not open source;
see `LICENSE` before using any code outside source inspection.

## Clean Clone Checks

- `go list -mod=mod ./...`
- `go test ./...`
- `go test -race ./internal/bridge ./internal/daemon ./internal/mcp`
- `go run golang.org/x/vuln/cmd/govulncheck@latest ./...`

## Release Verification

Release assets are published through GitHub Releases. A release is auditable by
matching the release tag to this repository, checking `personastack-connector
version`, and verifying the published checksum and sigstore bundles.

The Connector stores bridge credentials, PersonaStack MCP credentials, local
runtime credentials, and local MCP proxy secrets in OS credential storage or an
owner-only encrypted fallback store.
