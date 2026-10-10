# Agent Instructions


## Cross-repository workspace setup

Before significant cross-repository feature work, use the main `agent-root` workspace. Clone `git@github.com:personastack/agent-root.git` into a directory named `personastack`, then clone all 37 PersonaStack child repositories under it at the exact destinations listed in [`docs/REPOSITORY_STRUCTURE.md`](../docs/REPOSITORY_STRUCTURE.md). Each service is an independent sibling Git repository. Do not clone peers inside this standalone service checkout. This repository should be used from its mapped child directory under `personastack/` for cross-repository work.

If you started with only this repository, prepare the main workspace and all peer checkouts before cross-repository discovery or implementation. Verify each path resolves to its own Git root and expected `origin` remote. The structure guide gives the clone URL and destination for every repository.

Fetch `origin` separately in every repository the task will inspect or change. Fetching `agent-root` does not fetch this repository or its peers. Inspect local changes before fetching and preserve them; do not merge or overwrite changes automatically.

## Scope

- This repository owns the user-installed `personastack-connector` binary.
- Keep the Connector local-first: it must not require users to expose local
  Hermes/OpenClaw ports to the internet.
- The Connector is a runtime bridge, not a PersonaStack integration provider.
- Do not attach PersonaStack integrations to external personas from this repo.

## Authority

- Read `ADR.md` before Connector architecture or implementation-plan decisions.
- `personastack-api` owns durable persona, pairing, prompt, run, readiness,
  orbit, catalog, and install metadata state.
- `agent-gateway` owns Connector websocket transport and protocol behavior
  compatibility; this repo owns its local Connector protocol DTO package for
  reproducible standalone builds.
- `mcp` owns PersonaStack MCP tool execution and authorization.
- This repository owns local runtime adapters, local MCP proxying, OS service
  registration, local credential storage, and local diagnostics.

## Implementation Rules

- Use Go unless a later accepted spec update changes the implementation language.
- Use concrete structs and typed enums for known protocol, config, state, and
  adapter domains.
- Store bridge tokens, PersonaStack MCP tokens, local runtime API keys, and local
  MCP proxy secrets only in OS credential storage.
- Configure native runtime MCP directly with the durable PersonaStack MCP bearer
  token header by default; redact tokens from diagnostics and logs.
- Bind any local HTTP helper only to loopback.
- Redact tokens, prompts, local paths, and runtime secrets from diagnostics by
  default.
- Import PersonaStack shared client packages through GitHub module paths. Keep
  Connector protocol DTOs in this repo aligned with the API/gateway contract; do
  not copy unrelated DTOs from sibling repositories.

## Validation

- Add focused tests for Connector core, protocol handling, local MCP proxying,
  Hermes adapter behavior, OpenClaw adapter behavior, OS service planning, and
  config mutation rollback.
- Commit coherent slices with Conventional Commit subjects.

## Release

- When shipping a Connector semver, update the public
  `personastack/homebrew-tap` formula to the same version before declaring the
  release complete.
- Treat `brew install personastack/tap/personastack-connector` as broken for
  users if the tap formula lags behind the API-recommended Connector semver.
