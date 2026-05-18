# Agent Instructions

## Scope

- This repository owns the user-installed `personastack-connector` binary.
- Keep the Connector local-first: it must not require users to expose local
  Hermes/OpenClaw ports to the internet.
- The Connector is a runtime bridge, not a PersonaStack integration provider.
- Do not attach PersonaStack integrations to external personas from this repo.

## Authority

- `personastack-api` owns durable persona, pairing, prompt, run, readiness,
  orbit, catalog, and install metadata state.
- `agent-gateway` owns Connector websocket transport and the versioned Connector
  protocol package.
- `mcp` owns PersonaStack MCP tool execution and authorization.
- This repository owns local runtime adapters, local MCP proxying, OS service
  registration, local credential storage, and local diagnostics.

## Implementation Rules

- Use Go unless a later accepted spec update changes the implementation language.
- Use concrete structs and typed enums for known protocol, config, state, and
  adapter domains.
- Store bridge tokens, PersonaStack MCP tokens, local runtime API keys, and local
  MCP proxy secrets only in OS credential storage.
- Keep native runtime config free of PersonaStack bearer tokens in the default
  path by using the Connector-owned stdio MCP proxy.
- Bind any local HTTP helper only to loopback.
- Redact tokens, prompts, local paths, and runtime secrets from diagnostics by
  default.
- Import PersonaStack shared protocol/client packages through GitHub module
  paths; do not copy DTOs from sibling repositories.

## Validation

- Add focused tests for Connector core, protocol handling, local MCP proxying,
  Hermes adapter behavior, OpenClaw adapter behavior, OS service planning, and
  config mutation rollback.
- Commit coherent slices with Conventional Commit subjects.
