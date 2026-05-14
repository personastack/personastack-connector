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
