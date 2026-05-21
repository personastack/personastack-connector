# Architecture Decision Records

This repository records accepted connector-local architecture decisions here.

## Accepted Decisions

- The connector is a local, user-installed bridge that owns runtime discovery, local MCP configuration, bridge websocket sessions, wake probes, and native Hermes/OpenClaw run dispatch.
- PersonaStack API remains the authority for external persona bindings, credentials, connector release metadata, run admission, and persisted run state.
- Agent Gateway owns live websocket routing and wake/dispatch delivery, including multi-replica routing through Redis when the connected connector is owned by another gateway replica.
- Native Hermes/OpenClaw MCP configuration is managed by the connector MCP package, not by runtime adapters.
- A binding is dispatchable only after the gateway can reach the connector and the connector has accepted a wake probe for a runtime whose MCP configuration has been verified.
- 2026-05-21: Connector installs PersonaStack MCP directly into Hermes/OpenClaw native config using the durable persona-scoped bearer token as a bearer header. This intentionally exposes the token to owner-local native runtime config so the user can call PersonaStack MCP from Hermes/OpenClaw outside PersonaStack-dispatched runs.
