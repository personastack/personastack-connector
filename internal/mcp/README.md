# mcp

`mcp` owns local PersonaStack MCP proxy entry points and native runtime MCP
configuration.

`mcp install` writes native Hermes/OpenClaw streamable HTTP config that points
directly at PersonaStack MCP and includes the durable persona-scoped bearer
token in the runtime's owner-local config.

Live verification performs an MCP initialize, initialized notification, and
tools/list call with the binding credential before daemon heartbeats report MCP
as verified.

The stdio proxy remains available as an explicit fallback/debug path. It loads
the paired binding's PersonaStack MCP URL/token, forwards stdio JSON-RPC
messages to the PersonaStack MCP Streamable HTTP endpoint with bearer auth,
preserves negotiated MCP session/protocol headers, and converts JSON/SSE
responses back into stdio JSON-RPC lines.
