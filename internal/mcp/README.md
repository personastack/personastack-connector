# mcp

`mcp` owns local PersonaStack MCP proxy entry points and native runtime MCP
configuration.

`mcp install` writes native Hermes/OpenClaw config that launches
`personastack-connector mcp stdio --binding <connection_id>`. The stdio proxy
loads the paired binding's PersonaStack MCP URL/token, forwards stdio JSON-RPC
messages to the PersonaStack MCP Streamable HTTP endpoint with bearer auth,
preserves negotiated MCP session/protocol headers, and converts JSON/SSE
responses back into stdio JSON-RPC lines. Long-lived SSE responses return after
the first complete JSON-RPC event so native stdio callers are not blocked on
stream EOF.

Live verification performs an MCP initialize, initialized notification, and
tools/list call with the binding credential before daemon heartbeats report MCP
as verified.
