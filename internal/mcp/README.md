# mcp

`mcp` owns local PersonaStack MCP proxy entry points and native runtime MCP
configuration.

`mcp install` writes native Hermes/OpenClaw config that launches
`personastack-connector mcp stdio --binding <connection_id>`. The stdio proxy
loads the paired binding's PersonaStack MCP URL/token, forwards stdio JSON-RPC
messages to the PersonaStack MCP Streamable HTTP endpoint with bearer auth,
preserves negotiated MCP session/protocol headers, and converts JSON/SSE
responses back into stdio JSON-RPC lines.
