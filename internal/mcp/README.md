# mcp

`mcp` owns local PersonaStack MCP proxy entry points and native runtime MCP
configuration.

`mcp install` writes native Hermes/OpenClaw config that launches
`personastack-connector mcp stdio --binding <connection_id>`. The stdio proxy
loads the paired binding's PersonaStack MCP URL/token and forwards newline
delimited JSON-RPC requests to the PersonaStack MCP endpoint with bearer auth.
