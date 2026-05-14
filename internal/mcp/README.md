# mcp

`mcp` owns local PersonaStack MCP proxy entry points and native runtime MCP
configuration.

`mcp install` writes native Hermes/OpenClaw config that launches
`personastack-connector mcp stdio --binding <connection_id>`. The stdio proxy
still needs API-issued PersonaStack MCP credentials before it can forward real
MCP traffic.
