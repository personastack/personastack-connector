# personastack-connector

`cmd/personastack-connector` owns the local Connector CLI entry point.

The CLI supports pairing, runtime detection, foreground bridge execution, and
native MCP config installation. The stdio MCP proxy still requires API-issued
PersonaStack MCP credentials before real tool traffic can flow.
