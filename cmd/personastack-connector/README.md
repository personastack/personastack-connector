# personastack-connector

`cmd/personastack-connector` owns the local Connector CLI entry point.

The CLI supports pairing, status, diagnostics, runtime detection, foreground
bridge execution, and native MCP config installation. The stdio MCP proxy
still requires API-issued PersonaStack MCP credentials before real tool
traffic can flow.

The connector stays CLI-first and headless. Hermes Termux and OpenClaw mobile
nodes are not Connector hosts.

`status` reports the paired persona, runtime kind, runtime state, websocket
state, MCP state, and last wake probe in one compact line. `diagnostics`
prints the same operator state with repair actions and redacted local paths.
If a tray/menu companion exists, it should only mirror the same status,
repair, logs, and pairing actions; Linux headless usage stays CLI-first.
