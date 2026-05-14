# PersonaStack Connector Specification

## Purpose

`personastack-connector` is a user-installed local daemon and CLI that lets a
local Hermes Agent or OpenClaw runtime participate in a PersonaStack team as an
external persona.

## Responsibilities

- Pair a local machine to one or more API-owned external persona bindings.
- Open authenticated outbound websocket sessions to `agent-gateway`.
- Report runtime health, MCP configuration state, Connector version, and wake
  probe results.
- Receive API-composed run assignments from `agent-gateway`.
- Dispatch fully composed prompts into the selected local runtime.
- Stream or report local run events back through the Connector protocol.
- Configure PersonaStack MCP access in supported local runtimes.
- Keep local daemon startup persistent across user login or system restart.
- Store local secrets in OS credential storage.

## Non-Goals

- Do not own PersonaStack persona, stack, catalog, run, billing, or readiness
  product state.
- Do not render PromptStack prompts or reconstruct stack context locally.
- Do not expose native Hermes/OpenClaw tools as PersonaStack integrations.
- Do not require inbound public network access to the user's machine.
- Do not grant website chat reply authority to persistent Persona MCP tokens.

## External Authorities

- `personastack-api` owns durable external persona bindings, pairing sessions,
  run admission, PromptStack rendering, terminal run state, install metadata, and
  browser-visible state.
- `agent-gateway` owns pairing exchange ingress, websocket transport, routing,
  dispatch/cancel frames, protocol versioning, and Gateway-to-API callbacks.
- `mcp` owns PersonaStack MCP authentication, authorization, and tool execution.

## Local Object Model

- A Connector installation may manage multiple external persona bindings.
- Each binding has one PersonaStack connection id, persona id, external agent
  kind, bridge credential, PersonaStack MCP credential, native MCP server name,
  local runtime selection, and local readiness state.
- Local assignment state is persisted until the API has acknowledged a terminal
  run event.
- Native runtime run ids are local correlation values and are not PersonaStack
  run authority.

## CLI Surface

- `personastack-connector pair <code> --runtime auto --configure-mcp`
- `personastack-connector pair <code> --runtime hermes --configure-mcp`
- `personastack-connector pair <code> --runtime openclaw --configure-mcp`
- `personastack-connector status`
- `personastack-connector runtime detect`
- `personastack-connector mcp install`
- `personastack-connector mcp stdio --binding <connection_id>`
- `personastack-connector run --foreground`
- `personastack-connector unpair`

Default setup must require only package installation plus the pairing command.

## Runtime Adapters

Adapters implement the same finite operations:

- `detect`
- `configure_mcp`
- `verify_mcp`
- `start_run`
- `stream_or_poll_run`
- `cancel_run`
- `diagnose`

Adapter result states must be concrete typed enums, including:

- `runtime_missing`
- `runtime_stopped`
- `auth_missing`
- `capability_missing`
- `mcp_config_missing`
- `mcp_restart_required`
- `mcp_verified`
- `wake_probe_failed`
- `ready`

## MCP Strategy

- Default to a Connector-owned per-binding stdio MCP proxy.
- Native runtime config invokes
  `personastack-connector mcp stdio --binding <connection_id>`.
- The stdio proxy loads PersonaStack MCP credentials from OS credential storage.
- Heartbeat readiness treats MCP as verified only after the native config is
  present and a live PersonaStack MCP initialize/tools-list check succeeds.
- During an active run, Connector stores the API-issued run-scoped MCP token as
  the binding's active credential and the stdio proxy prefers it over the stable
  pairing credential; terminal cleanup clears the active token.
- Connector journals the active PersonaStack run id and native runtime run id in
  binding state while the run is active and clears both on terminal cleanup.
- Native runtime config must not contain PersonaStack bearer tokens by default.
- Streamable HTTP SSE responses may be long-lived; the stdio proxy must emit the
  first complete JSON-RPC SSE event back to stdio without waiting for stream EOF.
- Loopback HTTP MCP proxying is a fallback only and must use loopback binding,
  random port selection, a high-entropy local token, and owner-only local config
  permissions.

## Hermes Runtime

- Probe Hermes on `http://127.0.0.1:8642`.
- Use Hermes `/v1/runs` when available.
- Subscribe to Hermes run SSE events for terminal run observation and fall back
  to status polling only when events are unavailable.
- Put API-rendered `fully_composed_prompt` in Hermes `input` by default.
- Use Hermes `instructions` only when API provides explicit structured prompt
  fields.
- Map Hermes native run events to Connector protocol run events.
- Treat cancellation as best-effort until Hermes returns terminal state or the
  Connector cancellation timeout expires.

## OpenClaw Runtime

- Probe OpenClaw Gateway on `ws://127.0.0.1:18789`.
- Authenticate as an operator client.
- Use Gateway `agent`, `agent.wait`, and `sessions.abort` for full support.
- Put API-rendered `fully_composed_prompt` in `agent.params.message`.
- Use PersonaStack assignment id as the OpenClaw idempotency key/native run id.
- Verify MCP by effective tool visibility or controlled wake probe, not config
  write success alone.

## Security

- Pairing codes are short-lived, single-use, and redeemed through
  `agent-gateway`.
- Pairing exchange must use Connector proof-of-possession before bridge and MCP
  credentials are issued.
- Bridge credentials cannot call PersonaStack MCP tools.
- Persona MCP credentials cannot open Connector websocket sessions.
- Diagnostics must redact prompts, bearer tokens, runtime secrets, account ids
  where practical, local paths, and local endpoints.

## Packaging

- V1 release targets are macOS, Linux, and native Windows.
- WSL2 uses the Linux Connector inside the WSL2 environment.
- iOS and Android are not Connector host targets in V1.
- Release artifacts must be signed or checksummed before appearing in default
  setup UX.

## Testing

- Unit tests cover core binding state, protocol handling, local MCP proxying,
  adapter fakes, OS service planners, config edits, rollback behavior, and
  diagnostics redaction.
- Restart simulation tests prove paired bindings reload, reconnect, re-probe
  runtime health, and re-verify MCP after process restart.
- Fake Hermes and OpenClaw runtime tests must cover success, degraded fallback,
  cancel, reconnect, and MCP verification paths.
