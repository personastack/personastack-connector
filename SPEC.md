# PersonaStack Connector Specification

## Purpose

`personastack-connector` is a user-installed local daemon and CLI that lets a
local Hermes Agent or OpenClaw runtime participate in a PersonaStack team as an
external persona.

## Responsibilities

- Pair a local machine to one API-owned external persona binding at a time.
- Open authenticated outbound websocket sessions to `agent-gateway`.
- Negotiate a bounded Connector websocket protocol version with `agent-gateway`
  and reject connect responses that advertise an unsupported version.
- Report runtime health, MCP configuration state, Connector version, local host
  name, connection generation, native MCP server/tool naming metadata, and wake
  probe results.
- Receive API-composed run assignments from `agent-gateway`.
- Dispatch fully composed prompts into the selected local runtime.
- Stream or report local run events back through the Connector protocol.
- Configure PersonaStack MCP access in supported local runtimes.
- Keep local daemon startup persistent across user login or system restart.
- Keep the default connector setup CLI-first and headless.
- Store local secrets in OS credential storage.
- Keep the repository public source for user audit, not open source.

## Non-Goals

- Do not own PersonaStack persona, stack, catalog, run, billing, or readiness
  product state.
- Do not render PromptStack prompts or reconstruct stack context locally.
- Do not expose native Hermes/OpenClaw tools as PersonaStack integrations.
- Do not require inbound public network access to the user's machine.
- Do not grant website chat reply authority to persistent Persona MCP tokens.
- Do not target Termux, iOS, or Android as Connector hosts in V1.

## External Authorities

- `personastack-api` owns durable external persona bindings, pairing sessions,
  run admission, PromptStack rendering, terminal run state, install metadata, and
  browser-visible state.
- `agent-gateway` owns pairing exchange ingress, websocket transport, routing,
  dispatch/cancel frames, protocol versioning, and Gateway-to-API callbacks.
- `mcp` owns PersonaStack MCP authentication, authorization, and tool execution.
- This repository owns its Connector protocol DTO package for public build
  reproducibility. `agent-gateway` owns protocol behavior and compatibility.

## Local Object Model

- A Connector installation manages one external persona binding at a time.
  Pairing a new external persona replaces the previous local binding and local
  binding secrets for that machine.
- Pairing, connect, and heartbeat payloads include the local machine hostname as
  bounded non-secret operator-facing metadata.
- Each binding has one PersonaStack connection id, persona id, external agent
  kind, bridge credential, PersonaStack MCP credential, native MCP server name,
  local runtime selection, and local readiness state.
- Local assignment state is persisted until the API has acknowledged a terminal
  run event.
- Redelivered `run.start` frames for the currently active run id must reuse the
  journaled native run id and resend accepted/started frames instead of starting
  a second local runtime run.
- A `run.start` for a different run id while one run is active must fail locally
  without overwriting the active assignment or starting another native run.
- Duplicate command `message_id` values within one websocket session must be
  idempotent: waitable commands replay the cached reply and side-effect-only
  commands are not re-applied.
- Native runtime run ids are local correlation values and are not PersonaStack
  run authority.
- A new websocket session must advance the stored connection generation so
  reconnects use a fresh owner generation without inventing a second binding.
- If the Connector reconnects while a run is still locally active, it must
  replay the accepted and started state for that run before waiting for the
  next dispatch.
- A draining `server.draining` hint should start an overlapped replacement
  websocket attempt before the old websocket closes when possible, and the old
  websocket must stay usable until the drain deadline expires or the
  replacement is established.
- Connection status must not report wakeable from persisted probe timestamps
  alone; each websocket session must complete a live wake probe before it may
  report wakeable or accept a new `run.start`, unless the current runtime state
  is `ready` and already proves wakeability.
- A `config.refresh` bridge frame should re-run the local MCP installer for the
  active binding so native MCP config is rewritten and the local proxy is
  restarted before revocation cleanup runs.
- A `token.revoked` bridge frame deletes the local binding, clears OS credential
  storage for bridge/MCP/active-run secrets, best-effort cancels the active
  native run when one is journaled, and stops reconnecting that binding.

## CLI Surface

- `personastack-connector pair <code> --runtime auto`
- `personastack-connector pair <code> --runtime hermes`
- `personastack-connector pair <code> --runtime openclaw`
- `personastack-connector status`
- `personastack-connector status --repair`
- `personastack-connector runtime detect`
- `personastack-connector runtime repair`
- `personastack-connector mcp install`
- `personastack-connector mcp repair`
- `personastack-connector mcp stdio --binding <connection_id>`
- `personastack-connector run --foreground`
- `personastack-connector unpair`

Default setup must require only package installation plus the pairing command.
Pairing configures PersonaStack MCP access by default; any future MCP opt-out or
repair flag is advanced-only and must not be required by the primary setup
command. OpenClaw pairing must collect or locate an approved operator credential
locally through Connector CLI prompts, OS credential storage, local-only flags
such as `--openclaw-token`, `--openclaw-password`, or `--openclaw-device-token`,
or the matching `OPENCLAW_GATEWAY_*` environment variables before it reports
local runtime setup as usable. Browser setup surfaces must not collect OpenClaw
tokens, passwords, or device credentials.

The V1 Connector does not expose a local HTTP UI/control listener. CLI control
is local process execution and native MCP uses stdio; any future local control
server must bind loopback only. If a tray/menu surface is added, it is only an
optional convenience mirror for status, repair, logs, and pairing, and it must
not be required for headless Linux.

## Runtime Adapters

Adapters implement the same finite runtime operations:

- `detect`
- `start_run`
- `stream_or_poll_run`
- `cancel_run`
- `diagnose`

Native MCP configuration and verification are Connector-level operations owned
by `internal/mcp`, not runtime-adapter methods. The daemon combines adapter
runtime detection with `internal/mcp` live verification before reporting
`wakeable`.

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
- Hermes tools should register as `mcp_<native_mcp_server_name>_<tool_name>`.
- Native runtime config must point at the Connector's stable user shim path, not
  a transient package or test executable path.
- The stdio proxy loads PersonaStack MCP credentials from OS credential storage.
- Heartbeat readiness treats MCP as verified only after the native config is
  present and a live PersonaStack MCP initialize/tools-list check succeeds.
- During an active run, Connector stores the API-issued run-scoped MCP token as
  the binding's active credential and the stdio proxy prefers it over the stable
  pairing credential; terminal cleanup clears the active token.
- Connector journals the active PersonaStack run id, assignment id, and native
  runtime run id in binding state while the run is active and clears them on
  terminal cleanup.
- Native runtime config must not contain PersonaStack bearer tokens by default.
- Native runtime config must not become a plaintext header export surface; any
  required auth material stays Connector-local and is handled by the stdio
  proxy, not by exposing native runtime config or tools as PersonaStack surface.
- Direct remote PersonaStack MCP headers are advanced-only fallback and must
  show a credential-storage warning in UX.
- Streamable HTTP SSE responses may be long-lived; the stdio proxy must emit the
  first complete JSON-RPC SSE event back to stdio without waiting for stream EOF.
- After MCP initialization completes, the stdio proxy must open the Streamable
  HTTP GET session stream, forward JSON-RPC SSE `data:` payloads to stdio as
  JSON lines, ignore non-JSON readiness/keepalive events, and reconnect with
  `Last-Event-ID` when the stream closes.
- Loopback HTTP MCP proxying is a fallback only and must use loopback binding,
  random port selection, a high-entropy local token, and owner-only local config
  permissions.

## Hermes Runtime

- Probe Hermes on `http://127.0.0.1:8642`.
- Use Hermes `/v1/runs` when available.
- Hermes run submission may return `run_id` or `id`; Connector treats the
  returned value as the Hermes native run id and never sends the PersonaStack
  run id as a caller-supplied native id.
- Subscribe to Hermes run SSE events for terminal run observation and fall back
  to status polling only when events are unavailable.
- Missing Hermes run SSE or stop features are degraded fallbacks, not hard
  runtime failures, when run submission and run status are available.
- Hermes response and chat-completions fallbacks must report degraded streaming
  and cancel support and must not claim full wakeability.
- Put API-rendered `fully_composed_prompt` in Hermes `input` by default.
- Include PersonaStack run id, assignment id, native MCP server name, and native
  MCP namespace as bounded non-secret Hermes run metadata.
- Use Hermes `instructions` only when API provides explicit structured prompt
  fields.
- Configure Hermes MCP through the top-level `mcp_servers` map with the
  per-binding native MCP server name; config edits must be atomic and keep an
  owner-only backup of the prior config.
- Map Hermes native run events to Connector protocol run events.
- Treat cancellation as best-effort until Hermes returns terminal state or the
  Connector cancellation timeout expires.

## OpenClaw Runtime

- Probe OpenClaw Gateway on `ws://127.0.0.1:18789`.
- Authenticate as an operator client.
- Use Gateway `agent`, `agent.wait`, and `sessions.abort` for full support.
- Put API-rendered `fully_composed_prompt` in `agent.params.message`.
- Use PersonaStack assignment id as the OpenClaw idempotency key/native run id.
- Include PersonaStack run id, assignment id, native MCP server name, and native
  MCP namespace in OpenClaw `agent` params as bounded non-secret metadata.
- Verify MCP by effective tool visibility or controlled wake probe, not config
  write success alone.
- OpenClaw CLI fallback is degraded only and must not claim Gateway streaming or
  cancel parity unless the same native run id can be waited and cancelled.

## Security

- Pairing codes are short-lived, single-use, and redeemed through
  `agent-gateway`.
- Pairing exchange must use Connector proof-of-possession before bridge and MCP
  credentials are issued.
- If the pairing exchange returns `unsupported_connector_version`, Connector
  must surface the finite failure state and exact update command to the user.
- Bridge credentials cannot call PersonaStack MCP tools.
- Persona MCP credentials cannot open Connector websocket sessions.
- Diagnostics must redact prompts, bearer tokens, runtime secrets, account ids
  where practical, local paths, and local endpoints.

## Packaging

- V1 release targets are macOS and Linux.
- Termux is unsupported as a Connector host even if Hermes can run there.
- GitHub Actions must run unit tests and a GoReleaser snapshot dry-run on pull
  requests and `main` pushes. Snapshot artifacts are validation outputs only
  and must not be uploaded or documented as a distribution channel.
- CI builds must be self-contained from the connector repository checkout;
  clean public clones must pass `go list -mod=mod ./...` without sibling
  repository checkouts.
- Tagged releases build GoReleaser archives for macOS/Linux plus Linux
  `.deb` and `.rpm` packages, publish checksum/SBOM artifacts to a draft GitHub
  Release, upload a machine-readable release manifest plus manifest checksum,
  and emit GitHub provenance attestations for `dist/*`.
- Every upload batch is keyed by one shipping-agent-selected semver tag
  `v<semver>` and matching release notes at `docs/release-notes/v<semver>.md`.
  Canonical asset URLs are derived as
  `https://github.com/personastack/personastack-connector/releases/download/v<semver>/personastack-connector_<semver>_<os>_<arch>.<ext>`,
  with shared checksum, checksum signature, release-manifest, manifest checksum,
  and manifest signature assets under the same tag path.
- macOS archives are signed and notarized only when the release environment
  provides the Apple Developer ID and App Store Connect inputs; default install
  UX must remain gated until those inputs are present.
- macOS release artifacts stay split by `darwin/amd64` and `darwin/arm64`, and
  setup continues to register a LaunchAgent after installation instead of
  shipping an app bundle or DMG path.
- Tagged releases also publish cosign-bundled signatures for the checksum file
  and the release manifest, so advanced guidance can verify release artifacts
  without making verification part of the primary install command.
- Post-release activation must publish generated release-manifest metadata into
  the PersonaStack API admin connector-release endpoint by sending only the
  semver, commit, and minimum protocol after the release workflow has completed
  and public assets have been verified. The API owns derivation of every
  recommended OS/arch/package asset URL and install command.
- The binary must expose `personastack-connector version` so install flows and
  support diagnostics can verify the downloaded artifact.
- V1 signed distribution channels are GitHub Release archives for macOS and
  Linux plus Linux `.deb` and `.rpm` packages. Tagged releases generate and
  push a Homebrew formula to the public `personastack/homebrew-tap` repository
  from the signed checksum file.
- Connector release metadata must only mark installer defaults eligible when
  signed release metadata is active; Linux package defaults stay off until the
  release signing gate is enabled.
- V1 binary distribution stays GitHub Releases only; mirrored binary hosts are
  a later decision, not part of the public install path.
- Public source visibility is for audit only. Repository docs and package
  metadata must not describe the Connector as open source unless the license
  changes to an OSI-style open-source grant.
- Signed auto-update launch scope stays deferred until a separate
  `personastack-ship` decision; package-manager/manual update prompts remain
  the default guidance.
- Stable public release activation is a separate `personastack-ship` gate and
  is not part of routine implementation completion.
- WSL2 uses the Linux Connector inside the WSL2 environment.
- Linux service installation prefers `systemd --user` and falls back to an XDG
  autostart desktop entry when user systemd is unavailable.
- OpenClaw mobile nodes are not Connector hosts; they require Gateway on
  macOS, Linux, or the Linux Connector inside WSL2.
- iOS and Android are not Connector host targets in V1.
- Native Windows release artifacts are not supported in V1.
- Default setup must render a simple one-line Homebrew, `.deb`, or `.rpm`
  command. Advanced verification may use signed release metadata, but it must
  not be part of the primary install command.
- The release workflow must declare the protected `release` environment, and
  release policy checks must use GitHub CLI metadata to fail closed when active
  `v*` tag rulesets or required release reviewers are missing.
- Public release activation remains a separate `personastack-ship` gate.
- GitHub repository visibility is public at
  `https://github.com/personastack/personastack-connector`; install and support
  docs may link to public release artifacts.

## Testing

- Unit tests cover core binding state, protocol handling, local MCP proxying,
  adapter fakes, OS service planners, config edits, rollback behavior, and
  diagnostics redaction.
- Restart simulation tests prove paired bindings reload, reconnect, re-probe
  runtime health, and re-verify MCP after process restart.
- Fake Hermes and OpenClaw runtime tests must cover success, degraded fallback,
  cancel, reconnect, and MCP verification paths.
- Connector CLI pairing tests must cover clear success and degraded setup states.
