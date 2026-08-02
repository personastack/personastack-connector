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
  name, host OS, host architecture, connection generation, native MCP server/tool naming metadata, prompt-safe
  external runtime capability summaries, and wake probe results.
- Receive API-composed run assignments from `agent-gateway`.
- Dispatch fully composed prompts into the selected local runtime.
- Stream or report local run events back through the Connector protocol.
- Configure PersonaStack MCP access in supported local runtimes.
- Keep local daemon startup persistent across user login or system restart.
- Keep the daemon process alive while no binding is paired so OS service
  supervisors do not throttle it into a dead setup state before pairing.
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
- Connector service scope is explicit and finite: `user_launch_agent` runs as
  the logged-in user, `system_launch_daemon` runs as a macOS root LaunchDaemon
  for pre-login Hermes/OpenClaw bridge availability, and
  `linux_system_service` runs as a Linux systemd system service for boot-time
  startup without a logged-in desktop session.
- System-scope state is stored under
  `/Library/Application Support/PersonaStack/Connector`. User-scope state
  remains in the user's config directory.
- Pairing, connect, and heartbeat payloads include the local machine hostname as
  bounded non-secret operator-facing metadata.
- Initial connect and heartbeat payloads include the compiled Connector version,
  host OS, and host architecture so API-owned external persona state can display
  the installed Connector and browser surfaces can choose the correct upgrade
  command when recommended releases advance.
- Each binding has one PersonaStack connection id, persona id, external agent
  kind, bridge credential, PersonaStack MCP credential, native MCP server name,
  and local readiness state. It does not persist an account candidate, profile
  candidate, Hermes home, or OpenClaw agent id.
- Local assignment state is persisted until the API has acknowledged a terminal
  run event.
- Redelivered `run.start` frames for the currently active run id must reuse the
  journaled native run id and resend accepted/started frames instead of starting
  a second local runtime run.
- Connector websocket sessions require `external-agent-v4`. The Connector
  reports redacted account/profile inventory after connect. `run.start` carries
  the API-selected opaque target and the Connector validates it again locally.
  Connector-emitted run lifecycle and terminal frames must include the active
  binding `connection_generation` so the API can reject stale Connector session
  callbacks.
- A `run.start` for a different run id while local state still has an active
  assignment must clear the stale local assignment and continue. The API owns run
  admission and a gateway-dispatched new run is proof that the previous local
  assignment is no longer authoritative.
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
- Connector run observation must honor the API-owned `run.start` deadline.
  Missing deadlines leave the local observation bound only by the websocket
  session context.
- A draining `server.draining` hint must close the current websocket session and
  reconnect through the same binding loop with a fresh connection generation.
  The Connector must not run overlapping websocket sessions for one binding.
- Draining reconnects must not hot-loop. The Connector waits at least its
  reconnect minimum before reconnecting, may honor a later gateway drain
  deadline, and caps that wait by the reconnect maximum.
- Unexpected established websocket read failures, including read-deadline
  expiration, are retryable session failures and must participate in reconnect
  backoff instead of being treated as clean success.
- Binding startup failures that happen before the websocket binding loop starts,
  including local MCP proxy startup and credential/session construction errors,
  must use per-binding backoff rather than retrying every foreground scan tick.
- Local state mutations are owned by the current websocket generation. Stale
  generations must not record heartbeat or wake-probe timestamps, activate or
  clear runs, cancel native runs, refresh MCP config, revoke bindings, or update
  runtime credentials after a newer generation exists.
- Connection status must not report wakeable from persisted probe timestamps
  alone; each websocket session must complete a live wake probe before it may
  report wakeable or accept a new `run.start`, unless the current runtime state
  is `ready` and already proves wakeability.
- A `config.refresh` bridge frame must carry the API-selected opaque target.
  The Connector resolves it again and rewrites MCP configuration only in that
  account and Hermes profile. It retains the target only for the websocket
  session. Missing or stale targets are errors, never a fallback to the
  Connector account or root home.
- A root system Connector must launch Hermes and OpenClaw as the selected
  account with its primary and supplementary groups, `HOME`, and runtime home.
  It must use a target-scoped loopback endpoint derived from the opaque target
  so a default-account listener cannot be treated as the selected profile.
  The endpoint assignment is session-only and is never persisted with a
  binding. A port conflict or failed selected-account launch leaves the target
  not ready. It never falls back to another account or profile.
- A `token.revoked` bridge frame deletes the local binding, clears OS credential
  storage for bridge/MCP/active-run secrets, best-effort cancels the active
  native run when one is journaled, and stops reconnecting that binding.

## CLI Surface

- `personastack-connector pair <code> --runtime auto`
- `personastack-connector pair <code> --runtime hermes`
- `personastack-connector pair <code> --runtime openclaw`
- `personastack-connector pair <code> --service-scope user`
- `personastack-connector pair <code> --service-scope system`
- `personastack-connector status`
- `personastack-connector status --repair`
- `personastack-connector runtime detect`
- `personastack-connector runtime repair`
- `personastack-connector mcp install`
- `personastack-connector mcp repair`
- `personastack-connector mcp stdio --binding <connection_id>`
- `personastack-connector run --foreground`
- `personastack-connector run --foreground --service-scope user`
- `personastack-connector run --foreground --service-scope system`
- `personastack-connector service install --service-scope user`
- `personastack-connector service install --service-scope system`
- `personastack-connector service uninstall --service-scope user`
- `personastack-connector service uninstall --service-scope system`
- `personastack-connector unpair`

Default setup must require only package installation plus the pairing command.
Pairing reports inventory and waits for the API-selected account/profile before
changing native MCP configuration. `status --repair`, `runtime repair`,
`runtime * configure`, `mcp install`, and `mcp repair` must reject a paired
binding rather than write a default or legacy profile. OpenClaw pairing must locate an approved operator credential locally
before prompting the user. Discovery must prefer Connector credential storage,
local-only flags such as `--openclaw-token`, `--openclaw-password`, or
`--openclaw-device-token`, the matching `OPENCLAW_GATEWAY_*` environment
variables, and OpenClaw-owned user config, env, device auth, or service env
files. If no local credential is found, the CLI prompt must tell the user the
easiest OpenClaw command to retrieve or rotate one. Browser setup surfaces must
not collect OpenClaw tokens, passwords, or device credentials.
System service scope keeps the Connector bridge online before login. It must
report runtime readiness honestly and must not claim `wakeable` until the native
Hermes/OpenClaw runtime, native MCP configuration, and wake probe are live.
Discovery authority is the daemon effective UID. A non-root daemon reports only
its own accessible profiles. A root system daemon reports root plus eligible
non-system local accounts. Discovery warnings are local diagnostics and never
make pairing fail. Browser-visible inventory contains only opaque candidate IDs,
safe labels, runtime kind, and readiness. It contains no paths, UIDs, or local
credentials.

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
- optional prompt-safe native capability discovery; transient discovery errors must leave native capabilities unreported rather than reporting an empty list
- Native capability reports must include discovery status and the exact native
  sources represented by the report. Partial discovery must report valid
  same-source capabilities with `partial` status so receivers can replace only
  the reported sources and preserve failed or unknown sources.
- Unchanged capability reports must be retransmitted periodically while the
  websocket session remains connected so best-effort downstream persistence can
  recover from transient API or Redis write failures without waiting for local
  capability content to change.

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

- Default to direct native Hermes/OpenClaw PersonaStack MCP configuration using
  the binding's durable persona-scoped MCP bearer token.
- Hermes tools should register as `mcp_<native_mcp_server_name>_<tool_name>`.
- Heartbeat readiness treats MCP as verified only after the native config is
  present and a live PersonaStack MCP initialize/tools-list check succeeds.
- Hermes MCP verification additionally requires the active Hermes API server
  tool registry to report the configured native MCP server from
  `hermes tools list --platform api_server`; direct PersonaStack MCP endpoint
  reachability alone is not enough to report `mcp_verified`.
- Hermes repair must remove `no_mcp` from `platform_toolsets.api_server`;
  presence of that sentinel is a failed native MCP configuration because Hermes
  excludes MCP tools from API-server runs even when `mcp_servers` is populated.
- Connector journals the active PersonaStack run id, assignment id, and native
  runtime run id in binding state while the run is active and clears them on
  terminal cleanup.
- If native runtime start succeeds but native run id journaling fails, Connector
  must best-effort cancel that native run and include the native run id in the
  failure frame instead of leaving a still-executing uncorrelated native run.
- Native runtime config intentionally contains a direct PersonaStack bearer
  header for the external persona's durable MCP credential so users can call
  PersonaStack MCP from Hermes/OpenClaw outside PersonaStack-dispatched runs.
  That durable credential must not authorize website persona-chat media byte
  transfer. Connector-backed external personas may use normal text chat
  completion behavior, but media upload/download transfer tools must remain
  unavailable until a future Connector/Hermes/OpenClaw contract supplies
  API-verifiable active website-chat turn authority, bounded byte transfer,
  40 MiB local enforcement, retention, and ownership checks.
- Target-scoped MCP writes must preserve unrelated native runtime config,
  write an owner-only first backup, use atomic replacement, and refuse to
  overwrite an unrecognized same-name MCP server by reporting a conflict state.
- Direct config diagnostics must redact bearer tokens from logs, status, and
  repair output.
- Heartbeat `diagnostic_code` values must be stable snake_case values derived
  from the concrete MCP verification or adapter failure.
- The Connector-owned stdio proxy remains available only for explicit local
  fallback/debug use, not the default installed MCP path.
- External run-start handling must not require or store run-scoped PersonaStack
  MCP bearer tokens.
- Streamable HTTP SSE responses may be long-lived; the stdio proxy must emit the
  first complete JSON-RPC SSE event back to stdio without waiting for stream EOF.
- After MCP initialization completes, the stdio proxy must open the Streamable
  HTTP GET session stream, forward JSON-RPC SSE `data:` payloads to stdio as
  JSON lines, ignore non-JSON readiness/keepalive events, and reconnect with
  `Last-Event-ID` when the stream closes.
- Loopback HTTP MCP proxying is a fallback only and must use loopback binding,
  random port selection, a high-entropy local token, and owner-only local config
  permissions.
- MCP repair diagnostics are typed and must distinguish missing config, parse
  errors, same-name config conflicts, missing local token, rejected MCP token,
  unreachable MCP endpoint, and restart-required states.

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
- Connector-dispatched Hermes `/v1/runs` requests must set
  `include_native_tools=true` so Hermes builds its normal native tool registry
  and adds the configured PersonaStack MCP server instead of replacing native
  tools with MCP-only tooling.
- Include PersonaStack run id, assignment id, native MCP server name, and native
  MCP namespace as bounded non-secret Hermes run metadata.
- Use Hermes `instructions` only when API provides explicit structured prompt
  fields.
- Configure Hermes MCP through the top-level `mcp_servers` map with the
  per-binding native MCP server name; config edits must be atomic and keep an
  owner-only backup of the prior config.
- Hermes named profiles are selected only by the API-provided opaque candidate.
  The Connector resolves that candidate to its discovered profile home for one
  operation. A root service starts `hermes gateway` with the selected account's
  UID, GID, supplementary groups, `HOME`, and `HERMES_HOME`. An unprivileged
  service may only target its effective account. It must not persist a chosen
  profile or emulate selection by rewriting the Connector's own home.
- Map Hermes native run events to Connector protocol run events.
- Treat cancellation as best-effort until Hermes returns terminal state or the
  Connector cancellation timeout expires.
- Hermes runtime feature discovery uses `/v1/capabilities` and reports verified
  runtime features such as delegated task acceptance, status, streaming, and
  cancellation. Prompt-safe native tool summaries use
  `hermes tools list --platform api_server`; Connector must resolve the Hermes binary
  from `HERMES_BIN`, PATH, documented user/FHS/Nix install locations, and
  platform-specific launcher paths while silently skipping missing candidates.
  Connector must not parse local Hermes config to infer native tools.

## OpenClaw Runtime

- Probe OpenClaw Gateway on `ws://127.0.0.1:18789`.
- Authenticate as an operator client.
- Use Gateway `agent`, `agent.wait`, and `sessions.abort` for full support.
- Put API-rendered `fully_composed_prompt` in `agent.params.message`.
- Use PersonaStack assignment id as the OpenClaw idempotency key/native run id.
- OpenClaw `agent.params` must stay within the Gateway schema: `message`,
  `idempotencyKey`, and optional `agentId`. Connector must not send
  PersonaStack run metadata, native MCP fields, or alternate run id fields in
  `agent.params`; current OpenClaw Gateway rejects unknown root params.
- Verify MCP by effective tool visibility or controlled wake probe, not config
  write success alone.
- OpenClaw CLI fallback is degraded only and must not claim Gateway streaming or
  cancel parity unless the same native run id can be waited and cancelled.
- OpenClaw capability discovery uses Gateway `skills.status` when available and
  reports one prompt-safe capability summary per ready skill. When
  `skills.status` is unavailable but Gateway `tools.catalog` is available,
  Connector may complete discovery by reporting prompt-safe tool-group summaries
  from the exact `openclaw_tools_catalog` source instead of mislabeling them as
  ready skills.

## Security

- Pairing codes are short-lived, single-use, and redeemed through
  `agent-gateway`.
- Pairing exchange must use Connector proof-of-possession before bridge and MCP
  credentials are issued.
- Pairing exchange must send Connector protocol version support with
  `external-agent-v3`. If the pairing exchange returns
  `unsupported_connector_version`, Connector must surface the finite failure
  state and exact update command to the user.
- Bridge credentials cannot call PersonaStack MCP tools.
- Persona MCP credentials cannot open Connector websocket sessions.
- Diagnostics must redact prompts, bearer tokens, runtime secrets, account ids
  where practical, local paths, and local endpoints.

## Packaging

- V1 release targets are macOS `darwin/amd64`, macOS `darwin/arm64`, Linux
  `linux/amd64`, and Linux `linux/arm64`.
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
- macOS release artifacts stay split by `darwin/amd64` and `darwin/arm64`.
  User-scope setup registers `~/Library/LaunchAgents/ai.personastack.connector.plist`.
  System-scope setup registers `/Library/LaunchDaemons/ai.personastack.connector.plist`
  with `KeepAlive`, `RunAtLoad`, `ThrottleInterval=30`, `ProcessType=Background`,
  and logs under `/Library/Logs/PersonaStack/`.
- macOS system-scope setup requires `sudo` and does not prevent machine sleep.
  The persona may report offline while the Mac is asleep; Connector must
  reconnect and re-probe after wake.
- Service uninstall removes only Connector-owned OS background service
  registration. It must not delete pairing state, local config, credentials, or
  logs.
- macOS service uninstall unloads, disables, and removes the selected
  `ai.personastack.connector` LaunchAgent or LaunchDaemon plist. System-scope
  uninstall requires `sudo`.
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
- Linux user-scope service installation prefers `systemd --user` and falls back
  to an XDG autostart desktop entry when user systemd is unavailable.
- Linux system-scope service installation writes a systemd system unit with
  `Restart=always`, `RestartSec=30`, `After=network-online.target`, explicit
  target user, explicit `HOME`, and `WantedBy=multi-user.target`.
- Linux service uninstall disables the `systemd --user` unit when present,
  removes the systemd user unit and default-target wants symlink, and removes
  the XDG autostart fallback entry.
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
