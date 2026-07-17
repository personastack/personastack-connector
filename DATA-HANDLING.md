# Connector Data Handling Policy

## Scope

This policy describes the user-installed `personastack-connector` daemon and CLI for Hermes and OpenClaw external personas. It supplements the PersonaStack Privacy Policy and Terms. The user controls the local machine, native runtime, local files, OS account, and OS service scope.

## What Connector processes

Connector pairs one local external-persona binding at a time. It opens an authenticated outbound websocket session to PersonaStack `agent-gateway`. It sends bounded connection metadata, including hostname, operating system, architecture, Connector version, readiness, and run lifecycle state.

Connector receives API-rendered prompts and admitted run commands. It dispatches them to the selected local Hermes or OpenClaw runtime. It can read local runtime readiness and capability information. It returns run-output deltas, final output, and tool-event summaries through the authenticated gateway. Native runtime inputs, outputs, files, tools, logs, and retention remain subject to the native runtime and local machine configuration.

Connector configures PersonaStack MCP access in the selected native runtime. It can edit native MCP configuration and creates an owner-only backup before a recognized configuration update. It does not expose a public or LAN-facing control listener. CLI and stdio MCP use local process interfaces. Any fallback HTTP helper binds only to loopback.

## Local credentials and state

Connector stores bridge credentials, PersonaStack MCP credentials, local MCP proxy credentials, and supported OpenClaw credentials in OS credential storage. It also maintains an owner-only encrypted fallback secret store when needed. The fallback encryption key is stored locally and protected by the operating-system account. It is not the PersonaStack KEK/DEK system or an equivalent credential-vault boundary. Its local binding state records the connection, persona, selected runtime, and bounded run or readiness state.

Connector redacts prompts, bearer tokens, runtime secrets, local paths, and local endpoints from diagnostics where practical. Redaction reduces exposure but does not make local runtime or system logs a substitute for a secure secret-management process.

## Deletion and revocation

The `unpair` command removes the local binding and its Connector-managed credentials. A `token.revoked` command from the authenticated gateway also cancels a known active native run on a best-effort basis, removes the local binding, removes Connector-managed credentials, and stops reconnecting it. Neither action currently removes native MCP configuration written earlier by Connector or its owner-only configuration backup. Those files can contain a durable PersonaStack MCP credential until a verified cleanup path exists.

Uninstalling an OS background service removes only that service registration. It does not remove pairing state, local configuration, credentials, runtime configuration backups, native runtime data, or local logs. Account deletion is not a substitute for local unpairing until PersonaStack implements and verifies end-to-end local-runtime deletion propagation.

## Operator responsibilities

Run Connector only on a machine and native runtime you control or are authorized to operate. Review native MCP configuration changes and protect the local operating-system account, credential store, runtime configuration, files, logs, and backups. Disconnect or unpair Connector before transferring control of the machine or runtime.

## Changes

Material changes to this document require a versioned update to PersonaStack's public legal notices before production use.
