# externalagentprotocol

`externalagentprotocol` owns the typed websocket contract between
`agent-gateway` and user-installed PersonaStack Connector processes.

The package is importable by the Connector repository. It defines endpoint
constants, finite enums, and concrete frame payload structs only. Gateway
runtime routing, Redis ownership, API callbacks, and Connector-local adapter
behavior live outside this package.
