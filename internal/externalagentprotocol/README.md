# externalagentprotocol

`externalagentprotocol` owns the Connector-local copy of the typed websocket
and pairing exchange DTOs used between PersonaStack Connector and
`agent-gateway`.

The Connector keeps this package in-repo so public source checkouts build
without sibling repository access. Cross-service protocol behavior and
compatibility remain owned by `agent-gateway` and the PersonaStack architecture
contract.
