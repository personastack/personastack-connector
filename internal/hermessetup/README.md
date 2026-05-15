# hermessetup

`hermessetup` owns local Hermes API server setup state for the Connector.

It merges `~/.hermes/.env` without dropping unrelated keys, keeps the Hermes API
key in OS credential storage for Connector auth, and provides diagnostics for
Hermes API startup and local config repair.
