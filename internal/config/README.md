# config

`config` owns local Connector binding and state shapes.

The scaffold uses an in-memory store so command behavior can be tested without
choosing credential or disk persistence. Secret material lives in OS
credential storage by default and falls back to an owner-only encrypted secret
store when the platform keyring is unavailable.
