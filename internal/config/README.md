# config

`config` owns local Connector binding and state shapes.

The file store persists non-secret binding state in the user's config
directory. Secret material lives in OS credential storage by default and falls
back to an owner-only encrypted secret store when the platform keyring is
unavailable.
