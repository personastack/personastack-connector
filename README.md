# PersonaStack Connector historical source

This repository retains the previous standalone local-agent implementation for historical inspection. Managed HermesAgent and OpenClaw connections use PersonaStack for macOS and its embedded Background Agents helper. See the [Local agents guide](https://personastack.ai/docs/personas/local-agents/).

Standalone release and Homebrew publication are disabled in this source. The release workflow has no tag trigger or publication job. The macOS app owns the replacement implementation. The historical source is not a build input for the API, app, or Ship.

This source change does not archive the GitHub repository, alter existing releases, migrate a binding, uninstall software, or delete user data. Those operations require a separate authorized cutover.

This repository is public source for user audit. It is not open source: the license permits inspection, but does not grant reuse, modification, redistribution, hosting, or commercial rights without written permission.

See [DATA-HANDLING.md](DATA-HANDLING.md) for the historical implementation's local data, credential, network, and deletion boundaries. Existing policy documents and historical versions are unchanged.

Historical validation remains available through the repository's CI. It produces no supported distribution. Existing tags, releases and source remain historical evidence.
