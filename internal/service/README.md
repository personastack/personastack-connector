# service

`service` owns OS service installation and uninstall for the Connector daemon.

It writes and removes restart-persistent service registrations:

- macOS LaunchAgent
- macOS LaunchDaemon for supported system scope
- Linux `systemd --user` unit
- Linux XDG autostart fallback
