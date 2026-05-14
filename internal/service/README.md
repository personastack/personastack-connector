# service

`service` owns user-level service installation for the Connector daemon.

It writes one restart-persistent user service per OS:

- macOS LaunchAgent
- Linux `systemd --user` unit
- Windows Scheduled Task
