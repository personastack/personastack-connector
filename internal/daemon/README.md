# daemon

`daemon` owns the long-running Connector bridge loop. It loads local bindings,
opens outbound Agent Gateway websocket sessions, and translates gateway frames
into local runtime adapter calls.
