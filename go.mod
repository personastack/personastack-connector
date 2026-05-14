module github.com/personastack/personastack-connector

go 1.26.1

require (
	github.com/google/uuid v1.6.0
	github.com/personastack/agent-gateway v0.0.0
)

require (
	github.com/gorilla/websocket v1.5.0
	github.com/zalando/go-keyring v0.2.8
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/danieljoos/wincred v1.2.3 // indirect
	github.com/godbus/dbus/v5 v5.2.2 // indirect
	golang.org/x/sys v0.39.0 // indirect
)

replace github.com/personastack/agent-gateway => ../agent-gateway
