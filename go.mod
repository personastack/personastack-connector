module github.com/personastack/personastack-connector

go 1.26.1

require (
	github.com/google/uuid v1.6.0
	github.com/personastack/agent-gateway v0.0.0
)

require github.com/gorilla/websocket v1.5.0

require gopkg.in/yaml.v3 v3.0.1 // indirect

replace github.com/personastack/agent-gateway => ../agent-gateway
