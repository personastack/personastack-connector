package externalagentprotocol

// PairingExchangeRequest is sent by Connector after the user runs the browser
// pairing command.
type PairingExchangeRequest struct {
	Code                      string        `json:"code"`
	RuntimeKind               RuntimeKind   `json:"runtime_kind"`
	SupportedRuntimeKinds     []RuntimeKind `json:"supported_runtime_kinds,omitempty"`
	ConnectorVersion          string        `json:"connector_version"`
	ProtocolVersion           string        `json:"protocol_version"`
	SupportedProtocolVersions []string      `json:"supported_protocol_versions,omitempty"`
	OS                        string        `json:"os,omitempty"`
	Arch                      string        `json:"arch,omitempty"`
	DevicePublicKey           string        `json:"device_public_key"`
	DeviceKeyProof            string        `json:"device_key_proof"`
	Hostname                  string        `json:"hostname,omitempty"`
	HostnameHash              string        `json:"hostname_hash"`
	GatewayWebsocketURL       string        `json:"gateway_websocket_url"`
	ConfigureMCP              bool          `json:"configure_mcp"`
}

type PairingExchangeErrorCode string

const (
	PairingExchangeErrorUnsupportedConnectorVersion PairingExchangeErrorCode = "unsupported_connector_version"
)

type PairingExchangeErrorResponse struct {
	ErrorCode               PairingExchangeErrorCode `json:"error_code"`
	Message                 string                   `json:"message"`
	MinimumConnectorVersion string                   `json:"minimum_connector_version,omitempty"`
	UpdateCommand           string                   `json:"update_command,omitempty"`
}

// PairingExchangeResponse returns the durable API-owned bridge binding.
type PairingExchangeResponse struct {
	PersonaID              string      `json:"persona_id"`
	ConnectionID           string      `json:"connection_id"`
	CredentialID           string      `json:"credential_id"`
	RuntimeKind            RuntimeKind `json:"runtime_kind"`
	ConnectionGeneration   int64       `json:"connection_generation"`
	GatewayWebsocketURL    string      `json:"gateway_websocket_url"`
	NativeMCPServerName    string      `json:"native_mcp_server_name"`
	NativeMCPToolNamespace string      `json:"native_mcp_tool_namespace"`
	PersonaMCPURL          string      `json:"persona_mcp_url,omitempty"`
	PersonaMCPToken        string      `json:"persona_mcp_token,omitempty"`
}
