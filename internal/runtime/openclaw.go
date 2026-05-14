package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

const defaultOpenClawGatewayURL = "ws://127.0.0.1:18789"

type OpenClawAdapter struct {
	GatewayURL string
	Token      string
	Dialer     *websocket.Dialer
}

func NewOpenClawAdapter(gatewayURL string, token string) OpenClawAdapter {
	if strings.TrimSpace(gatewayURL) == "" {
		gatewayURL = defaultOpenClawGatewayURL
	}
	return OpenClawAdapter{GatewayURL: strings.TrimSpace(gatewayURL), Token: strings.TrimSpace(token), Dialer: websocket.DefaultDialer}
}

func (adapter OpenClawAdapter) Kind() AdapterKind {
	return AdapterKindOpenClaw
}

func (adapter OpenClawAdapter) Detect() Detection {
	if adapter.Token == "" {
		return Detection{Kind: AdapterKindOpenClaw, State: AdapterStateAuthMissing, Note: "OPENCLAW_GATEWAY_TOKEN is required"}
	}
	conn, err := adapter.dial(context.Background())
	if err != nil {
		return Detection{Kind: AdapterKindOpenClaw, State: AdapterStateRuntimeMissing, Note: err.Error()}
	}
	defer conn.Close()
	if err := conn.WriteJSON(openClawRequest{ID: "detect-1", Method: "health"}); err != nil {
		return Detection{Kind: AdapterKindOpenClaw, State: AdapterStateRuntimeStopped, Note: err.Error()}
	}
	return Detection{Kind: AdapterKindOpenClaw, State: AdapterStateReady, Note: "OpenClaw Gateway reachable"}
}

func (adapter OpenClawAdapter) ConfigureMCP(bindingID string) error {
	if strings.TrimSpace(bindingID) == "" {
		return fmt.Errorf("binding id required")
	}
	return fmt.Errorf("OpenClaw MCP config edit is not implemented")
}

func (adapter OpenClawAdapter) VerifyMCP(bindingID string) (AdapterState, error) {
	if strings.TrimSpace(bindingID) == "" {
		return AdapterStateMCPConfigMissing, fmt.Errorf("binding id required")
	}
	return AdapterStateMCPConfigMissing, fmt.Errorf("OpenClaw MCP verification is not implemented")
}

func (adapter OpenClawAdapter) StartRun(assignmentID string, fullyComposedPrompt string) (string, error) {
	conn, err := adapter.dial(context.Background())
	if err != nil {
		return "", err
	}
	defer conn.Close()
	request := openClawRequest{
		ID:     strings.TrimSpace(assignmentID),
		Method: "agent",
		Params: map[string]string{
			"message":        fullyComposedPrompt,
			"idempotencyKey": strings.TrimSpace(assignmentID),
		},
	}
	if err := conn.WriteJSON(request); err != nil {
		return "", fmt.Errorf("OpenClaw agent dispatch: %w", err)
	}
	var response openClawResponse
	if err := conn.ReadJSON(&response); err != nil {
		return "", fmt.Errorf("OpenClaw agent ack: %w", err)
	}
	if response.Error != "" {
		return "", fmt.Errorf("OpenClaw agent error: %s", response.Error)
	}
	return strings.TrimSpace(assignmentID), nil
}

func (adapter OpenClawAdapter) CancelRun(nativeRunID string) error {
	conn, err := adapter.dial(context.Background())
	if err != nil {
		return err
	}
	defer conn.Close()
	request := openClawRequest{
		ID:     "cancel-" + strings.TrimSpace(nativeRunID),
		Method: "sessions.abort",
		Params: map[string]string{
			"runId": strings.TrimSpace(nativeRunID),
		},
	}
	if err := conn.WriteJSON(request); err != nil {
		return fmt.Errorf("OpenClaw cancel: %w", err)
	}
	return nil
}

func (adapter OpenClawAdapter) Diagnose() Detection {
	return adapter.Detect()
}

func (adapter OpenClawAdapter) dial(ctx context.Context) (*websocket.Conn, error) {
	dialer := adapter.Dialer
	if dialer == nil {
		dialer = websocket.DefaultDialer
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	headers := http.Header{}
	if adapter.Token != "" {
		headers.Set("Authorization", "Bearer "+adapter.Token)
	}
	conn, _, err := dialer.DialContext(ctx, adapter.GatewayURL, headers)
	if err != nil {
		return nil, fmt.Errorf("connect OpenClaw Gateway: %w", err)
	}
	return conn, nil
}

type openClawRequest struct {
	ID     string `json:"id"`
	Method string `json:"method"`
	Params any    `json:"params,omitempty"`
}

type openClawResponse struct {
	ID     string          `json:"id"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  string          `json:"error,omitempty"`
}
