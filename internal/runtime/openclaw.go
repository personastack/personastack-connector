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
	var health openClawResponse
	if err := conn.ReadJSON(&health); err != nil {
		return Detection{Kind: AdapterKindOpenClaw, State: AdapterStateRuntimeStopped, Note: err.Error()}
	}
	if health.Error != "" {
		return Detection{Kind: AdapterKindOpenClaw, State: AdapterStateRuntimeStopped, Note: health.Error}
	}
	if detection, failed := adapter.probeOpenClawMethod(conn, "detect-2", "status"); failed {
		return detection
	}
	if detection, failed := adapter.probeOpenClawMethod(conn, "detect-3", "agents.list"); failed {
		return detection
	}
	if err := conn.WriteJSON(openClawRequest{ID: "detect-4", Method: "hello", Params: map[string]any{"client": "personastack-connector", "protocol_min": 1, "protocol_max": 1}}); err != nil {
		return Detection{Kind: AdapterKindOpenClaw, State: AdapterStateCapabilityMissing, Note: err.Error()}
	}
	var hello openClawResponse
	if err := conn.ReadJSON(&hello); err != nil {
		return Detection{Kind: AdapterKindOpenClaw, State: AdapterStateCapabilityMissing, Note: err.Error()}
	}
	if hello.Error != "" {
		return Detection{Kind: AdapterKindOpenClaw, State: AdapterStateCapabilityMissing, Note: hello.Error}
	}
	features, err := openClawFeaturesFromResult(hello.Result)
	if err != nil {
		return Detection{Kind: AdapterKindOpenClaw, State: AdapterStateCapabilityMissing, Note: err.Error()}
	}
	missing := features.missingRequiredMethods()
	if len(missing) > 0 {
		return Detection{Kind: AdapterKindOpenClaw, State: AdapterStateCapabilityMissing, Note: strings.Join(missing, ",") + " missing"}
	}
	return Detection{Kind: AdapterKindOpenClaw, State: AdapterStateReady, Note: "OpenClaw Gateway reachable"}
}

func (adapter OpenClawAdapter) probeOpenClawMethod(conn *websocket.Conn, requestID string, method string) (Detection, bool) {
	if err := conn.WriteJSON(openClawRequest{ID: requestID, Method: method}); err != nil {
		return Detection{Kind: AdapterKindOpenClaw, State: AdapterStateCapabilityMissing, Note: err.Error()}, true
	}
	var response openClawResponse
	if err := conn.ReadJSON(&response); err != nil {
		return Detection{Kind: AdapterKindOpenClaw, State: AdapterStateCapabilityMissing, Note: err.Error()}, true
	}
	if response.Error != "" {
		return Detection{Kind: AdapterKindOpenClaw, State: AdapterStateCapabilityMissing, Note: response.Error}, true
	}
	return Detection{}, false
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

func (adapter OpenClawAdapter) WaitRun(ctx context.Context, nativeRunID string) (RunResult, error) {
	conn, err := adapter.dial(ctx)
	if err != nil {
		return RunResult{}, err
	}
	defer conn.Close()
	request := openClawRequest{
		ID:     "wait-" + strings.TrimSpace(nativeRunID),
		Method: "agent.wait",
		Params: map[string]string{
			"runId": strings.TrimSpace(nativeRunID),
		},
	}
	if err := conn.WriteJSON(request); err != nil {
		return RunResult{}, fmt.Errorf("OpenClaw wait: %w", err)
	}
	var response openClawResponse
	if err := conn.ReadJSON(&response); err != nil {
		return RunResult{}, fmt.Errorf("OpenClaw wait response: %w", err)
	}
	if response.Error != "" {
		return RunResult{}, fmt.Errorf("OpenClaw wait error: %s", response.Error)
	}
	var result openClawRunResult
	if len(response.Result) > 0 {
		_ = json.Unmarshal(response.Result, &result)
	}
	switch strings.ToLower(strings.TrimSpace(result.Status)) {
	case "failed", "error":
		return RunResult{Status: RunStatusFailed, Output: strings.TrimSpace(result.Error)}, nil
	case "cancelled", "canceled":
		return RunResult{Status: RunStatusCancelled}, nil
	default:
		return RunResult{Status: RunStatusSucceeded, Output: strings.TrimSpace(result.Output)}, nil
	}
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
	var response openClawResponse
	if err := conn.ReadJSON(&response); err != nil {
		return fmt.Errorf("OpenClaw cancel response: %w", err)
	}
	if response.Error != "" {
		return fmt.Errorf("OpenClaw cancel error: %s", response.Error)
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

type openClawRunResult struct {
	Status string `json:"status"`
	Output string `json:"output"`
	Error  string `json:"error"`
}

type openClawFeatures struct {
	methods map[string]struct{}
}

func openClawFeaturesFromResult(raw json.RawMessage) (openClawFeatures, error) {
	var envelope struct {
		Features struct {
			Methods any `json:"methods"`
		} `json:"features"`
	}
	if len(raw) == 0 {
		return openClawFeatures{}, fmt.Errorf("features missing")
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return openClawFeatures{}, err
	}
	methods := map[string]struct{}{}
	switch value := envelope.Features.Methods.(type) {
	case []any:
		for _, entry := range value {
			method := strings.TrimSpace(fmt.Sprint(entry))
			if method != "" {
				methods[method] = struct{}{}
			}
		}
	case map[string]any:
		for key, rawEnabled := range value {
			enabled, ok := rawEnabled.(bool)
			if ok && enabled {
				methods[strings.TrimSpace(key)] = struct{}{}
			}
		}
	default:
		return openClawFeatures{}, fmt.Errorf("features.methods missing")
	}
	return openClawFeatures{methods: methods}, nil
}

func (features openClawFeatures) missingRequiredMethods() []string {
	required := []string{"health", "status", "agents.list", "agent", "agent.wait", "sessions.abort"}
	missing := []string{}
	for _, method := range required {
		if _, ok := features.methods[method]; !ok {
			missing = append(missing, method)
		}
	}
	return missing
}
