package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

const defaultOpenClawGatewayURL = "ws://127.0.0.1:18789"

type OpenClawAdapter struct {
	GatewayURL      string
	Token           string
	Password        string
	DeviceToken     string
	AgentID         string
	DeviceTokenSink func(string) error
	Dialer          *websocket.Dialer
}

func NewOpenClawAdapter(gatewayURL string, token string) OpenClawAdapter {
	return NewOpenClawAdapterWithAuth(gatewayURL, OpenClawAuth{Token: token}, "")
}

type OpenClawAuth struct {
	Token       string
	Password    string
	DeviceToken string
}

func NewOpenClawAdapterWithAuth(gatewayURL string, auth OpenClawAuth, agentID string) OpenClawAdapter {
	if strings.TrimSpace(gatewayURL) == "" {
		gatewayURL = defaultOpenClawGatewayURL
	}
	return OpenClawAdapter{
		GatewayURL:  strings.TrimSpace(gatewayURL),
		Token:       strings.TrimSpace(auth.Token),
		Password:    strings.TrimSpace(auth.Password),
		DeviceToken: strings.TrimSpace(auth.DeviceToken),
		AgentID:     strings.TrimSpace(agentID),
		Dialer:      websocket.DefaultDialer,
	}
}

func (adapter OpenClawAdapter) Kind() AdapterKind {
	return AdapterKindOpenClaw
}

func (adapter OpenClawAdapter) Detect() Detection {
	if !adapter.hasAuth() {
		return Detection{Kind: AdapterKindOpenClaw, State: AdapterStateAuthMissing, Note: "OpenClaw operator token, password, or device token is required"}
	}
	conn, err := adapter.dial(context.Background())
	if err != nil {
		return Detection{Kind: AdapterKindOpenClaw, State: AdapterStateRuntimeMissing, Note: err.Error()}
	}
	defer conn.Close()
	if err := adapter.connectOperator(conn); err != nil {
		if openClawConnectErrorIsCapability(err) {
			return Detection{Kind: AdapterKindOpenClaw, State: AdapterStateCapabilityMissing, Note: err.Error()}
		}
		return Detection{Kind: AdapterKindOpenClaw, State: AdapterStateAuthMissing, Note: err.Error()}
	}
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
	if _, detection, failed := adapter.probeOpenClawMethod(conn, "detect-2", "status"); failed {
		return detection
	}
	agentsRaw, detection, failed := adapter.probeOpenClawMethod(conn, "detect-3", "agents.list")
	if failed {
		return detection
	}
	agents, err := openClawAgentsFromResult(agentsRaw)
	if err != nil {
		return Detection{Kind: AdapterKindOpenClaw, State: AdapterStateCapabilityMissing, Note: err.Error()}
	}
	if err := adapter.validateAgentSelection(agents); err != nil {
		return Detection{Kind: AdapterKindOpenClaw, State: AdapterStateCapabilityMissing, Note: err.Error()}
	}
	return Detection{Kind: AdapterKindOpenClaw, State: AdapterStateReady, Note: "OpenClaw Gateway reachable"}
}

func (adapter OpenClawAdapter) probeOpenClawMethod(conn *websocket.Conn, requestID string, method string) (json.RawMessage, Detection, bool) {
	if err := conn.WriteJSON(openClawRequest{ID: requestID, Method: method}); err != nil {
		return nil, Detection{Kind: AdapterKindOpenClaw, State: AdapterStateCapabilityMissing, Note: err.Error()}, true
	}
	var response openClawResponse
	if err := conn.ReadJSON(&response); err != nil {
		return nil, Detection{Kind: AdapterKindOpenClaw, State: AdapterStateCapabilityMissing, Note: err.Error()}, true
	}
	if response.Error != "" {
		return nil, Detection{Kind: AdapterKindOpenClaw, State: AdapterStateCapabilityMissing, Note: response.Error}, true
	}
	return response.Result, Detection{}, false
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

func (adapter OpenClawAdapter) StartRun(runRequest RunRequest) (string, error) {
	assignmentID := strings.TrimSpace(runRequest.AssignmentID)
	if assignmentID == "" {
		return "", fmt.Errorf("OpenClaw assignment id required")
	}
	conn, err := adapter.dial(context.Background())
	if err != nil {
		return "", err
	}
	defer conn.Close()
	if err := adapter.connectOperator(conn); err != nil {
		return "", err
	}
	params := map[string]any{
		"message":                strings.TrimSpace(runRequest.FullyComposedPrompt),
		"idempotencyKey":         assignmentID,
		"runId":                  assignmentID,
		"nativeMcpServerName":    strings.TrimSpace(runRequest.NativeMCPServerName),
		"nativeMcpToolNamespace": strings.TrimSpace(runRequest.NativeMCPToolNamespace),
		"metadata":               runMetadata(runRequest),
	}
	if adapter.AgentID != "" {
		params["agentId"] = adapter.AgentID
	}
	request := openClawRequest{
		ID:     assignmentID,
		Method: "agent",
		Params: params,
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
	var accepted openClawRunResult
	if len(response.Result) > 0 {
		_ = json.Unmarshal(response.Result, &accepted)
	}
	status := strings.ToLower(strings.TrimSpace(accepted.Status))
	if status != "" && status != "accepted" && status != "in_flight" && status != "running" {
		return "", fmt.Errorf("OpenClaw agent rejected with status %q", accepted.Status)
	}
	return strings.TrimSpace(assignmentID), nil
}

func (adapter OpenClawAdapter) WaitRun(ctx context.Context, nativeRunID string) (RunResult, error) {
	if err := ctx.Err(); err != nil {
		return RunResult{}, err
	}
	conn, err := adapter.dial(ctx)
	if err != nil {
		return RunResult{}, err
	}
	defer conn.Close()
	if err := adapter.connectOperator(conn); err != nil {
		return RunResult{}, err
	}
	for {
		if err := ctx.Err(); err != nil {
			return RunResult{}, err
		}
		setOpenClawDeadline(conn, ctx, 35*time.Second)
		request := openClawRequest{
			ID:     "wait-" + strings.TrimSpace(nativeRunID),
			Method: "agent.wait",
			Params: map[string]any{
				"runId":     strings.TrimSpace(nativeRunID),
				"timeoutMs": 30000,
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
			if openClawErrorIsTimeout(response.Error) && ctx.Err() == nil {
				continue
			}
			return RunResult{}, fmt.Errorf("OpenClaw wait error: %s", response.Error)
		}
		result, terminal := openClawRunResultFromResponse(response.Result)
		if !terminal {
			if err := ctx.Err(); err != nil {
				return RunResult{}, err
			}
			continue
		}
		switch strings.ToLower(strings.TrimSpace(result.Status)) {
		case "failed", "error":
			return RunResult{Status: RunStatusFailed, Output: strings.TrimSpace(firstNonEmpty(result.Error, result.Output))}, nil
		case "cancelled", "canceled", "aborted":
			return RunResult{Status: RunStatusCancelled}, nil
		case "timeout":
			return RunResult{}, fmt.Errorf("OpenClaw wait timed out")
		default:
			return RunResult{Status: RunStatusSucceeded, Output: strings.TrimSpace(result.Output)}, nil
		}
	}
}

func (adapter OpenClawAdapter) CancelRun(nativeRunID string) error {
	conn, err := adapter.dial(context.Background())
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := adapter.connectOperator(conn); err != nil {
		return err
	}
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
	conn, _, err := dialer.DialContext(ctx, adapter.GatewayURL, http.Header{})
	if err != nil {
		return nil, fmt.Errorf("connect OpenClaw Gateway: %w", err)
	}
	return conn, nil
}

func (adapter OpenClawAdapter) hasAuth() bool {
	return adapter.Token != "" || adapter.Password != "" || adapter.DeviceToken != ""
}

func (adapter OpenClawAdapter) connectOperator(conn *websocket.Conn) error {
	setOpenClawDeadline(conn, context.Background(), 10*time.Second)
	var challenge openClawResponse
	if err := conn.ReadJSON(&challenge); err != nil {
		return fmt.Errorf("read OpenClaw connect challenge: %w", err)
	}
	if challenge.Method != "connect.challenge" && challenge.Type != "connect.challenge" {
		return fmt.Errorf("expected OpenClaw connect.challenge, got %q", firstNonEmpty(challenge.Method, challenge.Type))
	}
	auth := map[string]string{}
	switch {
	case adapter.DeviceToken != "":
		auth["deviceToken"] = adapter.DeviceToken
	case adapter.Token != "":
		auth["token"] = adapter.Token
	case adapter.Password != "":
		auth["password"] = adapter.Password
	default:
		return fmt.Errorf("OpenClaw operator auth missing")
	}
	request := openClawRequest{
		Type:   "req",
		ID:     "ps-connect",
		Method: "connect",
		Params: map[string]any{
			"minProtocol": 3,
			"maxProtocol": 4,
			"client": map[string]string{
				"id":       "personastack-connector",
				"version":  "dev",
				"platform": runtime.GOOS,
				"mode":     "operator",
			},
			"role":   "operator",
			"scopes": []string{"operator.read", "operator.write"},
			"auth":   auth,
		},
	}
	if err := conn.WriteJSON(request); err != nil {
		return fmt.Errorf("write OpenClaw connect: %w", err)
	}
	setOpenClawDeadline(conn, context.Background(), 10*time.Second)
	var hello openClawResponse
	if err := conn.ReadJSON(&hello); err != nil {
		return fmt.Errorf("read OpenClaw hello-ok: %w", err)
	}
	if hello.Error != "" {
		return fmt.Errorf("OpenClaw connect error: %s", hello.Error)
	}
	if hello.Method != "hello-ok" && hello.Type != "hello-ok" {
		return fmt.Errorf("expected OpenClaw hello-ok, got %q", firstNonEmpty(hello.Method, hello.Type))
	}
	helloResult, err := openClawHelloFromResult(hello.Result)
	if err != nil {
		return err
	}
	if !helloResult.hasScope("operator.read") || !helloResult.hasScope("operator.write") {
		return fmt.Errorf("OpenClaw operator.read and operator.write scopes required")
	}
	missing := helloResult.Features.missingRequiredMethods()
	if len(missing) > 0 {
		return fmt.Errorf("%s missing", strings.Join(missing, ","))
	}
	if helloResult.Auth.DeviceToken != "" && adapter.DeviceTokenSink != nil {
		if err := adapter.DeviceTokenSink(helloResult.Auth.DeviceToken); err != nil {
			return fmt.Errorf("store OpenClaw device token: %w", err)
		}
	}
	return nil
}

func setOpenClawDeadline(conn *websocket.Conn, ctx context.Context, fallback time.Duration) {
	deadline := time.Now().Add(fallback)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	_ = conn.SetReadDeadline(deadline)
	_ = conn.SetWriteDeadline(deadline)
}

type openClawRequest struct {
	Type   string `json:"type,omitempty"`
	ID     string `json:"id"`
	Method string `json:"method"`
	Params any    `json:"params,omitempty"`
}

type openClawResponse struct {
	Type   string          `json:"type,omitempty"`
	ID     string          `json:"id"`
	Method string          `json:"method,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  string          `json:"error,omitempty"`
}

type openClawRunResult struct {
	Status string `json:"status"`
	Output string `json:"output"`
	Error  string `json:"error"`
}

type openClawHello struct {
	Protocol int              `json:"protocol"`
	Role     string           `json:"role"`
	Scopes   []string         `json:"scopes"`
	Features openClawFeatures `json:"features"`
	Auth     struct {
		DeviceToken string `json:"deviceToken"`
	} `json:"auth"`
}

type openClawAgent struct {
	ID       string `json:"id"`
	Enabled  *bool  `json:"enabled"`
	Disabled bool   `json:"disabled"`
}

type openClawFeatures struct {
	methods map[string]struct{}
}

func openClawFeaturesFromResult(raw json.RawMessage) (openClawFeatures, error) {
	var envelope struct {
		Methods any `json:"methods"`
	}
	if len(raw) == 0 {
		return openClawFeatures{}, fmt.Errorf("features missing")
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return openClawFeatures{}, err
	}
	return openClawFeaturesFromAny(envelope.Methods)
}

func openClawHelloFromResult(raw json.RawMessage) (openClawHello, error) {
	var hello openClawHello
	if len(raw) == 0 {
		return hello, fmt.Errorf("hello-ok result missing")
	}
	var envelope struct {
		Protocol int `json:"protocol"`
		Features struct {
			Methods any `json:"methods"`
		} `json:"features"`
		Auth struct {
			DeviceToken string `json:"deviceToken"`
		} `json:"auth"`
		Role   string   `json:"role"`
		Scopes []string `json:"scopes"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return hello, err
	}
	if envelope.Protocol < 3 || envelope.Protocol > 4 {
		return hello, fmt.Errorf("OpenClaw protocol %d unsupported", envelope.Protocol)
	}
	features, err := openClawFeaturesFromAny(envelope.Features.Methods)
	if err != nil {
		return hello, err
	}
	hello.Protocol = envelope.Protocol
	hello.Role = strings.TrimSpace(envelope.Role)
	hello.Scopes = envelope.Scopes
	hello.Features = features
	hello.Auth.DeviceToken = strings.TrimSpace(envelope.Auth.DeviceToken)
	return hello, nil
}

func openClawFeaturesFromAny(value any) (openClawFeatures, error) {
	methods := map[string]struct{}{}
	switch value := value.(type) {
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

func (hello openClawHello) hasScope(scope string) bool {
	for _, candidate := range hello.Scopes {
		if strings.TrimSpace(candidate) == scope {
			return true
		}
	}
	return false
}

func openClawAgentsFromResult(raw json.RawMessage) ([]openClawAgent, error) {
	var envelope struct {
		Agents []openClawAgent `json:"agents"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return nil, err
	}
	return envelope.Agents, nil
}

func (adapter OpenClawAdapter) validateAgentSelection(agents []openClawAgent) error {
	if adapter.AgentID != "" {
		for _, agent := range agents {
			if strings.TrimSpace(agent.ID) == adapter.AgentID && openClawAgentUsable(agent) {
				return nil
			}
		}
		return fmt.Errorf("configured OpenClaw agent %q not found", adapter.AgentID)
	}
	usable := 0
	for _, agent := range agents {
		if openClawAgentUsable(agent) {
			usable++
		}
	}
	if usable == 1 {
		return nil
	}
	if usable == 0 {
		return fmt.Errorf("no usable OpenClaw agents")
	}
	return fmt.Errorf("agent_selection_required")
}

func openClawAgentUsable(agent openClawAgent) bool {
	if strings.TrimSpace(agent.ID) == "" || agent.Disabled {
		return false
	}
	return agent.Enabled == nil || *agent.Enabled
}

func openClawRunResultFromResponse(raw json.RawMessage) (openClawRunResult, bool) {
	var result openClawRunResult
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &result)
	}
	switch strings.ToLower(strings.TrimSpace(result.Status)) {
	case "completed", "success", "succeeded", "failed", "error", "cancelled", "canceled", "aborted", "timeout":
		return result, true
	default:
		return result, false
	}
}

func openClawErrorIsTimeout(value string) bool {
	return strings.Contains(strings.ToLower(value), "timeout")
}

func openClawConnectErrorIsCapability(err error) bool {
	message := strings.ToLower(err.Error())
	return strings.Contains(message, " missing") || strings.Contains(message, "protocol") || strings.Contains(message, "features.")
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
