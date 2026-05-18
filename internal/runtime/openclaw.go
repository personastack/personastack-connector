package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const defaultOpenClawGatewayURL = "ws://127.0.0.1:18789"
const minOpenClawProtocolVersion = 3
const maxOpenClawProtocolVersion = 4

type OpenClawAdapter struct {
	GatewayURL      string
	Token           string
	Password        string
	DeviceToken     string
	AgentID         string
	DeviceTokenSink func(string) error
	Dialer          *websocket.Dialer
	fallbackCache   *openClawFallbackCache
}

type openClawFallbackCache struct {
	mu      sync.Mutex
	results map[string]openClawFallbackResult
}

type openClawFallbackResult struct {
	result   RunResult
	degraded bool
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
		fallbackCache: &openClawFallbackCache{
			results: map[string]openClawFallbackResult{},
		},
	}
}

func (adapter OpenClawAdapter) Kind() AdapterKind {
	return AdapterKindOpenClaw
}

func (adapter OpenClawAdapter) Detect() Detection {
	if !adapter.hasAuth() {
		return Detection{Kind: AdapterKindOpenClaw, State: AdapterStateAuthMissing, Note: "OpenClaw operator token, password, or device token is required"}
	}
	connectCtx, cancel := context.WithTimeout(context.Background(), openClawSetupRetryBudget)
	defer cancel()
	conn, err := adapter.connectOperatorWithRetry(connectCtx)
	if err != nil {
		if openClawConnectErrorIsCapability(err) {
			return Detection{Kind: AdapterKindOpenClaw, State: AdapterStateCapabilityMissing, Note: err.Error()}
		}
		return Detection{Kind: AdapterKindOpenClaw, State: AdapterStateRuntimeMissing, Note: err.Error()}
	}
	defer conn.Close()
	if err := conn.WriteJSON(openClawRequest{Type: "req", ID: "detect-1", Method: "health"}); err != nil {
		return Detection{Kind: AdapterKindOpenClaw, State: AdapterStateRuntimeStopped, Note: err.Error()}
	}
	var health openClawResponse
	if err := readOpenClawResponse(conn, "detect-1", &health); err != nil {
		return Detection{Kind: AdapterKindOpenClaw, State: AdapterStateRuntimeStopped, Note: err.Error()}
	}
	if errText := health.errorString(); errText != "" {
		return Detection{Kind: AdapterKindOpenClaw, State: AdapterStateRuntimeStopped, Note: errText}
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
	if err := conn.WriteJSON(openClawRequest{Type: "req", ID: requestID, Method: method}); err != nil {
		return nil, Detection{Kind: AdapterKindOpenClaw, State: AdapterStateCapabilityMissing, Note: err.Error()}, true
	}
	var response openClawResponse
	if err := readOpenClawResponse(conn, requestID, &response); err != nil {
		return nil, Detection{Kind: AdapterKindOpenClaw, State: AdapterStateCapabilityMissing, Note: err.Error()}, true
	}
	if errText := response.errorString(); errText != "" {
		return nil, Detection{Kind: AdapterKindOpenClaw, State: AdapterStateCapabilityMissing, Note: errText}, true
	}
	if !response.isResponseOK() {
		return nil, Detection{Kind: AdapterKindOpenClaw, State: AdapterStateCapabilityMissing, Note: "OpenClaw response not ok"}, true
	}
	return response.payload(), Detection{}, false
}

type openClawToolsCatalogResult struct {
	AgentID string                      `json:"agentId"`
	Groups  []openClawToolsCatalogGroup `json:"groups"`
}

type openClawToolsCatalogGroup struct {
	ID       string                     `json:"id"`
	Label    string                     `json:"label"`
	Source   string                     `json:"source"`
	PluginID string                     `json:"pluginId,omitempty"`
	Tools    []openClawToolsCatalogTool `json:"tools"`
}

type openClawToolsCatalogTool struct {
	ID       string `json:"id"`
	Label    string `json:"label"`
	Source   string `json:"source"`
	PluginID string `json:"pluginId,omitempty"`
}

type OpenClawMCPVerificationResult struct {
	OK   bool
	Note string
}

func (adapter OpenClawAdapter) VerifyMCPCatalog(ctx context.Context, serverName string) OpenClawMCPVerificationResult {
	expectedServerName := strings.TrimSpace(serverName)
	if expectedServerName == "" {
		return OpenClawMCPVerificationResult{Note: "OpenClaw native MCP server name required"}
	}
	connectCtx, cancel := context.WithTimeout(ctx, openClawSetupRetryBudget)
	defer cancel()
	conn, err := adapter.connectOperatorWithRetry(connectCtx)
	if err != nil {
		return OpenClawMCPVerificationResult{Note: "OpenClaw tools.catalog connect failed: " + err.Error()}
	}
	defer conn.Close()
	params := map[string]any{"includePlugins": true}
	if trimmed := strings.TrimSpace(adapter.AgentID); trimmed != "" {
		params["agentId"] = trimmed
	}
	if err := conn.WriteJSON(openClawRequest{
		Type:   "req",
		ID:     "verify-mcp-catalog-1",
		Method: "tools.catalog",
		Params: params,
	}); err != nil {
		return OpenClawMCPVerificationResult{Note: "OpenClaw tools.catalog request failed: " + err.Error()}
	}
	var response openClawResponse
	if err := readOpenClawResponse(conn, "verify-mcp-catalog-1", &response); err != nil {
		return OpenClawMCPVerificationResult{Note: "OpenClaw tools.catalog response failed: " + err.Error()}
	}
	if errText := response.errorString(); errText != "" {
		return OpenClawMCPVerificationResult{Note: "OpenClaw tools.catalog error: " + errText}
	}
	catalog, err := openClawToolsCatalogFromResult(response.payload())
	if err != nil {
		return OpenClawMCPVerificationResult{Note: "OpenClaw tools.catalog invalid: " + err.Error()}
	}
	if !catalog.hasNativeMCPServer(expectedServerName) {
		return OpenClawMCPVerificationResult{Note: "OpenClaw tools.catalog missing configured MCP server " + expectedServerName}
	}
	return OpenClawMCPVerificationResult{
		OK:   true,
		Note: "OpenClaw effective tool catalog visible for " + expectedServerName,
	}
}

func (adapter OpenClawAdapter) StreamOrPollRun(ctx context.Context, nativeRunID string, handle RunEventHandler) (RunResult, error) {
	return adapter.openClawStreamOrPollRun(ctx, nativeRunID, handle)
}

func (adapter OpenClawAdapter) WaitRun(ctx context.Context, nativeRunID string) (RunResult, error) {
	return adapter.StreamOrPollRun(ctx, nativeRunID, nil)
}

func (adapter OpenClawAdapter) StartRun(runRequest RunRequest) (string, error) {
	assignmentID := strings.TrimSpace(runRequest.AssignmentID)
	if assignmentID == "" {
		return "", fmt.Errorf("OpenClaw assignment id required")
	}
	connectCtx, cancel := context.WithTimeout(context.Background(), openClawSetupRetryBudget)
	defer cancel()
	conn, err := adapter.connectOperatorWithRetry(connectCtx)
	if err != nil {
		cliResult, cliErr := adapter.startOpenClawCLI(runRequest)
		if cliErr == nil {
			adapter.storeFallbackResult(assignmentID, cliResult)
			return assignmentID, nil
		}
		return "", fmt.Errorf("OpenClaw CLI fallback after gateway connect failure: %v; fallback error: %w", err, cliErr)
	}
	defer conn.Close()
	params := map[string]any{
		"message":        strings.TrimSpace(runRequest.FullyComposedPrompt),
		"idempotencyKey": assignmentID,
	}
	agentID := strings.TrimSpace(adapter.AgentID)
	if agentID == "" {
		agentID = "main"
	}
	params["agentId"] = agentID
	request := openClawRequest{
		Type:   "req",
		ID:     assignmentID,
		Method: "agent",
		Params: params,
	}
	if err := conn.WriteJSON(request); err != nil {
		return "", fmt.Errorf("OpenClaw agent dispatch: %w", err)
	}
	var response openClawResponse
	if err := readOpenClawResponse(conn, assignmentID, &response); err != nil {
		return "", fmt.Errorf("OpenClaw agent ack: %w", err)
	}
	if errText := response.errorString(); errText != "" {
		return "", fmt.Errorf("OpenClaw agent error: %s", errText)
	}
	if !response.isResponseOK() {
		return "", fmt.Errorf("OpenClaw agent response not ok")
	}
	var accepted openClawRunResult
	if payload := response.payload(); len(payload) > 0 {
		_ = json.Unmarshal(payload, &accepted)
	}
	status := strings.ToLower(strings.TrimSpace(accepted.Status))
	if status != "" && status != "accepted" && status != "in_flight" && status != "running" {
		return "", fmt.Errorf("OpenClaw agent rejected with status %q", accepted.Status)
	}
	return strings.TrimSpace(assignmentID), nil
}

func (adapter OpenClawAdapter) CancelRun(nativeRunID string) error {
	if result, ok := adapter.loadFallbackResult(nativeRunID); ok && result.degraded {
		return fmt.Errorf("OpenClaw CLI fallback does not support cancellation")
	}
	connectCtx, cancel := context.WithTimeout(context.Background(), openClawSetupRetryBudget)
	defer cancel()
	conn, err := adapter.connectOperatorWithRetry(connectCtx)
	if err != nil {
		return err
	}
	defer conn.Close()
	request := openClawRequest{
		Type:   "req",
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
	if err := readOpenClawResponse(conn, request.ID, &response); err != nil {
		return fmt.Errorf("OpenClaw cancel response: %w", err)
	}
	if errText := response.errorString(); errText != "" {
		return fmt.Errorf("OpenClaw cancel error: %s", errText)
	}
	if !response.isResponseOK() {
		return fmt.Errorf("OpenClaw cancel response not ok")
	}
	return nil
}

func (adapter OpenClawAdapter) Diagnose() Detection {
	return adapter.Detect()
}

func (adapter OpenClawAdapter) startOpenClawCLI(runRequest RunRequest) (openClawFallbackResult, error) {
	cliPath, err := exec.LookPath("openclaw")
	if err != nil {
		return openClawFallbackResult{}, fmt.Errorf("openclaw CLI not found: %w", err)
	}
	assignmentID := strings.TrimSpace(runRequest.AssignmentID)
	if assignmentID == "" {
		return openClawFallbackResult{}, fmt.Errorf("OpenClaw assignment id required")
	}
	message := strings.TrimSpace(runRequest.FullyComposedPrompt)
	if message == "" {
		return openClawFallbackResult{}, fmt.Errorf("OpenClaw prompt required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), openClawSetupRetryBudget)
	defer cancel()
	args := []string{"agent", "--json", "--session-id", assignmentID, "--message", message}
	if adapter.AgentID != "" {
		args = append(args, "--agent", adapter.AgentID)
	}
	cmd := exec.CommandContext(ctx, cliPath, args...)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return openClawFallbackResult{}, fmt.Errorf("openclaw agent --json: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	parsed, err := parseOpenClawCLIResult(stdout.Bytes())
	if err != nil {
		return openClawFallbackResult{}, err
	}
	parsed.degraded = true
	return parsed, nil
}

func parseOpenClawCLIResult(raw []byte) (openClawFallbackResult, error) {
	var response struct {
		Payloads []struct {
			Text string `json:"text"`
		} `json:"payloads"`
		Meta struct {
			Transport      string `json:"transport"`
			FallbackFrom   string `json:"fallbackFrom"`
			FallbackReason string `json:"fallbackReason"`
		} `json:"meta"`
		Output  string          `json:"output"`
		Text    string          `json:"text"`
		Message string          `json:"message"`
		Result  json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(raw, &response); err != nil {
		return openClawFallbackResult{}, fmt.Errorf("decode openclaw agent json: %w", err)
	}
	output := strings.TrimSpace(response.Output)
	if output == "" {
		output = strings.TrimSpace(response.Text)
	}
	if output == "" {
		output = strings.TrimSpace(response.Message)
	}
	if output == "" {
		for _, payload := range response.Payloads {
			if text := strings.TrimSpace(payload.Text); text != "" {
				if output == "" {
					output = text
				} else {
					output += "\n" + text
				}
			}
		}
	}
	if output == "" && len(response.Result) > 0 {
		var text string
		if err := json.Unmarshal(response.Result, &text); err == nil {
			output = strings.TrimSpace(text)
		}
	}
	degraded := strings.EqualFold(strings.TrimSpace(response.Meta.Transport), "embedded") || strings.EqualFold(strings.TrimSpace(response.Meta.FallbackFrom), "gateway")
	return openClawFallbackResult{
		result: RunResult{
			Status: RunStatusSucceeded,
			Output: output,
		},
		degraded: degraded,
	}, nil
}

func (adapter OpenClawAdapter) storeFallbackResult(runID string, result openClawFallbackResult) {
	if adapter.fallbackCache == nil {
		return
	}
	trimmed := strings.TrimSpace(runID)
	if trimmed == "" {
		return
	}
	adapter.fallbackCache.mu.Lock()
	defer adapter.fallbackCache.mu.Unlock()
	adapter.fallbackCache.results[trimmed] = result
}

func (adapter OpenClawAdapter) loadFallbackResult(runID string) (openClawFallbackResult, bool) {
	if adapter.fallbackCache == nil {
		return openClawFallbackResult{}, false
	}
	trimmed := strings.TrimSpace(runID)
	if trimmed == "" {
		return openClawFallbackResult{}, false
	}
	adapter.fallbackCache.mu.Lock()
	defer adapter.fallbackCache.mu.Unlock()
	result, ok := adapter.fallbackCache.results[trimmed]
	return result, ok
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
	if !challenge.isEvent("connect.challenge") {
		return fmt.Errorf("expected OpenClaw connect.challenge, got %q", firstNonEmpty(challenge.Event, challenge.Method, challenge.Type))
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
			"minProtocol": minOpenClawProtocolVersion,
			"maxProtocol": maxOpenClawProtocolVersion,
			"client": map[string]string{
				"id":       "gateway-client",
				"version":  "dev",
				"platform": runtime.GOOS,
				"mode":     "backend",
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
	if errText := hello.errorString(); errText != "" {
		if retryAfter, retryable := hello.retryableStartupSidecars(); retryable {
			_ = conn.Close()
			return openClawRetryableError{info: openClawErrorInfo{
				Code:       "UNAVAILABLE",
				Reason:     "startup-sidecars",
				Message:    errText,
				RetryAfter: retryAfter,
			}}
		}
		return fmt.Errorf("OpenClaw connect error: %s", errText)
	}
	payload := hello.payload()
	payloadType := openClawPayloadType(payload)
	if !hello.isResponseOK() {
		if retryAfter, retryable := hello.retryableStartupSidecars(); retryable {
			_ = conn.Close()
			return openClawRetryableError{info: openClawErrorInfo{
				Code:       "UNAVAILABLE",
				Reason:     "startup-sidecars",
				Message:    firstNonEmpty(hello.errorString(), "OpenClaw connect response not ok"),
				RetryAfter: retryAfter,
			}}
		}
		return fmt.Errorf("OpenClaw connect response not ok")
	}
	helloResult, err := openClawHelloFromResult(payload)
	if err != nil && payloadType != "hello-ok" && hello.Method != "hello-ok" && hello.Type != "hello-ok" {
		return fmt.Errorf("expected OpenClaw hello-ok, got %q", firstNonEmpty(payloadType, hello.Event, hello.Method, hello.Type))
	}
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
	OK      *bool           `json:"ok,omitempty"`
	Type    string          `json:"type,omitempty"`
	ID      string          `json:"id"`
	Event   string          `json:"event,omitempty"`
	Method  string          `json:"method,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   any             `json:"error,omitempty"`
}

func (response openClawResponse) payload() json.RawMessage {
	if len(response.Payload) > 0 {
		return response.Payload
	}
	return response.Result
}

func (response openClawResponse) isEvent(event string) bool {
	return response.Type == "event" && response.Event == event || response.Type == event || response.Method == event
}

func readOpenClawResponse(conn *websocket.Conn, requestID string, response *openClawResponse) error {
	for {
		var next openClawResponse
		if err := conn.ReadJSON(&next); err != nil {
			return err
		}
		if next.Type == "event" && next.ID == "" {
			continue
		}
		if requestID == "" || next.ID == requestID {
			*response = next
			return nil
		}
	}
}

func (response openClawResponse) isResponseOK() bool {
	if response.OK != nil && !*response.OK {
		return false
	}
	if response.Type == "res" {
		return response.OK != nil && *response.OK
	}
	return response.Type == "" || response.Type == "hello-ok" || response.Type == "hello"
}

func (response openClawResponse) errorString() string {
	switch value := response.Error.(type) {
	case nil:
		return ""
	case string:
		return strings.TrimSpace(value)
	case map[string]any:
		for _, key := range []string{"message", "reason", "code"} {
			if raw, ok := value[key]; ok && strings.TrimSpace(fmt.Sprint(raw)) != "" {
				return strings.TrimSpace(fmt.Sprint(raw))
			}
		}
	}
	return strings.TrimSpace(fmt.Sprint(response.Error))
}

func openClawPayloadType(raw json.RawMessage) string {
	var envelope struct {
		Type string `json:"type"`
	}
	_ = json.Unmarshal(raw, &envelope)
	return strings.TrimSpace(envelope.Type)
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
			DeviceToken string   `json:"deviceToken"`
			Role        string   `json:"role"`
			Scopes      []string `json:"scopes"`
		} `json:"auth"`
		Role   string   `json:"role"`
		Scopes []string `json:"scopes"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return hello, err
	}
	if envelope.Protocol < minOpenClawProtocolVersion || envelope.Protocol > maxOpenClawProtocolVersion {
		return hello, fmt.Errorf("OpenClaw protocol %d unsupported", envelope.Protocol)
	}
	features, err := openClawFeaturesFromAny(envelope.Features.Methods)
	if err != nil {
		return hello, err
	}
	hello.Protocol = envelope.Protocol
	hello.Role = strings.TrimSpace(firstNonEmpty(envelope.Role, envelope.Auth.Role))
	hello.Scopes = envelope.Scopes
	if len(hello.Scopes) == 0 {
		hello.Scopes = envelope.Auth.Scopes
	}
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

func openClawToolsCatalogFromResult(raw json.RawMessage) (openClawToolsCatalogResult, error) {
	var catalog openClawToolsCatalogResult
	if len(raw) == 0 {
		return catalog, fmt.Errorf("catalog missing")
	}
	if err := json.Unmarshal(raw, &catalog); err != nil {
		return catalog, err
	}
	return catalog, nil
}

func (catalog openClawToolsCatalogResult) hasNativeMCPServer(expectedServerName string) bool {
	target := strings.TrimSpace(expectedServerName)
	if target == "" {
		return false
	}
	for _, group := range catalog.Groups {
		if strings.TrimSpace(group.PluginID) != target && strings.TrimSpace(group.Label) != target && strings.TrimSpace(group.ID) != "plugin:"+target {
			continue
		}
		if len(group.Tools) > 0 {
			return true
		}
	}
	return false
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
		if strings.TrimSpace(result.Output) != "" || strings.TrimSpace(result.Error) != "" {
			return result, true
		}
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
