package runtime

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const defaultHermesURL = "http://127.0.0.1:8642"
const defaultHermesCancelWait = 15 * time.Second

type HermesAdapter struct {
	BaseURL string
	APIKey  string
	Client  *http.Client
}

func NewHermesAdapter(baseURL string, apiKey string) HermesAdapter {
	if strings.TrimSpace(baseURL) == "" {
		baseURL = defaultHermesURL
	}
	return HermesAdapter{BaseURL: strings.TrimRight(baseURL, "/"), APIKey: strings.TrimSpace(apiKey), Client: defaultHTTPClient()}
}

func (adapter HermesAdapter) Kind() AdapterKind {
	return AdapterKindHermes
}

func (adapter HermesAdapter) Detect() Detection {
	if err := adapter.validateLoopbackBaseURL(); err != nil {
		return Detection{Kind: AdapterKindHermes, State: AdapterStateRuntimeMissing, Note: err.Error()}
	}
	client := adapter.client()
	resp, err := client.Get(adapter.BaseURL + "/health")
	if err != nil {
		return Detection{Kind: AdapterKindHermes, State: AdapterStateRuntimeMissing, Note: "Hermes API unavailable"}
	}
	_ = resp.Body.Close()
	if resp.StatusCode >= 300 {
		return Detection{Kind: AdapterKindHermes, State: AdapterStateRuntimeStopped, Note: fmt.Sprintf("health status %d", resp.StatusCode)}
	}
	if adapter.APIKey == "" {
		return Detection{Kind: AdapterKindHermes, State: AdapterStateAuthMissing, Note: "HERMES_API_SERVER_KEY is required"}
	}
	if detection, failed := adapter.probeOptionalAuthenticatedEndpoint("/health/detailed"); failed {
		return detection
	}
	if detection, failed := adapter.probeOptionalAuthenticatedEndpoint("/v1/models"); failed {
		return detection
	}
	req, _ := http.NewRequest(http.MethodGet, adapter.BaseURL+"/v1/capabilities", nil)
	req.Header.Set("Authorization", "Bearer "+adapter.APIKey)
	resp, err = client.Do(req)
	if err != nil {
		return Detection{Kind: AdapterKindHermes, State: AdapterStateCapabilityMissing, Note: err.Error()}
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return Detection{Kind: AdapterKindHermes, State: AdapterStateAuthMissing, Note: "Hermes API key rejected"}
	}
	if resp.StatusCode >= 300 {
		return Detection{Kind: AdapterKindHermes, State: AdapterStateCapabilityMissing, Note: fmt.Sprintf("capabilities status %d", resp.StatusCode)}
	}
	var capabilities hermesCapabilities
	if err := json.NewDecoder(resp.Body).Decode(&capabilities); err != nil {
		return Detection{Kind: AdapterKindHermes, State: AdapterStateCapabilityMissing, Note: err.Error()}
	}
	missing := capabilities.missingRequiredFeatures()
	if len(missing) > 0 {
		return Detection{Kind: AdapterKindHermes, State: AdapterStateCapabilityMissing, Note: strings.Join(missing, ",") + " missing"}
	}
	degraded := capabilities.degradedFallbackFeatures()
	if len(degraded) > 0 {
		return Detection{Kind: AdapterKindHermes, State: AdapterStateReady, Note: "Hermes API ready with degraded fallback: " + strings.Join(degraded, ",") + " missing"}
	}
	return Detection{Kind: AdapterKindHermes, State: AdapterStateReady, Note: "Hermes API ready"}
}

func (adapter HermesAdapter) probeOptionalAuthenticatedEndpoint(path string) (Detection, bool) {
	req, err := http.NewRequest(http.MethodGet, adapter.BaseURL+path, nil)
	if err != nil {
		return Detection{Kind: AdapterKindHermes, State: AdapterStateCapabilityMissing, Note: err.Error()}, true
	}
	req.Header.Set("Authorization", "Bearer "+adapter.APIKey)
	resp, err := adapter.client().Do(req)
	if err != nil {
		return Detection{}, false
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return Detection{Kind: AdapterKindHermes, State: AdapterStateAuthMissing, Note: "Hermes API key rejected"}, true
	}
	return Detection{}, false
}

func (adapter HermesAdapter) ConfigureMCP(bindingID string) error {
	if strings.TrimSpace(bindingID) == "" {
		return fmt.Errorf("binding id required")
	}
	return fmt.Errorf("Hermes MCP config edit is not implemented")
}

func (adapter HermesAdapter) VerifyMCP(bindingID string) (AdapterState, error) {
	if strings.TrimSpace(bindingID) == "" {
		return AdapterStateMCPConfigMissing, fmt.Errorf("binding id required")
	}
	return AdapterStateMCPConfigMissing, fmt.Errorf("Hermes MCP verification is not implemented")
}

func (adapter HermesAdapter) StartRun(request RunRequest) (string, error) {
	if err := adapter.validateLoopbackBaseURL(); err != nil {
		return "", err
	}
	body := map[string]any{
		"input":                strings.TrimSpace(request.FullyComposedPrompt),
		"session_id":           strings.TrimSpace(firstNonEmpty(request.RunID, request.AssignmentID)),
		"conversation":         strings.TrimSpace(request.AssignmentID),
		"native_mcp_server":    strings.TrimSpace(request.NativeMCPServerName),
		"native_mcp_namespace": strings.TrimSpace(request.NativeMCPToolNamespace),
		"metadata":             runMetadata(request),
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequest(http.MethodPost, adapter.BaseURL+"/v1/runs", bytes.NewReader(raw))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	if adapter.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+adapter.APIKey)
	}
	resp, err := adapter.client().Do(req)
	if err != nil {
		return "", fmt.Errorf("Hermes run dispatch: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return "", fmt.Errorf("Hermes run dispatch status %d", resp.StatusCode)
	}
	var decoded hermesRunResponse
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return "", err
	}
	nativeRunID := strings.TrimSpace(firstNonEmpty(decoded.RunID, decoded.ID))
	if nativeRunID == "" {
		return "", fmt.Errorf("Hermes response missing run id")
	}
	return nativeRunID, nil
}

func (adapter HermesAdapter) WaitRun(ctx context.Context, nativeRunID string) (RunResult, error) {
	if err := adapter.validateLoopbackBaseURL(); err != nil {
		return RunResult{}, err
	}
	trimmedRunID := strings.TrimSpace(nativeRunID)
	if trimmedRunID == "" {
		return RunResult{}, fmt.Errorf("native run id required")
	}
	if result, terminal, err := adapter.waitRunEvents(ctx, trimmedRunID); err != nil {
		return RunResult{}, err
	} else if terminal {
		return result, nil
	}
	return adapter.waitRunStatus(ctx, trimmedRunID)
}

func (adapter HermesAdapter) waitRunEvents(ctx context.Context, nativeRunID string) (RunResult, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, adapter.BaseURL+"/v1/runs/"+strings.TrimSpace(nativeRunID)+"/events", nil)
	if err != nil {
		return RunResult{}, false, err
	}
	req.Header.Set("Accept", "text/event-stream")
	if adapter.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+adapter.APIKey)
	}
	resp, err := adapter.client().Do(req)
	if err != nil {
		return RunResult{}, false, nil
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusNotImplemented || resp.StatusCode == http.StatusMethodNotAllowed {
		return RunResult{}, false, nil
	}
	if resp.StatusCode >= 300 {
		return RunResult{}, false, fmt.Errorf("Hermes run events status %d", resp.StatusCode)
	}
	result, terminal, err := readHermesRunEvents(resp.Body)
	if err != nil {
		return RunResult{}, false, err
	}
	return result, terminal, nil
}

func readHermesRunEvents(body io.Reader) (RunResult, bool, error) {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	var data []string
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			if result, terminal := hermesRunEventResult(strings.Join(data, "\n")); terminal {
				return result, true, nil
			}
			data = nil
			continue
		}
		if after, ok := strings.CutPrefix(line, "data:"); ok {
			data = append(data, strings.TrimSpace(after))
		}
	}
	if err := scanner.Err(); err != nil {
		return RunResult{}, false, fmt.Errorf("read Hermes run events: %w", err)
	}
	if result, terminal := hermesRunEventResult(strings.Join(data, "\n")); terminal {
		return result, true, nil
	}
	return RunResult{}, false, nil
}

func hermesRunEventResult(raw string) (RunResult, bool) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || trimmed == "[DONE]" {
		return RunResult{}, false
	}
	var event hermesRunEvent
	if err := json.Unmarshal([]byte(trimmed), &event); err != nil {
		return RunResult{}, false
	}
	if len(event.Data) > 0 {
		var nested hermesRunEvent
		if err := json.Unmarshal(event.Data, &nested); err == nil {
			event = mergeHermesRunEvents(event, nested)
		}
	}
	status := strings.ToLower(strings.TrimSpace(firstNonEmpty(event.Status, event.Type, event.Event)))
	switch status {
	case "run.completed", "completed", "succeeded", "success":
		return RunResult{Status: RunStatusSucceeded, Output: strings.TrimSpace(event.Output)}, true
	case "run.failed", "failed", "error":
		return RunResult{Status: RunStatusFailed, Output: strings.TrimSpace(firstNonEmpty(event.Error, event.Output))}, true
	case "run.cancelled", "run.canceled", "cancelled", "canceled":
		return RunResult{Status: RunStatusCancelled}, true
	default:
		return RunResult{}, false
	}
}

func mergeHermesRunEvents(parent hermesRunEvent, child hermesRunEvent) hermesRunEvent {
	if strings.TrimSpace(parent.Status) == "" {
		parent.Status = child.Status
	}
	if strings.TrimSpace(parent.Output) == "" {
		parent.Output = child.Output
	}
	if strings.TrimSpace(parent.Error) == "" {
		parent.Error = child.Error
	}
	return parent
}

func (adapter HermesAdapter) runStatus(ctx context.Context, nativeRunID string) (RunResult, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, adapter.BaseURL+"/v1/runs/"+strings.TrimSpace(nativeRunID), nil)
	if err != nil {
		return RunResult{}, false, err
	}
	if adapter.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+adapter.APIKey)
	}
	resp, err := adapter.client().Do(req)
	if err != nil {
		return RunResult{}, false, fmt.Errorf("Hermes run status: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return RunResult{}, false, fmt.Errorf("Hermes run status %d", resp.StatusCode)
	}
	var decoded hermesRunStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return RunResult{}, false, err
	}
	switch strings.ToLower(strings.TrimSpace(decoded.Status)) {
	case "completed", "succeeded", "success":
		return RunResult{Status: RunStatusSucceeded, Output: strings.TrimSpace(decoded.Output)}, true, nil
	case "failed", "error":
		return RunResult{Status: RunStatusFailed, Output: strings.TrimSpace(firstNonEmpty(decoded.Error, decoded.Output))}, true, nil
	case "cancelled", "canceled":
		return RunResult{Status: RunStatusCancelled}, true, nil
	default:
		return RunResult{}, false, nil
	}
}

func (adapter HermesAdapter) waitRunStatus(ctx context.Context, nativeRunID string) (RunResult, error) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		result, terminal, err := adapter.runStatus(ctx, nativeRunID)
		if err != nil {
			return RunResult{}, err
		}
		if terminal {
			return result, nil
		}
		select {
		case <-ctx.Done():
			return RunResult{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

func (adapter HermesAdapter) CancelRun(nativeRunID string) error {
	if err := adapter.validateLoopbackBaseURL(); err != nil {
		return err
	}
	trimmedRunID := strings.TrimSpace(nativeRunID)
	if trimmedRunID == "" {
		return fmt.Errorf("native run id required")
	}
	req, err := http.NewRequest(http.MethodPost, adapter.BaseURL+"/v1/runs/"+strings.TrimSpace(nativeRunID)+"/stop", nil)
	if err != nil {
		return err
	}
	if adapter.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+adapter.APIKey)
	}
	resp, err := adapter.client().Do(req)
	if err != nil {
		return fmt.Errorf("Hermes stop: %w", err)
	}
	defer resp.Body.Close()
	unsupportedStop := resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusNotImplemented || resp.StatusCode == http.StatusMethodNotAllowed
	if resp.StatusCode >= 300 && !unsupportedStop {
		return fmt.Errorf("Hermes stop status %d", resp.StatusCode)
	}
	ctx, cancel := context.WithTimeout(context.Background(), defaultHermesCancelWait)
	defer cancel()
	_, err = adapter.waitRunStatus(ctx, trimmedRunID)
	if unsupportedStop && err != nil {
		return nil
	}
	if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

func (adapter HermesAdapter) Diagnose() Detection {
	return adapter.Detect()
}

func (adapter HermesAdapter) client() *http.Client {
	checkRedirect := func(req *http.Request, via []*http.Request) error {
		if err := validateHermesLoopbackURL(req.URL); err != nil {
			return err
		}
		return nil
	}
	if adapter.Client != nil {
		copied := *adapter.Client
		copied.CheckRedirect = checkRedirect
		return &copied
	}
	client := defaultHTTPClient()
	client.CheckRedirect = checkRedirect
	return client
}

func (adapter HermesAdapter) validateLoopbackBaseURL() error {
	parsed, err := url.Parse(strings.TrimSpace(adapter.BaseURL))
	if err != nil {
		return fmt.Errorf("parse Hermes API URL: %w", err)
	}
	return validateHermesLoopbackURL(parsed)
}

func validateHermesLoopbackURL(parsed *url.URL) error {
	if parsed.Scheme != "http" {
		return fmt.Errorf("Hermes API URL must use http loopback")
	}
	host := parsed.Hostname()
	if host == "localhost" {
		ips, err := net.LookupIP(host)
		if err != nil {
			return fmt.Errorf("resolve Hermes API host: %w", err)
		}
		for _, ip := range ips {
			if !ip.IsLoopback() {
				return fmt.Errorf("Hermes API URL must be loopback")
			}
		}
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("Hermes API URL must be loopback")
	}
	return nil
}

type hermesCapabilities struct {
	Features struct {
		RunSubmission bool `json:"run_submission"`
		RunStatus     bool `json:"run_status"`
		RunEventsSSE  bool `json:"run_events_sse"`
		RunStop       bool `json:"run_stop"`
	} `json:"features"`
}

func (capabilities hermesCapabilities) missingRequiredFeatures() []string {
	missing := []string{}
	if !capabilities.Features.RunSubmission {
		missing = append(missing, "run_submission")
	}
	if !capabilities.Features.RunStatus {
		missing = append(missing, "run_status")
	}
	return missing
}

func (capabilities hermesCapabilities) degradedFallbackFeatures() []string {
	missing := []string{}
	if !capabilities.Features.RunEventsSSE {
		missing = append(missing, "run_events_sse")
	}
	if !capabilities.Features.RunStop {
		missing = append(missing, "run_stop")
	}
	return missing
}

type hermesRunResponse struct {
	ID    string `json:"id"`
	RunID string `json:"run_id"`
}

type hermesRunEvent struct {
	Type   string          `json:"type"`
	Event  string          `json:"event"`
	Status string          `json:"status"`
	Output string          `json:"output"`
	Error  string          `json:"error"`
	Data   json.RawMessage `json:"data"`
}

type hermesRunStatusResponse struct {
	Status string `json:"status"`
	Output string `json:"output"`
	Error  string `json:"error"`
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
