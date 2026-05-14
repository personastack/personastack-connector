package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

const defaultHermesURL = "http://127.0.0.1:8642"

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
	client := adapter.client()
	resp, err := client.Get(adapter.BaseURL + "/health")
	if err != nil {
		return Detection{Kind: AdapterKindHermes, State: AdapterStateRuntimeMissing, Note: err.Error()}
	}
	_ = resp.Body.Close()
	if resp.StatusCode >= 300 {
		return Detection{Kind: AdapterKindHermes, State: AdapterStateRuntimeStopped, Note: fmt.Sprintf("health status %d", resp.StatusCode)}
	}
	if adapter.APIKey == "" {
		return Detection{Kind: AdapterKindHermes, State: AdapterStateAuthMissing, Note: "HERMES_API_SERVER_KEY is required"}
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
	return Detection{Kind: AdapterKindHermes, State: AdapterStateReady, Note: "Hermes API ready"}
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

func (adapter HermesAdapter) StartRun(assignmentID string, fullyComposedPrompt string) (string, error) {
	body := map[string]string{
		"input":        fullyComposedPrompt,
		"conversation": assignmentID,
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
	if strings.TrimSpace(decoded.ID) == "" {
		return "", fmt.Errorf("Hermes response missing run id")
	}
	return strings.TrimSpace(decoded.ID), nil
}

func (adapter HermesAdapter) WaitRun(ctx context.Context, nativeRunID string) (RunResult, error) {
	trimmedRunID := strings.TrimSpace(nativeRunID)
	if trimmedRunID == "" {
		return RunResult{}, fmt.Errorf("native run id required")
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		result, terminal, err := adapter.runStatus(ctx, trimmedRunID)
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
		return RunResult{Status: RunStatusFailed, Output: strings.TrimSpace(decoded.Error)}, true, nil
	case "cancelled", "canceled":
		return RunResult{Status: RunStatusCancelled}, true, nil
	default:
		return RunResult{}, false, nil
	}
}

func (adapter HermesAdapter) CancelRun(nativeRunID string) error {
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
	if resp.StatusCode >= 300 {
		return fmt.Errorf("Hermes stop status %d", resp.StatusCode)
	}
	return nil
}

func (adapter HermesAdapter) Diagnose() Detection {
	return adapter.Detect()
}

func (adapter HermesAdapter) client() *http.Client {
	if adapter.Client != nil {
		return adapter.Client
	}
	return defaultHTTPClient()
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
	if !capabilities.Features.RunEventsSSE {
		missing = append(missing, "run_events_sse")
	}
	if !capabilities.Features.RunStop {
		missing = append(missing, "run_stop")
	}
	return missing
}

type hermesRunResponse struct {
	ID string `json:"id"`
}

type hermesRunStatusResponse struct {
	Status string `json:"status"`
	Output string `json:"output"`
	Error  string `json:"error"`
}
