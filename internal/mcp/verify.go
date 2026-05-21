package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/personastack/personastack-connector/internal/config"
)

type LiveVerifyResult struct {
	OK             bool
	Note           string
	DiagnosticCode string
}

func VerifyBindingLive(ctx context.Context, binding config.Binding, client *http.Client) LiveVerifyResult {
	mcpURL := strings.TrimSpace(binding.PersonaMCPURL)
	token := mcpTokenForBinding(binding)
	if mcpURL == "" || token == "" {
		return LiveVerifyResult{Note: "PersonaStack MCP credential missing", DiagnosticCode: "mcp_token_missing"}
	}
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	session := stdioProxySession{protocolVersion: defaultMCPProtocolVersion}
	proxy := StdioProxy{httpClient: client}
	initialize := []byte(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"` + defaultMCPProtocolVersion + `","capabilities":{},"clientInfo":{"name":"personastack-connector","version":"verify"}}}`)
	raw, err := proxy.forward(ctx, mcpURL, token, initialize, &session)
	if err != nil {
		return LiveVerifyResult{Note: "initialize failed: " + err.Error(), DiagnosticCode: diagnosticCodeForMCPLiveError(err)}
	}
	if err := requireJSONRPCResult(raw); err != nil {
		return LiveVerifyResult{Note: "initialize invalid: " + err.Error(), DiagnosticCode: "runtime_error"}
	}
	_, err = proxy.forward(ctx, mcpURL, token, []byte(`{"jsonrpc":"2.0","method":"notifications/initialized"}`), &session)
	if err != nil {
		return LiveVerifyResult{Note: "initialized notification failed: " + err.Error(), DiagnosticCode: diagnosticCodeForMCPLiveError(err)}
	}
	raw, err = proxy.forward(ctx, mcpURL, token, []byte(`{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}`), &session)
	if err != nil {
		return LiveVerifyResult{Note: "tools/list failed: " + err.Error(), DiagnosticCode: diagnosticCodeForMCPLiveError(err)}
	}
	if err := requireJSONRPCResult(raw); err != nil {
		return LiveVerifyResult{Note: "tools/list invalid: " + err.Error(), DiagnosticCode: "runtime_error"}
	}
	return LiveVerifyResult{OK: true, Note: "PersonaStack MCP endpoint verified"}
}

func diagnosticCodeForMCPLiveError(err error) string {
	text := strings.ToLower(err.Error())
	switch {
	case strings.Contains(text, "status 401"), strings.Contains(text, "status 403"):
		return "mcp_token_rejected"
	case strings.Contains(text, "connection refused"), strings.Contains(text, "no such host"), strings.Contains(text, "timeout"), strings.Contains(text, "deadline exceeded"), strings.Contains(text, "post mcp request"):
		return "mcp_endpoint_unreachable"
	default:
		return "runtime_error"
	}
}

func requireJSONRPCResult(raw []byte) error {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return fmt.Errorf("empty response")
	}
	var envelope rpcMessage
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return err
	}
	if len(envelope.Result) == 0 {
		return fmt.Errorf("missing result")
	}
	return nil
}
