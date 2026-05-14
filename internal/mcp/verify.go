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
	OK   bool
	Note string
}

func VerifyBindingLive(ctx context.Context, binding config.Binding, client *http.Client) LiveVerifyResult {
	mcpURL := strings.TrimSpace(binding.PersonaMCPURL)
	token := mcpTokenForBinding(binding)
	if mcpURL == "" || token == "" {
		return LiveVerifyResult{Note: "PersonaStack MCP credential missing"}
	}
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	session := stdioProxySession{protocolVersion: defaultMCPProtocolVersion}
	proxy := StdioProxy{httpClient: client}
	initialize := []byte(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"` + defaultMCPProtocolVersion + `","capabilities":{},"clientInfo":{"name":"personastack-connector","version":"verify"}}}`)
	raw, err := proxy.forward(ctx, mcpURL, token, initialize, &session)
	if err != nil {
		return LiveVerifyResult{Note: "initialize failed: " + err.Error()}
	}
	if err := requireJSONRPCResult(raw); err != nil {
		return LiveVerifyResult{Note: "initialize invalid: " + err.Error()}
	}
	_, err = proxy.forward(ctx, mcpURL, token, []byte(`{"jsonrpc":"2.0","method":"notifications/initialized"}`), &session)
	if err != nil {
		return LiveVerifyResult{Note: "initialized notification failed: " + err.Error()}
	}
	raw, err = proxy.forward(ctx, mcpURL, token, []byte(`{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}`), &session)
	if err != nil {
		return LiveVerifyResult{Note: "tools/list failed: " + err.Error()}
	}
	if err := requireJSONRPCResult(raw); err != nil {
		return LiveVerifyResult{Note: "tools/list invalid: " + err.Error()}
	}
	return LiveVerifyResult{OK: true, Note: "PersonaStack MCP endpoint verified"}
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
