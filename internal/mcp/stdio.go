package mcp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/personastack/personastack-connector/internal/config"
)

var ErrMissingBinding = errors.New("missing binding")
var ErrMissingMCPToken = errors.New("missing persona mcp token")

const defaultMCPProtocolVersion = "2025-11-25"

type StdioProxy struct {
	store      config.Store
	httpClient *http.Client
}

type stdioProxySession struct {
	sessionID       string
	protocolVersion string
	initialized     bool
}

type rpcMessage struct {
	ID     json.RawMessage `json:"id,omitempty"`
	Method string          `json:"method,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
}

type initializeResult struct {
	ProtocolVersion string `json:"protocolVersion"`
}

func NewStdioProxy(store config.Store) StdioProxy {
	return StdioProxy{store: store}
}

func (proxy StdioProxy) Serve(ctx context.Context, bindingID config.ConnectionID, stdin io.Reader, stdout io.Writer, stderr io.Writer) error {
	binding, ok := proxy.store.Binding(bindingID)
	if !ok {
		return fmt.Errorf("mcp stdio binding %q: %w", bindingID, ErrMissingBinding)
	}
	mcpURL := strings.TrimSpace(binding.PersonaMCPURL)
	token := mcpTokenForBinding(binding)
	if mcpURL == "" || token == "" {
		return fmt.Errorf("mcp stdio binding %q: %w", bindingID, ErrMissingMCPToken)
	}
	session := stdioProxySession{protocolVersion: defaultMCPProtocolVersion}
	scanner := bufio.NewScanner(stdin)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		response, err := proxy.forward(ctx, mcpURL, token, line, &session)
		if err != nil {
			_, _ = fmt.Fprintf(stderr, "PersonaStack MCP proxy error: %v\n", err)
			return err
		}
		if len(response) == 0 {
			continue
		}
		if _, err := stdout.Write(response); err != nil {
			return fmt.Errorf("write mcp response: %w", err)
		}
		if len(response) == 0 || response[len(response)-1] != '\n' {
			if _, err := stdout.Write([]byte("\n")); err != nil {
				return fmt.Errorf("write mcp response newline: %w", err)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read mcp stdio: %w", err)
	}
	return nil
}

func mcpTokenForBinding(binding config.Binding) string {
	if token := strings.TrimSpace(binding.ActiveRunMCPToken); token != "" {
		return token
	}
	return strings.TrimSpace(binding.PersonaMCPToken)
}

func (proxy StdioProxy) forward(ctx context.Context, mcpURL string, token string, payload []byte, session *stdioProxySession) ([]byte, error) {
	var message rpcMessage
	if err := json.Unmarshal(payload, &message); err != nil {
		return nil, fmt.Errorf("decode mcp stdio message: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, mcpURL, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("Authorization", "Bearer "+token)
	if session.sessionID != "" {
		req.Header.Set("MCP-Session-Id", session.sessionID)
	}
	if session.initialized || session.sessionID != "" {
		req.Header.Set("MCP-Protocol-Version", session.protocolVersion)
	}
	client := proxy.httpClient
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("post mcp request: %w", err)
	}
	defer resp.Body.Close()
	raw, err := readMCPHTTPResponse(resp)
	if err != nil {
		return nil, fmt.Errorf("read mcp response: %w", err)
	}
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("mcp status %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	session.updateFromResponseHeaders(resp.Header)
	session.updateFromMessages(message, raw)
	return raw, nil
}

func readMCPHTTPResponse(resp *http.Response) ([]byte, error) {
	if strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "text/event-stream") {
		return readSSEJSONPayloads(resp.Body)
	}
	return io.ReadAll(resp.Body)
}

func readSSEJSONPayloads(body io.Reader) ([]byte, error) {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	var output bytes.Buffer
	var eventData []string
	flush := func() bool {
		if len(eventData) == 0 {
			return false
		}
		if output.Len() > 0 {
			output.WriteByte('\n')
		}
		output.WriteString(strings.Join(eventData, "\n"))
		eventData = nil
		return true
	}
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if flush() {
				return output.Bytes(), nil
			}
			continue
		}
		if strings.HasPrefix(line, "data:") {
			eventData = append(eventData, strings.TrimSpace(strings.TrimPrefix(line, "data:")))
		}
	}
	flush()
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}

func (session *stdioProxySession) updateFromResponseHeaders(headers http.Header) {
	sessionID := strings.TrimSpace(headers.Get("MCP-Session-Id"))
	if sessionID != "" {
		session.sessionID = sessionID
	}
}

func (session *stdioProxySession) updateFromMessages(request rpcMessage, response []byte) {
	if request.Method == "notifications/initialized" {
		session.initialized = true
		return
	}
	if request.Method != "initialize" || len(response) == 0 {
		return
	}
	var envelope rpcMessage
	if err := json.Unmarshal(response, &envelope); err != nil {
		return
	}
	var result initializeResult
	if err := json.Unmarshal(envelope.Result, &result); err != nil {
		return
	}
	if strings.TrimSpace(result.ProtocolVersion) != "" {
		session.protocolVersion = strings.TrimSpace(result.ProtocolVersion)
	}
}
