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
	"sync"
	"time"

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

type lockedLineWriter struct {
	mu     sync.Mutex
	writer io.Writer
}

type mcpGetStream struct {
	mu          sync.Mutex
	started     bool
	lastEventID string
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
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
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
	stream := &mcpGetStream{}
	output := &lockedLineWriter{writer: stdout}
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
		if session.readyForGetStream() {
			stream.start(ctx, proxy.httpClientOrDefault(), mcpURL, token, session, output, stderr)
		}
		if len(response) == 0 {
			continue
		}
		if err := output.writeLine(response); err != nil {
			return err
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

func (proxy StdioProxy) httpClientOrDefault() *http.Client {
	if proxy.httpClient != nil {
		return proxy.httpClient
	}
	return http.DefaultClient
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
	resp, err := proxy.httpClientOrDefault().Do(req)
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

func (writer *lockedLineWriter) writeLine(payload []byte) error {
	if writer == nil || writer.writer == nil || len(payload) == 0 {
		return nil
	}
	writer.mu.Lock()
	defer writer.mu.Unlock()
	if _, err := writer.writer.Write(payload); err != nil {
		return fmt.Errorf("write mcp response: %w", err)
	}
	if payload[len(payload)-1] != '\n' {
		if _, err := writer.writer.Write([]byte("\n")); err != nil {
			return fmt.Errorf("write mcp response newline: %w", err)
		}
	}
	return nil
}

func (stream *mcpGetStream) start(ctx context.Context, client *http.Client, mcpURL string, token string, session stdioProxySession, output *lockedLineWriter, stderr io.Writer) {
	if stream == nil || client == nil || output == nil {
		return
	}
	stream.mu.Lock()
	if stream.started {
		stream.mu.Unlock()
		return
	}
	stream.started = true
	stream.mu.Unlock()
	go stream.run(ctx, client, mcpURL, token, session, output, stderr)
}

func (stream *mcpGetStream) run(ctx context.Context, client *http.Client, mcpURL string, token string, session stdioProxySession, output *lockedLineWriter, stderr io.Writer) {
	for ctx.Err() == nil {
		err := stream.once(ctx, client, mcpURL, token, session, output)
		if ctx.Err() != nil {
			return
		}
		if errors.Is(err, errMCPGetStreamUnsupported) {
			return
		}
		if err != nil && stderr != nil {
			_, _ = fmt.Fprintf(stderr, "PersonaStack MCP stream warning: %v\n", err)
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

var errMCPGetStreamUnsupported = errors.New("mcp get stream unsupported")

func (stream *mcpGetStream) once(ctx context.Context, client *http.Client, mcpURL string, token string, session stdioProxySession, output *lockedLineWriter) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, mcpURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("MCP-Session-Id", session.sessionID)
	req.Header.Set("MCP-Protocol-Version", session.protocolVersion)
	if lastEventID := stream.currentLastEventID(); lastEventID != "" {
		req.Header.Set("Last-Event-ID", lastEventID)
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("open mcp get stream: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusMethodNotAllowed || resp.StatusCode == http.StatusNotAcceptable || resp.StatusCode == http.StatusNotImplemented {
		return errMCPGetStreamUnsupported
	}
	if resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("mcp get stream status %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	if !strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "text/event-stream") {
		return errMCPGetStreamUnsupported
	}
	return readSSEStream(resp.Body, func(event sseEvent) error {
		if event.ID != "" {
			stream.setLastEventID(event.ID)
		}
		payload := event.dataPayload()
		if !json.Valid(payload) {
			return nil
		}
		return output.writeLine(payload)
	})
}

func (stream *mcpGetStream) currentLastEventID() string {
	stream.mu.Lock()
	defer stream.mu.Unlock()
	return stream.lastEventID
}

func (stream *mcpGetStream) setLastEventID(value string) {
	stream.mu.Lock()
	defer stream.mu.Unlock()
	stream.lastEventID = strings.TrimSpace(value)
}

type sseEvent struct {
	ID   string
	Data []string
}

func (event sseEvent) dataPayload() []byte {
	return []byte(strings.Join(event.Data, "\n"))
}

func readSSEJSONPayloads(body io.Reader) ([]byte, error) {
	var output bytes.Buffer
	err := readSSEStream(body, func(event sseEvent) error {
		if output.Len() > 0 {
			output.WriteByte('\n')
		}
		output.Write(event.dataPayload())
		return io.EOF
	})
	if errors.Is(err, io.EOF) {
		return output.Bytes(), nil
	}
	return output.Bytes(), err
}

func readSSEStream(body io.Reader, handle func(sseEvent) error) error {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	var event sseEvent
	var eventData []string
	flush := func() error {
		if len(eventData) == 0 {
			return nil
		}
		event.Data = eventData
		eventData = nil
		err := handle(event)
		event = sseEvent{}
		return err
	}
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if err := flush(); err != nil {
				return err
			}
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		if strings.HasPrefix(line, "id:") {
			event.ID = strings.TrimSpace(strings.TrimPrefix(line, "id:"))
			continue
		}
		if strings.HasPrefix(line, "data:") {
			eventData = append(eventData, strings.TrimSpace(strings.TrimPrefix(line, "data:")))
		}
	}
	if err := flush(); err != nil {
		return err
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

func (session stdioProxySession) readyForGetStream() bool {
	return session.initialized && strings.TrimSpace(session.sessionID) != ""
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
