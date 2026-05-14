package mcp

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/personastack/personastack-connector/internal/config"
)

var ErrMissingBinding = errors.New("missing binding")
var ErrMissingMCPToken = errors.New("missing persona mcp token")

type StdioProxy struct {
	store      config.Store
	httpClient *http.Client
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
	token := strings.TrimSpace(binding.PersonaMCPToken)
	if mcpURL == "" || token == "" {
		return fmt.Errorf("mcp stdio binding %q: %w", bindingID, ErrMissingMCPToken)
	}
	scanner := bufio.NewScanner(stdin)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		response, err := proxy.forward(ctx, mcpURL, token, line)
		if err != nil {
			_, _ = fmt.Fprintf(stderr, "PersonaStack MCP proxy error: %v\n", err)
			return err
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

func (proxy StdioProxy) forward(ctx context.Context, mcpURL string, token string, payload []byte) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, mcpURL, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("Authorization", "Bearer "+token)
	client := proxy.httpClient
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("post mcp request: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read mcp response: %w", err)
	}
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("mcp status %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	return raw, nil
}
