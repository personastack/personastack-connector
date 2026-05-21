package mcp

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/personastack/personastack-connector/internal/config"
)

func ServeLoopbackHTTPProxy(ctx context.Context, binding config.Binding, client *http.Client) error {
	errs, err := StartLoopbackHTTPProxy(ctx, binding, client)
	if err != nil {
		return err
	}
	return <-errs
}

func StartLoopbackHTTPProxy(ctx context.Context, binding config.Binding, client *http.Client) (<-chan error, error) {
	return StartLoopbackHTTPProxyWithStore(ctx, nil, binding, client)
}

func StartLoopbackHTTPProxyWithStore(ctx context.Context, store config.Store, binding config.Binding, client *http.Client) (<-chan error, error) {
	localURL, token, err := loopbackProxyConfig(binding)
	if err != nil {
		return nil, err
	}
	parsed, err := url.Parse(localURL)
	if err != nil {
		return nil, fmt.Errorf("parse local mcp proxy url: %w", err)
	}
	if parsed.Scheme != "http" || parsed.Hostname() != "127.0.0.1" {
		return nil, fmt.Errorf("local mcp proxy url must use 127.0.0.1 http")
	}
	listener, err := net.Listen("tcp", parsed.Host)
	if err != nil {
		return nil, fmt.Errorf("listen local mcp proxy: %w", err)
	}
	if client == nil {
		client = http.DefaultClient
	}
	server := &http.Server{
		Handler: loopbackHTTPProxyHandler{
			binding:    binding,
			store:      store,
			localPath:  parsed.EscapedPath(),
			localToken: token,
			client:     client,
		},
		ReadHeaderTimeout: 10 * time.Second,
	}
	errs := make(chan error, 1)
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	go func() {
		err := server.Serve(listener)
		if err == http.ErrServerClosed {
			err = nil
		}
		errs <- err
		close(errs)
	}()
	return errs, nil
}

func VerifyLoopbackHTTPProxy(ctx context.Context, binding config.Binding, client *http.Client) LiveVerifyResult {
	localURL, token, err := loopbackProxyConfig(binding)
	if err != nil {
		return LiveVerifyResult{Note: err.Error()}
	}
	localBinding := binding
	localBinding.PersonaMCPURL = localURL
	localBinding.PersonaMCPToken = token
	return VerifyBindingLive(ctx, localBinding, client)
}

func loopbackProxyConfig(binding config.Binding) (string, string, error) {
	localURL := strings.TrimSpace(binding.LocalMCPProxyURL)
	token := strings.TrimSpace(binding.LocalMCPProxyToken)
	if localURL == "" || token == "" {
		return "", "", fmt.Errorf("local mcp proxy url/token missing")
	}
	return localURL, token, nil
}

type loopbackHTTPProxyHandler struct {
	binding    config.Binding
	store      config.Store
	localPath  string
	localToken string
	client     *http.Client
}

func (handler loopbackHTTPProxyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.EscapedPath() != handler.localPath {
		http.NotFound(w, r)
		return
	}
	binding := handler.currentBinding()
	localToken := firstNonEmpty(handler.localMCPProxyToken(binding), handler.localToken)
	if r.Header.Get("Authorization") != "Bearer "+localToken {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if r.Method != http.MethodPost && r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	remoteURL := strings.TrimSpace(binding.PersonaMCPURL)
	remoteToken := mcpTokenForBinding(binding)
	if remoteURL == "" || remoteToken == "" {
		http.Error(w, "PersonaStack MCP credential missing", http.StatusBadGateway)
		return
	}
	req, err := http.NewRequestWithContext(r.Context(), r.Method, remoteURL, r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	copyMCPProxyHeaders(req.Header, r.Header)
	req.Header.Set("Authorization", "Bearer "+remoteToken)
	resp, err := handler.client.Do(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	copyMCPProxyHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func (handler loopbackHTTPProxyHandler) currentBinding() config.Binding {
	if handler.store == nil {
		return handler.binding
	}
	binding, ok := handler.store.Binding(handler.binding.ConnectionID)
	if !ok {
		return handler.binding
	}
	return binding
}

func (handler loopbackHTTPProxyHandler) localMCPProxyToken(binding config.Binding) string {
	if strings.TrimSpace(binding.LocalMCPProxyURL) != strings.TrimSpace(handler.binding.LocalMCPProxyURL) {
		return ""
	}
	return strings.TrimSpace(binding.LocalMCPProxyToken)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func copyMCPProxyHeaders(dst http.Header, src http.Header) {
	for _, key := range []string{"Accept", "Content-Type", "MCP-Protocol-Version", "MCP-Session-Id", "Last-Event-ID"} {
		values := src.Values(key)
		if len(values) == 0 {
			continue
		}
		dst.Del(key)
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}
