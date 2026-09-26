package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/personastack/personastack-connector/internal/config"
)

func TestStdioProxyForwardsJSONLines(t *testing.T) {
	var authHeader string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{}}`))
	}))
	defer server.Close()
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{{
		ConnectionID:    "conn-1",
		PersonaMCPURL:   server.URL,
		PersonaMCPToken: "token-1",
	}}})
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := NewStdioProxy(store).Serve(context.Background(), "conn-1", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"initialize"}`+"\n"), &stdout, &stderr)
	if err != nil {
		t.Fatalf("Serve() error = %v stderr=%s", err, stderr.String())
	}
	if authHeader != "Bearer token-1" {
		t.Fatalf("auth header = %q", authHeader)
	}
	if !strings.Contains(stdout.String(), `"result"`) {
		t.Fatalf("unexpected stdout: %s", stdout.String())
	}
}

func TestStdioProxyForwardContractFencesRejectedInputsAndCancellation(t *testing.T) {
	t.Parallel()

	t.Run("maps canonical request", func(t *testing.T) {
		t.Parallel()
		proxy := StdioProxy{httpClient: &http.Client{Transport: verifyContractRoundTripper(func(request *http.Request) (*http.Response, error) {
			if request.Method != http.MethodPost || request.URL.String() != "https://mcp.example.test/mcp" {
				t.Fatalf("request = %s %s", request.Method, request.URL)
			}
			if request.Header.Get("Authorization") != "Bearer durable-token" || request.Header.Get("Content-Type") != "application/json" || request.Header.Get("Accept") != "application/json, text/event-stream" {
				t.Fatalf("request headers = %+v", request.Header)
			}
			if request.Header.Get("MCP-Session-Id") != "session-1" || request.Header.Get("MCP-Protocol-Version") != "2025-11-25" {
				t.Fatalf("session headers = %+v", request.Header)
			}
			body, err := io.ReadAll(request.Body)
			if err != nil {
				t.Fatalf("read request body: %v", err)
			}
			if !jsonRawMessagesEqual(body, []byte(`{"jsonrpc":"2.0","id":7,"method":"tools/list","params":{}}`)) {
				t.Fatalf("request body = %s", body)
			}
			return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"jsonrpc":"2.0","id":7,"result":{"tools":[]}}`)), Request: request}, nil
		})}}
		session := &stdioProxySession{sessionID: "session-1", protocolVersion: defaultMCPProtocolVersion, initialized: true}
		response, err := proxy.forward(t.Context(), "https://mcp.example.test/mcp", "durable-token", []byte(`{"jsonrpc":"2.0","id":7,"method":"tools/list","params":{}}`), session, nil)
		if err != nil || !jsonRawMessagesEqual(response, []byte(`{"jsonrpc":"2.0","id":7,"result":{"tools":[]}}`)) {
			t.Fatalf("forward response = %s, error = %v", response, err)
		}
	})

	for _, testCase := range []struct {
		name string
		ctx  context.Context
		body []byte
	}{
		{name: "malformed JSON", ctx: t.Context(), body: []byte(`{"jsonrpc":`)},
		{name: "cancelled context", ctx: cancelledContext(t), body: []byte(`{"jsonrpc":"2.0","id":7,"method":"tools/list"}`)},
	} {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			proxy := StdioProxy{httpClient: &http.Client{Transport: verifyContractRoundTripper(func(request *http.Request) (*http.Response, error) {
				t.Fatalf("unexpected request: %s %s", request.Method, request.URL)
				return nil, nil
			})}}
			_, err := proxy.forward(testCase.ctx, "https://mcp.example.test/mcp", "durable-token", testCase.body, &stdioProxySession{protocolVersion: defaultMCPProtocolVersion}, nil)
			if err == nil {
				t.Fatal("expected rejected forward")
			}
		})
	}
}

func TestMCPGetStreamOnceCancelledContextMakesNoHTTPRequest(t *testing.T) {
	t.Parallel()

	stream := &mcpGetStream{}
	err := stream.once(cancelledContext(t), &http.Client{Transport: verifyContractRoundTripper(func(request *http.Request) (*http.Response, error) {
		t.Fatalf("unexpected request: %s %s", request.Method, request.URL)
		return nil, nil
	})}, "https://mcp.example.test/mcp", "durable-token", stdioProxySession{sessionID: "session-1", protocolVersion: defaultMCPProtocolVersion}, &lockedLineWriter{writer: io.Discard})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("once() error = %v, want context canceled", err)
	}
}

func cancelledContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	return ctx
}

func TestStdioProxyUsesDurablePersonaToken(t *testing.T) {
	var authHeader string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{}}`))
	}))
	defer server.Close()
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{{
		ConnectionID:    "conn-1",
		PersonaMCPURL:   server.URL,
		PersonaMCPToken: "stable-token",
		ActiveRunID:     "run-1",
	}}})
	err := NewStdioProxy(store).Serve(context.Background(), "conn-1", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`+"\n"), &bytes.Buffer{}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("Serve() error = %v", err)
	}
	if authHeader != "Bearer stable-token" {
		t.Fatalf("auth header = %q", authHeader)
	}
}

func TestStdioProxyCarriesMCPSessionHeaders(t *testing.T) {
	var initializedSessionHeader string
	var initializedProtocolHeader string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			w.Header().Set("Content-Type", "text/event-stream")
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
			<-r.Context().Done()
			return
		}
		var message rpcMessage
		if err := json.NewDecoder(r.Body).Decode(&message); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		switch message.Method {
		case "initialize":
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("MCP-Session-Id", "session-1")
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-11-25","capabilities":{},"serverInfo":{"name":"personastack","version":"test"}}}`))
		case "notifications/initialized":
			initializedSessionHeader = r.Header.Get("MCP-Session-Id")
			initializedProtocolHeader = r.Header.Get("MCP-Protocol-Version")
			w.WriteHeader(http.StatusAccepted)
		default:
			t.Fatalf("unexpected method: %s", message.Method)
		}
	}))
	defer server.Close()
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{{
		ConnectionID:    "conn-1",
		PersonaMCPURL:   server.URL,
		PersonaMCPToken: "token-1",
	}}})
	input := strings.Join([]string{
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`,
		`{"jsonrpc":"2.0","method":"notifications/initialized"}`,
		"",
	}, "\n")
	var stdout bytes.Buffer
	err := NewStdioProxy(store).Serve(context.Background(), "conn-1", strings.NewReader(input), &stdout, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("Serve() error = %v", err)
	}
	if initializedSessionHeader != "session-1" {
		t.Fatalf("MCP-Session-Id = %q", initializedSessionHeader)
	}
	if initializedProtocolHeader != "2025-11-25" {
		t.Fatalf("MCP-Protocol-Version = %q", initializedProtocolHeader)
	}
	if strings.Count(stdout.String(), "\n") != 1 {
		t.Fatalf("notification should not emit an empty stdio response, stdout=%q", stdout.String())
	}
}

func TestStdioProxyDecodesSSEJSONPayload(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: message\n"))
		_, _ = w.Write([]byte(`data: {"jsonrpc":"2.0","id":1,"result":{"ok":true}}` + "\n\n"))
	}))
	defer server.Close()
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{{
		ConnectionID:    "conn-1",
		PersonaMCPURL:   server.URL,
		PersonaMCPToken: "token-1",
	}}})
	var stdout bytes.Buffer
	err := NewStdioProxy(store).Serve(context.Background(), "conn-1", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`+"\n"), &stdout, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("Serve() error = %v", err)
	}
	if !strings.Contains(stdout.String(), `"ok":true`) {
		t.Fatalf("unexpected stdout: %s", stdout.String())
	}
}

func TestStdioProxySkipsReadinessSSEBeforeJSONRPCPayload(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: ready\n"))
		_, _ = w.Write([]byte("data: connected\n\n"))
		_, _ = w.Write([]byte("event: message\n"))
		_, _ = w.Write([]byte(`data: {"jsonrpc":"2.0","id":1,"result":{"ok":true}}` + "\n\n"))
	}))
	defer server.Close()
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{{
		ConnectionID:    "conn-1",
		PersonaMCPURL:   server.URL,
		PersonaMCPToken: "token-1",
	}}})
	var stdout bytes.Buffer
	err := NewStdioProxy(store).Serve(context.Background(), "conn-1", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`+"\n"), &stdout, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("Serve() error = %v", err)
	}
	if strings.Contains(stdout.String(), "connected") || !strings.Contains(stdout.String(), `"ok":true`) {
		t.Fatalf("unexpected stdout: %s", stdout.String())
	}
}

func TestStdioProxySkipsJSONRPCSSEWithDifferentRequestID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: message\n"))
		_, _ = w.Write([]byte(`data: {"jsonrpc":"2.0","method":"notifications/progress","params":{"message":"working"}}` + "\n\n"))
		_, _ = w.Write([]byte("event: message\n"))
		_, _ = w.Write([]byte(`data: {"jsonrpc":"2.0","id":99,"result":{"wrong":true}}` + "\n\n"))
		_, _ = w.Write([]byte("event: message\n"))
		_, _ = w.Write([]byte(`data: {"jsonrpc":"2.0","id":1,"result":{"ok":true}}` + "\n\n"))
	}))
	defer server.Close()
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{{
		ConnectionID:    "conn-1",
		PersonaMCPURL:   server.URL,
		PersonaMCPToken: "token-1",
	}}})
	var stdout bytes.Buffer
	err := NewStdioProxy(store).Serve(context.Background(), "conn-1", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`+"\n"), &stdout, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("Serve() error = %v", err)
	}
	if strings.Contains(stdout.String(), `"wrong":true`) || !strings.Contains(stdout.String(), `"ok":true`) {
		t.Fatalf("unexpected stdout: %s", stdout.String())
	}
}

type stdioLineCapture struct {
	pending string
	lines   chan string
}

func (capture *stdioLineCapture) Write(payload []byte) (int, error) {
	capture.pending += string(payload)
	for {
		line, rest, ok := strings.Cut(capture.pending, "\n")
		if !ok {
			break
		}
		capture.pending = rest
		capture.lines <- line
	}
	return len(payload), nil
}

func TestStdioProxyForwardsMatchingToolProgressBeforeFinalResponse(t *testing.T) {
	t.Parallel()
	finalGate := make(chan struct{})
	var releaseFinal sync.Once
	defer releaseFinal.Do(func() { close(finalGate) })
	streamReader, streamWriter := io.Pipe()
	defer streamReader.Close()
	transport := verifyContractRoundTripper(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodPost {
			return nil, fmt.Errorf("unexpected method: %s", request.Method)
		}
		go func() {
			for _, event := range []string{
				`{"jsonrpc":"2.0","id":99,"result":{"other":true}}`,
				`{"jsonrpc":"2.0","method":"notifications/progress","params":{"progressToken":"other-call","message":"other progress"}}`,
				`{"jsonrpc":"2.0","method":"notifications/progress","params":{"progressToken":"our-call","message":"working"}}`,
			} {
				_, _ = io.WriteString(streamWriter, "event: message\ndata: "+event+"\n\n")
			}
			<-finalGate
			_, _ = io.WriteString(streamWriter, "event: message\ndata: "+`{"jsonrpc":"2.0","id":7,"result":{"ok":true}}`+"\n\n")
			_ = streamWriter.Close()
		}()
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: streamReader, Request: request}, nil
	})
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{{
		ConnectionID: "conn-1", PersonaMCPURL: "https://mcp.example.test/mcp", PersonaMCPToken: "token-1",
	}}})
	proxy := NewStdioProxy(store)
	proxy.httpClient = &http.Client{Transport: transport}
	capture := &stdioLineCapture{lines: make(chan string, 4)}
	done := make(chan error, 1)
	go func() {
		done <- proxy.Serve(t.Context(), "conn-1", strings.NewReader(`{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"desktop_control_shell","_meta":{"progressToken":"our-call"}}}`+"\n"), capture, io.Discard)
	}()
	select {
	case line := <-capture.lines:
		if !strings.Contains(line, `"message":"working"`) || strings.Contains(line, "other") {
			t.Fatalf("first stdio line = %s", line)
		}
	case err := <-done:
		t.Fatalf("Serve() finished before progress: %v", err)
	case <-time.After(time.Second):
		t.Fatal("matching progress was not sent before final response")
	}
	if len(capture.lines) != 0 {
		t.Fatalf("unexpected extra stdio lines before final response: %d", len(capture.lines))
	}
	releaseFinal.Do(func() { close(finalGate) })
	select {
	case line := <-capture.lines:
		if !strings.Contains(line, `"ok":true`) || strings.Contains(line, "other") {
			t.Fatalf("final stdio line = %s", line)
		}
	case <-time.After(time.Second):
		t.Fatal("final response not sent")
	}
	if err := <-done; err != nil {
		t.Fatalf("Serve() error = %v", err)
	}
}

func TestStdioProxyDoesNotForwardProgressFromFailedHTTPResponse(t *testing.T) {
	t.Parallel()
	transport := verifyContractRoundTripper(func(request *http.Request) (*http.Response, error) {
		body := strings.Join([]string{
			"event: message",
			`data: {"jsonrpc":"2.0","method":"notifications/progress","params":{"progressToken":"our-call","message":"must stay private"}}`,
			"",
			"event: message",
			`data: {"jsonrpc":"2.0","id":7,"error":{"code":-32000,"message":"rejected"}}`,
			"",
			"",
		}, "\n")
		return &http.Response{StatusCode: http.StatusForbidden, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
	})
	proxy := StdioProxy{httpClient: &http.Client{Transport: transport}}
	capture := &stdioLineCapture{lines: make(chan string, 2)}
	_, err := proxy.forward(t.Context(), "https://mcp.example.test/mcp", "token-1", []byte(`{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"_meta":{"progressToken":"our-call"}}}`), &stdioProxySession{}, &lockedLineWriter{writer: capture})
	if err == nil || !strings.Contains(err.Error(), "mcp status 403") {
		t.Fatalf("forward() error = %v", err)
	}
	if len(capture.lines) != 0 {
		t.Fatalf("failed HTTP response forwarded %d progress lines", len(capture.lines))
	}
}

func TestStdioProxyFailsWhenSSEEndsWithoutJSONRPCPayload(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: ready\n"))
		_, _ = w.Write([]byte("data: connected\n\n"))
	}))
	defer server.Close()
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{{
		ConnectionID:    "conn-1",
		PersonaMCPURL:   server.URL,
		PersonaMCPToken: "token-1",
	}}})
	err := NewStdioProxy(store).Serve(context.Background(), "conn-1", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`+"\n"), &bytes.Buffer{}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "without JSON-RPC event") {
		t.Fatalf("Serve() error = %v", err)
	}
}

func TestStdioProxyIgnoresNonJSONRPCMessageEvents(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: message\n"))
		_, _ = w.Write([]byte("data: connected\n\n"))
		_, _ = w.Write([]byte("event: message\n"))
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
		_, _ = w.Write([]byte("event: message\n"))
		_, _ = w.Write([]byte(`data: {"type":"ready"}` + "\n\n"))
		_, _ = w.Write([]byte("event: message\n"))
		_, _ = w.Write([]byte(`data: {"jsonrpc":"2.0","id":1,"result":{"ok":true}}` + "\n\n"))
	}))
	defer server.Close()
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{{
		ConnectionID:    "conn-1",
		PersonaMCPURL:   server.URL,
		PersonaMCPToken: "token-1",
	}}})
	var stdout bytes.Buffer
	err := NewStdioProxy(store).Serve(context.Background(), "conn-1", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`+"\n"), &stdout, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("Serve() error = %v", err)
	}
	if strings.Contains(stdout.String(), "connected") || !strings.Contains(stdout.String(), `"ok":true`) {
		t.Fatalf("unexpected stdout: %s", stdout.String())
	}
}

func TestStdioProxyFailsMalformedJSONRPCSSEEvent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: message\n"))
		_, _ = w.Write([]byte(`data: {"jsonrpc":` + "\n\n"))
	}))
	defer server.Close()
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{{
		ConnectionID:    "conn-1",
		PersonaMCPURL:   server.URL,
		PersonaMCPToken: "token-1",
	}}})
	err := NewStdioProxy(store).Serve(context.Background(), "conn-1", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`+"\n"), &bytes.Buffer{}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "malformed JSON-RPC-looking payload") {
		t.Fatalf("Serve() error = %v", err)
	}
}

func TestStdioProxyReturnsAfterFirstLongLivedSSEEvent(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: message\n"))
		_, _ = w.Write([]byte(`data: {"jsonrpc":"2.0","id":1,"result":{"ok":true}}` + "\n\n"))
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		<-release
	}))
	defer func() {
		close(release)
		server.Close()
	}()
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{{
		ConnectionID:    "conn-1",
		PersonaMCPURL:   server.URL,
		PersonaMCPToken: "token-1",
	}}})
	done := make(chan error, 1)
	go func() {
		var stdout bytes.Buffer
		err := NewStdioProxy(store).Serve(context.Background(), "conn-1", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`+"\n"), &stdout, &bytes.Buffer{})
		if err == nil && !strings.Contains(stdout.String(), `"ok":true`) {
			err = errors.New("missing first SSE payload")
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("Serve() did not return after first SSE event")
	}
}

func TestStdioProxyReturnsAfterFirstLongLivedCRLFSSEEvent(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: message\r\n"))
		_, _ = w.Write([]byte(`data: {"jsonrpc":"2.0","id":1,"result":{"ok":true}}` + "\r\n\r\n"))
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		<-release
	}))
	defer func() {
		close(release)
		server.Close()
	}()
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{{
		ConnectionID:    "conn-1",
		PersonaMCPURL:   server.URL,
		PersonaMCPToken: "token-1",
	}}})
	done := make(chan error, 1)
	go func() {
		var stdout bytes.Buffer
		err := NewStdioProxy(store).Serve(context.Background(), "conn-1", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`+"\n"), &stdout, &bytes.Buffer{})
		if err == nil && !strings.Contains(stdout.String(), `"ok":true`) {
			err = errors.New("missing first SSE payload")
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("Serve() did not return after first CRLF SSE event")
	}
}

func TestStdioProxyBridgesGetStreamEventsToStdout(t *testing.T) {
	release := make(chan struct{})
	streamSent := make(chan struct{})
	var getSessionHeader string
	var getProtocolHeader string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			getSessionHeader = r.Header.Get("MCP-Session-Id")
			getProtocolHeader = r.Header.Get("MCP-Protocol-Version")
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("event: ready\n"))
			_, _ = w.Write([]byte("data: connected\n\n"))
			_, _ = w.Write([]byte("id: event-1\n"))
			_, _ = w.Write([]byte("event: message\n"))
			_, _ = w.Write([]byte(`data: {"jsonrpc":"2.0","method":"notifications/progress","params":{"message":"working"}}` + "\n\n"))
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
			close(streamSent)
			select {
			case <-release:
			case <-r.Context().Done():
			}
			return
		}
		var message rpcMessage
		if err := json.NewDecoder(r.Body).Decode(&message); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		switch message.Method {
		case "initialize":
			w.Header().Set("MCP-Session-Id", "session-1")
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-11-25","capabilities":{}}}`))
		case "notifications/initialized":
			w.WriteHeader(http.StatusAccepted)
		case "tools/list":
			select {
			case <-streamSent:
			case <-time.After(time.Second):
				t.Fatalf("GET stream did not open")
			}
			time.Sleep(50 * time.Millisecond)
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":2,"result":{"tools":[]}}`))
		default:
			t.Fatalf("unexpected method: %s", message.Method)
		}
	}))
	defer func() {
		close(release)
		server.Close()
	}()
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{{
		ConnectionID:    "conn-1",
		PersonaMCPURL:   server.URL,
		PersonaMCPToken: "token-1",
	}}})
	input := strings.Join([]string{
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`,
		`{"jsonrpc":"2.0","method":"notifications/initialized"}`,
		`{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}`,
		"",
	}, "\n")
	var stdout bytes.Buffer
	err := NewStdioProxy(store).Serve(context.Background(), "conn-1", strings.NewReader(input), &stdout, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("Serve() error = %v", err)
	}
	if getSessionHeader != "session-1" || getProtocolHeader != "2025-11-25" {
		t.Fatalf("GET stream headers session=%q protocol=%q", getSessionHeader, getProtocolHeader)
	}
	if !strings.Contains(stdout.String(), `"notifications/progress"`) || strings.Contains(stdout.String(), "connected") {
		t.Fatalf("unexpected stdout: %s", stdout.String())
	}
}

func TestVerifyBindingLiveChecksInitializeAndToolsList(t *testing.T) {
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var message rpcMessage
		if err := json.NewDecoder(r.Body).Decode(&message); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		methods = append(methods, message.Method)
		w.Header().Set("Content-Type", "application/json")
		switch message.Method {
		case "initialize":
			w.Header().Set("MCP-Session-Id", "session-1")
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-11-25","capabilities":{}}}`))
		case "notifications/initialized":
			w.WriteHeader(http.StatusAccepted)
		case "tools/list":
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":2,"result":{"tools":[]}}`))
		default:
			t.Fatalf("unexpected method: %s", message.Method)
		}
	}))
	defer server.Close()
	result := VerifyBindingLive(context.Background(), config.Binding{
		ConnectionID:    "conn-1",
		PersonaMCPURL:   server.URL,
		PersonaMCPToken: "token-1",
	}, server.Client())
	if !result.OK {
		t.Fatalf("VerifyBindingLive() = %+v", result)
	}
	if strings.Join(methods, ",") != "initialize,notifications/initialized,tools/list" {
		t.Fatalf("methods = %v", methods)
	}
}

func TestVerifyBindingLiveRejectsBadToolsListResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var message rpcMessage
		if err := json.NewDecoder(r.Body).Decode(&message); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		switch message.Method {
		case "initialize":
			w.Header().Set("MCP-Session-Id", "session-1")
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-11-25","capabilities":{}}}`))
		case "notifications/initialized":
			w.WriteHeader(http.StatusAccepted)
		case "tools/list":
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":2}`))
		default:
			t.Fatalf("unexpected method: %s", message.Method)
		}
	}))
	defer server.Close()
	result := VerifyBindingLive(context.Background(), config.Binding{
		ConnectionID:    "conn-1",
		PersonaMCPURL:   server.URL,
		PersonaMCPToken: "token-1",
	}, server.Client())
	if result.OK {
		t.Fatalf("VerifyBindingLive() = %+v", result)
	}
	if !strings.Contains(result.Note, "tools/list invalid") {
		t.Fatalf("unexpected note: %q", result.Note)
	}
}

func TestStdioProxyMissingToken(t *testing.T) {
	store := config.NewMemoryStore(config.State{Bindings: []config.Binding{{ConnectionID: "conn-1"}}})
	err := NewStdioProxy(store).Serve(context.Background(), "conn-1", strings.NewReader(""), &bytes.Buffer{}, &bytes.Buffer{})
	if !errors.Is(err, ErrMissingMCPToken) {
		t.Fatalf("Serve() error = %v, want ErrMissingMCPToken", err)
	}
}
