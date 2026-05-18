package runtime

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	stdruntime "runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestOpenClawAdapterDetectRequiresGatewayFeatures(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()
		openClawTestAcceptOperator(t, conn, "token-1", `["health","status","agents.list","agent","agent.wait","sessions.abort"]`)
		var health openClawRequest
		if err := conn.ReadJSON(&health); err != nil {
			t.Fatalf("read health: %v", err)
		}
		if health.Method != "health" {
			t.Fatalf("expected health, got %+v", health)
		}
		_ = conn.WriteJSON(openClawResponse{ID: health.ID, Result: json.RawMessage(`{"ok":true}`)})
		var status openClawRequest
		if err := conn.ReadJSON(&status); err != nil {
			t.Fatalf("read status: %v", err)
		}
		if status.Method != "status" {
			t.Fatalf("expected status, got %+v", status)
		}
		_ = conn.WriteJSON(openClawResponse{ID: status.ID, Result: json.RawMessage(`{"ok":true}`)})
		var agents openClawRequest
		if err := conn.ReadJSON(&agents); err != nil {
			t.Fatalf("read agents.list: %v", err)
		}
		if agents.Method != "agents.list" {
			t.Fatalf("expected agents.list, got %+v", agents)
		}
		_ = conn.WriteJSON(openClawResponse{ID: agents.ID, Result: json.RawMessage(`{"agents":[{"id":"agent-1"}]}`)})
	}))
	defer server.Close()

	detection := NewOpenClawAdapter("ws"+server.URL[len("http"):], "token-1").Detect()
	if detection.State != AdapterStateReady {
		t.Fatalf("expected ready, got %+v", detection)
	}
}

func TestOpenClawAdapterRejectsUnsupportedProtocolVersion(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()
		_ = conn.WriteJSON(openClawResponse{Type: "connect.challenge"})
		var connect openClawRequest
		if err := conn.ReadJSON(&connect); err != nil {
			t.Fatalf("read connect: %v", err)
		}
		_ = conn.WriteJSON(openClawResponse{Type: "hello-ok", Result: json.RawMessage(`{"protocol":2,"role":"operator","scopes":["operator.read","operator.write"],"features":{"methods":["health","status","agents.list","agent","agent.wait","sessions.abort"]}}`)})
	}))
	defer server.Close()

	detection := NewOpenClawAdapter("ws"+server.URL[len("http"):], "token-1").Detect()
	if detection.State != AdapterStateCapabilityMissing {
		t.Fatalf("expected capability missing, got %+v", detection)
	}
	if !strings.Contains(detection.Note, "unsupported") {
		t.Fatalf("unexpected note: %q", detection.Note)
	}
}

func TestOpenClawAdapterDetectRejectsMissingFeature(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()
		openClawTestAcceptOperator(t, conn, "token-1", `{"health":true,"status":true,"agents.list":true,"agent":true,"sessions.abort":true}`)
	}))
	defer server.Close()

	detection := NewOpenClawAdapter("ws"+server.URL[len("http"):], "token-1").Detect()
	if detection.State != AdapterStateCapabilityMissing {
		t.Fatalf("expected capability missing, got %+v", detection)
	}
	if detection.Note != "agent.wait missing" {
		t.Fatalf("unexpected note: %q", detection.Note)
	}
}

func TestOpenClawAdapterStartRunUsesAgentMethod(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()
		openClawTestAcceptOperator(t, conn, "token-1", `["health","status","agents.list","agent","agent.wait","sessions.abort"]`)
		var request openClawRequest
		if err := conn.ReadJSON(&request); err != nil {
			t.Fatalf("read request: %v", err)
		}
		if request.Method != "agent" || request.ID != "assignment-1" {
			t.Fatalf("unexpected request: %+v", request)
		}
		params, ok := request.Params.(map[string]any)
		if !ok {
			t.Fatalf("unexpected params: %+v", request.Params)
		}
		if params["message"] != "prompt" || params["idempotencyKey"] != "assignment-1" {
			t.Fatalf("unexpected params: %+v", params)
		}
		for _, key := range []string{"runId", "nativeMcpServerName", "nativeMcpToolNamespace", "metadata"} {
			if _, ok := params[key]; ok {
				t.Fatalf("unexpected param %q: %+v", key, params)
			}
		}
		_ = conn.WriteJSON(openClawResponse{ID: request.ID, Result: []byte(`{"accepted":true}`)})
	}))
	defer server.Close()

	runID, err := NewOpenClawAdapter("ws"+server.URL[len("http"):], "token-1").StartRun(RunRequest{
		RunID:               "run-1",
		AssignmentID:        "assignment-1",
		FullyComposedPrompt: "prompt",
		NativeMCPServerName: "personastack-conn-1",
	})
	if err != nil {
		t.Fatalf("start run: %v", err)
	}
	if runID != "assignment-1" {
		t.Fatalf("run id = %q", runID)
	}
}

func TestOpenClawAdapterStartRunRejectsNotOKEnvelope(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()
		openClawTestAcceptOperator(t, conn, "token-1", `["health","status","agents.list","agent","agent.wait","sessions.abort"]`)
		var request openClawRequest
		if err := conn.ReadJSON(&request); err != nil {
			t.Fatalf("read request: %v", err)
		}
		_ = conn.WriteJSON(openClawResponse{Type: "res", ID: request.ID, OK: boolRef(false), Error: map[string]any{"message": "rejected"}})
	}))
	defer server.Close()

	_, err := NewOpenClawAdapter("ws"+server.URL[len("http"):], "token-1").StartRun(RunRequest{
		AssignmentID:        "assignment-1",
		FullyComposedPrompt: "prompt",
	})
	if err == nil {
		t.Fatal("expected not-ok agent response to fail")
	}
}

func TestOpenClawAdapterStartRunRejectsUntypedNotOKEnvelope(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()
		openClawTestAcceptOperator(t, conn, "token-1", `["health","status","agents.list","agent","agent.wait","sessions.abort"]`)
		var request openClawRequest
		if err := conn.ReadJSON(&request); err != nil {
			t.Fatalf("read request: %v", err)
		}
		_ = conn.WriteJSON(openClawResponse{ID: request.ID, OK: boolRef(false)})
	}))
	defer server.Close()

	_, err := NewOpenClawAdapter("ws"+server.URL[len("http"):], "token-1").StartRun(RunRequest{
		AssignmentID:        "assignment-1",
		FullyComposedPrompt: "prompt",
	})
	if err == nil {
		t.Fatal("expected untyped not-ok agent response to fail")
	}
}

func TestOpenClawAdapterWaitRunUsesAgentWait(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()
		openClawTestAcceptOperator(t, conn, "token-1", `["health","status","agents.list","agent","agent.wait","sessions.abort"]`)
		var request openClawRequest
		if err := conn.ReadJSON(&request); err != nil {
			t.Fatalf("read request: %v", err)
		}
		if request.Method != "agent.wait" || request.ID != "wait-run-1" {
			t.Fatalf("unexpected request: %+v", request)
		}
		_ = conn.WriteJSON(openClawResponse{ID: request.ID, Result: []byte(`{"status":"completed","output":"done"}`)})
	}))
	defer server.Close()

	result, err := NewOpenClawAdapter("ws"+server.URL[len("http"):], "token-1").WaitRun(context.Background(), "run-1")
	if err != nil {
		t.Fatalf("wait run: %v", err)
	}
	if result.Status != RunStatusSucceeded || result.Output != "done" {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestOpenClawAdapterWaitRunRejectsNotOKEnvelope(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()
		openClawTestAcceptOperator(t, conn, "token-1", `["health","status","agents.list","agent","agent.wait","sessions.abort"]`)
		var request openClawRequest
		if err := conn.ReadJSON(&request); err != nil {
			t.Fatalf("read request: %v", err)
		}
		_ = conn.WriteJSON(openClawResponse{Type: "res", ID: request.ID, OK: boolRef(false), Error: map[string]any{"message": "wait failed"}})
	}))
	defer server.Close()

	_, err := NewOpenClawAdapter("ws"+server.URL[len("http"):], "token-1").WaitRun(context.Background(), "run-1")
	if err == nil {
		t.Fatal("expected not-ok wait response to fail")
	}
}

func TestOpenClawAdapterWaitRunFailedOutputFallback(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()
		openClawTestAcceptOperator(t, conn, "token-1", `["health","status","agents.list","agent","agent.wait","sessions.abort"]`)
		var request openClawRequest
		if err := conn.ReadJSON(&request); err != nil {
			t.Fatalf("read request: %v", err)
		}
		_ = conn.WriteJSON(openClawResponse{ID: request.ID, Result: []byte(`{"status":"failed","output":"failed output"}`)})
	}))
	defer server.Close()

	result, err := NewOpenClawAdapter("ws"+server.URL[len("http"):], "token-1").WaitRun(context.Background(), "run-1")
	if err != nil {
		t.Fatalf("wait run: %v", err)
	}
	if result.Status != RunStatusFailed || result.Output != "failed output" {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestOpenClawAdapterWaitRunDoesNotSucceedAfterContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()
		openClawTestAcceptOperator(t, conn, "token-1", `["health","status","agents.list","agent","agent.wait","sessions.abort"]`)
		var request openClawRequest
		if err := conn.ReadJSON(&request); err != nil {
			t.Fatalf("read request: %v", err)
		}
		cancel()
		_ = conn.WriteJSON(openClawResponse{ID: request.ID, Result: []byte(`{"status":"running"}`)})
	}))
	defer server.Close()

	result, err := NewOpenClawAdapter("ws"+server.URL[len("http"):], "token-1").WaitRun(ctx, "run-1")
	if err == nil {
		t.Fatalf("expected context cancellation, got result %+v", result)
	}
}

func TestOpenClawAdapterStreamOrPollRunForwardsBroadcastEvents(t *testing.T) {
	events := []RunEvent{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()
		openClawTestAcceptOperator(t, conn, "token-1", `["health","status","agents.list","agent","agent.wait","sessions.abort"]`)
		var request openClawRequest
		if err := conn.ReadJSON(&request); err != nil {
			t.Fatalf("read agent.wait: %v", err)
		}
		if request.Method != "agent.wait" {
			t.Fatalf("expected agent.wait, got %+v", request)
		}
		_ = conn.WriteJSON(openClawResponse{Type: "event", Event: "chat", Payload: json.RawMessage(`{"deltaText":"chunk"}`)})
		_ = conn.WriteJSON(openClawResponse{Type: "event", Event: "session.tool", Payload: json.RawMessage(`{"toolName":"browser","phase":"started","summary":"opening"}`)})
		_ = conn.WriteJSON(openClawResponse{ID: request.ID, Result: []byte(`{"status":"completed","output":"done"}`)})
	}))
	defer server.Close()

	result, err := NewOpenClawAdapter("ws"+server.URL[len("http"):], "token-1").StreamOrPollRun(context.Background(), "run-1", func(event RunEvent) error {
		events = append(events, event)
		return nil
	})
	if err != nil {
		t.Fatalf("StreamOrPollRun() error = %v", err)
	}
	if result.Status != RunStatusSucceeded || result.Output != "done" {
		t.Fatalf("unexpected result: %+v", result)
	}
	if len(events) != 3 {
		t.Fatalf("unexpected events: %+v", events)
	}
	if events[0].Kind != RunEventStarted {
		t.Fatalf("first event = %+v", events[0])
	}
	if events[1].Kind != RunEventOutputDelta || events[1].Delta != "chunk" {
		t.Fatalf("second event = %+v", events[1])
	}
	if events[2].Kind != RunEventToolEvent || events[2].ToolName != "browser" || events[2].ToolPhase != "started" || events[2].Summary != "opening" {
		t.Fatalf("third event = %+v", events[2])
	}
}

func TestOpenClawAdapterStreamOrPollRunRetriesStartupSidecars(t *testing.T) {
	var attempts int32
	startedAt := "2026-05-14T19:31:05Z"
	events := []RunEvent{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()
		openClawTestAcceptOperator(t, conn, "token-1", `["health","status","agents.list","agent","agent.wait","sessions.abort"]`)
		for {
			var request openClawRequest
			if err := conn.ReadJSON(&request); err != nil {
				t.Fatalf("read agent.wait: %v", err)
			}
			if request.Method != "agent.wait" {
				t.Fatalf("expected agent.wait, got %+v", request)
			}
			switch atomic.AddInt32(&attempts, 1) {
			case 1:
				_ = conn.WriteJSON(openClawResponse{
					Type:   "res",
					ID:     request.ID,
					OK:     boolRef(false),
					Error:  map[string]any{"details": map[string]any{"code": "UNAVAILABLE", "reason": "startup-sidecars", "retryAfterMs": 1}},
					Result: nil,
				})
			case 2:
				_ = conn.WriteJSON(openClawResponse{ID: request.ID, Result: json.RawMessage(`{"status":"completed","output":"done","startedAt":"` + startedAt + `"}`)})
				return
			default:
				t.Fatalf("unexpected retry attempt %d", attempts)
			}
		}
	}))
	defer server.Close()

	result, err := NewOpenClawAdapter("ws"+server.URL[len("http"):], "token-1").StreamOrPollRun(context.Background(), "run-1", func(event RunEvent) error {
		events = append(events, event)
		return nil
	})
	if err != nil {
		t.Fatalf("StreamOrPollRun() error = %v", err)
	}
	if result.Status != RunStatusSucceeded || result.Output != "done" {
		t.Fatalf("unexpected result: %+v", result)
	}
	if atomic.LoadInt32(&attempts) != 2 {
		t.Fatalf("attempts = %d", attempts)
	}
	if len(events) != 1 || events[0].Kind != RunEventStarted {
		t.Fatalf("unexpected events: %+v", events)
	}
	parsedStartedAt, err := time.Parse(time.RFC3339Nano, startedAt)
	if err != nil {
		t.Fatalf("parse startedAt: %v", err)
	}
	if !events[0].StartedAt.Equal(parsedStartedAt) {
		t.Fatalf("startedAt = %s want %s", events[0].StartedAt, parsedStartedAt)
	}
}

func TestOpenClawAdapterCancelRunReadsAbortResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()
		openClawTestAcceptOperator(t, conn, "token-1", `["health","status","agents.list","agent","agent.wait","sessions.abort"]`)
		var request openClawRequest
		if err := conn.ReadJSON(&request); err != nil {
			t.Fatalf("read request: %v", err)
		}
		if request.Method != "sessions.abort" || request.ID != "cancel-run-1" {
			t.Fatalf("unexpected request: %+v", request)
		}
		_ = conn.WriteJSON(openClawResponse{ID: request.ID, Result: []byte(`{"ok":true}`)})
	}))
	defer server.Close()

	err := NewOpenClawAdapter("ws"+server.URL[len("http"):], "token-1").CancelRun("run-1")
	if err != nil {
		t.Fatalf("cancel run: %v", err)
	}
}

func TestOpenClawAdapterCancelRunSurfacesAbortError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()
		openClawTestAcceptOperator(t, conn, "token-1", `["health","status","agents.list","agent","agent.wait","sessions.abort"]`)
		var request openClawRequest
		if err := conn.ReadJSON(&request); err != nil {
			t.Fatalf("read request: %v", err)
		}
		_ = conn.WriteJSON(openClawResponse{ID: request.ID, Error: "not found"})
	}))
	defer server.Close()

	err := NewOpenClawAdapter("ws"+server.URL[len("http"):], "token-1").CancelRun("run-1")
	if err == nil {
		t.Fatalf("expected cancel error")
	}
}

func TestOpenClawAdapterCancelRunRejectsNotOKEnvelope(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()
		openClawTestAcceptOperator(t, conn, "token-1", `["health","status","agents.list","agent","agent.wait","sessions.abort"]`)
		var request openClawRequest
		if err := conn.ReadJSON(&request); err != nil {
			t.Fatalf("read request: %v", err)
		}
		_ = conn.WriteJSON(openClawResponse{Type: "res", ID: request.ID, OK: boolRef(false)})
	}))
	defer server.Close()

	err := NewOpenClawAdapter("ws"+server.URL[len("http"):], "token-1").CancelRun("run-1")
	if err == nil {
		t.Fatal("expected not-ok cancel response to fail")
	}
}

func TestOpenClawAdapterCLIJSONFallbackUsesCachedResult(t *testing.T) {
	t.Setenv("PATH", tempPathWithOpenClawCLI(t))
	adapter := NewOpenClawAdapterWithAuth("ws://127.0.0.1:1", OpenClawAuth{Token: "token-1"}, "agent-1")

	runID, err := adapter.StartRun(RunRequest{
		AssignmentID:        "assignment-1",
		FullyComposedPrompt: "prompt",
	})
	if err != nil {
		t.Fatalf("StartRun() error = %v", err)
	}
	if runID != "assignment-1" {
		t.Fatalf("run id = %q", runID)
	}

	events := []RunEvent{}
	result, err := adapter.StreamOrPollRun(context.Background(), runID, func(event RunEvent) error {
		events = append(events, event)
		return nil
	})
	if err != nil {
		t.Fatalf("StreamOrPollRun() error = %v", err)
	}
	if result.Status != RunStatusSucceeded || result.Output != "fallback output" {
		t.Fatalf("unexpected result: %+v", result)
	}
	if len(events) != 1 || events[0].Kind != RunEventStarted {
		t.Fatalf("unexpected events: %+v", events)
	}

	if err := adapter.CancelRun(runID); err == nil || !strings.Contains(err.Error(), "does not support cancellation") {
		t.Fatalf("CancelRun() error = %v", err)
	}
}

func tempPathWithOpenClawCLI(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	script := filepath.Join(dir, "openclaw")
	raw := []byte("#!/bin/sh\nprintf '%s\\n' '{\"meta\":{\"transport\":\"embedded\",\"fallbackFrom\":\"gateway\"},\"payloads\":[{\"text\":\"fallback output\"}]}'\n")
	if stdruntime.GOOS == "windows" {
		script += ".bat"
		raw = []byte("@echo {\"meta\":{\"transport\":\"embedded\",\"fallbackFrom\":\"gateway\"},\"payloads\":[{\"text\":\"fallback output\"}]}\r\n")
	}
	if err := os.WriteFile(script, raw, 0o755); err != nil {
		t.Fatalf("write fake openclaw: %v", err)
	}
	return dir + string(os.PathListSeparator) + os.Getenv("PATH")
}

func TestOpenClawAdapterDetectRequiresAgentSelection(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()
		openClawTestAcceptOperator(t, conn, "token-1", `["health","status","agents.list","agent","agent.wait","sessions.abort"]`)
		var health openClawRequest
		if err := conn.ReadJSON(&health); err != nil {
			t.Fatalf("read health: %v", err)
		}
		_ = conn.WriteJSON(openClawResponse{Type: "event", Event: "health", Payload: json.RawMessage(`{"ok":true}`)})
		_ = conn.WriteJSON(openClawResponse{ID: health.ID, Result: json.RawMessage(`{"ok":true}`)})
		var status openClawRequest
		if err := conn.ReadJSON(&status); err != nil {
			t.Fatalf("read status: %v", err)
		}
		_ = conn.WriteJSON(openClawResponse{ID: status.ID, Result: json.RawMessage(`{"ok":true}`)})
		var agents openClawRequest
		if err := conn.ReadJSON(&agents); err != nil {
			t.Fatalf("read agents.list: %v", err)
		}
		_ = conn.WriteJSON(openClawResponse{ID: agents.ID, Result: json.RawMessage(`{"agents":[{"id":"agent-1"},{"id":"agent-2"}]}`)})
	}))
	defer server.Close()

	detection := NewOpenClawAdapter("ws"+server.URL[len("http"):], "token-1").Detect()
	if detection.State != AdapterStateCapabilityMissing || detection.Note != "agent_selection_required" {
		t.Fatalf("unexpected detection: %+v", detection)
	}
}

func TestOpenClawAdapterDetectRejectsDisabledOnlyAgent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()
		openClawTestAcceptOperator(t, conn, "token-1", `["health","status","agents.list","agent","agent.wait","sessions.abort"]`)
		var health openClawRequest
		if err := conn.ReadJSON(&health); err != nil {
			t.Fatalf("read health: %v", err)
		}
		_ = conn.WriteJSON(openClawResponse{ID: health.ID, Result: json.RawMessage(`{"ok":true}`)})
		var status openClawRequest
		if err := conn.ReadJSON(&status); err != nil {
			t.Fatalf("read status: %v", err)
		}
		_ = conn.WriteJSON(openClawResponse{ID: status.ID, Result: json.RawMessage(`{"ok":true}`)})
		var agents openClawRequest
		if err := conn.ReadJSON(&agents); err != nil {
			t.Fatalf("read agents.list: %v", err)
		}
		_ = conn.WriteJSON(openClawResponse{ID: agents.ID, Result: json.RawMessage(`{"agents":[{"id":"agent-1","enabled":false}]}`)})
	}))
	defer server.Close()

	detection := NewOpenClawAdapter("ws"+server.URL[len("http"):], "token-1").Detect()
	if detection.State != AdapterStateCapabilityMissing || detection.Note != "no usable OpenClaw agents" {
		t.Fatalf("unexpected detection: %+v", detection)
	}
}

func TestOpenClawAdapterStoresDeviceTokenFromHello(t *testing.T) {
	stored := ""
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()
		openClawTestAcceptOperatorWithDevice(t, conn, "token-1", `["health","status","agents.list","agent","agent.wait","sessions.abort"]`, "device-2")
		var health openClawRequest
		if err := conn.ReadJSON(&health); err != nil {
			t.Fatalf("read health: %v", err)
		}
		_ = conn.WriteJSON(openClawResponse{ID: health.ID, Result: json.RawMessage(`{"ok":true}`)})
		var status openClawRequest
		if err := conn.ReadJSON(&status); err != nil {
			t.Fatalf("read status: %v", err)
		}
		_ = conn.WriteJSON(openClawResponse{ID: status.ID, Result: json.RawMessage(`{"ok":true}`)})
		var agents openClawRequest
		if err := conn.ReadJSON(&agents); err != nil {
			t.Fatalf("read agents.list: %v", err)
		}
		_ = conn.WriteJSON(openClawResponse{ID: agents.ID, Result: json.RawMessage(`{"agents":[{"id":"agent-1"}]}`)})
	}))
	defer server.Close()

	adapter := NewOpenClawAdapter("ws"+server.URL[len("http"):], "token-1")
	adapter.DeviceTokenSink = func(token string) error {
		stored = token
		return nil
	}
	detection := adapter.Detect()
	if detection.State != AdapterStateReady {
		t.Fatalf("expected ready, got %+v", detection)
	}
	if stored != "device-2" {
		t.Fatalf("stored device token = %q", stored)
	}
}

func TestOpenClawAdapterAcceptsResHelloPayload(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()
		_ = conn.WriteJSON(openClawResponse{Type: "event", Event: "connect.challenge"})
		var connect openClawRequest
		if err := conn.ReadJSON(&connect); err != nil {
			t.Fatalf("read connect: %v", err)
		}
		payload := `{"type":"hello-ok","protocol":4,"features":{"methods":["health","status","agents.list","agent","agent.wait","sessions.abort"]},"auth":{"role":"operator","scopes":["operator.read","operator.write"]}}`
		_ = conn.WriteJSON(openClawResponse{Type: "res", ID: connect.ID, OK: boolRef(true), Payload: json.RawMessage(payload)})
		var health openClawRequest
		if err := conn.ReadJSON(&health); err != nil {
			t.Fatalf("read health: %v", err)
		}
		_ = conn.WriteJSON(openClawResponse{ID: health.ID, Result: json.RawMessage(`{"ok":true}`)})
		var status openClawRequest
		if err := conn.ReadJSON(&status); err != nil {
			t.Fatalf("read status: %v", err)
		}
		_ = conn.WriteJSON(openClawResponse{ID: status.ID, Result: json.RawMessage(`{"ok":true}`)})
		var agents openClawRequest
		if err := conn.ReadJSON(&agents); err != nil {
			t.Fatalf("read agents.list: %v", err)
		}
		_ = conn.WriteJSON(openClawResponse{ID: agents.ID, Result: json.RawMessage(`{"agents":[{"id":"agent-1"}]}`)})
	}))
	defer server.Close()

	detection := NewOpenClawAdapter("ws"+server.URL[len("http"):], "token-1").Detect()
	if detection.State != AdapterStateReady {
		t.Fatalf("expected ready, got %+v", detection)
	}
}

func TestOpenClawAdapterVerifyMCPCatalogUsesLiveCatalogNames(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()
		openClawTestAcceptOperator(t, conn, "token-1", `["health","status","agents.list","agent","agent.wait","sessions.abort"]`)
		var request openClawRequest
		if err := conn.ReadJSON(&request); err != nil {
			t.Fatalf("read tools.catalog: %v", err)
		}
		if request.Method != "tools.catalog" {
			t.Fatalf("expected tools.catalog, got %+v", request)
		}
		params, ok := request.Params.(map[string]any)
		if !ok || params["includePlugins"] != true {
			t.Fatalf("unexpected tools.catalog params: %+v", request.Params)
		}
		_ = conn.WriteJSON(openClawResponse{
			ID: request.ID,
			Result: json.RawMessage(`{
  "agentId":"agent-1",
  "groups":[
    {
      "id":"plugin:personastack-conn-1",
      "label":"personastack-conn-1",
      "source":"plugin",
      "pluginId":"personastack-conn-1",
      "tools":[
        {"id":"calendar","label":"Calendar","source":"plugin","pluginId":"personastack-conn-1"}
      ]
    }
  ]
}`),
		})
	}))
	defer server.Close()

	result := NewOpenClawAdapterWithAuth("ws"+server.URL[len("http"):], OpenClawAuth{Token: "token-1"}, "agent-1").VerifyMCPCatalog(context.Background(), "personastack-conn-1")
	if !result.OK {
		t.Fatalf("expected catalog verification to pass: %+v", result)
	}
	if !strings.Contains(result.Note, "effective tool catalog visible") {
		t.Fatalf("unexpected note: %q", result.Note)
	}
}

func openClawTestAcceptOperator(t *testing.T, conn *websocket.Conn, token string, methods string) {
	t.Helper()
	openClawTestAcceptOperatorWithDevice(t, conn, token, methods, "")
}

func openClawTestAcceptOperatorWithDevice(t *testing.T, conn *websocket.Conn, token string, methods string, deviceToken string) {
	t.Helper()
	_ = conn.WriteJSON(openClawResponse{Type: "connect.challenge"})
	var connect openClawRequest
	if err := conn.ReadJSON(&connect); err != nil {
		t.Fatalf("read connect: %v", err)
	}
	if connect.Method != "connect" {
		t.Fatalf("expected connect, got %+v", connect)
	}
	if connect.Type != "req" {
		t.Fatalf("expected request frame type, got %+v", connect)
	}
	params := connect.Params.(map[string]any)
	client := params["client"].(map[string]any)
	if client["id"] != "gateway-client" || client["mode"] != "backend" {
		t.Fatalf("expected backend gateway-client connect, got %+v", client)
	}
	if params["role"] != "operator" {
		t.Fatalf("expected operator connect, got %+v", params)
	}
	auth := params["auth"].(map[string]any)
	if auth["token"] != token && auth["deviceToken"] != token {
		t.Fatalf("unexpected auth: %+v", auth)
	}
	result := `{"protocol":4,"role":"operator","scopes":["operator.read","operator.write"],"features":{"methods":` + methods + `},"auth":{"deviceToken":"` + deviceToken + `"}}`
	_ = conn.WriteJSON(openClawResponse{Type: "hello-ok", Result: json.RawMessage(result)})
}

func boolRef(value bool) *bool {
	return &value
}
