package runtime

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

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
		metadata, ok := params["metadata"].(map[string]any)
		if !ok {
			t.Fatalf("missing metadata: %+v", params)
		}
		if params["message"] != "prompt" || params["idempotencyKey"] != "assignment-1" || params["runId"] != "assignment-1" || params["nativeMcpServerName"] != "personastack-conn-1" || metadata["personastack_run_id"] != "run-1" {
			t.Fatalf("unexpected params: %+v", params)
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
	params := connect.Params.(map[string]any)
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
