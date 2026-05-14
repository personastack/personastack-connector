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
		if r.Header.Get("Authorization") != "Bearer token-1" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()
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
		_ = conn.WriteJSON(openClawResponse{ID: agents.ID, Result: json.RawMessage(`{"agents":[]}`)})
		var hello openClawRequest
		if err := conn.ReadJSON(&hello); err != nil {
			t.Fatalf("read hello: %v", err)
		}
		if hello.Method != "hello" {
			t.Fatalf("expected hello, got %+v", hello)
		}
		_ = conn.WriteJSON(openClawResponse{ID: hello.ID, Result: json.RawMessage(`{"features":{"methods":["health","status","agents.list","agent","agent.wait","sessions.abort"]}}`)})
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
		_ = conn.WriteJSON(openClawResponse{ID: agents.ID, Result: json.RawMessage(`{"agents":[]}`)})
		var hello openClawRequest
		if err := conn.ReadJSON(&hello); err != nil {
			t.Fatalf("read hello: %v", err)
		}
		_ = conn.WriteJSON(openClawResponse{ID: hello.ID, Result: json.RawMessage(`{"features":{"methods":{"health":true,"status":true,"agents.list":true,"agent":true,"sessions.abort":true}}}`)})
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
		if r.Header.Get("Authorization") != "Bearer token-1" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()
		var request openClawRequest
		if err := conn.ReadJSON(&request); err != nil {
			t.Fatalf("read request: %v", err)
		}
		if request.Method != "agent" || request.ID != "assignment-1" {
			t.Fatalf("unexpected request: %+v", request)
		}
		_ = conn.WriteJSON(openClawResponse{ID: request.ID, Result: []byte(`{"accepted":true}`)})
	}))
	defer server.Close()

	runID, err := NewOpenClawAdapter("ws"+server.URL[len("http"):], "token-1").StartRun("assignment-1", "prompt")
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

func TestOpenClawAdapterCancelRunReadsAbortResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()
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
