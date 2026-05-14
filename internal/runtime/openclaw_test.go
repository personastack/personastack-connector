package runtime

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/websocket"
)

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
