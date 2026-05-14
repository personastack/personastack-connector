package runtime

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHermesAdapterDetectsRunSubmission(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health":
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		case "/v1/capabilities":
			if r.Header.Get("Authorization") != "Bearer key-1" {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			_, _ = w.Write([]byte(`{"features":{"run_submission":true,"run_status":true,"run_events_sse":true,"run_stop":true}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	detection := NewHermesAdapter(server.URL, "key-1").Detect()
	if detection.State != AdapterStateReady {
		t.Fatalf("expected ready, got %+v", detection)
	}
}

func TestHermesAdapterDetectRequiresRunLifecycleFeatures(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health":
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		case "/v1/capabilities":
			_, _ = w.Write([]byte(`{"features":{"run_submission":true}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	detection := NewHermesAdapter(server.URL, "key-1").Detect()
	if detection.State != AdapterStateCapabilityMissing {
		t.Fatalf("expected capability missing, got %+v", detection)
	}
	if detection.Note != "run_status,run_events_sse,run_stop missing" {
		t.Fatalf("unexpected note: %q", detection.Note)
	}
}

func TestHermesAdapterStartRun(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/runs" {
			http.NotFound(w, r)
			return
		}
		var body map[string]string
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		if body["conversation"] != "assignment-1" || body["input"] != "prompt" {
			t.Fatalf("unexpected body: %+v", body)
		}
		_, _ = w.Write([]byte(`{"id":"hermes-run-1"}`))
	}))
	defer server.Close()

	runID, err := NewHermesAdapter(server.URL, "key-1").StartRun("assignment-1", "prompt")
	if err != nil {
		t.Fatalf("start run: %v", err)
	}
	if runID != "hermes-run-1" {
		t.Fatalf("run id = %q", runID)
	}
}

func TestHermesAdapterWaitRunUsesSSEEvents(t *testing.T) {
	statusPolled := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/runs/hermes-run-1/events":
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("event: message\n"))
			_, _ = w.Write([]byte(`data: {"type":"run.completed","output":"done"}` + "\n\n"))
		case "/v1/runs/hermes-run-1":
			statusPolled = true
			_, _ = w.Write([]byte(`{"status":"completed","output":"polled"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	result, err := NewHermesAdapter(server.URL, "key-1").WaitRun(context.Background(), "hermes-run-1")
	if err != nil {
		t.Fatalf("wait run: %v", err)
	}
	if result.Status != RunStatusSucceeded || result.Output != "done" {
		t.Fatalf("unexpected result: %+v", result)
	}
	if statusPolled {
		t.Fatalf("status endpoint should not be polled after terminal SSE event")
	}
}
