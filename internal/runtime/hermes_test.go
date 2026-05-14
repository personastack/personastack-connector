package runtime

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHermesAdapterDetectsRunSubmission(t *testing.T) {
	probedDetailedHealth := false
	probedModels := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health":
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		case "/health/detailed":
			probedDetailedHealth = true
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		case "/v1/models":
			probedModels = true
			_, _ = w.Write([]byte(`{"data":[]}`))
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
	if !probedDetailedHealth || !probedModels {
		t.Fatalf("expected detailed health and models probes")
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
	if detection.Note != "run_status missing" {
		t.Fatalf("unexpected note: %q", detection.Note)
	}
}

func TestHermesAdapterDetectReportsDegradedFallbacks(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health":
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		case "/v1/capabilities":
			_, _ = w.Write([]byte(`{"features":{"run_submission":true,"run_status":true}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	detection := NewHermesAdapter(server.URL, "key-1").Detect()
	if detection.State != AdapterStateReady {
		t.Fatalf("expected ready with degraded fallback, got %+v", detection)
	}
	if detection.Note != "Hermes API ready with degraded fallback: run_events_sse,run_stop missing" {
		t.Fatalf("unexpected note: %q", detection.Note)
	}
}

func TestHermesAdapterStartRun(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/runs" {
			http.NotFound(w, r)
			return
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		metadata, ok := body["metadata"].(map[string]any)
		if !ok {
			t.Fatalf("metadata missing: %+v", body)
		}
		if body["session_id"] != "run-1" || body["conversation"] != "assignment-1" || body["input"] != "prompt" || body["native_mcp_server"] != "personastack-conn-1" || metadata["personastack_run_id"] != "run-1" {
			t.Fatalf("unexpected body: %+v", body)
		}
		_, _ = w.Write([]byte(`{"run_id":"hermes-run-1"}`))
	}))
	defer server.Close()

	runID, err := NewHermesAdapter(server.URL, "key-1").StartRun(RunRequest{
		RunID:               "run-1",
		AssignmentID:        "assignment-1",
		FullyComposedPrompt: "prompt",
		NativeMCPServerName: "personastack-conn-1",
	})
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
