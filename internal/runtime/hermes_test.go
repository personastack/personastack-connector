package runtime

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
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

func TestHermesAdapterDescribeNativeCapabilitiesUsesRuntimeFeatures(t *testing.T) {
	binDir := t.TempDir()
	hermesPath := filepath.Join(binDir, "hermes")
	err := os.WriteFile(hermesPath, []byte("#!/bin/sh\necho 'enabled skills Skills'\n"), 0o700)
	if err != nil {
		t.Fatalf("write hermes stub: %v", err)
	}
	t.Setenv("PATH", binDir)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/capabilities":
			if r.Header.Get("Authorization") != "Bearer key-1" {
				t.Fatalf("missing auth header: %q", r.Header.Get("Authorization"))
			}
			_, _ = w.Write([]byte(`{"features":{"run_submission":true,"run_status":true,"run_events_sse":true,"run_stop":false}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	capabilities, err := NewHermesAdapter(server.URL, "key-1").DescribeNativeCapabilities(context.Background(), "personastack-conn-1")
	if err != nil {
		t.Fatalf("DescribeNativeCapabilities() error = %v", err)
	}
	if len(capabilities) != 4 {
		t.Fatalf("expected three runtime features and one native tool, got %#v", capabilities)
	}
	if capabilities[0].Summary != "can accept delegated tasks" || capabilities[2].Summary != "can stream progress updates" {
		t.Fatalf("unexpected Hermes capability summaries: %#v", capabilities)
	}
}

func TestHermesAdapterDescribeNativeCapabilitiesKeepsRuntimeFeaturesWhenToolsListFails(t *testing.T) {
	binDir := t.TempDir()
	hermesPath := filepath.Join(binDir, "hermes")
	err := os.WriteFile(hermesPath, []byte("#!/bin/sh\nexit 1\n"), 0o700)
	if err != nil {
		t.Fatalf("write hermes stub: %v", err)
	}
	t.Setenv("PATH", binDir)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/capabilities":
			_, _ = w.Write([]byte(`{"features":{"run_submission":true,"run_status":true,"run_events_sse":true,"run_stop":false}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	capabilities, err := NewHermesAdapter(server.URL, "key-1").DescribeNativeCapabilities(context.Background(), "personastack-conn-1")
	if err == nil || !strings.Contains(err.Error(), "Hermes tools list") {
		t.Fatalf("DescribeNativeCapabilities() error = %v", err)
	}
	if len(capabilities) != 3 {
		t.Fatalf("expected runtime capabilities despite tools-list failure, got %#v", capabilities)
	}
	for _, capability := range capabilities {
		if capability.Source != NativeCapabilitySourceHermesRuntimeAPI {
			t.Fatalf("unexpected tools-list capability after tools-list failure: %#v", capability)
		}
	}
}

func TestHermesAdapterDescribeNativeCapabilitiesKeepsToolsWhenCapabilitiesFail(t *testing.T) {
	binDir := t.TempDir()
	hermesPath := filepath.Join(binDir, "hermes")
	err := os.WriteFile(hermesPath, []byte("#!/bin/sh\necho 'enabled skills Skills'\n"), 0o700)
	if err != nil {
		t.Fatalf("write hermes stub: %v", err)
	}
	t.Setenv("PATH", binDir)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	capabilities, err := NewHermesAdapter(server.URL, "key-1").DescribeNativeCapabilities(context.Background(), "personastack-conn-1")
	if err == nil || !strings.Contains(err.Error(), "Hermes capabilities status 503") {
		t.Fatalf("DescribeNativeCapabilities() error = %v", err)
	}
	if len(capabilities) != 1 || capabilities[0].Source != NativeCapabilitySourceHermesToolsList {
		t.Fatalf("expected tools-list capability despite capabilities failure, got %#v", capabilities)
	}
	if !capabilities[0].Degraded {
		t.Fatalf("expected tools-list capability to be degraded after capabilities failure, got %#v", capabilities[0])
	}
}

func TestResolveHermesBinaryUsesExplicitOverride(t *testing.T) {
	binDir := t.TempDir()
	hermesPath := filepath.Join(binDir, "custom-hermes")
	err := os.WriteFile(hermesPath, []byte("#!/bin/sh\n"), 0o700)
	if err != nil {
		t.Fatalf("write hermes stub: %v", err)
	}
	t.Setenv("HERMES_BIN", hermesPath)
	t.Setenv("PATH", "")

	got, err := resolveHermesBinary()
	if err != nil {
		t.Fatalf("resolveHermesBinary() error = %v", err)
	}
	if got != hermesPath {
		t.Fatalf("resolveHermesBinary() = %q, want %q", got, hermesPath)
	}
}

func TestResolveHermesBinaryFallsBackToUserInstall(t *testing.T) {
	homeDir := t.TempDir()
	hermesPath := filepath.Join(homeDir, ".local", "bin", "hermes")
	if err := os.MkdirAll(filepath.Dir(hermesPath), 0o700); err != nil {
		t.Fatalf("create hermes dir: %v", err)
	}
	if err := os.WriteFile(hermesPath, []byte("#!/bin/sh\n"), 0o700); err != nil {
		t.Fatalf("write hermes stub: %v", err)
	}
	t.Setenv("HERMES_BIN", "")
	t.Setenv("HOME", homeDir)
	t.Setenv("PATH", "")
	withHermesLookPath(t, func(string) (string, error) {
		return "", exec.ErrNotFound
	})

	got, err := resolveHermesBinary()
	if err != nil {
		t.Fatalf("resolveHermesBinary() error = %v", err)
	}
	if got != hermesPath {
		t.Fatalf("resolveHermesBinary() = %q, want %q", got, hermesPath)
	}
}

func TestResolveHermesBinarySkipsMissingCandidates(t *testing.T) {
	homeDir := t.TempDir()
	t.Setenv("HERMES_BIN", "")
	t.Setenv("HOME", homeDir)
	t.Setenv("PATH", "")
	withHermesLookPath(t, func(string) (string, error) {
		return "", exec.ErrNotFound
	})

	got, err := resolveHermesBinary()
	if err == nil {
		t.Fatalf("resolveHermesBinary() = %q, want error", got)
	}
	if !strings.Contains(err.Error(), "Hermes binary not found") {
		t.Fatalf("resolveHermesBinary() error = %v", err)
	}
}

func TestHermesToolListCapabilitiesUsesResolvedBinary(t *testing.T) {
	homeDir := t.TempDir()
	hermesPath := filepath.Join(homeDir, ".local", "bin", "hermes")
	if err := os.MkdirAll(filepath.Dir(hermesPath), 0o700); err != nil {
		t.Fatalf("create hermes dir: %v", err)
	}
	if err := os.WriteFile(hermesPath, []byte("#!/bin/sh\necho 'enabled computer_use Computer Use'\n"), 0o700); err != nil {
		t.Fatalf("write hermes stub: %v", err)
	}
	t.Setenv("HERMES_BIN", "")
	t.Setenv("HOME", homeDir)
	t.Setenv("PATH", "")
	withHermesLookPath(t, func(string) (string, error) {
		return "", exec.ErrNotFound
	})

	capabilities, err := hermesToolListCapabilities(context.Background(), "personastack-conn-1")
	if err != nil {
		t.Fatalf("hermesToolListCapabilities() error = %v", err)
	}
	if len(capabilities) != 1 || capabilities[0].CapabilityID != "computer_use" {
		t.Fatalf("capabilities = %#v", capabilities)
	}
}

func TestParseHermesToolsListReportsEnabledCLITools(t *testing.T) {
	capabilities := parseHermesToolsList(`
Built-in toolsets (cli):
  ✓ enabled  web  🔍 Web Search & Scraping
  ✓ enabled  browser  🌐 Browser Automation
  ✓ enabled  terminal  💻 Terminal & Processes
  ✓ enabled  file  📁 File Operations
  ✓ enabled  code_execution  ⚡ Code Execution
  ✓ enabled  vision  👁️  Vision / Image Analysis
  ✗ disabled  video  🎬 Video Analysis
  ✓ enabled  image_gen  🎨 Image Generation
  ✗ disabled  video_gen  🎬 Video Generation
  ✗ disabled  x_search  🐦 X Search
  ✗ disabled  moa  🧠 Mixture of Agents
  ✓ enabled  tts  🔊 Text-to-Speech
  ✓ enabled  skills  📚 Skills
  ✓ enabled  todo  📋 Task Planning
  ✓ enabled  memory  💾 Memory
  ✓ enabled  session_search  🔎 Session Search
  ✓ enabled  clarify  ❓ Clarifying Questions
  ✓ enabled  delegation  👥 Task Delegation
  ✓ enabled  cronjob  ⏰ Cron Jobs
  ✓ enabled  messaging  📨 Cross-Platform Messaging
  ✗ disabled  homeassistant  🏠 Home Assistant
  ✗ disabled  spotify  🎵 Spotify
  ✗ disabled  yuanbao  🤖 Yuanbao
  ✓ enabled  computer_use  🖱️  Computer Use (macOS)

MCP servers:
  personastack_2d647f33a8262545  all tools enabled
  personastack_544eb62b75647f8c  all tools enabled
  personastack_ae5e947e1d736189  all tools enabled
  ✓ enabled  mcp_personastack-conn-1_persona_chat_reply  PersonaStack reply
`, "personastack-conn-1")
	expected := []string{
		"web",
		"browser",
		"terminal",
		"file",
		"code_execution",
		"vision",
		"image_gen",
		"tts",
		"skills",
		"todo",
		"memory",
		"session_search",
		"clarify",
		"delegation",
		"cronjob",
		"messaging",
		"computer_use",
	}
	got := make([]string, 0, len(capabilities))
	for _, capability := range capabilities {
		got = append(got, capability.CapabilityID)
	}
	if !slices.Equal(got, expected) {
		t.Fatalf("tools = %#v, want %#v", got, expected)
	}
	if capabilities[0].Source != NativeCapabilitySourceHermesToolsList || capabilities[0].Kind != NativeCapabilityKindNativeTool {
		t.Fatalf("unexpected source/kind: %#v", capabilities[0])
	}
	if capabilities[0].Summary != "web (Web Search & Scraping)" {
		t.Fatalf("unexpected web summary: %q", capabilities[0].Summary)
	}
	if capabilities[len(capabilities)-1].Summary != "computer_use (Computer Use (macOS))" {
		t.Fatalf("unexpected computer_use summary: %q", capabilities[len(capabilities)-1].Summary)
	}
}

func withHermesLookPath(t *testing.T, fn func(string) (string, error)) {
	t.Helper()
	previous := hermesLookPath
	hermesLookPath = fn
	t.Cleanup(func() {
		hermesLookPath = previous
	})
}

func TestParseHermesToolsListDedupesAfterBoundingIDs(t *testing.T) {
	prefix := strings.Repeat("a", 96)
	capabilities := parseHermesToolsList(
		"✓ enabled  "+prefix+"first  First\n"+
			"✓ enabled  "+prefix+"second  Second\n",
		"personastack-conn-1",
	)
	if len(capabilities) != 1 {
		t.Fatalf("expected one bounded capability, got %#v", capabilities)
	}
	if capabilities[0].CapabilityID != prefix {
		t.Fatalf("unexpected bounded capability id: %q", capabilities[0].CapabilityID)
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
	if detection.Note != "Hermes API ready with degraded fallback: supports_streaming=false supports_cancel=false" {
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
		if metadata["native_mcp_server"] != "personastack-conn-1" || metadata["native_mcp_namespace"] != "personastack" {
			t.Fatalf("unexpected body: %+v", body)
		}
		if body["include_native_tools"] != true {
			t.Fatalf("unexpected body: %+v", body)
		}
		_, _ = w.Write([]byte(`{"run_id":"hermes-run-1"}`))
	}))
	defer server.Close()

	runID, err := NewHermesAdapter(server.URL, "key-1").StartRun(RunRequest{
		RunID:                  "run-1",
		AssignmentID:           "assignment-1",
		FullyComposedPrompt:    "prompt",
		NativeMCPServerName:    "personastack-conn-1",
		NativeMCPToolNamespace: "personastack",
	})
	if err != nil {
		t.Fatalf("start run: %v", err)
	}
	if runID != "hermes-run-1" {
		t.Fatalf("run id = %q", runID)
	}
}

func TestHermesAdapterStartRunBoundsNativeMCPFields(t *testing.T) {
	longRunID := strings.Repeat("r", maxRunMetadataValueRunes+10)
	longAssignmentID := strings.Repeat("a", maxRunMetadataValueRunes+10)
	longServerName := strings.Repeat("s", maxRunMetadataValueRunes+10)
	longNamespace := strings.Repeat("n", maxRunMetadataValueRunes+10)
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
		if len([]rune(body["session_id"].(string))) != maxRunMetadataValueRunes {
			t.Fatalf("unbounded session_id: %q", body["session_id"])
		}
		if len([]rune(body["conversation"].(string))) != maxRunMetadataValueRunes {
			t.Fatalf("unbounded conversation: %q", body["conversation"])
		}
		if len([]rune(body["native_mcp_server"].(string))) != maxRunMetadataValueRunes {
			t.Fatalf("unbounded native_mcp_server: %q", body["native_mcp_server"])
		}
		if len([]rune(body["native_mcp_namespace"].(string))) != maxRunMetadataValueRunes {
			t.Fatalf("unbounded native_mcp_namespace: %q", body["native_mcp_namespace"])
		}
		if len([]rune(metadata["native_mcp_server"].(string))) != maxRunMetadataValueRunes {
			t.Fatalf("unbounded metadata native_mcp_server: %q", metadata["native_mcp_server"])
		}
		if len([]rune(metadata["native_mcp_namespace"].(string))) != maxRunMetadataValueRunes {
			t.Fatalf("unbounded metadata native_mcp_namespace: %q", metadata["native_mcp_namespace"])
		}
		_, _ = w.Write([]byte(`{"run_id":"hermes-run-1"}`))
	}))
	defer server.Close()

	runID, err := NewHermesAdapter(server.URL, "key-1").StartRun(RunRequest{
		RunID:                  longRunID,
		AssignmentID:           longAssignmentID,
		FullyComposedPrompt:    "prompt",
		NativeMCPServerName:    longServerName,
		NativeMCPToolNamespace: longNamespace,
	})
	if err != nil {
		t.Fatalf("start run: %v", err)
	}
	if runID != "hermes-run-1" {
		t.Fatalf("run id = %q", runID)
	}
}

func TestHermesAdapterFallsBackToResponsesWhenRunsUnavailable(t *testing.T) {
	var deleted bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health", "/v1/models", "/health/detailed":
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		case "/v1/capabilities":
			_, _ = w.Write([]byte(`{"features":{"run_submission":true,"run_status":true,"run_events_sse":true,"run_stop":true}}`))
		case "/v1/runs":
			http.NotFound(w, r)
		case "/v1/responses":
			if r.Method != http.MethodPost {
				http.NotFound(w, r)
				return
			}
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode response fallback body: %v", err)
			}
			if body["conversation"] != "assignment-1" {
				t.Fatalf("unexpected response fallback body: %+v", body)
			}
			_, _ = w.Write([]byte(`{"id":"resp-1","status":"completed","output_text":"done"}`))
		case "/v1/responses/resp-1":
			switch r.Method {
			case http.MethodGet:
				_, _ = w.Write([]byte(`{"id":"resp-1","status":"completed","output_text":"done"}`))
			case http.MethodDelete:
				deleted = true
				w.WriteHeader(http.StatusNoContent)
			default:
				http.NotFound(w, r)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	adapter := NewHermesAdapter(server.URL, "key-1")
	runID, err := adapter.StartRun(RunRequest{
		RunID:               "run-1",
		AssignmentID:        "assignment-1",
		FullyComposedPrompt: "prompt",
	})
	if err != nil {
		t.Fatalf("start run: %v", err)
	}
	if runID != hermesResponsesRunPrefix+"resp-1" {
		t.Fatalf("run id = %q", runID)
	}
	result, err := adapter.WaitRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("wait run: %v", err)
	}
	if result.Status != RunStatusSucceeded || result.Output != "done" {
		t.Fatalf("unexpected result: %+v", result)
	}
	if err := adapter.CancelRun(runID); err != nil {
		t.Fatalf("cancel run: %v", err)
	}
	if !deleted {
		t.Fatalf("expected response delete on cancel")
	}
}

func TestHermesAdapterRejectsNonLoopbackURL(t *testing.T) {
	detection := NewHermesAdapter("http://192.0.2.10:8642", "key-1").Detect()
	if detection.State != AdapterStateRuntimeMissing {
		t.Fatalf("expected runtime missing, got %+v", detection)
	}
	if detection.Note != "Hermes API URL must be loopback" {
		t.Fatalf("unexpected note: %q", detection.Note)
	}
	_, err := NewHermesAdapter("http://192.0.2.10:8642", "key-1").StartRun(RunRequest{FullyComposedPrompt: "prompt"})
	if err == nil {
		t.Fatalf("expected StartRun to reject non-loopback URL")
	}
}

func TestHermesAdapterRejectsRedirectToNonLoopbackURL(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://192.0.2.10/health", http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	detection := NewHermesAdapter(server.URL, "key-1").Detect()
	if detection.State != AdapterStateRuntimeStopped {
		t.Fatalf("expected runtime stopped, got %+v", detection)
	}
}

func TestHermesAdapterDetectReportsHermesSetupDiagnostics(t *testing.T) {
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)
	detection := NewHermesAdapter("http://127.0.0.1:65535", "").Detect()
	if detection.State != AdapterStateRuntimeStopped {
		t.Fatalf("expected runtime stopped, got %+v", detection)
	}
	if !strings.Contains(detection.Note, "API_SERVER_ENABLED") {
		t.Fatalf("expected local Hermes env diagnostics, got %q", detection.Note)
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

func TestHermesAdapterStreamOrPollRunForwardsDeltaAndStarted(t *testing.T) {
	events := []RunEvent{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/runs/hermes-run-1/events":
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("data: {\"type\":\"progress\",\"data\":{\"deltaText\":\"chunk\"}}\n\n"))
		case "/v1/runs/hermes-run-1":
			_ = json.NewEncoder(w).Encode(map[string]string{
				"status": "completed",
				"output": "done",
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	result, err := NewHermesAdapter(server.URL, "key-1").StreamOrPollRun(context.Background(), "hermes-run-1", func(event RunEvent) error {
		events = append(events, event)
		return nil
	})
	if err != nil {
		t.Fatalf("StreamOrPollRun() error = %v", err)
	}
	if result.Status != RunStatusSucceeded || result.Output != "done" {
		t.Fatalf("unexpected result: %+v", result)
	}
	if len(events) != 2 || events[0].Kind != RunEventStarted || events[1].Kind != RunEventOutputDelta || events[1].Delta != "chunk" {
		t.Fatalf("unexpected events: %+v", events)
	}
	if events[0].StartedAt.IsZero() {
		t.Fatalf("started event missing timestamp")
	}
}

func TestHermesAdapterStreamOrPollRunDoesNotDuplicateTerminalOutput(t *testing.T) {
	events := []RunEvent{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/runs/hermes-run-1/events":
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("data: {\"type\":\"run.completed\",\"output\":\"done\"}\n\n"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	result, err := NewHermesAdapter(server.URL, "key-1").StreamOrPollRun(context.Background(), "hermes-run-1", func(event RunEvent) error {
		events = append(events, event)
		return nil
	})
	if err != nil {
		t.Fatalf("StreamOrPollRun() error = %v", err)
	}
	if result.Status != RunStatusSucceeded || result.Output != "done" {
		t.Fatalf("unexpected result: %+v", result)
	}
	if len(events) != 1 || events[0].Kind != RunEventStarted {
		t.Fatalf("unexpected events: %+v", events)
	}
}

func TestHermesAdapterStreamOrPollRunAllowsSlowSSETerminalEvent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/runs/hermes-run-1/events":
			w.Header().Set("Content-Type", "text/event-stream")
			time.Sleep(40 * time.Millisecond)
			_, _ = w.Write([]byte("event: message\n"))
			_, _ = w.Write([]byte(`data: {"type":"run.completed","output":"done"}` + "\n\n"))
		case "/v1/runs/hermes-run-1":
			_ = json.NewEncoder(w).Encode(map[string]string{
				"status": "failed",
				"output": "should-not-poll",
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	adapter := NewHermesAdapter(server.URL, "key-1")
	adapter.Client = &http.Client{Timeout: 10 * time.Millisecond}
	result, err := adapter.StreamOrPollRun(context.Background(), "hermes-run-1", nil)
	if err != nil {
		t.Fatalf("StreamOrPollRun() error = %v", err)
	}
	if result.Status != RunStatusSucceeded || result.Output != "done" {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestHermesAdapterStreamOrPollRunFallsBackWhenSSEReadFails(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/runs/hermes-run-1/events":
			w.Header().Set("Content-Type", "text/event-stream")
			hijacker, ok := w.(http.Hijacker)
			if !ok {
				t.Fatal("response writer does not support hijacking")
			}
			conn, _, err := hijacker.Hijack()
			if err != nil {
				t.Fatalf("hijack: %v", err)
			}
			_, _ = conn.Write([]byte("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\r\n"))
			_ = conn.Close()
		case "/v1/runs/hermes-run-1":
			_ = json.NewEncoder(w).Encode(map[string]string{
				"status": "completed",
				"output": "polled",
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	result, err := NewHermesAdapter(server.URL, "key-1").StreamOrPollRun(context.Background(), "hermes-run-1", nil)
	if err != nil {
		t.Fatalf("StreamOrPollRun() error = %v", err)
	}
	if result.Status != RunStatusSucceeded || result.Output != "polled" {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestHermesAdapterStreamOrPollRunFallsBackWhenSSETransientStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/runs/hermes-run-1/events":
			http.Error(w, "try later", http.StatusTooManyRequests)
		case "/v1/runs/hermes-run-1":
			_ = json.NewEncoder(w).Encode(map[string]string{
				"status": "completed",
				"output": "polled",
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	result, err := NewHermesAdapter(server.URL, "key-1").StreamOrPollRun(context.Background(), "hermes-run-1", nil)
	if err != nil {
		t.Fatalf("StreamOrPollRun() error = %v", err)
	}
	if result.Status != RunStatusSucceeded || result.Output != "polled" {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestHermesAdapterWaitRunMapsTerminalStatuses(t *testing.T) {
	tests := []struct {
		name   string
		status string
		want   RunStatus
		output string
	}{
		{name: "failed", status: "failed", want: RunStatusFailed, output: "boom"},
		{name: "failed output fallback", status: "error", want: RunStatusFailed, output: "output-only"},
		{name: "cancelled", status: "cancelled", want: RunStatusCancelled},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v1/runs/hermes-run-1/events":
					http.NotFound(w, r)
				case "/v1/runs/hermes-run-1":
					_ = json.NewEncoder(w).Encode(map[string]string{
						"status": tt.status,
						"error":  failedErrorForTest(tt.name, tt.output),
						"output": tt.output,
					})
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			result, err := NewHermesAdapter(server.URL, "key-1").WaitRun(context.Background(), "hermes-run-1")
			if err != nil {
				t.Fatalf("WaitRun() error = %v", err)
			}
			if result.Status != tt.want || result.Output != tt.output {
				t.Fatalf("result = %+v", result)
			}
		})
	}
}

func failedErrorForTest(name string, output string) string {
	if name == "failed output fallback" {
		return ""
	}
	return output
}

func TestHermesAdapterCancelRunWaitsForTerminalState(t *testing.T) {
	stopped := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/runs/hermes-run-1/stop":
			stopped = true
			w.WriteHeader(http.StatusAccepted)
		case "/v1/runs/hermes-run-1/events":
			http.NotFound(w, r)
		case "/v1/runs/hermes-run-1":
			if !stopped {
				_, _ = w.Write([]byte(`{"status":"running"}`))
				return
			}
			_, _ = w.Write([]byte(`{"status":"cancelled"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	if err := NewHermesAdapter(server.URL, "key-1").CancelRun("hermes-run-1"); err != nil {
		t.Fatalf("CancelRun() error = %v", err)
	}
	if !stopped {
		t.Fatalf("stop endpoint was not called")
	}
}

func TestHermesAdapterCancelRunPollsStatusWhenEventsStayOpen(t *testing.T) {
	statusPolled := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/runs/hermes-run-1/stop":
			w.WriteHeader(http.StatusAccepted)
		case "/v1/runs/hermes-run-1/events":
			t.Fatalf("cancel should poll status directly instead of opening SSE")
		case "/v1/runs/hermes-run-1":
			statusPolled = true
			_, _ = w.Write([]byte(`{"status":"cancelled"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	if err := NewHermesAdapter(server.URL, "key-1").CancelRun("hermes-run-1"); err != nil {
		t.Fatalf("CancelRun() error = %v", err)
	}
	if !statusPolled {
		t.Fatalf("status endpoint was not polled")
	}
}

func TestHermesAdapterCancelRunIgnoresUnsupportedStopMissingStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/runs/hermes-run-1/stop":
			http.NotFound(w, r)
		case "/v1/runs/hermes-run-1":
			http.NotFound(w, r)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	if err := NewHermesAdapter(server.URL, "key-1").CancelRun("hermes-run-1"); err != nil {
		t.Fatalf("CancelRun() error = %v", err)
	}
}
