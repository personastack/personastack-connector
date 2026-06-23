package runtime

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const openClawSetupRetryBudget = 30 * time.Second
const maxOpenClawTrajectoryToolSummaryRunes = 8192

type openClawRetryableError struct {
	info openClawErrorInfo
}

func (err openClawRetryableError) Error() string {
	if strings.TrimSpace(err.info.Message) != "" {
		return err.info.Message
	}
	return "OpenClaw startup-sidecars retryable error"
}

func (err openClawRetryableError) RetryAfter() time.Duration {
	if err.info.RetryAfter > 0 {
		return err.info.RetryAfter
	}
	return time.Second
}

type openClawErrorInfo struct {
	Code       string
	Reason     string
	Message    string
	RetryAfter time.Duration
}

func (info openClawErrorInfo) retryableStartupSidecars() bool {
	return strings.EqualFold(strings.TrimSpace(info.Code), "UNAVAILABLE") && strings.EqualFold(strings.TrimSpace(info.Reason), "startup-sidecars")
}

func (adapter OpenClawAdapter) connectOperatorWithRetry(ctx context.Context) (*websocket.Conn, error) {
	deadline := time.Now().Add(openClawSetupRetryBudget)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	for {
		conn, err := adapter.dial(ctx)
		if err != nil {
			return nil, err
		}
		if err := adapter.connectOperator(conn); err != nil {
			var retryErr openClawRetryableError
			if errorsAsOpenClawRetryable(err, &retryErr) {
				conn.Close()
				wait := retryErr.RetryAfter()
				if time.Now().Add(wait).After(deadline) {
					return nil, err
				}
				timer := time.NewTimer(wait)
				select {
				case <-ctx.Done():
					timer.Stop()
					conn.Close()
					return nil, ctx.Err()
				case <-timer.C:
					conn.Close()
					continue
				}
			}
			conn.Close()
			return nil, err
		}
		return conn, nil
	}
}

func errorsAsOpenClawRetryable(err error, target *openClawRetryableError) bool {
	if err == nil || target == nil {
		return false
	}
	return errors.As(err, target)
}

func (adapter OpenClawAdapter) openClawStreamOrPollRun(ctx context.Context, nativeRunID string, handle RunEventHandler) (RunResult, error) {
	if err := ctx.Err(); err != nil {
		return RunResult{}, err
	}
	if cached, ok := adapter.loadFallbackResult(nativeRunID); ok {
		if err := (&runEventState{}).emitStarted(handle, time.Time{}); err != nil {
			return RunResult{}, err
		}
		return cached.result, nil
	}
	deadline := time.Now().Add(openClawSetupRetryBudget)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	conn, err := adapter.connectOperatorWithRetry(ctx)
	if err != nil {
		return RunResult{}, err
	}
	defer conn.Close()
	session := newOpenClawRPCSession(conn, handle)
	defer session.close()
	for {
		if err := ctx.Err(); err != nil {
			return RunResult{}, err
		}
		setOpenClawDeadline(conn, ctx, 35*time.Second)
		request := openClawRequest{
			Type:   "req",
			ID:     "wait-" + strings.TrimSpace(nativeRunID),
			Method: "agent.wait",
			Params: map[string]any{
				"runId":     strings.TrimSpace(nativeRunID),
				"timeoutMs": 30000,
			},
		}
		response, err := session.call(ctx, request)
		if err != nil {
			if openClawErrorIsTimeout(err.Error()) && strings.TrimSpace(session.output()) != "" {
				if err := adapter.emitOpenClawTrajectoryToolEvents(ctx, nativeRunID, handle, session); err != nil {
					return RunResult{}, err
				}
				return RunResult{Status: RunStatusSucceeded, Output: session.output()}, nil
			}
			if openClawErrorIsTimeout(err.Error()) && ctx.Err() == nil {
				continue
			}
			var retryErr openClawRetryableError
			if errorsAsOpenClawRetryable(err, &retryErr) && ctx.Err() == nil {
				wait := retryErr.RetryAfter()
				if time.Now().Add(wait).After(deadline) {
					return RunResult{}, err
				}
				timer := time.NewTimer(wait)
				select {
				case <-ctx.Done():
					timer.Stop()
					return RunResult{}, ctx.Err()
				case <-timer.C:
					continue
				}
			}
			return RunResult{}, err
		}
		if errText := response.errorString(); errText != "" {
			if openClawErrorIsTimeout(errText) && strings.TrimSpace(session.output()) != "" {
				if err := adapter.emitOpenClawTrajectoryToolEvents(ctx, nativeRunID, handle, session); err != nil {
					return RunResult{}, err
				}
				return RunResult{Status: RunStatusSucceeded, Output: session.output()}, nil
			}
			if openClawErrorIsTimeout(errText) && ctx.Err() == nil {
				continue
			}
			if retryInfo := response.errorInfo(); retryInfo.retryableStartupSidecars() && ctx.Err() == nil {
				wait := retryInfo.RetryAfter
				if wait <= 0 {
					wait = time.Second
				}
				if time.Now().Add(wait).After(deadline) {
					return RunResult{}, fmt.Errorf("OpenClaw wait error: %s", errText)
				}
				timer := time.NewTimer(wait)
				select {
				case <-ctx.Done():
					timer.Stop()
					return RunResult{}, ctx.Err()
				case <-timer.C:
					continue
				}
			}
			return RunResult{}, fmt.Errorf("OpenClaw wait error: %s", errText)
		}
		if !response.isResponseOK() {
			return RunResult{}, fmt.Errorf("OpenClaw wait response not ok")
		}
		result, terminal := openClawRunResultFromResponse(response.payload())
		if !terminal {
			if output := strings.TrimSpace(session.output()); output != "" {
				if err := session.emitStarted(handle, openClawRunStartedAtOrNow(response.payload())); err != nil {
					return RunResult{}, err
				}
				if err := adapter.emitOpenClawTrajectoryToolEvents(ctx, nativeRunID, handle, session); err != nil {
					return RunResult{}, err
				}
				return RunResult{Status: RunStatusSucceeded, Output: output}, nil
			}
			if err := ctx.Err(); err != nil {
				return RunResult{}, err
			}
			continue
		}
		if err := session.emitStarted(handle, openClawRunStartedAtOrNow(response.payload())); err != nil {
			return RunResult{}, err
		}
		switch strings.ToLower(strings.TrimSpace(result.Status)) {
		case "failed", "error":
			if err := adapter.emitOpenClawTrajectoryToolEvents(ctx, nativeRunID, handle, session); err != nil {
				return RunResult{}, err
			}
			return RunResult{Status: RunStatusFailed, Output: strings.TrimSpace(firstNonEmpty(result.Error, result.Output))}, nil
		case "cancelled", "canceled", "aborted":
			return RunResult{Status: RunStatusCancelled}, nil
		case "timeout":
			continue
		default:
			if err := adapter.emitOpenClawTrajectoryToolEvents(ctx, nativeRunID, handle, session); err != nil {
				return RunResult{}, err
			}
			return RunResult{Status: RunStatusSucceeded, Output: strings.TrimSpace(result.Output)}, nil
		}
	}
}

type openClawRPCSession struct {
	conn          *websocket.Conn
	writeMu       sync.Mutex
	pendingMu     sync.Mutex
	pending       map[string]chan openClawResponse
	closed        chan struct{}
	errMu         sync.Mutex
	err           error
	handle        RunEventHandler
	startedMu     sync.Mutex
	started       bool
	outputMu      sync.Mutex
	outputBuilder strings.Builder
	toolMu        sync.Mutex
	toolEvents    int
	toolEventKeys map[string]struct{}
}

func newOpenClawRPCSession(conn *websocket.Conn, handle RunEventHandler) *openClawRPCSession {
	session := &openClawRPCSession{
		conn:    conn,
		pending: map[string]chan openClawResponse{},
		closed:  make(chan struct{}),
		handle:  handle,
	}
	go session.readLoop()
	return session
}

func (session *openClawRPCSession) close() {
	if session == nil {
		return
	}
	_ = session.conn.Close()
	<-session.closed
}

func (session *openClawRPCSession) call(ctx context.Context, request openClawRequest) (openClawResponse, error) {
	if session == nil {
		return openClawResponse{}, fmt.Errorf("OpenClaw session required")
	}
	responseCh := make(chan openClawResponse, 1)
	session.pendingMu.Lock()
	if session.err != nil {
		err := session.err
		session.pendingMu.Unlock()
		return openClawResponse{}, err
	}
	session.pending[request.ID] = responseCh
	session.pendingMu.Unlock()
	if err := session.writeJSON(request); err != nil {
		session.removePending(request.ID)
		return openClawResponse{}, err
	}
	select {
	case response, ok := <-responseCh:
		if !ok {
			return openClawResponse{}, session.currentError()
		}
		return response, nil
	case <-ctx.Done():
		session.removePending(request.ID)
		return openClawResponse{}, ctx.Err()
	case <-session.closed:
		session.removePending(request.ID)
		select {
		case response, ok := <-responseCh:
			if ok {
				return response, nil
			}
		default:
		}
		if err := session.currentError(); err != nil {
			return openClawResponse{}, err
		}
		return openClawResponse{}, fmt.Errorf("OpenClaw connection closed")
	}
}

func (session *openClawRPCSession) writeJSON(request openClawRequest) error {
	session.writeMu.Lock()
	defer session.writeMu.Unlock()
	return session.conn.WriteJSON(request)
}

func (session *openClawRPCSession) removePending(id string) {
	session.pendingMu.Lock()
	delete(session.pending, id)
	session.pendingMu.Unlock()
}

func (session *openClawRPCSession) currentError() error {
	session.errMu.Lock()
	defer session.errMu.Unlock()
	return session.err
}

func (session *openClawRPCSession) fail(err error) {
	session.errMu.Lock()
	if session.err == nil {
		session.err = err
	}
	session.errMu.Unlock()
	close(session.closed)
	session.pendingMu.Lock()
	for id, ch := range session.pending {
		delete(session.pending, id)
		close(ch)
	}
	session.pendingMu.Unlock()
}

func (session *openClawRPCSession) readLoop() {
	defer func() {
		if session.currentError() == nil {
			close(session.closed)
		}
	}()
	for {
		var response openClawResponse
		if err := session.conn.ReadJSON(&response); err != nil {
			session.fail(err)
			return
		}
		if response.ID != "" {
			session.pendingMu.Lock()
			ch, ok := session.pending[response.ID]
			if ok {
				delete(session.pending, response.ID)
			}
			session.pendingMu.Unlock()
			if ok {
				ch <- response
				close(ch)
				continue
			}
		}
		if response.Type == "event" || response.Event != "" || response.Method != "" {
			if err := session.handleBroadcast(response); err != nil {
				session.fail(err)
				return
			}
		}
	}
}

func (session *openClawRPCSession) handleBroadcast(response openClawResponse) error {
	handle := session.handle
	if handle == nil {
		return nil
	}
	startedAt, events, ok := openClawRunEventsForBroadcast(response)
	if !ok {
		return nil
	}
	if err := session.emitStarted(handle, startedAt); err != nil {
		return err
	}
	for _, event := range events {
		if event.Kind == RunEventOutputDelta && strings.TrimSpace(event.Delta) != "" {
			session.appendOutput(event.Delta)
		}
		if event.Kind == RunEventToolEvent {
			if !session.markToolEvent(event) {
				continue
			}
		}
		if err := handle(event); err != nil {
			return err
		}
	}
	return nil
}

func (session *openClawRPCSession) appendOutput(delta string) {
	session.outputMu.Lock()
	defer session.outputMu.Unlock()
	if session.outputBuilder.Len() > 0 {
		session.outputBuilder.WriteString("\n")
	}
	session.outputBuilder.WriteString(strings.TrimSpace(delta))
}

func (session *openClawRPCSession) output() string {
	session.outputMu.Lock()
	defer session.outputMu.Unlock()
	return strings.TrimSpace(session.outputBuilder.String())
}

func (session *openClawRPCSession) markToolEvent(event RunEvent) bool {
	session.toolMu.Lock()
	defer session.toolMu.Unlock()
	key := openClawToolEventKey(event)
	if session.toolEventKeys == nil {
		session.toolEventKeys = map[string]struct{}{}
	}
	if _, ok := session.toolEventKeys[key]; ok {
		return false
	}
	session.toolEventKeys[key] = struct{}{}
	session.toolEvents++
	return true
}

func (session *openClawRPCSession) hasToolEvents() bool {
	session.toolMu.Lock()
	defer session.toolMu.Unlock()
	return session.toolEvents > 0
}

func (session *openClawRPCSession) emitStarted(handle RunEventHandler, startedAt time.Time) error {
	if handle == nil {
		return nil
	}
	session.startedMu.Lock()
	if session.started {
		session.startedMu.Unlock()
		return nil
	}
	session.started = true
	session.startedMu.Unlock()
	if startedAt.IsZero() {
		startedAt = time.Now().UTC()
	}
	return handle(RunEvent{Kind: RunEventStarted, StartedAt: startedAt})
}

func openClawRunEventsForBroadcast(response openClawResponse) (time.Time, []RunEvent, bool) {
	eventName := strings.ToLower(strings.TrimSpace(firstNonEmpty(response.Event, response.Method, response.Type)))
	if eventName == "" {
		return time.Time{}, nil, false
	}
	payload := response.payload()
	startedAt, hasStartedAt := openClawRunStartedAt(payload)
	events := []RunEvent{}
	switch eventName {
	case "agent", "session.message", "session.tool", "chat":
		if delta := openClawJSONString(payload, "deltaText", "delta", "text", "message"); delta != "" {
			events = append(events, RunEvent{Kind: RunEventOutputDelta, Delta: delta})
		}
		if toolName := openClawJSONString(payload, "toolName", "tool", "name"); toolName != "" {
			events = append(events, RunEvent{
				Kind:       RunEventToolEvent,
				ToolName:   toolName,
				ToolPhase:  openClawJSONString(payload, "phase", "status"),
				ToolCallID: openClawJSONString(payload, "toolCallId", "tool_call_id"),
				Summary:    openClawJSONString(payload, "summary", "message", "text"),
			})
		}
		if eventName == "agent" || len(events) > 0 || hasStartedAt {
			return startedAt, events, true
		}
	}
	return time.Time{}, nil, false
}

func (adapter OpenClawAdapter) emitOpenClawTrajectoryToolEvents(ctx context.Context, nativeRunID string, handle RunEventHandler, session *openClawRPCSession) error {
	if handle == nil || strings.TrimSpace(nativeRunID) == "" || session == nil {
		return nil
	}
	events, err := adapter.openClawTrajectoryToolEvents(ctx, nativeRunID)
	if err != nil || len(events) == 0 {
		return nil
	}
	if err := session.emitStarted(handle, time.Now().UTC()); err != nil {
		return err
	}
	for _, event := range events {
		if !session.markToolEvent(event) {
			continue
		}
		if err := handle(event); err != nil {
			return err
		}
	}
	return nil
}

func (adapter OpenClawAdapter) openClawTrajectoryToolEvents(ctx context.Context, nativeRunID string) ([]RunEvent, error) {
	paths, err := adapter.openClawTrajectoryFiles()
	if err != nil || len(paths) == 0 {
		return nil, err
	}
	events := []RunEvent{}
	seen := map[string]struct{}{}
	for _, path := range paths {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		fileEvents, err := openClawTrajectoryToolEventsFromFile(path, nativeRunID)
		if err != nil {
			continue
		}
		for _, event := range fileEvents {
			key := openClawToolEventKey(event)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			events = append(events, event)
		}
	}
	return events, nil
}

func (adapter OpenClawAdapter) openClawTrajectoryFiles() ([]string, error) {
	agentID := strings.TrimSpace(adapter.AgentID)
	if agentID == "" {
		agentID = "main"
	}
	dirs := []string{}
	if value := strings.TrimSpace(os.Getenv("OPENCLAW_TRAJECTORY_DIR")); value != "" {
		dirs = append(dirs, value)
	}
	sessionDir := filepath.Join(openClawStateDir(), "agents", agentID, "sessions")
	dirs = append(dirs, sessionDir)
	filesByPath := map[string]trajectoryFile{}
	for _, dir := range dirs {
		files, err := openClawTrajectoryFilesInDir(dir)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, err
		}
		for _, file := range files {
			filesByPath[file.path] = file
		}
	}
	pointerFiles, err := filepath.Glob(filepath.Join(sessionDir, "*.trajectory-path.json"))
	if err != nil {
		return nil, err
	}
	for _, pointerFile := range pointerFiles {
		path := openClawTrajectoryPathFromPointer(pointerFile)
		if path == "" {
			continue
		}
		info, err := os.Stat(path)
		if err != nil || info.IsDir() {
			continue
		}
		filesByPath[path] = trajectoryFile{path: path, modTime: info.ModTime()}
	}
	files := []trajectoryFile{}
	for _, file := range filesByPath {
		files = append(files, file)
	}
	sort.Slice(files, func(i, j int) bool {
		return files[i].modTime.After(files[j].modTime)
	})
	if len(files) > 16 {
		files = files[:16]
	}
	out := make([]string, 0, len(files))
	for _, file := range files {
		out = append(out, file.path)
	}
	return out, nil
}

type trajectoryFile struct {
	path    string
	modTime time.Time
}

func openClawTrajectoryFilesInDir(dir string) ([]trajectoryFile, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	files := []trajectoryFile{}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".trajectory.jsonl") {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return nil, err
		}
		files = append(files, trajectoryFile{path: filepath.Join(dir, entry.Name()), modTime: info.ModTime()})
	}
	return files, nil
}

func openClawTrajectoryPathFromPointer(path string) string {
	raw, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	var pointer map[string]json.RawMessage
	if err := json.Unmarshal(raw, &pointer); err != nil {
		return ""
	}
	for _, key := range []string{"path", "trajectoryPath", "trajectory_path", "file", "jsonl"} {
		value, ok := pointer[key]
		if !ok {
			continue
		}
		var text string
		if err := json.Unmarshal(value, &text); err == nil && strings.TrimSpace(text) != "" {
			return strings.TrimSpace(text)
		}
	}
	return ""
}

func openClawStateDir() string {
	if value := strings.TrimSpace(os.Getenv("OPENCLAW_HOME")); value != "" {
		return filepath.Join(value, ".openclaw")
	}
	homeDir, err := os.UserHomeDir()
	if err != nil || strings.TrimSpace(homeDir) == "" {
		return ".openclaw"
	}
	return filepath.Join(homeDir, ".openclaw")
}

func openClawTrajectoryToolEventsFromFile(path string, nativeRunID string) ([]RunEvent, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	events := []RunEvent{}
	for scanner.Scan() {
		event, ok := openClawTrajectoryToolEvent(scanner.Bytes(), nativeRunID)
		if ok {
			events = append(events, event)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return events, nil
}

func openClawTrajectoryToolEvent(raw []byte, nativeRunID string) (RunEvent, bool) {
	var event struct {
		Type  string `json:"type"`
		RunID string `json:"runId"`
		Data  struct {
			Name            string          `json:"name"`
			ToolCallID      string          `json:"toolCallId"`
			ToolCallIDSnake string          `json:"tool_call_id"`
			Arguments       json.RawMessage `json:"arguments"`
			Status          string          `json:"status"`
			IsError         bool            `json:"isError"`
			Result          json.RawMessage `json:"result"`
			Output          string          `json:"output"`
			Error           string          `json:"error"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &event); err != nil {
		return RunEvent{}, false
	}
	if strings.TrimSpace(event.RunID) != strings.TrimSpace(nativeRunID) {
		return RunEvent{}, false
	}
	toolName := strings.TrimSpace(event.Data.Name)
	if toolName == "" {
		return RunEvent{}, false
	}
	toolCallID := strings.TrimSpace(firstNonEmpty(event.Data.ToolCallID, event.Data.ToolCallIDSnake))
	switch strings.ToLower(strings.TrimSpace(event.Type)) {
	case "tool.call":
		return RunEvent{
			Kind:       RunEventToolEvent,
			ToolName:   toolName,
			ToolPhase:  "started",
			ToolCallID: toolCallID,
			Summary:    openClawTrajectoryJSONKeysSummary(event.Data.Arguments),
		}, true
	case "tool.result":
		phase := "completed"
		if event.Data.IsError || strings.EqualFold(strings.TrimSpace(event.Data.Status), "failed") || strings.EqualFold(strings.TrimSpace(event.Data.Status), "error") {
			phase = "failed"
		}
		summary := openClawTrajectoryJSONKeysSummary(event.Data.Result)
		if summary == "" && event.Data.Output != "" {
			summary = "output"
		}
		if summary == "" && event.Data.Error != "" {
			summary = "error"
		}
		return RunEvent{
			Kind:       RunEventToolEvent,
			ToolName:   toolName,
			ToolPhase:  phase,
			ToolCallID: toolCallID,
			Summary:    summary,
		}, true
	default:
		return RunEvent{}, false
	}
}

func openClawToolEventKey(event RunEvent) string {
	if toolCallID := strings.TrimSpace(event.ToolCallID); toolCallID != "" {
		return strings.Join([]string{"id", toolCallID, event.ToolPhase}, "\x00")
	}
	return strings.Join([]string{"summary", event.ToolName, event.ToolPhase, event.Summary}, "\x00")
}

func openClawTrajectoryJSONKeysSummary(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil || len(object) == 0 {
		return ""
	}
	summary := make(map[string]bool, len(object))
	for key := range object {
		summary[key] = true
	}
	encoded, err := json.Marshal(summary)
	if err != nil {
		return ""
	}
	return boundedOpenClawTrajectoryToolSummary(string(encoded))
}

func boundedOpenClawTrajectoryToolSummary(value string) string {
	runes := []rune(strings.TrimSpace(value))
	if len(runes) <= maxOpenClawTrajectoryToolSummaryRunes {
		return string(runes)
	}
	return string(runes[:maxOpenClawTrajectoryToolSummaryRunes]) + "..."
}

func openClawJSONString(raw json.RawMessage, names ...string) string {
	if len(raw) == 0 {
		return ""
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return ""
	}
	for _, name := range names {
		if value, ok := envelope[name]; ok {
			var text string
			if err := json.Unmarshal(value, &text); err == nil {
				return strings.TrimSpace(text)
			}
		}
	}
	return ""
}

func openClawRunStartedAt(raw json.RawMessage) (time.Time, bool) {
	if len(raw) == 0 {
		return time.Time{}, false
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return time.Time{}, false
	}
	for _, name := range []string{"startedAt", "started_at", "startedAtMs", "started_at_ms"} {
		if value, ok := envelope[name]; ok {
			if startedAt, ok := parseOpenClawJSONTime(value); ok {
				return startedAt, true
			}
		}
	}
	return time.Time{}, false
}

func openClawRunStartedAtOrNow(raw json.RawMessage) time.Time {
	if startedAt, ok := openClawRunStartedAt(raw); ok {
		return startedAt
	}
	return time.Now().UTC()
}

func parseOpenClawJSONTime(raw json.RawMessage) (time.Time, bool) {
	if len(raw) == 0 {
		return time.Time{}, false
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		if parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(text)); err == nil {
			return parsed.UTC(), true
		}
	}
	var millis int64
	if err := json.Unmarshal(raw, &millis); err == nil {
		return time.UnixMilli(millis).UTC(), true
	}
	var value float64
	if err := json.Unmarshal(raw, &value); err == nil {
		return time.UnixMilli(int64(value)).UTC(), true
	}
	return time.Time{}, false
}

func (response openClawResponse) errorInfo() openClawErrorInfo {
	switch value := response.Error.(type) {
	case nil:
		return openClawErrorInfo{}
	case string:
		return openClawErrorInfo{Message: strings.TrimSpace(value)}
	case map[string]any:
		return openClawErrorInfoFromMap(value)
	default:
		return openClawErrorInfo{Message: strings.TrimSpace(fmt.Sprint(response.Error))}
	}
}

func openClawErrorInfoFromMap(value map[string]any) openClawErrorInfo {
	info := openClawErrorInfo{}
	info.Code = openClawErrorString(value, "code", "errorCode")
	info.Message = openClawErrorString(value, "message", "errorMessage", "reason")
	info.Reason = openClawErrorString(value, "reason")
	info.RetryAfter = time.Duration(openClawErrorInt(value, "retryAfterMs")) * time.Millisecond
	if details, ok := value["details"].(map[string]any); ok {
		if info.Code == "" {
			info.Code = openClawErrorString(details, "code", "errorCode")
		}
		if info.Message == "" {
			info.Message = openClawErrorString(details, "message", "errorMessage", "reason")
		}
		if info.Reason == "" {
			info.Reason = openClawErrorString(details, "reason")
		}
		if info.RetryAfter == 0 {
			info.RetryAfter = time.Duration(openClawErrorInt(details, "retryAfterMs")) * time.Millisecond
		}
	}
	if strings.EqualFold(strings.TrimSpace(info.Code), "UNAVAILABLE") && strings.TrimSpace(info.Message) == "" {
		info.Message = "OpenClaw unavailable"
	}
	return info
}

func openClawErrorString(value map[string]any, names ...string) string {
	for _, name := range names {
		if raw, ok := value[name]; ok {
			if text := strings.TrimSpace(fmt.Sprint(raw)); text != "" {
				return text
			}
		}
	}
	return ""
}

func openClawErrorInt(value map[string]any, names ...string) int64 {
	for _, name := range names {
		if raw, ok := value[name]; ok {
			switch typed := raw.(type) {
			case int:
				return int64(typed)
			case int64:
				return typed
			case float64:
				return int64(typed)
			case json.Number:
				if parsed, err := typed.Int64(); err == nil {
					return parsed
				}
			}
		}
	}
	return 0
}

func (response openClawResponse) retryableStartupSidecars() (time.Duration, bool) {
	info := response.errorInfo()
	if !info.retryableStartupSidecars() {
		return 0, false
	}
	if info.RetryAfter <= 0 {
		info.RetryAfter = time.Second
	}
	return info.RetryAfter, true
}
