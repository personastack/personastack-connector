package hermessetup

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type envState struct {
	lines []envLine
	index map[string]int
}

type envLine struct {
	raw     string
	key     string
	value   string
	setting bool
}

func loadEnvState(path string) (envState, error) {
	state := envState{
		lines: []envLine{},
		index: map[string]int{},
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return state, nil
		}
		return envState{}, fmt.Errorf("read Hermes env: %w", err)
	}
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	for scanner.Scan() {
		line := scanner.Text()
		entry := parseEnvLine(line)
		if entry.setting {
			state.index[entry.key] = len(state.lines)
		}
		state.lines = append(state.lines, entry)
	}
	if err := scanner.Err(); err != nil {
		return envState{}, fmt.Errorf("scan Hermes env: %w", err)
	}
	return state, nil
}

func parseEnvLine(line string) envLine {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" || strings.HasPrefix(trimmed, "#") {
		return envLine{raw: line}
	}
	if strings.HasPrefix(trimmed, "export ") {
		trimmed = strings.TrimSpace(strings.TrimPrefix(trimmed, "export "))
	}
	key, value, ok := strings.Cut(trimmed, "=")
	if !ok {
		return envLine{raw: line}
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return envLine{raw: line}
	}
	return envLine{
		raw:     line,
		key:     key,
		value:   value,
		setting: true,
	}
}

func (state *envState) upsert(values map[string]string) {
	if state.index == nil {
		state.index = map[string]int{}
	}
	for key, value := range values {
		if index, ok := state.index[key]; ok {
			state.lines[index].value = value
			state.lines[index].raw = ""
			continue
		}
		state.index[key] = len(state.lines)
		state.lines = append(state.lines, envLine{
			key:     key,
			value:   value,
			setting: true,
		})
	}
}

func (state envState) write() []byte {
	var builder strings.Builder
	for _, line := range state.lines {
		if !line.setting {
			builder.WriteString(line.raw)
			builder.WriteByte('\n')
			continue
		}
		builder.WriteString(line.key)
		builder.WriteByte('=')
		builder.WriteString(line.value)
		builder.WriteByte('\n')
	}
	return []byte(builder.String())
}

func (state envState) matches(values map[string]string) bool {
	for key, value := range values {
		if strings.TrimSpace(stateValue(state, key)) != strings.TrimSpace(value) {
			return false
		}
	}
	return true
}

func stateValue(state envState, key string) string {
	index, ok := state.index[key]
	if !ok || index >= len(state.lines) {
		return ""
	}
	return state.lines[index].value
}

func ensureOwnerOnlyFile(path string, raw []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create Hermes dir: %w", err)
	}
	return writeFileAtomic(path, raw, 0o600)
}

func writeFileAtomic(path string, raw []byte, mode os.FileMode) error {
	temp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create temp Hermes file: %w", err)
	}
	tempPath := temp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tempPath)
		}
	}()
	if err := temp.Chmod(mode); err != nil {
		_ = temp.Close()
		return fmt.Errorf("secure temp Hermes file: %w", err)
	}
	if _, err := temp.Write(raw); err != nil {
		_ = temp.Close()
		return fmt.Errorf("write temp Hermes file: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close temp Hermes file: %w", err)
	}
	if err := replaceFile(tempPath, path); err != nil {
		return fmt.Errorf("replace Hermes file: %w", err)
	}
	cleanup = false
	return nil
}

func replaceFile(tempPath string, path string) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return os.Rename(tempPath, path)
}
