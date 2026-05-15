package diagnostics

import (
	"strings"
	"testing"
)

func TestRedactDiagnostics(t *testing.T) {
	input := "Bearer abc.def token=secret prompt=hello path=/tmp/personastack PERSONASTACK_CONNECTOR_LOCAL_MCP_CONN_1=token-1 port=23119 ws://127.0.0.1:18789/socket"
	got := NewRedactor().Redact(input)

	for _, leaked := range []string{"abc.def", "secret", "hello", "/Users/eg", "127.0.0.1:18789", "token-1", "23119", "/tmp/personastack"} {
		if strings.Contains(got, leaked) {
			t.Fatalf("redacted output leaked %q: %s", leaked, got)
		}
	}
	for _, marker := range []string{"[REDACTED]", "[LOCAL_PATH]", "[LOCAL_ENDPOINT]"} {
		if !strings.Contains(got, marker) {
			t.Fatalf("redacted output missing %q: %s", marker, got)
		}
	}
}
