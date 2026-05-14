package diagnostics

import (
	"strings"
	"testing"
)

func TestRedactDiagnostics(t *testing.T) {
	input := "Bearer abc.def token=secret prompt=hello /Users/eg/.config ws://127.0.0.1:18789/socket"
	got := NewRedactor().Redact(input)

	for _, leaked := range []string{"abc.def", "secret", "hello", "/Users/eg", "127.0.0.1:18789"} {
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
