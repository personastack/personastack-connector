package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/personastack/personastack-connector/internal/mcp"
)

func TestRunHelp(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	err := Run(context.Background(), []string{"--help"}, strings.NewReader(""), &stdout, &stderr)
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	output := stdout.String()
	for _, want := range []string{"pair <code>", "runtime detect", "mcp stdio --binding", "service plan", "run --foreground", "version"} {
		if !strings.Contains(output, want) {
			t.Fatalf("help output missing %q: %s", want, output)
		}
	}
}

func TestRunVersion(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := Run(context.Background(), []string{"version"}, strings.NewReader(""), &stdout, &stderr)
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if !strings.Contains(stdout.String(), "personastack-connector version=") {
		t.Fatalf("unexpected version output: %s", stdout.String())
	}
}

func TestMCPStdioMissingBinding(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	err := Run(context.Background(), []string{"mcp", "stdio", "--binding", "fake"}, strings.NewReader(""), &stdout, &stderr)
	if !errors.Is(err, mcp.ErrMissingBinding) {
		t.Fatalf("Run error = %v, want ErrMissingBinding", err)
	}
}

func TestRunServicePlan(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	err := Run(context.Background(), []string{"service", "plan"}, strings.NewReader(""), &stdout, &stderr)
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if !strings.Contains(stdout.String(), "service plan kind=") {
		t.Fatalf("unexpected service plan output: %s", stdout.String())
	}
}
