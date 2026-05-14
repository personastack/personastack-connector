package mcp

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/personastack/personastack-connector/internal/config"
)

var ErrMissingBinding = errors.New("missing binding")

type StdioProxy struct {
	store config.Store
}

func NewStdioProxy(store config.Store) StdioProxy {
	return StdioProxy{store: store}
}

func (proxy StdioProxy) Serve(ctx context.Context, bindingID config.ConnectionID, stdin io.Reader, stdout io.Writer, stderr io.Writer) error {
	_, ok := proxy.store.Binding(bindingID)
	if !ok {
		return fmt.Errorf("mcp stdio binding %q: %w", bindingID, ErrMissingBinding)
	}

	_, err := fmt.Fprintln(stderr, "PersonaStack MCP stdio proxy is not implemented in this scaffold")
	if err != nil {
		return fmt.Errorf("write mcp stdio placeholder: %w", err)
	}

	select {
	case <-ctx.Done():
		return fmt.Errorf("mcp stdio stopped: %w", ctx.Err())
	default:
		return nil
	}
}
