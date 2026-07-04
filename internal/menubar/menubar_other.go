//go:build !darwin

package menubar

import "context"

func Start(_ context.Context, _ Options) Controller {
	return Noop()
}

func RunWithController(ctx context.Context, _ Options, run RunFunc) error {
	return run(ctx, Noop())
}
