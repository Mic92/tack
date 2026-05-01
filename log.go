package main

// Tiny helpers for stashing a *slog.Logger on a context.Context. The stdlib
// doesn't define a context key for slog, so by convention every app rolls
// its own — the unexported key type ensures we can't collide with anyone
// else's value, and FromContext falls back to slog.Default() so callers
// don't need to special-case a missing logger.

import (
	"context"
	"log/slog"
)

// loggerCtxKey is unexported so only this package can write to the slot.
type loggerCtxKey struct{}

// loggerInto returns a copy of ctx that carries logger.
func loggerInto(ctx context.Context, logger *slog.Logger) context.Context {
	return context.WithValue(ctx, loggerCtxKey{}, logger)
}

// loggerFrom retrieves the logger attached with loggerInto, or slog.Default
// if none has been set.
func loggerFrom(ctx context.Context) *slog.Logger {
	if ctx == nil {
		return slog.Default()
	}
	if l, ok := ctx.Value(loggerCtxKey{}).(*slog.Logger); ok {
		return l
	}
	return slog.Default()
}
