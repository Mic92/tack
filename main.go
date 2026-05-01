// Tack is a custom Tangled spindle that translates sh.tangled.pipeline
// trigger records into Buildkite builds, and publishes Buildkite job state
// back as sh.tangled.pipeline.status events on a WebSocket stream that the
// Tangled appview can consume.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	charmlog "github.com/charmbracelet/log"
)

// config is the runtime configuration, sourced from environment variables and
// flags. Env vars match the README so this can be swapped in for the stock
// spindle without surprises.
type config struct {
	Addr         string
	OwnerDID     string
	JetstreamURL string
}

func loadConfig() (config, error) {
	cfg := config{
		Addr:         envOr("TACK_LISTEN_ADDR", ":8080"),
		OwnerDID:     os.Getenv("TACK_OWNER_DID"),
		JetstreamURL: envOr("TACK_JETSTREAM_URL", "wss://jetstream1.us-west.bsky.network/subscribe"),
	}
	addrFlag := flag.String("addr", cfg.Addr, "HTTP listen address (overrides TACK_LISTEN_ADDR)")
	flag.Parse()
	cfg.Addr = *addrFlag

	if cfg.OwnerDID == "" {
		return cfg, errors.New("TACK_OWNER_DID is required")
	}
	return cfg, nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	// Logging setup. charmbracelet/log implements slog.Handler, so we wrap
	// it in slog.New to share the same backend with libraries that expect
	// a *slog.Logger (notably the jetstream client).
	charmHandler := charmlog.NewWithOptions(os.Stderr, charmlog.Options{
		Level:           charmlog.DebugLevel,
		ReportTimestamp: true,
	})
	logger := slog.New(charmHandler)
	slog.SetDefault(logger)

	// Config loading
	cfg, err := loadConfig()
	if err != nil {
		logger.Error("invalid configuration", "err", err)
		os.Exit(2)
	}

	// Root context: cancelled on SIGINT/SIGTERM, with the logger attached
	// so any function we hand it to can pull it back out via loggerFrom.
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt, syscall.SIGTERM,
	)
	defer stop()
	ctx = loggerInto(ctx, logger)

	// Start the JetStream listener in the background.
	if err := startJetstream(ctx, cfg); err != nil {
		logger.Error("failed to start jetstream consumer", "err", err)
		os.Exit(1)
	}

	// Run the HTTP server. This blocks until ctx is cancelled or the
	// listener errors.
	if err := runHTTP(ctx, cfg); err != nil {
		logger.Error("http server error", "err", err)
		os.Exit(1)
	}
}
