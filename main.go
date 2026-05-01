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
	Hostname     string
	OwnerDID     string
	JetstreamURL string
	DBPath       string
	// Dev flips the knot event-stream scheme from wss:// to ws://.
	// Useful when running against a local knot during development.
	Dev bool
}

func loadConfig() (config, error) {
	cfg := config{
		Addr:         envOr("TACK_LISTEN_ADDR", ":8080"),
		Hostname:     os.Getenv("TACK_HOSTNAME"),
		OwnerDID:     os.Getenv("TACK_OWNER_DID"),
		JetstreamURL: envOr("TACK_JETSTREAM_URL", "wss://jetstream1.us-west.bsky.network/subscribe"),
		DBPath:       envOr("TACK_DB_PATH", "tack.db"),
		Dev:          os.Getenv("TACK_DEV") != "",
	}
	addrFlag := flag.String("addr", cfg.Addr, "HTTP listen address (overrides TACK_LISTEN_ADDR)")
	flag.Parse()
	cfg.Addr = *addrFlag

	if cfg.OwnerDID == "" {
		return cfg, errors.New("TACK_OWNER_DID is required")
	}

	// Hostname identifies *us* in sh.tangled.repo records (the .spindle
	// field). Without it we have no way to know which repos point at us
	// and therefore which knots we should subscribe to for pipeline
	// triggers — so we refuse to start rather than silently subscribe to
	// nothing.
	if cfg.Hostname == "" {
		return cfg, errors.New("TACK_HOSTNAME is required")
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

	// Open (or create) the SQLite store. Holds jetstream cursor +
	// observed Tangled membership records. Closed last during shutdown
	// so anything writing to it during teardown still succeeds.
	st, err := openStore(cfg.DBPath)
	if err != nil {
		logger.Error("failed to open store", "err", err, "path", cfg.DBPath)
		os.Exit(1)
	}
	defer func() {
		if err := st.Close(); err != nil {
			logger.Error("close store", "err", err)
		}
	}()
	logger.Info("store open", "path", cfg.DBPath)

	// In-process broker for the /events fan-out. Wraps the store so
	// publishes are durable and reconnecting subscribers can resume by
	// cursor. Constructed before the consumers in case we ever want
	// them to publish synthetic status events at startup.
	br := newBroker(st)

	// Start the knot event-stream consumer first so the jetstream
	// loop has somewhere to register newly-observed knots into.
	knots, err := startKnotConsumer(ctx, cfg, st)
	if err != nil {
		logger.Error("failed to start knot consumer", "err", err)
		os.Exit(1)
	}
	defer knots.Stop()

	// Start the JetStream listener in the background. It hands the knot
	// consumer any new knot referenced by an incoming sh.tangled.repo
	// record so we don't have to wait for a restart to pick it up.
	if err := startJetstream(ctx, cfg, st, knots); err != nil {
		logger.Error("failed to start jetstream consumer", "err", err)
		os.Exit(1)
	}

	// Run the HTTP server. This blocks until ctx is cancelled or the
	// listener errors.
	if err := runHTTP(ctx, cfg, br); err != nil {
		logger.Error("http server error", "err", err)
		os.Exit(1)
	}
}
