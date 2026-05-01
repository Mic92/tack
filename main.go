// Tack is a custom Tangled spindle that translates sh.tangled.pipeline
// trigger records into Buildkite builds, and publishes Buildkite job state
// back as sh.tangled.pipeline.status events on a WebSocket stream that the
// Tangled appview can consume.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	charmlog "github.com/charmbracelet/log"

	"github.com/mitchellh/tack/internal/buildkite"
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

	// Buildkite-mode configuration. BuildkiteToken is the switch:
	// when empty we fall back to the in-process fake provider
	// (useful for local development against a real Tangled
	// jetstream); when set, the other Buildkite fields are
	// required and tack will refuse to start without them.
	BuildkiteToken         string
	BuildkiteOrg           string
	BuildkitePipeline      string
	BuildkiteWebhookSecret string
	BuildkiteWebhookMode   buildkite.WebhookMode
}

func loadConfig() (config, error) {
	cfg := config{
		Addr:                   envOr("TACK_LISTEN_ADDR", ":8080"),
		Hostname:               os.Getenv("TACK_HOSTNAME"),
		OwnerDID:               os.Getenv("TACK_OWNER_DID"),
		JetstreamURL:           envOr("TACK_JETSTREAM_URL", "wss://jetstream1.us-west.bsky.network/subscribe"),
		DBPath:                 envOr("TACK_DB_PATH", "tack.db"),
		Dev:                    os.Getenv("TACK_DEV") != "",
		BuildkiteToken:         os.Getenv("TACK_BUILDKITE_TOKEN"),
		BuildkiteOrg:           os.Getenv("TACK_BUILDKITE_ORG"),
		BuildkitePipeline:      os.Getenv("TACK_BUILDKITE_PIPELINE"),
		BuildkiteWebhookSecret: os.Getenv("TACK_BUILDKITE_WEBHOOK_SECRET"),
		BuildkiteWebhookMode: buildkite.WebhookMode(
			envOr("TACK_BUILDKITE_WEBHOOK_MODE", string(buildkite.WebhookModeToken)),
		),
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

	// If the operator opted into Buildkite mode (by supplying a
	// token), every other Buildkite knob has to be present. Half-
	// configured Buildkite leads to confusing failures deep in the
	// provider; catch it at startup.
	if cfg.BuildkiteToken != "" {
		if cfg.BuildkiteOrg == "" {
			return cfg, errors.New("TACK_BUILDKITE_ORG is required when TACK_BUILDKITE_TOKEN is set")
		}
		if cfg.BuildkitePipeline == "" {
			return cfg, errors.New("TACK_BUILDKITE_PIPELINE is required when TACK_BUILDKITE_TOKEN is set")
		}
		if cfg.BuildkiteWebhookSecret == "" {
			return cfg, errors.New("TACK_BUILDKITE_WEBHOOK_SECRET is required when TACK_BUILDKITE_TOKEN is set")
		}
		switch cfg.BuildkiteWebhookMode {
		case buildkite.WebhookModeToken, buildkite.WebhookModeSignature:
		default:
			return cfg, fmt.Errorf("TACK_BUILDKITE_WEBHOOK_MODE must be %q or %q; got %q",
				buildkite.WebhookModeToken, buildkite.WebhookModeSignature,
				cfg.BuildkiteWebhookMode,
			)
		}
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

	// Provider that turns Tangled pipeline triggers into
	// pipeline.status events. The Buildkite provider is the real
	// integration; the fake one stands in when no Buildkite token is
	// configured so the full jetstream → knot → /events flow is
	// still exercisable locally without a Buildkite account.
	//
	// bkProvider is kept as a typed pointer separately because the
	// /webhooks/buildkite handler needs the concrete *buildkiteProvider
	// (for HandleWebhook + signature verification), not the abstract
	// Provider surface.
	var (
		provider   Provider
		bkProvider *buildkiteProvider
	)
	if cfg.BuildkiteToken != "" {
		bkProvider = newBuildkiteProvider(
			br, st,
			buildkite.NewClient(cfg.BuildkiteToken, cfg.BuildkiteOrg),
			cfg.BuildkitePipeline,
			cfg.BuildkiteWebhookSecret,
			cfg.BuildkiteWebhookMode,
			logger,
		)
		provider = bkProvider
		logger.Info("buildkite provider enabled",
			"org", cfg.BuildkiteOrg,
			"pipeline", cfg.BuildkitePipeline,
			"webhook_mode", cfg.BuildkiteWebhookMode,
		)
	} else {
		provider = newFakeProvider(br, logger)
		logger.Info("fake provider enabled (set TACK_BUILDKITE_TOKEN to use buildkite)")
	}

	// Start the knot event-stream consumer first so the jetstream
	// loop has somewhere to register newly-observed knots into. It
	// gets the provider so each incoming pipeline trigger has
	// something to dispatch to.
	knots, err := startKnotConsumer(ctx, cfg, st, provider)
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
	if err := runHTTP(ctx, cfg, br, provider, bkProvider); err != nil {
		logger.Error("http server error", "err", err)
		os.Exit(1)
	}
}
