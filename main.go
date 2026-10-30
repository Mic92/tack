// Tack is a custom Tangled spindle that turns knot ref updates into builds on
// external CI providers and exposes their state through Tangled's CI XRPC API.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/bluesky-social/indigo/atproto/syntax"
	charmlog "github.com/charmbracelet/log"
	"tangled.org/core/xrpc/serviceauth"

	"go.mitchellh.com/tack/internal/buildkite"
)

// config is the runtime configuration, read from TACK_* environment variables and command-line
// flags. The README documents each variable. The names differ from the upstream spindle's
// SPINDLE_SERVER_* variables, so an upstream spindle's environment does not carry over unchanged.
type config struct {
	Addr         string
	Hostname     string
	OwnerDID     string
	JetstreamURL string
	DBPath       string
	// RepoDir holds sparse, mutable workflow checkouts. It is separate from
	// provider workspaces: Tack reads configuration here but never executes it.
	RepoDir string
	// Dev is for local infrastructure. It switches knot event streams to ws://, allows plain http
	// for knot, PDS, and blob requests, and lets the outbound HTTP client dial loopback and private
	// addresses. Never set it in production.
	Dev bool

	// CORSOrigins lists the browser origins allowed to call /xrpc (CORS is the browser rule that
	// hides a response from a page on another origin unless the server opts in). The Tangled web
	// app calls spindles directly from the browser, so the default is https://tangled.org. "*"
	// allows any origin, and an empty list disables CORS.
	CORSOrigins []string
	// KeyPath optionally names a P-256 PEM private key whose public half is published in the
	// did:web document. Tack signs nothing with it. It exists so operators can serve a document
	// that matches the upstream spindle's. Empty means the document has no verification method.
	KeyPath string
	// ContactEmail is the operator contact reported by org.tangled.spindle.describeSpindle. Empty
	// omits the contact from the response.
	ContactEmail string

	// Buildkite-mode configuration. BuildkiteToken is the switch:
	// when empty we fall back to the in-process fake provider
	// (useful for local development against a real Tangled
	// jetstream); when set, the other Buildkite fields are
	// required and tack will refuse to start without them.
	//
	// BuildkiteOrg is the *default* org used when a workflow YAML
	// doesn't specify one of its own. The pipeline a workflow runs
	// against is no longer global — it's pulled from the workflow
	// body itself (see workflowConfig in provider_buildkite.go).
	BuildkiteToken         string
	BuildkiteOrg           string
	BuildkiteWebhookSecret string
	BuildkiteWebhookMode   buildkite.WebhookMode

	// Tekton mode is explicit because it only works from inside a
	// Kubernetes cluster. When enabled, tack creates PipelineRuns in
	// TektonNamespace using its pod's service account credentials.
	TektonEnabled   bool
	TektonNamespace string

	// Sourcehut mode is gated on a token: when empty the provider is
	// not registered and workflows naming `tack.sourcehut` route
	// nowhere. SourcehutInstance overrides the public builds.sr.ht
	// endpoint for users running their own deployment; the token must
	// authenticate against whichever instance is targeted.
	SourcehutToken    string
	SourcehutInstance string
}

// loadConfig resolves environment defaults, applies flag overrides, and rejects
// partial provider configurations before any network or database work begins.
func loadConfig() (config, error) {
	dbPath := envOr("TACK_DB_PATH", "tack.db")
	cfg := config{
		Addr:                   envOr("TACK_LISTEN_ADDR", ":8080"),
		Hostname:               os.Getenv("TACK_HOSTNAME"),
		OwnerDID:               os.Getenv("TACK_OWNER_DID"),
		JetstreamURL:           envOr("TACK_JETSTREAM_URL", "wss://jetstream1.us-west.bsky.network/subscribe"),
		DBPath:                 dbPath,
		RepoDir:                envOr("TACK_REPO_DIR", dbPath+".repos"),
		Dev:                    os.Getenv("TACK_DEV") != "",
		CORSOrigins:            corsOriginsFromEnv(),
		KeyPath:                os.Getenv("TACK_KEY_PATH"),
		ContactEmail:           os.Getenv("TACK_CONTACT_EMAIL"),
		BuildkiteToken:         os.Getenv("TACK_BUILDKITE_TOKEN"),
		BuildkiteOrg:           os.Getenv("TACK_BUILDKITE_ORG"),
		BuildkiteWebhookSecret: os.Getenv("TACK_BUILDKITE_WEBHOOK_SECRET"),
		BuildkiteWebhookMode: buildkite.WebhookMode(
			envOr("TACK_BUILDKITE_WEBHOOK_MODE", string(buildkite.WebhookModeToken)),
		),
		TektonEnabled:     os.Getenv("TACK_TEKTON_ENABLED") == "1",
		TektonNamespace:   envOr("TACK_TEKTON_NAMESPACE", "default"),
		SourcehutToken:    os.Getenv("TACK_SOURCEHUT_TOKEN"),
		SourcehutInstance: os.Getenv("TACK_SOURCEHUT_INSTANCE"),
	}
	addrFlag := flag.String("addr", cfg.Addr, "HTTP listen address (overrides TACK_LISTEN_ADDR)")
	flag.Parse()
	cfg.Addr = *addrFlag

	if cfg.OwnerDID == "" {
		return cfg, errors.New("TACK_OWNER_DID is required")
	}

	// Hostname identifies Tack. Legacy sh.tangled.repo records name it in their spindle field,
	// and current repo manifests name the did:web derived from it (see config.DID). Without it no
	// repository can select Tack, so refuse to start rather than silently serve nothing.
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
	if cfg.TektonEnabled && cfg.TektonNamespace == "" {
		return cfg, errors.New("TACK_TEKTON_NAMESPACE is required when TACK_TEKTON_ENABLED=1")
	}

	return cfg, nil
}

// DID returns Tack's service identity, did:web:<Hostname>. A repo manifest selects Tack only when
// its spindle field equals this string exactly, and service auth tokens must name it as their
// audience. It is also the DID published in the did:web document and by describeSpindle.
func (c config) DID() syntax.DID {
	return serviceauth.DidWeb(c.Hostname)
}

// corsOriginsFromEnv parses TACK_CORS_ORIGINS as a comma-separated list of origins, dropping
// blank entries. An unset variable means the Tangled web app at https://tangled.org. A variable
// that is set but empty yields no origins, which disables CORS, so operators who do not want
// browser access can turn it off without naming a placeholder origin.
func corsOriginsFromEnv() []string {
	raw, ok := os.LookupEnv("TACK_CORS_ORIGINS")
	if !ok {
		return []string{"https://tangled.org"}
	}
	var origins []string
	for origin := range strings.SplitSeq(raw, ",") {
		if origin = strings.TrimSpace(origin); origin != "" {
			origins = append(origins, origin)
		}
	}
	return origins
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

	// The broker durably records spindle-owned pipeline and status data for
	// the current CI query API.
	br := newBroker(st)

	// Providers turn Tangled pipeline triggers into pipeline.status
	// events. We always wire the fake provider in so workflows can
	// opt into it (via `tack: { fake: ... }`) for end-to-end testing
	// even when Buildkite credentials are present; the Buildkite
	// provider is added on top when configured. Routing is per-
	// workflow and driven by the workflow YAML — see providerRouter.
	//
	// bkProvider is kept as a typed pointer separately because the
	// /webhooks/buildkite handler needs the concrete *buildkiteProvider
	// (for HandleWebhook + signature verification), not the abstract
	// Provider surface.
	providers := map[string]Provider{
		"fake": newFakeProvider(br, logger),
	}
	logger.Info("fake provider enabled (workflow opt-in via `tack.fake:`)")

	var bkProvider *buildkiteProvider
	if cfg.BuildkiteToken != "" {
		bkProvider = newBuildkiteProvider(
			br, st,
			buildkite.NewClient(cfg.BuildkiteToken),
			cfg.BuildkiteOrg,
			cfg.BuildkiteWebhookSecret,
			cfg.BuildkiteWebhookMode,
			logger,
		)
		providers["buildkite"] = bkProvider
		logger.Info("buildkite provider enabled",
			"default_org", cfg.BuildkiteOrg,
			"webhook_mode", cfg.BuildkiteWebhookMode,
		)
	}
	if cfg.TektonEnabled {
		tkProvider, err := newInClusterTektonProvider(
			br, st,
			cfg.TektonNamespace,
			logger,
		)
		if err != nil {
			logger.Error("failed to configure tekton provider", "err", err)
			os.Exit(1)
		}
		providers["tekton"] = tkProvider
		logger.Info("tekton provider enabled",
			"namespace", cfg.TektonNamespace,
		)
	}
	if cfg.SourcehutToken != "" {
		shProvider := newSourcehutProvider(
			br, st,
			cfg.SourcehutToken,
			cfg.SourcehutInstance,
			logger,
		)
		providers["sourcehut"] = shProvider
		logger.Info("sourcehut provider enabled",
			"instance", shProvider.defaultInstance,
		)
	}
	provider := newProviderRouter(logger, providers)

	// Start the knot event-stream consumer first so the jetstream
	// loop has somewhere to register newly-observed knots into. It
	// gets the provider so each incoming pipeline trigger has
	// something to dispatch to.
	knots, err := startKnotConsumer(ctx, cfg, st, br, provider)
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
	if err := runHTTP(ctx, cfg, st, provider, bkProvider, knots.pipelines); err != nil {
		logger.Error("http server error", "err", err)
		os.Exit(1)
	}
}
