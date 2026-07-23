package main

// HTTP surface of the spindle.
//
// Three roles to keep in mind:
//
//  1. Verification: the Tangled appview hits /xrpc/sh.tangled.owner during
//     spindle registration to confirm the operator owns this instance.
//  2. CI XRPC: the appview queries spindle-owned pipelines and subscribes to
//     CBOR-framed logs under /xrpc/sh.tangled.ci.*.
//  3. Webhooks: Buildkite POSTs build/job state changes to
//     /webhooks/buildkite, which we translate into retained pipeline status.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"tangled.org/core/api/tangled"

	"go.mitchellh.com/tack/internal/buildkite"
)

// runHTTP starts the spindle's HTTP server and blocks until ctx is
// cancelled or the listener returns a fatal error. On ctx cancellation it
// performs a graceful shutdown with a bounded timeout.
//
// bkProvider may be nil. In that case /webhooks/buildkite responds 503 so a
// misdirected webhook gets a clear failure rather than a misleading 200.
func runHTTP(
	ctx context.Context,
	cfg config,
	st *store,
	provider Provider,
	bkProvider *buildkiteProvider,
	pipelines *pipelineService,
) error {
	logger := loggerFrom(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /", rootHandler())
	mux.HandleFunc("GET /xrpc/"+tangled.OwnerNSID, ownerHandler(logger, cfg.OwnerDID))
	mux.HandleFunc("POST /webhooks/buildkite", buildkiteWebhookHandler(logger, bkProvider))
	registerCiRoutes(mux, cfg, logger, st, provider, pipelines)

	srv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("listening", "addr", cfg.Addr, "owner", cfg.OwnerDID)
		errCh <- srv.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutting down")
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("http server: %w", err)
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}

// rootHandler responds at "/" with a friendly identifier page. Mainly
// useful as a liveness check during deployment, and as a calling card
// for the curious operator who hits the root in a browser.
func rootHandler() http.HandlerFunc {
	// Plain-text banner. "tack" rendered in figlet's larry3d font; the
	// shape mirrors the style the bluesky PDS serves at its own root,
	// which feels like the right vibe for an atproto-adjacent service.
	// The two backticks in the larry3d output collide with Go's raw
	// string delimiter, so we concatenate them as interpreted strings.
	const banner = ` __                    __
/\ \__                /\ \
\ \ ,_\    __      ___\ \ \/'\
 \ \ \/  /'__` + "`" + `\   /'___\ \ , <
  \ \ \_/\ \L\.\_/\ \__/\ \ \\` + "`" + `\
   \ \__\ \__/.\_\ \____\\ \_\ \_\
    \/__/\/__/\/_/\/____/ \/_/\/_/


This is a tack server: a tangled spindle for other CI services.

Most API routes are under /xrpc/

      Code: https://github.com/mitchellh/tack
`
	return func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		fmt.Fprint(w, banner)
	}
}

// ownerHandler implements sh.tangled.owner so the Tangled appview can verify
// this spindle's owner during registration.
func ownerHandler(logger *slog.Logger, owner string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(tangled.Owner_Output{Owner: owner}); err != nil {
			logger.Error("encode owner response", "err", err)
		}
	}
}

// buildkiteWebhookHandler receives Buildkite Pipelines webhook events,
// authenticates the request, and hands the decoded payload to the provider.
// Acknowledged but irrelevant events return 200 so Buildkite does not retry;
// internal failures return 5xx for operator visibility.
func buildkiteWebhookHandler(logger *slog.Logger, bk *buildkiteProvider) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if bk == nil {
			http.Error(w, "buildkite provider not configured", http.StatusServiceUnavailable)
			return
		}

		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			logger.Warn("buildkite webhook: read body", "err", err)
			http.Error(w, "read body", http.StatusBadRequest)
			return
		}
		if err := bk.VerifyWebhook(r.Header, body); err != nil {
			logger.Warn("buildkite webhook: verify failed",
				"err", err,
				"remote", r.RemoteAddr,
			)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		var payload buildkite.WebhookPayload
		if err := json.Unmarshal(body, &payload); err != nil {
			logger.Warn("buildkite webhook: decode body", "err", err)
			http.Error(w, "bad payload", http.StatusBadRequest)
			return
		}
		if event := r.Header.Get("X-Buildkite-Event"); event != "" {
			payload.Event = event
		}

		if err := bk.HandleWebhook(r.Context(), payload); err != nil {
			logger.Error("buildkite webhook: handle",
				"err", err,
				"event", payload.Event,
				"build_uuid", payload.Build.ID,
			)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}
