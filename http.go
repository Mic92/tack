package main

// HTTP surface of the spindle.
//
// Three roles to keep in mind:
//
//  1. Verification: the Tangled appview hits /xrpc/sh.tangled.owner during
//     spindle registration to confirm the operator owns this instance.
//  2. Event stream: the appview holds a long-lived websocket against
//     /events to receive sh.tangled.pipeline.status frames as builds
//     progress. Today this is just a keep-alive; payloads land once the
//     Buildkite webhook receiver is wired up.
//  3. Webhooks: Buildkite POSTs build/job state changes to
//     /webhooks/buildkite, which we'll translate into pipeline.status
//     events on (2).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"tangled.org/core/api/tangled"
)

// runHTTP starts the spindle's HTTP server and blocks until ctx is
// cancelled or the listener returns a fatal error. On ctx cancellation it
// performs a graceful shutdown with a bounded timeout.
//
// The logger is read from ctx via loggerFrom.
func runHTTP(ctx context.Context, cfg config) error {
	logger := loggerFrom(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /", rootHandler())
	mux.HandleFunc("GET /events", eventsHandler(logger))
	mux.HandleFunc("GET /xrpc/"+tangled.OwnerNSID, ownerHandler(logger, cfg.OwnerDID))
	mux.HandleFunc("POST /webhooks/buildkite", buildkiteWebhookHandler())

	srv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Run ListenAndServe on a goroutine so we can race it against ctx.Done.
	errCh := make(chan error, 1)
	go func() {
		logger.Info("listening", "addr", cfg.Addr, "owner", cfg.OwnerDID)
		errCh <- srv.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutting down")
	case err := <-errCh:
		// ErrServerClosed means we shut ourselves down cleanly elsewhere
		// — anything else is a real failure to report.
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("http server: %w", err)
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}

// rootHandler responds at "/" with a small identifier. Mainly useful as a
// liveness check during deployment.
func rootHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "tack: a Tangled spindle backed by Buildkite")
	}
}

// ownerHandler implements sh.tangled.owner so the Tangled appview can verify
// this spindle's owner during registration.
func ownerHandler(logger *slog.Logger, owner string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(tangled.Owner_Output{Owner: owner}); err != nil {
			logger.Error("encode owner response", "err", err)
		}
	}
}

// buildkiteWebhookHandler is a placeholder until we implement Buildkite ->
// pipeline.status translation.
func buildkiteWebhookHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not implemented", http.StatusNotImplemented)
	}
}

// eventsHandler upgrades to a WebSocket and emits no events yet. It exists
// so the Tangled appview can connect; once we wire up Buildkite webhooks
// this is where sh.tangled.pipeline.status frames will be sent.
//
// We send a periodic ping to keep intermediaries (load balancers, tunnels)
// from idling the connection, and watch for client reads to detect a
// disconnect.
func eventsHandler(logger *slog.Logger) http.HandlerFunc {
	upgrader := websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
	}
	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			logger.Error("websocket upgrade failed", "err", err)
			return
		}
		defer conn.Close()
		logger.Debug("events client connected", "remote", r.RemoteAddr)

		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()

		// Detect client disconnect by trying to read; we don't expect any
		// payloads from the client, so any read result (including EOF)
		// signals the connection has gone away.
		go func() {
			for {
				if _, _, err := conn.NextReader(); err != nil {
					cancel()
					return
				}
			}
		}()

		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				logger.Debug("events client disconnected", "remote", r.RemoteAddr)
				return
			case <-ticker.C:
				if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(time.Second)); err != nil {
					logger.Debug("events ping failed", "err", err)
					return
				}
			}
		}
	}
}
