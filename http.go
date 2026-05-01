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
	"strconv"
	"time"

	"github.com/gorilla/websocket"
	"tangled.org/core/api/tangled"
)

// runHTTP starts the spindle's HTTP server and blocks until ctx is
// cancelled or the listener returns a fatal error. On ctx cancellation it
// performs a graceful shutdown with a bounded timeout.
//
// The logger is read from ctx via loggerFrom. The broker is the
// in-process pub/sub used by /events to fan published records out to
// connected websocket subscribers.
func runHTTP(ctx context.Context, cfg config, br *broker) error {
	logger := loggerFrom(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /", rootHandler())
	mux.HandleFunc("GET /events", eventsHandler(logger, br))
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

// eventsHandler upgrades to a WebSocket and streams persisted records
// to the connected client. The wire protocol mirrors the upstream
// Tangled spindle so the appview's eventconsumer treats us as a
// drop-in source:
//
//   - Optional ?cursor=<int64> resumes after that rowid; absent or 0
//     means "from the beginning of our retained log".
//   - We do a backfill pass first (everything with created > cursor),
//     then loop: on each broker signal, drain new rows; on a 30s
//     timer, write a websocket ping so intermediaries don't idle the
//     connection out.
//
// We subscribe to the broker *before* the backfill so any Publish that
// races between the cursor read and the loop entry is captured by the
// pending channel signal — the loop will see it on its first iteration
// and call streamEvents again, which is idempotent on the cursor.
func eventsHandler(logger *slog.Logger, br *broker) http.HandlerFunc {
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

		// Parse the resume cursor up front. An unparseable cursor is a
		// client bug, but rather than 4xx the upgraded connection we
		// log it and start from zero — same behaviour as the upstream
		// spindle.
		var cursor int64
		if raw := r.URL.Query().Get("cursor"); raw != "" {
			parsed, err := strconv.ParseInt(raw, 10, 64)
			if err != nil {
				logger.Warn("events: bad cursor, starting from 0",
					"cursor", raw, "err", err,
				)
			} else {
				cursor = parsed
			}
		}
		logger.Debug("events client connected",
			"remote", r.RemoteAddr, "cursor", cursor,
		)

		// Subscribe before the backfill so a Publish that races between
		// the EventsAfter read and our select loop is captured by the
		// pending channel signal — we'll re-drain on the first wake-up.
		sig := br.Subscribe()
		defer br.Unsubscribe(sig)

		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()

		// Detect client disconnect by trying to read; we don't expect
		// any payloads from the client, so any read outcome (including
		// EOF) signals the connection has gone away.
		go func() {
			for {
				if _, _, err := conn.NextReader(); err != nil {
					cancel()
					return
				}
			}
		}()

		// Initial backfill. If this fails the connection is unusable
		// (we can't promise ordering after a partial write) so just
		// return and let the client reconnect with the same cursor.
		if err := streamEvents(ctx, conn, br.st, &cursor); err != nil {
			logger.Debug("events backfill ended", "err", err, "cursor", cursor)
			return
		}

		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				logger.Debug("events client disconnected",
					"remote", r.RemoteAddr, "cursor", cursor,
				)
				return
			case <-sig:
				if err := streamEvents(ctx, conn, br.st, &cursor); err != nil {
					logger.Debug("events stream ended", "err", err, "cursor", cursor)
					return
				}
			case <-ticker.C:
				if err := conn.WriteControl(
					websocket.PingMessage, nil,
					time.Now().Add(time.Second),
				); err != nil {
					logger.Debug("events ping failed", "err", err)
					return
				}
			}
		}
	}
}

// streamEvents drains every event row with `created > *cursor`, writes
// each as a wire envelope frame, and advances *cursor in lockstep. The
// cursor is updated *after* the write succeeds so a half-flushed batch
// (interrupted by a websocket error) replays cleanly on the next
// connection.
//
// It is safe to call repeatedly: when there are no new rows the query
// returns an empty slice and we noop.
func streamEvents(ctx context.Context, conn *websocket.Conn, st *store, cursor *int64) error {
	rows, err := st.EventsAfter(ctx, *cursor)
	if err != nil {
		return fmt.Errorf("read events: %w", err)
	}
	for _, row := range rows {
		frame, err := json.Marshal(eventsEnvelope{
			Rkey:    row.Rkey,
			Nsid:    row.Nsid,
			Event:   row.EventJSON,
			Created: row.Created,
		})
		if err != nil {
			return fmt.Errorf("marshal envelope: %w", err)
		}
		if err := conn.WriteMessage(websocket.TextMessage, frame); err != nil {
			return fmt.Errorf("write frame: %w", err)
		}
		*cursor = row.Created
	}
	return nil
}
