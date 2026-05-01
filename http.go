package main

// HTTP surface of the spindle.
//
// Four roles to keep in mind:
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
//  4. Logs: GET /logs/{knot}/{pipelineRkey}/{workflow} delegates to the
//     configured Provider so the appview (or a curling operator) can
//     pull captured workflow output for a specific run.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/websocket"
	"tangled.org/core/api/tangled"

	"go.mitchellh.com/tack/internal/buildkite"
)

// runHTTP starts the spindle's HTTP server and blocks until ctx is
// cancelled or the listener returns a fatal error. On ctx cancellation it
// performs a graceful shutdown with a bounded timeout.
//
// The logger is read from ctx via loggerFrom. The broker is the
// in-process pub/sub used by /events to fan published records out to
// connected websocket subscribers. bkProvider may be nil — when a
// deployment runs the fake provider, /webhooks/buildkite still
// registers but responds 503, so a misdirected Buildkite webhook
// gets a clear "this spindle isn't accepting Buildkite events" rather
// than a misleading 200.
func runHTTP(ctx context.Context, cfg config, br *broker, provider Provider, bkProvider *buildkiteProvider) error {
	logger := loggerFrom(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /", rootHandler())
	mux.HandleFunc("GET /events", eventsHandler(logger, br))
	mux.HandleFunc("GET /logs/{knot}/{pipelineRkey}/{workflow}", logsHandler(logger, provider))
	mux.HandleFunc("GET /xrpc/"+tangled.OwnerNSID, ownerHandler(logger, cfg.OwnerDID))
	mux.HandleFunc("POST /webhooks/buildkite", buildkiteWebhookHandler(logger, bkProvider))

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

// buildkiteWebhookHandler receives Buildkite Pipelines webhook events,
// authenticates the request against whichever scheme the provider was
// configured with, and hands the decoded payload to the provider for
// translation into a sh.tangled.pipeline.status publish.
//
// Authentication is intentionally fail-closed: when bk is nil (no
// Buildkite provider configured) we 503 instead of accepting events
// silently. The body is buffered up front because signature mode
// HMACs the raw bytes — we can't rely on the JSON decoder reading
// the request body before verification.
//
// Acknowledgement contract with Buildkite: we 200 on any well-formed
// event we accepted (including events we deliberately ignore, like
// job.* or builds we don't track), and 5xx only on internal failure
// the operator should look at. A 4xx/5xx makes Buildkite retry,
// which we don't want for "this isn't an event we care about".
func buildkiteWebhookHandler(logger *slog.Logger, bk *buildkiteProvider) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if bk == nil {
			http.Error(w, "buildkite provider not configured",
				http.StatusServiceUnavailable)
			return
		}

		// Cap body size so a malicious sender can't exhaust
		// memory; Buildkite payloads in practice are well under
		// 64 KiB but a generous-but-bounded ceiling is the
		// right shape here.
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
		// The X-Buildkite-Event header is authoritative for the
		// event name; the body field is convenience but doesn't
		// always match exactly. Prefer the header.
		if h := r.Header.Get("X-Buildkite-Event"); h != "" {
			payload.Event = h
		}

		// Translate + publish on the request context so a slow
		// store/broker doesn't outlive an aborted webhook
		// connection.
		if err := bk.HandleWebhook(r.Context(), payload); err != nil {
			logger.Error("buildkite webhook: handle", "err", err,
				"event", payload.Event,
				"build_uuid", payload.Build.ID,
			)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}

// logsHandler serves captured workflow logs over a WebSocket,
// matching the wire protocol of the upstream Tangled spindle so the
// appview's log proxy (appview/pipelines.Logs) treats us as a drop-in
// source. The path shape is
//
//	GET /logs/{knot}/{pipelineRkey}/{workflow}
//
// which matches the (knot, pipelineRkey, workflow) tuple
// Provider.Spawn is invoked with — the same identity used in the
// pipeline ATURI. Workflow names commonly contain a dot (e.g.
// "test.yml"); ServeMux path patterns match a single segment, so the
// literal value flows straight through r.PathValue.
//
// Wire shape per frame: a single TextMessage carrying one JSON
// LogLine record (defined in provider.go; byte-compatible with
// tangled.org/core/spindle/models.LogLine). The Provider hands us a
// channel of LogLine values; we marshal each one and forward it as
// one frame so the appview's per-line decode path works unchanged.
//
// Error mapping is intentionally done *before* the WebSocket upgrade:
// ErrLogsNotFound becomes 404 and any other Logs() error becomes 500
// so the appview's websocket.DefaultDialer surfaces a real HTTP
// status rather than an immediate close.
func logsHandler(logger *slog.Logger, provider Provider) http.HandlerFunc {
	upgrader := websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
	}
	return func(w http.ResponseWriter, r *http.Request) {
		knot := r.PathValue("knot")
		pipelineRkey := r.PathValue("pipelineRkey")
		workflow := r.PathValue("workflow")

		// Defensive: ServeMux won't match an empty segment, but a
		// future router change shouldn't be allowed to silently
		// produce an "all logs" query.
		if knot == "" || pipelineRkey == "" || workflow == "" {
			http.Error(w, "missing path component", http.StatusBadRequest)
			return
		}

		// Establish the log channel BEFORE the WebSocket upgrade so
		// ErrLogsNotFound / backend errors surface as a real HTTP
		// status to the appview's dialer rather than as an immediate
		// post-upgrade close. ctx scopes the producer's lifetime —
		// it's cancelled below the moment the client disconnects.
		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()

		ch, err := provider.Logs(ctx, knot, pipelineRkey, workflow)
		if err != nil {
			if errors.Is(err, ErrLogsNotFound) {
				http.Error(w, "logs not found", http.StatusNotFound)
				return
			}
			logger.Error("logs fetch failed",
				"err", err,
				"knot", knot,
				"pipeline_rkey", pipelineRkey,
				"workflow", workflow,
			)
			http.Error(w, "logs unavailable", http.StatusInternalServerError)
			return
		}

		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			// Upgrade already wrote a response; just record the
			// failure for diagnostics.
			logger.Error("logs websocket upgrade failed",
				"err", err,
				"knot", knot,
				"pipeline_rkey", pipelineRkey,
				"workflow", workflow,
			)
			return
		}
		defer func() {
			// Send a close frame on the way out so the appview proxy
			// sees a clean shutdown. Mirrors upstream
			// spindle.(*Spindle).Logs.
			_ = conn.WriteControl(
				websocket.CloseMessage,
				websocket.FormatCloseMessage(
					websocket.CloseNormalClosure, "log stream complete",
				),
				time.Now().Add(time.Second),
			)
			conn.Close()
		}()

		// Detect client disconnect by trying to read; we don't expect
		// any payloads from the client, so any read outcome (including
		// EOF) signals the connection has gone away. The cancel hits
		// the producer goroutine inside the Provider, which stops
		// sending and closes ch — our drain loop then exits cleanly.
		go func() {
			for {
				if _, _, err := conn.NextReader(); err != nil {
					cancel()
					return
				}
			}
		}()

		// Drain the channel; closure means the run is complete (or
		// the producer hit ctx). Marshal-then-write each LogLine as
		// a single TextMessage frame.
		for {
			select {
			case <-ctx.Done():
				return
			case line, ok := <-ch:
				if !ok {
					return
				}
				frame, err := json.Marshal(line)
				if err != nil {
					// The struct is fully internal; a marshal failure
					// is a programmer bug. Log and bail rather than
					// poison the stream with a half-frame.
					logger.Error("marshal log line",
						"err", err,
						"knot", knot,
						"pipeline_rkey", pipelineRkey,
						"workflow", workflow,
					)
					return
				}
				if err := conn.WriteMessage(websocket.TextMessage, frame); err != nil {
					logger.Debug("logs frame write failed",
						"err", err,
						"knot", knot,
						"pipeline_rkey", pipelineRkey,
						"workflow", workflow,
					)
					return
				}
			}
		}
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
