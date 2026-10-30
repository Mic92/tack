package main

// XRPC endpoints for the CI API each spindle serves: the legacy sh.tangled.ci.* methods and the
// current org.tangled.* methods. Tangled moved pipeline state out of the legacy appview and into
// spindles, so clients query, stream, and act on pipelines through Tack directly.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/gorilla/websocket"
	"tangled.org/core/api/tangled"

	"go.mitchellh.com/tack/internal/orgtangled"
)

// wsWriteWait bounds every data/control write so a peer that stops reading
// cannot pin a subscription goroutine forever in the kernel send buffer.
const wsWriteWait = 10 * time.Second

// registerCIRoutes installs the CI API for both protocol generations: the legacy
// sh.tangled.ci.* methods and the current org.tangled.* methods. Queries and log
// subscriptions are public, as they are upstream, because pipeline state is not secret.
// Procedures that start or act on builds require service auth (the signed JWT one
// atproto service sends another to prove who is calling) because they can create
// externally billed work. cancelPipeline is the exception: it always fails with
// MethodNotImplemented, so there is nothing to protect.
//
// The legacy routes remain because the legacy appview still calls them. The v1.20
// upstream spindle answers 404 for them, but serving both generations costs nothing
// and keeps older Tangled deployments working.
//
// One WebSocket upgrader is shared by both log routes. Its CheckOrigin comes from the
// CORS policy because the Tangled web app opens the log WebSocket from the browser,
// and CORS headers do not restrict upgrades.
func registerCIRoutes(
	mux *http.ServeMux,
	logger *slog.Logger,
	st *store,
	provider Provider,
	pipelines *pipelineService,
	auth *serviceAuth,
	cors corsPolicy,
) {
	upgrader := websocket.Upgrader{
		ReadBufferSize:  10_000,
		WriteBufferSize: 10_000,
		CheckOrigin:     cors.CheckOrigin,
	}

	mux.HandleFunc(
		"GET /xrpc/"+tangled.CiQueryPipelinesNSID,
		ciQueryPipelinesHandler(logger, st),
	)
	mux.HandleFunc(
		"GET /xrpc/"+tangled.CiGetPipelineNSID,
		ciGetPipelineHandler(logger, st),
	)
	mux.HandleFunc(
		"GET /xrpc/"+tangled.CiSubscribePipelineLogsNSID,
		ciSubscribePipelineLogsHandler(logger, st, provider, upgrader, false),
	)
	mux.Handle(
		"POST /xrpc/"+tangled.CiTriggerPipelineNSID,
		auth.Middleware(ciTriggerPipelineHandler(logger, pipelines)),
	)

	mux.HandleFunc(
		"GET /xrpc/"+orgtangled.NSIDQueryPipelines,
		orgQueryPipelinesHandler(logger, pipelines),
	)
	mux.HandleFunc(
		"GET /xrpc/"+orgtangled.NSIDGetPipeline,
		orgGetPipelineHandler(logger, st),
	)
	mux.HandleFunc(
		"GET /xrpc/"+orgtangled.NSIDSubscribePipelineLogs,
		ciSubscribePipelineLogsHandler(logger, st, provider, upgrader, true),
	)
	mux.Handle(
		"POST /xrpc/"+orgtangled.NSIDRetryPipeline,
		auth.Middleware(orgRetryPipelineHandler(logger, pipelines)),
	)
	mux.Handle(
		"POST /xrpc/"+orgtangled.NSIDApprovePipeline,
		auth.Middleware(orgApprovePipelineHandler(logger, pipelines)),
	)
	mux.HandleFunc(
		"POST /xrpc/"+orgtangled.NSIDCancelPipeline,
		orgCancelPipelineHandler(),
	)
	mux.Handle(
		"POST /xrpc/"+orgtangled.NSIDNotifyEvent,
		auth.Middleware(notifyEventHandler(logger, pipelines, newReplayGuard())),
	)
}

// ciQueryPipelinesHandler validates the lexicon query parameters and projects
// retained internal pipeline/status events into sh.tangled.ci.pipeline values.
func ciQueryPipelinesHandler(logger *slog.Logger, st *store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		repo := r.URL.Query().Get("repo")
		if repo == "" {
			writeXRPCError(w, http.StatusBadRequest, "InvalidRequest", "repo is required")
			return
		}
		limit := 30
		if raw := r.URL.Query().Get("limit"); raw != "" {
			parsed, err := strconv.Atoi(raw)
			if err != nil || parsed < 1 || parsed > 250 {
				writeXRPCError(w, http.StatusBadRequest, "InvalidRequest", "limit must be between 1 and 250")
				return
			}
			limit = parsed
		}
		cursor := r.URL.Query().Get("cursor")
		if cursor != "" {
			if _, err := strconv.ParseInt(cursor, 10, 64); err != nil {
				writeXRPCError(w, http.StatusBadRequest, "InvalidRequest", "cursor must be an integer")
				return
			}
		}

		pipelines, cursor, total, err := st.QueryPipelines(
			r.Context(),
			repo,
			r.URL.Query()["commits"],
			cursor,
			r.URL.Query()["kinds"],
			limit,
		)
		if err != nil {
			logger.Error("query pipelines", "err", err, "repo", repo)
			writeXRPCError(w, http.StatusInternalServerError, "InternalError", "failed to query pipelines")
			return
		}
		response := tangled.CiQueryPipelines_Output{Pipelines: pipelines, Total: total}
		if cursor != "" {
			response.Cursor = &cursor
		}
		writeJSON(w, http.StatusOK, response)
	}
}

// ciGetPipelineHandler returns one spindle-local pipeline by TID. A TID is
// validated before touching SQLite so malformed identifiers get the lexicon's
// InvalidRequest response rather than an ambiguous not-found.
func ciGetPipelineHandler(logger *slog.Logger, st *store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		pipeline := r.URL.Query().Get("pipeline")
		if _, err := syntax.ParseTID(pipeline); err != nil {
			writeXRPCError(w, http.StatusBadRequest, "InvalidRequest", "pipeline must be a valid TID")
			return
		}
		result, err := st.GetPipeline(r.Context(), pipeline)
		if errors.Is(err, sql.ErrNoRows) {
			writeXRPCError(w, http.StatusNotFound, "PipelineNotFound", "pipeline not found")
			return
		}
		if err != nil {
			logger.Error("get pipeline", "err", err, "pipeline", pipeline)
			writeXRPCError(w, http.StatusInternalServerError, "InternalError", "failed to get pipeline")
			return
		}
		writeJSON(w, http.StatusOK, result)
	}
}

// ciTriggerPipelineHandler implements the legacy sh.tangled.ci.triggerPipeline. It
// reads the caller that [serviceAuth.Middleware] verified and attached to the request
// context, and leaves repository authorization and trigger conversion to
// pipelineService. Bodies are capped at 1 MiB because the lexicon has no blobs and
// legitimate requests are tiny.
func ciTriggerPipelineHandler(
	logger *slog.Logger,
	pipelines *pipelineService,
) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, ok := principalFrom(r.Context())
		if !ok {
			writeXRPCError(w, http.StatusForbidden, "AuthRequired", "actor DID is missing")
			return
		}
		actor := principal.Issuer
		var input tangled.CiTriggerPipeline_Input
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
		if err := decoder.Decode(&input); err != nil {
			writeXRPCError(w, http.StatusBadRequest, "InvalidRequest", "invalid request body")
			return
		}
		pipeline, err := pipelines.triggerPipeline(r.Context(), actor.String(), &input)
		if err != nil {
			logger.Warn("trigger pipeline rejected", "err", err, "actor", actor, "repo", input.Repo)
			writeXRPCError(w, http.StatusBadRequest, "InvalidRequest", err.Error())
			return
		}
		writeJSON(w, http.StatusOK, tangled.CiTriggerPipeline_Output{Pipeline: pipeline})
	})
}

// workflowLogStream is the validated unit fanned into one log subscription.
// It intentionally carries only a name; each worker resolves fresh provider
// snapshots so subscriptions can begin before asynchronous run mappings exist.
type workflowLogStream struct {
	workflow string
}

// maxLogWorkflows caps how many workflows one log subscription may follow. Each one
// starts a provider polling goroutine, so the cap bounds the work a single public
// request can cause. The value matches the upstream spindle.
const maxLogWorkflows = 100

// ciSubscribePipelineLogsHandler implements Tangled's log subscription for both API generations:
// a WebSocket that streams binary CBOR frames (each frame is an atproto event-stream header
// followed by a message body). The two use the same framing and field names, and differ only in
// the step outcome that current end markers carry (see logFrame). The handler validates every
// requested workflow before upgrading, runs one provider follower per workflow, and remains the
// sole WebSocket writer so Gorilla's one-concurrent-writer rule is never violated.
//
// scoped selects the current org.tangled.ci.subscribePipelineLogs contract. The
// caller must pass a repo DID, the pipeline is a record key rather than a TID, and a
// missing pipeline is reported as PipelineNotFound, the error name the lexicon
// declares. A pipeline that belongs to a different repository is also reported as
// not found, matching orgGetPipelineHandler, so a request cannot read another
// repository's logs by pairing its own repo with a foreign pipeline key.
func ciSubscribePipelineLogsHandler(
	logger *slog.Logger,
	st *store,
	provider Provider,
	upgrader websocket.Upgrader,
	scoped bool,
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var repo, pipeline string
		notFound := "NotFound"
		if scoped {
			var ok bool
			if repo, pipeline, ok = orgPipelineParams(w, r); !ok {
				return
			}
			notFound = "PipelineNotFound"
		} else {
			pipeline = r.URL.Query().Get("pipeline")
			if _, err := syntax.ParseTID(pipeline); err != nil {
				writeXRPCError(w, http.StatusBadRequest, "InvalidRequest", "pipeline must be a valid TID")
				return
			}
		}
		raw, _, err := st.GetRawPipeline(r.Context(), pipeline)
		if errors.Is(err, sql.ErrNoRows) || (err == nil && scoped && pipelineRepoDID(raw) != repo) {
			writeXRPCError(w, http.StatusNotFound, notFound, "pipeline not found")
			return
		}
		if err != nil || raw.TriggerMetadata == nil || raw.TriggerMetadata.Repo == nil {
			logger.Error("load pipeline for logs", "err", err, "pipeline", pipeline)
			writeXRPCError(w, http.StatusInternalServerError, "InternalError", "failed to load pipeline")
			return
		}

		// Build the authoritative workflow set from the immutable pipeline record;
		// provider mappings are intentionally not consulted until after upgrade.
		available := make(map[string]struct{}, len(raw.Workflows))
		for _, wf := range raw.Workflows {
			if wf != nil && wf.Name != "" {
				available[wf.Name] = struct{}{}
			}
		}
		workflows := r.URL.Query()["workflows"]
		if len(workflows) == 0 {
			// Omitted workflows means all, preserving the order captured at compile
			// time so interleaved streams are deterministic at startup.
			workflows = make([]string, 0, len(available))
			for _, wf := range raw.Workflows {
				if wf != nil && wf.Name != "" {
					workflows = append(workflows, wf.Name)
				}
			}
		}
		seenWorkflows := make(map[string]struct{}, len(workflows))
		uniqueWorkflows := make([]string, 0, len(workflows))
		for _, workflow := range workflows {
			if _, seen := seenWorkflows[workflow]; seen {
				continue
			}
			seenWorkflows[workflow] = struct{}{}
			uniqueWorkflows = append(uniqueWorkflows, workflow)
		}
		workflows = uniqueWorkflows
		if len(workflows) == 0 {
			writeXRPCError(w, http.StatusBadRequest, "InvalidRequest", "pipeline has no workflows")
			return
		}
		if len(workflows) > maxLogWorkflows {
			writeXRPCError(w, http.StatusBadRequest, "InvalidRequest", "too many workflows requested")
			return
		}

		// Derive one cancellation domain for provider followers, the disconnect
		// reader, and the writer loop. Any side ending tears down the others.
		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()
		streams := make([]workflowLogStream, 0, len(workflows))
		for _, name := range workflows {
			if _, ok := available[name]; !ok {
				writeXRPCError(w, http.StatusNotFound, "WorkflowNotFound", fmt.Sprintf("workflow %q not found", name))
				return
			}
			streams = append(streams, workflowLogStream{workflow: name})
		}

		// Upgrade only after all parameter and pipeline checks. Before this point we
		// can still return useful XRPC HTTP errors to the caller.
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			logger.Error("pipeline logs websocket upgrade", "err", err)
			return
		}
		defer func() {
			_ = conn.WriteControl(
				websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, "log stream complete"),
				time.Now().Add(wsWriteWait),
			)
			conn.Close()
		}()

		// We do not expect application messages from the client. A blocking read is
		// solely the prompt disconnect detector for provider polling goroutines.
		go func() {
			for {
				if _, _, err := conn.NextReader(); err != nil {
					cancel()
					return
				}
			}
		}()

		// Followers may block briefly on provider APIs. A bounded fan-in buffer lets
		// another workflow continue without allowing an unbounded slow-client queue.
		events := make(chan workflowLogLine, 128)
		var wg sync.WaitGroup
		for _, stream := range streams {
			wg.Add(1)
			go func(stream workflowLogStream) {
				defer wg.Done()
				followProviderLogs(
					ctx, logger, st, provider,
					raw.TriggerMetadata.Repo.Knot, pipeline, stream.workflow,
					events,
				)
			}(stream)
		}
		go func() {
			wg.Wait()
			close(events)
		}()

		// This loop is the only writer: both data frames and keep-alive control
		// frames pass through it, avoiding concurrent writes to conn.
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := conn.WriteControl(
					websocket.PingMessage, nil, time.Now().Add(wsWriteWait),
				); err != nil {
					return
				}
			case event, ok := <-events:
				if !ok {
					return
				}
				if err := conn.SetWriteDeadline(time.Now().Add(wsWriteWait)); err != nil {
					return
				}
				writer, err := conn.NextWriter(websocket.BinaryMessage)
				if err != nil {
					return
				}
				if err := logFrame(scoped, event).Serialize(writer); err != nil {
					writer.Close()
					logger.Error("serialize pipeline log", "err", err)
					return
				}
				if err := writer.Close(); err != nil {
					return
				}
			}
		}
	}
}

// followProviderLogs adapts providers' snapshot-oriented log APIs to Tangled's
// long-lived subscription. It retries while a workflow is active and emits
// only frames appended since the previous snapshot. This also closes the race
// where a client subscribes before an asynchronous provider has persisted its
// run mapping.
func followProviderLogs(
	ctx context.Context,
	logger *slog.Logger,
	st *store,
	provider Provider,
	knot, pipeline, workflow string,
	out chan<- workflowLogLine,
) {
	followProviderLogsWithInterval(
		ctx, logger, st, provider, knot, pipeline, workflow, out, 15*time.Second,
	)
}

// followProviderLogsWithInterval contains the polling implementation with an
// injectable steady-state interval for tests. Providers expose snapshots, not
// append-only cursors, so it tracks starts and data counts independently per
// step and withholds end controls until the workflow itself is terminal.
func followProviderLogsWithInterval(
	ctx context.Context,
	logger *slog.Logger,
	st *store,
	provider Provider,
	knot, pipeline, workflow string,
	out chan<- workflowLogLine,
	pollInterval time.Duration,
) {
	// A snapshot repeats all prior frames. Step-local counters are necessary:
	// output growing in step 0 inserts frames before step 1 and makes a single
	// global frame offset skip or duplicate data.
	started := make(map[int]bool)
	emittedData := make(map[int]int)
	terminalRetries := 0
	providerFailures := 0
	for {
		nextPoll := pollInterval
		var endLines []LogLine
		lines, err := provider.Logs(ctx, knot, pipeline, workflow)
		if err == nil {
			terminalRetries = 0
			providerFailures = 0
			snapshotData := make(map[int]int)
		drainSnapshot:
			for {
				select {
				case <-ctx.Done():
					return
				case line, ok := <-lines:
					if !ok {
						for step, count := range snapshotData {
							if count > emittedData[step] {
								emittedData[step] = count
							}
						}
						break drainSnapshot
					}
					if line.Kind == LogKindControl {
						if line.StepStatus == StepStatusEnd {
							// Snapshot providers may synthesize an end marker even while the
							// external job is still running. Delay it until retained status is
							// terminal or later output would appear after a closed UI block.
							endLines = append(endLines, line)
							continue
						}
						if started[line.StepId] {
							continue
						}
						started[line.StepId] = true
						select {
						case out <- workflowLogLine{workflow: workflow, line: line}:
						case <-ctx.Done():
							return
						}
						continue
					}

					index := snapshotData[line.StepId]
					snapshotData[line.StepId] = index + 1
					if index >= emittedData[line.StepId] {
						select {
						case out <- workflowLogLine{workflow: workflow, line: line}:
						case <-ctx.Done():
							return
						}
					}
				}
			}
		} else if errors.Is(err, ErrLogsNotFound) {
			// Provider mappings are persisted asynchronously immediately after
			// a run is created. Retry this startup race quickly without using
			// the steady-state API polling cadence.
			nextPoll = min(pollInterval, time.Second)
		} else {
			providerFailures++
			nextPoll = logPollBackoff(pollInterval, providerFailures, time.Minute)
			logger.Debug("refresh workflow logs",
				"err", err,
				"pipeline", pipeline,
				"workflow", workflow,
				"retry_in", nextPoll,
			)
		}

		status, _, _, _, statusErr := st.workflowStatus(ctx, knot, pipeline, workflow)
		if statusErr != nil {
			logger.Debug("read workflow status for logs",
				"err", statusErr, "pipeline", pipeline, "workflow", workflow,
			)
		}
		if isTerminalPipelineStatus(status) {
			// A terminal status usually races the provider's final log availability.
			// Give transient API failures three bounded retries before ending the
			// subscription; ErrLogsNotFound is a legitimate empty-log result.
			if err != nil && !errors.Is(err, ErrLogsNotFound) && terminalRetries < 3 {
				terminalRetries++
				delay := logPollBackoff(3*time.Second, terminalRetries, 15*time.Second)
				timer := time.NewTimer(delay)
				select {
				case <-ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
				continue
			}
			for _, line := range endLines {
				select {
				case out <- workflowLogLine{workflow: workflow, line: line}:
				case <-ctx.Done():
					return
				}
			}
			return
		}
		timer := time.NewTimer(nextPoll)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// logPollBackoff returns capped exponential backoff where failures=1 means the
// base delay. Provider rate limits use the 15/30/60-second sequence; terminal
// final-log retries use a shorter 3/6/12-second sequence.
func logPollBackoff(base time.Duration, failures int, maximum time.Duration) time.Duration {
	delay := base
	for range failures - 1 {
		if delay >= maximum/2 {
			return maximum
		}
		delay *= 2
	}
	return min(delay, maximum)
}

// isTerminalPipelineStatus mirrors Tangled's four workflow finish states.
func isTerminalPipelineStatus(status string) bool {
	switch status {
	case "success", "failed", "timeout", "cancelled":
		return true
	default:
		return false
	}
}

// workflowLogLine is one provider log line tagged with the workflow it came from.
// Followers send these protocol-neutral values so the same polling code serves both
// API generations. Only the WebSocket writer chooses an encoding, through logFrame.
type workflowLogLine struct {
	workflow string
	line     LogLine
}

// logFrame returns the wire message for event: an org.tangled frame when scoped is
// true and a legacy sh.tangled frame otherwise. Both serialize as the same atproto
// event-stream framing, but only the current frames carry step outcomes, because the
// generated sh.tangled type in the pinned Tangled v1.16 module has no field for them.
func logFrame(scoped bool, event workflowLogLine) interface{ Serialize(io.Writer) error } {
	if scoped {
		return orgLogEvent(event.workflow, event.line)
	}
	return ciLogEvent(event.workflow, event.line)
}

// orgLogEvent converts a LogLine into an org.tangled.ci.subscribePipelineLogs
// message. It reuses ciLogEvent so both generations apply the same defaults, then adds the step
// outcome to end markers (see LogLine.StepOutcome for why the web app needs it). Start markers
// never carry an outcome, even when the provider set one.
func orgLogEvent(workflow string, line LogLine) *orgtangled.LogEvent {
	legacy := ciLogEvent(workflow, line)
	if c := legacy.Control; c != nil {
		control := &orgtangled.LogControl{
			Time: c.Time, Workflow: c.Workflow, Step: c.Step, Content: c.Content,
			Command: c.Command, Status: c.Status, Kind: c.Kind,
		}
		if line.StepStatus == StepStatusEnd {
			control.Outcome = optionalString(line.StepOutcome)
		}
		return &orgtangled.LogEvent{Control: control}
	}
	d := legacy.Data
	return &orgtangled.LogEvent{Data: &orgtangled.LogData{
		Time: d.Time, Workflow: d.Workflow, Step: d.Step, Content: d.Content, Stream: d.Stream,
	}}
}

// ciLogEvent converts the provider-neutral LogLine into one branch of the legacy
// sh.tangled.ci.subscribePipelineLogs union. It fills protocol defaults for providers that omit
// timestamps, stream names, or step kinds. orgLogEvent builds on it, so these defaults apply to
// both API generations.
func ciLogEvent(workflow string, line LogLine) *tangled.CiSubscribePipelineLogs_Event {
	timestamp := line.Time
	if timestamp.IsZero() {
		timestamp = time.Now()
	}
	timeString := timestamp.UTC().Format(time.RFC3339)
	if line.Kind == LogKindControl {
		kind := line.StepKind
		if kind != "system" {
			kind = "user"
		}
		return &tangled.CiSubscribePipelineLogs_Event{
			Control: &tangled.CiSubscribePipelineLogs_Control{
				Time:     timeString,
				Workflow: workflow,
				Step:     int64(line.StepId),
				Content:  line.Content,
				Command:  optionalString(line.StepCommand),
				Status:   optionalString(line.StepStatus),
				Kind:     &kind,
			},
		}
	}
	stream := line.Stream
	if stream != "stderr" {
		stream = "stdout"
	}
	return &tangled.CiSubscribePipelineLogs_Event{
		Data: &tangled.CiSubscribePipelineLogs_Data{
			Time:     timeString,
			Workflow: workflow,
			Step:     int64(line.StepId),
			Content:  line.Content,
			Stream:   stream,
		},
	}
}

// optionalString preserves the lexicon distinction between absent optional
// metadata and a meaningful non-empty value.
func optionalString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

// writeJSON writes the common JSON response envelope used by non-subscription
// XRPC methods.
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

// writeXRPCError emits the {error,message} shape required by ATProto XRPC.
func writeXRPCError(w http.ResponseWriter, status int, name, message string) {
	writeJSON(w, status, map[string]string{"error": name, "message": message})
}
