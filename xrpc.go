package main

// XRPC endpoints introduced when Tangled moved pipeline state ownership from
// appview to each spindle.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/gorilla/websocket"
	"tangled.org/core/api/tangled"
	"tangled.org/core/xrpc/serviceauth"
)

const wsWriteWait = 10 * time.Second

func registerCiRoutes(
	mux *http.ServeMux,
	cfg config,
	logger *slog.Logger,
	st *store,
	provider Provider,
	pipelines *pipelineService,
) {
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
		ciSubscribePipelineLogsHandler(logger, st, provider),
	)

	auth := serviceauth.NewServiceAuth(
		logger,
		identity.DefaultDirectory(),
		serviceauth.DidWeb(cfg.Hostname).String(),
	)
	mux.Handle(
		"POST /xrpc/"+tangled.CiTriggerPipelineNSID,
		auth.VerifyServiceAuth(ciTriggerPipelineHandler(logger, pipelines)),
	)
}

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

func ciTriggerPipelineHandler(
	logger *slog.Logger,
	pipelines *pipelineService,
) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		actor, ok := r.Context().Value(serviceauth.ActorDid).(syntax.DID)
		if !ok {
			writeXRPCError(w, http.StatusForbidden, "AuthRequired", "actor DID is missing")
			return
		}
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

type workflowLogStream struct {
	workflow string
}

func ciSubscribePipelineLogsHandler(
	logger *slog.Logger,
	st *store,
	provider Provider,
) http.HandlerFunc {
	upgrader := websocket.Upgrader{ReadBufferSize: 10_000, WriteBufferSize: 10_000}
	return func(w http.ResponseWriter, r *http.Request) {
		pipeline := r.URL.Query().Get("pipeline")
		if _, err := syntax.ParseTID(pipeline); err != nil {
			writeXRPCError(w, http.StatusBadRequest, "InvalidRequest", "pipeline must be a valid TID")
			return
		}
		raw, _, err := st.GetRawPipeline(r.Context(), pipeline)
		if errors.Is(err, sql.ErrNoRows) {
			writeXRPCError(w, http.StatusNotFound, "NotFound", "pipeline not found")
			return
		}
		if err != nil || raw.TriggerMetadata == nil || raw.TriggerMetadata.Repo == nil {
			logger.Error("load pipeline for logs", "err", err, "pipeline", pipeline)
			writeXRPCError(w, http.StatusInternalServerError, "InternalError", "failed to load pipeline")
			return
		}

		available := make(map[string]struct{}, len(raw.Workflows))
		for _, wf := range raw.Workflows {
			if wf != nil && wf.Name != "" {
				available[wf.Name] = struct{}{}
			}
		}
		workflows := r.URL.Query()["workflows"]
		if len(workflows) == 0 {
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

		go func() {
			for {
				if _, _, err := conn.NextReader(); err != nil {
					cancel()
					return
				}
			}
		}()

		events := make(chan *tangled.CiSubscribePipelineLogs_Event, 128)
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
				if err := event.Serialize(writer); err != nil {
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
// where appview subscribes before an asynchronous provider has persisted its
// run mapping.
func followProviderLogs(
	ctx context.Context,
	logger *slog.Logger,
	st *store,
	provider Provider,
	knot, pipeline, workflow string,
	out chan<- *tangled.CiSubscribePipelineLogs_Event,
) {
	followProviderLogsWithInterval(
		ctx, logger, st, provider, knot, pipeline, workflow, out, 15*time.Second,
	)
}

func followProviderLogsWithInterval(
	ctx context.Context,
	logger *slog.Logger,
	st *store,
	provider Provider,
	knot, pipeline, workflow string,
	out chan<- *tangled.CiSubscribePipelineLogs_Event,
	pollInterval time.Duration,
) {
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
							endLines = append(endLines, line)
							continue
						}
						if started[line.StepId] {
							continue
						}
						started[line.StepId] = true
						select {
						case out <- ciLogEvent(workflow, line):
						case <-ctx.Done():
							return
						}
						continue
					}

					index := snapshotData[line.StepId]
					snapshotData[line.StepId] = index + 1
					if index >= emittedData[line.StepId] {
						select {
						case out <- ciLogEvent(workflow, line):
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
				case out <- ciLogEvent(workflow, line):
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

func isTerminalPipelineStatus(status string) bool {
	switch status {
	case "success", "failed", "timeout", "cancelled":
		return true
	default:
		return false
	}
}

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

func optionalString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeXRPCError(w http.ResponseWriter, status int, name, message string) {
	writeJSON(w, status, map[string]string{"error": name, "message": message})
}
