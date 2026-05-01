package main

// Provider is the abstraction over "the thing that turns a Tangled
// pipeline trigger into pipeline.status events". It exists so the rest
// of tack can stay agnostic to whether a given trigger is dispatched to
// Buildkite, run by a stub for testing, or anything else we plug in later.

import (
	"context"
	"errors"
	"time"

	"tangled.org/core/api/tangled"
)

// LogLine is the on-the-wire shape of a single log frame emitted by a
// Provider. It mirrors tangled.org/core/spindle/models.LogLine on the
// JSON level without importing the upstream package — that package
// transitively pulls in git, vault, redis and a few hundred other
// modules just to expose a handful of types we don't otherwise need.
//
// The JSON tags here MUST stay byte-compatible with the upstream
// struct: the appview's log proxy decodes against the upstream type,
// so any tag drift breaks the appview's renderer.
type LogLine struct {
	Kind        string    `json:"kind"`
	Content     string    `json:"content"`
	Time        time.Time `json:"time"`
	StepId      int       `json:"step_id"`
	Stream      string    `json:"stream,omitempty"`
	StepStatus  string    `json:"step_status,omitempty"`
	StepKind    string    `json:"step_kind,omitempty"`
	StepCommand string    `json:"step_command,omitempty"`
}

// LogKind / StepStatus enum values, matching the upstream constants
// (LogKindData, LogKindControl, StepStatusStart, StepStatusEnd) on
// the wire. Use these instead of bare strings so we don't drift.
const (
	LogKindData     = "data"
	LogKindControl  = "control"
	StepStatusStart = "start"
	StepStatusEnd   = "end"
)

// ErrLogsNotFound is returned by Provider.Logs when the requested
// (knot, pipelineRkey, workflow) tuple has no recorded logs — either
// because that workflow never ran on this spindle, or because the
// provider has since dropped it. The HTTP /logs handler translates
// this into a 404 *before* upgrading to WebSocket so the appview's
// dialer sees a real HTTP error rather than an immediate close.
var ErrLogsNotFound = errors.New("logs not found")

// Provider dispatches a Tangled pipeline trigger to whatever backend
// actually runs the workflows, and exposes per-workflow log retrieval.
//
// Implementations are responsible for publishing
// sh.tangled.pipeline.status records back through whatever channel
// they were constructed with.
type Provider interface {
	// Spawn kicks off a pipeline run for every workflow in workflows.
	//
	// It MUST be non-blocking: the caller is the eventconsumer worker
	// that's shared across all knot subscriptions, so per-pipeline
	// work has to live on its own goroutine. A typical implementation
	// fans out into a goroutine per workflow and returns immediately.
	//
	// ctx is the consumer's app-scoped context (lives until shutdown,
	// not just for the duration of one event). Implementations are
	// expected to honour cancellation: in-flight runs should wind
	// down without issuing further publishes once ctx is done.
	//
	// knot is the knot hostname the trigger arrived on; it's the
	// authority half of the pipeline ATURI that pipeline.status
	// records reference. pipelineRkey is the trigger record's rkey
	// on that knot. workflows is the unmodified slice from the
	// decoded sh.tangled.pipeline record; implementations should
	// tolerate nil entries and zero-length names defensively, since
	// the lexicon doesn't enforce either.
	Spawn(
		ctx context.Context,
		knot string,
		pipelineRkey string,
		workflows []*tangled.Pipeline_Workflow,
	)

	// Logs returns a channel of log frames for a single workflow run,
	// identified by the same (knot, pipelineRkey, workflow) tuple
	// Spawn was invoked with.
	//
	// Each LogLine corresponds 1:1 to one frame written by the HTTP
	// /logs WebSocket handler — the handler marshals the LogLine and
	// emits it as a single TextMessage so appview clients see the
	// exact same wire shape they would from the stock Tangled spindle
	// (whose on-disk log file holds the same records, one per line).
	//
	// The returned channel is closed by the provider when the stream
	// is complete; a closed channel is the only "end of stream"
	// signal — there is no separate done channel. Implementations
	// MUST also stop sending and close the channel when ctx is
	// cancelled, so a disconnecting client doesn't leak a producer
	// goroutine.
	//
	// Backend errors that occur *after* a successful return (e.g.
	// the upstream Buildkite log socket dying mid-stream) are logged
	// internally and surface to the consumer as an early channel
	// close. The same observable behaviour the appview already
	// handles for a websocket that closes mid-stream.
	//
	// Returns ErrLogsNotFound if no logs exist for the requested
	// tuple. Any other returned error indicates a backend failure
	// and should surface as a 5xx to the HTTP caller. On a non-nil
	// error, the channel is nil and there is nothing to drain.
	Logs(
		ctx context.Context,
		knot string,
		pipelineRkey string,
		workflow string,
	) (<-chan LogLine, error)
}
