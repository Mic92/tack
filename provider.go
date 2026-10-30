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

// LogLine is one log frame produced by a Provider: a line of step output or a step start or end
// marker. It mirrors tangled.org/core/spindle/models.LogLine field for field without importing
// that package, which pulls in git, vault, redis, and hundreds of other modules for a few types.
//
// Tack never sends LogLine as JSON. The XRPC log subscription converts each one into an
// sh.tangled or org.tangled subscribePipelineLogs event (see ciLogEvent and orgLogEvent). The
// JSON tags match upstream so the struct stays a faithful copy that is easy to compare against
// when Tangled adds fields.
type LogLine struct {
	Kind        string    `json:"kind"`
	Content     string    `json:"content"`
	Time        time.Time `json:"time"`
	StepId      int       `json:"step_id"`
	Stream      string    `json:"stream,omitempty"`
	StepStatus  string    `json:"step_status,omitempty"`
	StepKind    string    `json:"step_kind,omitempty"`
	StepCommand string    `json:"step_command,omitempty"`
	// StepOutcome is how a step ended: StepOutcomeSuccess or StepOutcomeFailed. Providers set it
	// only on StepStatusEnd lines. Tack forwards it only on end frames of the current (Tangled
	// v1.20) log stream, because the sh.tangled type in the pinned Tangled v1.16 module has no
	// field for it. Leave it empty when the provider cannot tell, such as for a Buildkite job that
	// was skipped. The Tangled web app uses it to mark the failed step. If no step in a failed
	// workflow has an outcome, the app blames the last step.
	StepOutcome string `json:"step_outcome,omitempty"`
}

// These are the wire values for LogLine.Kind, LogLine.StepStatus, and LogLine.StepOutcome.
// They match the constants of the same names in tangled.org/core/spindle/models. Use them
// instead of bare strings so the values cannot drift from upstream.
const (
	LogKindData     = "data"
	LogKindControl  = "control"
	StepStatusStart = "start"
	StepStatusEnd   = "end"

	// The step outcomes are the enum values of the outcome field in the
	// org.tangled.ci.subscribePipelineLogs lexicon.
	StepOutcomeSuccess = "success"
	StepOutcomeFailed  = "failed"
)

// ErrLogsNotFound is returned by Provider.Logs when the requested
// (knot, pipelineRkey, workflow) tuple has no recorded logs — either
// because that workflow never ran on this spindle, or because the
// provider has since dropped it. The CI log subscription retries this while
// a workflow is active to cover asynchronous provider startup.
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
	// on that knot. actor is the DID that the knot consumer has
	// already authorized to spawn this work; concretely, the
	// publisher of the originating sh.tangled.repo record (i.e.
	// the repo owner). It is guaranteed non-empty: the consumer
	// never invokes Spawn for an unauthorized trigger, so providers
	// can treat actor as "the identity to attribute this run to"
	// without re-checking authorization. trigger is the decoded
	// record's TriggerMetadata (may be nil; the lexicon doesn't
	// enforce its presence) and carries the commit/branch/PR data
	// a real CI provider needs to kick off a build. workflows is
	// the unmodified slice from the decoded sh.tangled.pipeline
	// record; implementations should tolerate nil entries and
	// zero-length names defensively, since the lexicon doesn't
	// enforce either.
	Spawn(
		ctx context.Context,
		knot string,
		pipelineRkey string,
		actor string,
		trigger *tangled.Pipeline_TriggerMetadata,
		workflows []*tangled.Pipeline_Workflow,
	)

	// Logs returns a channel of log frames for a single workflow run,
	// identified by the same (knot, pipelineRkey, workflow) tuple
	// Spawn was invoked with.
	//
	// The XRPC log subscription turns each LogLine into one control or data event of either
	// sh.tangled.ci.subscribePipelineLogs (legacy) or org.tangled.ci.subscribePipelineLogs
	// (current), depending on which method the client called.
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
	// tuple. Any other returned error indicates a backend failure. On a
	// non-nil error, the channel is nil and there is nothing to drain.
	Logs(
		ctx context.Context,
		knot string,
		pipelineRkey string,
		workflow string,
	) (<-chan LogLine, error)
}
