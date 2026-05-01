package main

// Provider is the abstraction over "the thing that turns a Tangled
// pipeline trigger into pipeline.status events". It exists so the rest
// of tack can stay agnostic to whether a given trigger is dispatched to
// Buildkite, run by a stub for testing, or anything else we plug in later.

import (
	"context"

	"tangled.org/core/api/tangled"
)

// Provider dispatches a Tangled pipeline trigger to whatever backend
// actually runs the workflows.
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
}
