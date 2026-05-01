package main

// fakeProvider is a stand-in Provider implementation: it doesn't talk
// to any external CI. For each workflow in a triggered pipeline it
// spawns a goroutine that emits a fixed-cadence stream of
// sh.tangled.pipeline.status records — "running" every five seconds
// for thirty seconds, then a final "success" — through the broker.
//
// The point is to exercise the entire trigger → broker → /events →
// appview path end-to-end before any real CI integration exists. Once
// the Buildkite provider lands, this one stays around as a reference
// implementation and as the test double of choice when a test wants
// "something that publishes plausible status updates" without the
// timing weight of real builds.

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"tangled.org/core/api/tangled"
)

// Fake-job timing knobs. Pulled out as constants so it's obvious
// where the numbers come from and they can be tuned independently of
// the rest of the file. Total publishes per workflow =
// (fakeJobDuration / fakeJobInterval) heartbeats + 1 final success.
const (
	fakeJobDuration = 30 * time.Second
	fakeJobInterval = 5 * time.Second
)

// fakeProvider implements Provider against the in-process broker.
type fakeProvider struct {
	br  *broker
	log *slog.Logger
}

// Compile-time interface check — keeps the fake honest if Provider
// ever gains additional methods.
var _ Provider = (*fakeProvider)(nil)

// newFakeProvider constructs a fakeProvider bound to br. The provided
// logger is annotated with component=provider so its output stands
// apart from the knot-consumer / jetstream noise.
func newFakeProvider(br *broker, log *slog.Logger) *fakeProvider {
	return &fakeProvider{
		br:  br,
		log: log.With("component", "provider", "kind", "fake"),
	}
}

// Spawn satisfies Provider. It kicks off one runWorkflow goroutine per
// workflow, returning immediately so the eventconsumer worker that
// invoked us isn't blocked. Goroutines inherit ctx (app-scoped) and
// will exit early on cancellation.
func (p *fakeProvider) Spawn(
	ctx context.Context,
	knot string,
	pipelineRkey string,
	workflows []*tangled.Pipeline_Workflow,
) {
	if len(workflows) == 0 {
		// Without a workflow name there's no valid pipeline.status
		// record to publish. Log loudly enough that an operator
		// staring at the logs can tell the trigger arrived but
		// produced no fake activity.
		p.log.Warn("pipeline has no workflows; skipping fake run",
			"knot", knot, "rkey", pipelineRkey,
		)
		return
	}
	for _, wf := range workflows {
		// Defensive: the lexicon allows pointer entries and doesn't
		// enforce non-empty names. We can't publish a status for an
		// unnamed workflow, so just skip it.
		if wf == nil || wf.Name == "" {
			continue
		}
		go p.runWorkflow(ctx, knot, pipelineRkey, wf.Name)
	}
}

// runWorkflow emits a "running" status every fakeJobInterval until
// fakeJobDuration elapses, then a final "success". On ctx
// cancellation it returns without issuing the terminal publish — the
// broker's underlying store may already be closing during shutdown.
func (p *fakeProvider) runWorkflow(ctx context.Context, knot, pipelineRkey, workflow string) {
	// pipelineURI is what the appview parses out of the status record
	// to associate it back with the originating pipeline. Format
	// mirrors the upstream spindle's emission:
	// at://did:web:<knot>/<nsid>/<rkey>. The appview strips the
	// did:web: prefix and treats the remainder as the knot identifier.
	pipelineURI := fmt.Sprintf("at://did:web:%s/%s/%s",
		knot, tangled.PipelineNSID, pipelineRkey,
	)

	logger := p.log.With(
		"knot", knot,
		"pipeline_rkey", pipelineRkey,
		"workflow", workflow,
	)

	// Heartbeat phase. seq doubles as a per-workflow disambiguator
	// in the synthesized status rkey so concurrent fakes (across
	// workflows or pipelines) don't collide.
	deadline := time.Now().Add(fakeJobDuration)
	seq := 0
	for time.Now().Before(deadline) {
		if err := p.publishStatus(ctx, pipelineURI, workflow, "running", seq); err != nil {
			logger.Error("publish fake running status", "err", err, "seq", seq)
			return
		}
		seq++
		select {
		case <-ctx.Done():
			logger.Debug("fake job cancelled mid-run", "seq", seq)
			return
		case <-time.After(fakeJobInterval):
		}
	}

	// Terminal publish. "success" matches the upstream StatusKind
	// enum (see tangled.org/core/spindle/models) — the appview
	// routes status strings through that same enum.
	if err := p.publishStatus(ctx, pipelineURI, workflow, "success", seq); err != nil {
		logger.Error("publish fake success status", "err", err, "seq", seq)
		return
	}
	logger.Info("fake job complete")
}

// publishStatus assembles a tangled.PipelineStatus record, marshals
// it, and pushes it through the broker. The synthesized rkey just
// needs to be unique within our event log; the appview keys its rows
// on (spindle, rkey) so we mix in time + workflow + sequence to avoid
// collisions across concurrent workflows on the same pipeline.
func (p *fakeProvider) publishStatus(ctx context.Context, pipelineURI, workflow, status string, seq int) error {
	rec := tangled.PipelineStatus{
		LexiconTypeID: tangled.PipelineStatusNSID,
		Pipeline:      pipelineURI,
		Workflow:      workflow,
		Status:        status,
		CreatedAt:     time.Now().UTC().Format(time.RFC3339),
	}
	body, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("marshal pipeline.status: %w", err)
	}
	rkey := fmt.Sprintf("fake-%d-%s-%d", time.Now().UnixNano(), workflow, seq)
	if _, err := p.br.Publish(ctx, rkey, tangled.PipelineStatusNSID, body); err != nil {
		return fmt.Errorf("publish pipeline.status: %w", err)
	}
	return nil
}
