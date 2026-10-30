package main

// fakeProvider is a stand-in Provider implementation: it doesn't talk
// to any external CI. For each workflow in a triggered pipeline it
// spawns a goroutine that emits a fixed-cadence stream of
// sh.tangled.pipeline.status records — "running" every five seconds
// for thirty seconds, then a final "success" — through the broker.
// In parallel it appends synthetic LogLine records into an in-memory
// buffer, so log subscriptions against a fake run replay plausible output.
//
// The point is to exercise the trigger → persistence → XRPC path end-to-end
// before any real CI integration exists. Once
// the Buildkite provider lands, this one stays around as a reference
// implementation and as the test double of choice when a test wants
// "something that publishes plausible status updates" without the
// timing weight of real builds.

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
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

// fakeLogChanBuffer is the size of the buffered channel returned by
// fakeProvider.Logs. Big enough that a snapshot of a completed fake
// run (≈ 8 lines today) drops in without blocking the producer
// goroutine even if the consumer is briefly slow; small enough that
// a misbehaving consumer can't pin a runaway amount of memory.
const fakeLogChanBuffer = 64

// fakeProvider implements Provider against the in-process broker.
//
// logs holds the captured per-workflow LogLine slices, keyed by the
// fakeLogKey of (knot, pipelineRkey, workflow). The mutex guards the
// map *and* concurrent appends to any contained slice — runWorkflow
// is the only writer for a given key, but Logs() may snapshot the
// slice concurrently.
type fakeProvider struct {
	br  *broker
	log *slog.Logger

	mu   sync.Mutex
	logs map[string][]LogLine
}

// Compile-time interface check — keeps the fake honest if Provider
// ever gains additional methods.
var _ Provider = (*fakeProvider)(nil)

// newFakeProvider constructs a fakeProvider bound to br. The provided
// logger is annotated with component=provider so its output stands
// apart from the knot-consumer / jetstream noise.
func newFakeProvider(br *broker, log *slog.Logger) *fakeProvider {
	return &fakeProvider{
		br:   br,
		log:  log.With("component", "provider", "kind", "fake"),
		logs: make(map[string][]LogLine),
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
	_ string,
	_ *tangled.Pipeline_TriggerMetadata,
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

// Logs satisfies Provider. It snapshots the in-memory LogLine slice
// for (knot, pipelineRkey, workflow) and returns a buffered channel
// that a goroutine drains the snapshot into. The snapshot is taken
// under the mutex so subsequent appends by a still-running workflow
// won't be reflected — clients that want live updates re-poll. We
// don't follow live writes here; the fake's purpose is to exercise
// the wire format end-to-end, not to reproduce hpcloud/tail.
//
// The producer goroutine honours ctx so that a disconnecting HTTP
// client tears the channel send down promptly instead of waiting on
// an unbuffered receiver.
func (p *fakeProvider) Logs(
	ctx context.Context,
	knot string,
	pipelineRkey string,
	workflow string,
) (<-chan LogLine, error) {
	key := fakeLogKey(knot, pipelineRkey, workflow)

	p.mu.Lock()
	stored, ok := p.logs[key]
	if !ok {
		p.mu.Unlock()
		return nil, ErrLogsNotFound
	}
	// Copy under the lock so subsequent appendLogLine writes can't
	// race with our drain goroutine reading the slice header.
	snapshot := make([]LogLine, len(stored))
	copy(snapshot, stored)
	p.mu.Unlock()

	out := make(chan LogLine, fakeLogChanBuffer)
	go func() {
		defer close(out)
		for _, line := range snapshot {
			select {
			case <-ctx.Done():
				return
			case out <- line:
			}
		}
	}()
	return out, nil
}

// runWorkflow emits a "running" status every fakeJobInterval until
// fakeJobDuration elapses, then a final "success". Alongside each
// status it appends a corresponding LogLine into the in-memory log buffer so
// subscriptions return something coherent.
//
// On ctx cancellation it returns without issuing the terminal publish
// or the closing control frame — the broker's underlying store may
// already be closing during shutdown.
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

	// Start control frame. The appview's log renderer keys timing on
	// matching start/end control frames per step_id, so we always
	// emit one even though the fake has only a single synthetic step.
	p.appendLogLine(knot, pipelineRkey, workflow, LogLine{
		Kind:       LogKindControl,
		Time:       time.Now(),
		Content:    workflow,
		StepId:     0,
		StepStatus: StepStatusStart,
	})

	// Heartbeat phase. seq doubles as a per-workflow disambiguator
	// in the synthesized status rkey so concurrent fakes (across
	// workflows or pipelines) don't collide.
	deadline := time.Now().Add(fakeJobDuration)
	seq := 0
	for time.Now().Before(deadline) {
		p.appendLogLine(knot, pipelineRkey, workflow, LogLine{
			Kind: LogKindData,
			Time: time.Now(),
			Content: fmt.Sprintf("[fake] heartbeat %d at %s\n",
				seq, time.Now().UTC().Format(time.RFC3339),
			),
			StepId: 0,
			Stream: "stdout",
		})
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

	// End control marker and terminal status. The marker reports StepOutcomeSuccess because the
	// fake's single step always passes. Current (org.tangled.*) log subscribers such as the
	// Tangled web app read the outcome to decide which step failed, and legacy subscribers never
	// see it. The "success" status string matches the upstream StatusKind enum (see
	// tangled.org/core/spindle/models), the enum Tangled checks status strings against.
	p.appendLogLine(knot, pipelineRkey, workflow, LogLine{
		Kind:        LogKindControl,
		Time:        time.Now(),
		Content:     workflow,
		StepId:      0,
		StepStatus:  StepStatusEnd,
		StepOutcome: StepOutcomeSuccess,
	})
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

// appendLogLine records line as the next captured frame for the
// workflow, allocating a slice on first use. Holding the mutex across
// the append ensures a concurrent Logs() snapshot doesn't observe a
// half-grown slice header.
func (p *fakeProvider) appendLogLine(knot, pipelineRkey, workflow string, line LogLine) {
	key := fakeLogKey(knot, pipelineRkey, workflow)
	p.mu.Lock()
	defer p.mu.Unlock()
	p.logs[key] = append(p.logs[key], line)
}

// fakeLogKey is the canonical map key for the in-memory log buffer.
// Centralised so reader and writer can't drift on separator choice.
func fakeLogKey(knot, pipelineRkey, workflow string) string {
	return knot + "\x00" + pipelineRkey + "\x00" + workflow
}
