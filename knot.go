package main

// Knot event-stream subscriber.
//
// Tangled knot servers expose a websocket at ws[s]://<knot>/events that
// streams JSON-wrapped record events for that knot, including the
// sh.tangled.pipeline trigger records that drive CI. Pipeline triggers do
// *not* come over the AT Proto firehose (jetstream); the knot publishes
// them itself, so as a spindle we have to dial each knot whose repos have
// pointed at us.
//
// We use tangled-core's `eventconsumer` package, which already handles
// per-source connection management, retries, ordered processing and
// cursor tracking. We hand it:
//
//   1. The initial set of knots, derived from previously-observed
//      sh.tangled.repo records that named us as their .spindle field.
//   2. A ProcessFunc that, for now, simply logs every received event.
//      Once the build pipeline is wired up this is where pipeline
//      triggers will be translated into Buildkite builds.
//
// The jetstream consumer also gets a back-reference (via the
// KnotConsumer interface) so it can dynamically subscribe a new knot —
// or, conceptually, unsubscribe one — the moment a matching
// sh.tangled.repo record arrives, without waiting for a tack restart.

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"tangled.org/core/api/tangled"
	"tangled.org/core/eventconsumer"
)

// KnotConsumer is the surface area the rest of tack uses to interact
// with the knot event-stream subscriber. It exists so we can test
// knot interactions from tests.
//
// Implementations must be safe for concurrent use: AddKnot is invoked
// from the jetstream goroutine while the consumer's worker goroutines
// are independently processing inbound knot events.
//
// The fake at knot_fake.go provides a no-network implementation suitable
// for use from tests.
type KnotConsumer interface {
	// AddKnot subscribes to the given knot's /events websocket if not
	// already subscribed. Calling with the same knot more than once is
	// a no-op. An empty knot string is ignored. The supplied context
	// scopes the dial; cancelling it tears the subscription down.
	AddKnot(ctx context.Context, knot string)

	// RemoveKnot stops processing events from the given knot. It is the
	// inverse of AddKnot and must tolerate being called for a knot that
	// was never added (no-op). An empty knot string is ignored.
	//
	// The production implementation is currently a no-op: tangled-core's
	// eventconsumer does not expose a way to drop an individual source's
	// websocket. Tracked upstream as
	// https://tangled.org/did:plc:j5hmlfdrwkvtxm7cjmu7j2is/issues/510
	RemoveKnot(ctx context.Context, knot string)
}

// knotConsumer is the production KnotConsumer. It wraps
// eventconsumer.Consumer with the small surface the rest of tack actually
// wants: AddKnot for dynamic subscription, plus a Stop lifecycle hook
// owned by main.
//
// Wrapping (instead of exposing *eventconsumer.Consumer directly) keeps
// callers from importing eventconsumer just to construct a KnotSource,
// and lets us swap or extend the underlying transport later.
type knotConsumer struct {
	c   *eventconsumer.Consumer
	log *slog.Logger

	// provider dispatches each incoming pipeline trigger to whatever
	// backend actually runs it (today: the fake provider; tomorrow:
	// Buildkite). The consumer doesn't care which — it just hands
	// over the decoded record and lets the provider publish status
	// records back through its own broker connection.
	provider Provider
}

// Compile-time interface conformance check.
var _ KnotConsumer = (*knotConsumer)(nil)

// startKnotConsumer builds a knot event consumer pre-loaded with every
// knot already known to the store, starts its connection loops in the
// background, and returns the wrapper. The consumer keeps running until
// ctx is cancelled.
//
// Cursor persistence is intentionally in-memory for now: we only log
// events, so re-receiving a few seconds of pipeline triggers after a
// restart is harmless. When we start translating triggers into real
// Buildkite builds, this should switch to a SQLite-backed cursor store
// to avoid duplicate builds.
func startKnotConsumer(ctx context.Context, cfg config, st *store, provider Provider) (*knotConsumer, error) {
	logger := loggerFrom(ctx).With("component", "knotconsumer")

	knots, err := st.KnotsForSpindle(ctx, cfg.Hostname)
	if err != nil {
		return nil, fmt.Errorf("load known knots: %w", err)
	}

	kc := &knotConsumer{log: logger, provider: provider}

	ccfg := eventconsumer.NewConsumerConfig()
	ccfg.Logger = logger
	ccfg.Dev = cfg.Dev
	ccfg.ProcessFunc = kc.process
	for _, k := range knots {
		ccfg.Sources[eventconsumer.NewKnotSource(k)] = struct{}{}
		logger.Info("seeding knot source", "knot", k)
	}
	kc.c = eventconsumer.NewConsumer(*ccfg)

	// Start workers + per-source connection loops. Consumer.Start is
	// non-blocking; the goroutines it spawns observe ctx for shutdown.
	kc.c.Start(ctx)
	logger.Info("knot consumer started", "initial_knots", len(knots))

	return kc, nil
}

// AddKnot subscribes to a knot we hadn't been watching before. Safe to
// call repeatedly: eventconsumer.Consumer.AddSource deduplicates by the
// source's Key (the knot hostname), so passing the same knot twice is a
// no-op.
func (k *knotConsumer) AddKnot(ctx context.Context, knot string) {
	if knot == "" {
		return
	}
	k.log.Info("adding knot source", "knot", knot)
	k.c.AddSource(ctx, eventconsumer.NewKnotSource(knot))
}

// RemoveKnot is currently a no-op — see the interface comment for the
// upstream blocker. We still log at info so operators can see when the
// reconciliation logic *would* have unsubscribed; once eventconsumer
// gains a RemoveSource primitive, swap the body for a real call.
func (k *knotConsumer) RemoveKnot(_ context.Context, knot string) {
	if knot == "" {
		return
	}
	k.log.Info("remove knot source requested (no-op until upstream supports it)",
		"knot", knot,
	)
}

// Stop tears down all knot websocket connections and waits for the
// consumer's goroutines to exit. It must be called exactly once.
func (k *knotConsumer) Stop() {
	k.c.Stop()
}

// process is the ProcessFunc handed to eventconsumer. It runs once per
// inbound message, on a worker goroutine. For now we only care about
// pipeline records — everything else is logged at debug and dropped.
//
// Returning an error only logs it (the consumer keeps reading); the
// cursor is advanced before the ProcessFunc runs, so a returned error
// does *not* cause a replay.
func (k *knotConsumer) process(ctx context.Context, src eventconsumer.Source, msg eventconsumer.Message) error {
	switch msg.Nsid {
	case tangled.PipelineNSID:
		var p tangled.Pipeline
		if err := json.Unmarshal(msg.EventJson, &p); err != nil {
			k.log.Error("decode pipeline",
				"err", err,
				"knot", src.Key(),
				"rkey", msg.Rkey,
			)
			return err
		}

		// Pull a couple of fields out of the trigger metadata for log
		// context. They're all optional in the schema, so each one is
		// guarded — we want a noisy log entry, not a nil-deref.
		var (
			triggerKind string
			repoDid     string
			repoName    string
		)
		if p.TriggerMetadata != nil {
			triggerKind = p.TriggerMetadata.Kind
			if p.TriggerMetadata.Repo != nil {
				repoDid = p.TriggerMetadata.Repo.Did
				if p.TriggerMetadata.Repo.Repo != nil {
					repoName = *p.TriggerMetadata.Repo.Repo
				}
			}
		}

		k.log.Info("pipeline event",
			"knot", src.Key(),
			"rkey", msg.Rkey,
			"trigger", triggerKind,
			"repo_did", repoDid,
			"repo", repoName,
			"workflows", len(p.Workflows),
		)

		// Hand the trigger to whichever Provider was configured.
		// Spawn is non-blocking — it fans out into provider-owned
		// goroutines so this worker can move on to the next event.
		// The provider keeps ctx around for shutdown coordination.
		k.provider.Spawn(ctx, src.Key(), msg.Rkey, p.Workflows)

	default:
		// Knots may publish other record types over the same stream; we
		// don't care about them yet. Debug-only so it's available when
		// chasing "why isn't my pipeline firing" but doesn't drown out
		// info-level logs.
		k.log.Debug("ignored knot event",
			"knot", src.Key(),
			"nsid", msg.Nsid,
			"rkey", msg.Rkey,
		)
	}

	return nil
}
