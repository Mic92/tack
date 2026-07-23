package main

// Knot event-stream subscriber.
//
// Tangled knot servers expose a websocket at ws[s]://<knot>/events that
// streams JSON-wrapped record events for that knot. Since Tangled 1.14,
// sh.tangled.git.refUpdate is the CI input: the spindle discovers workflows
// and creates the sh.tangled.pipeline record itself.
//
// We use tangled-core's `eventconsumer` package, which already handles
// per-source connection management, retries, ordered processing and
// cursor tracking. We hand it:
//
//   1. The initial set of knots, derived from previously-observed
//      sh.tangled.repo records that named us as their .spindle field.
//   2. A ProcessFunc that turns authorized ref updates into provider builds.
//
// The jetstream consumer also gets a back-reference (via the
// KnotConsumer interface) so it can dynamically subscribe a new knot —
// or unsubscribe one — the moment a matching sh.tangled.repo record
// arrives, without waiting for a tack restart.

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"tangled.org/core/api/tangled"
	"tangled.org/core/eventconsumer"
	"tangled.org/core/eventconsumer/cursor"
	"tangled.org/core/eventstream"
	knotdb "tangled.org/core/knotserver/db"
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

	// cursors is the persistent cursor store the underlying consumer
	// reads at every (re)connect. We retain a reference here so we
	// can pre-seed an entry to "now" the first time we ever see a
	// knot — see seedCursorIfMissing for why.
	cursors cursor.Store

	// pipelines owns the post-1.13 protocol: it turns low-level knot ref
	// updates into retained spindle-local pipeline records and provider work.
	pipelines *pipelineService
	dev       bool
}

// Compile-time interface conformance check.
var _ KnotConsumer = (*knotConsumer)(nil)

// startKnotConsumer builds a knot event consumer pre-loaded with every
// knot already known to the store, starts its connection loops in the
// background, and returns the wrapper. The consumer keeps running until
// ctx is cancelled.
//
// Cursor persistence is backed by tangled-core's SQLite cursor store
// pointed at the same database file as the rest of tack. Without
// this, a restart would replay every recent pipeline event from each
// knot and fire a duplicate Buildkite build for each one. The
// cursor.SqliteStore opens its own *sql.DB on the file, but
// mattn/go-sqlite3 in WAL mode tolerates concurrent connections, and
// the `cursors` table the upstream package creates doesn't collide
// with any of our migrations.
func startKnotConsumer(
	ctx context.Context,
	cfg config,
	st *store,
	br *broker,
	provider Provider,
) (*knotConsumer, error) {
	logger := loggerFrom(ctx).With("component", "knotconsumer")

	knots, err := st.KnotsForSpindle(ctx, cfg.Hostname, cfg.OwnerDID)
	if err != nil {
		return nil, fmt.Errorf("load known knots: %w", err)
	}

	// Persistent per-source cursor store. Keyed by source.Key() (the
	// kind-prefixed knot hostname for KnotSource), so each knot resumes exactly
	// where it left off and we don't re-fire builds for events that
	// were already processed before the previous shutdown.
	cursorStore, err := cursor.NewSQLiteStore(cfg.DBPath)
	if err != nil {
		return nil, fmt.Errorf("open knot cursor store: %w", err)
	}

	kc := &knotConsumer{
		log:       logger,
		cursors:   cursorStore,
		pipelines: newPipelineService(ctx, cfg, st, br, provider),
		dev:       cfg.Dev,
	}

	ccfg := eventconsumer.NewConsumerConfig()
	ccfg.Logger = logger
	ccfg.ProcessFunc = kc.process
	ccfg.CursorStore = cursorStore
	for _, k := range knots {
		// Pin a brand-new knot's cursor to "now" before the consumer
		// ever connects, so a fresh tack install doesn't replay every
		// historical pipeline event the knot has retained and fire a
		// duplicate Buildkite build for each one.
		source := kc.source(k)
		eventconsumer.MigrateLegacyCursor(cursorStore, source)
		kc.seedCursorIfMissing(source)
		ccfg.Sources[source] = struct{}{}
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
	// Same first-run protection as in startKnotConsumer: a knot we
	// have never observed before must not retroactively fire
	// pipelines for triggers older than the moment we learned about
	// it. AddSource reads the cursor synchronously when it dials.
	source := k.source(knot)
	eventconsumer.MigrateLegacyCursor(k.cursors, source)
	k.seedCursorIfMissing(source)
	k.log.Info("adding knot source", "knot", knot)
	k.c.AddSource(ctx, source)
}

// seedCursorIfMissing writes the current time (as nanoseconds since
// the unix epoch, the same unit eventconsumer.worker uses when it
// advances cursors after each message) to the cursor store for knot
// iff no cursor is already persisted for it.
//
// Without this, a brand-new install — or the first time we ever
// dial a previously-unseen knot — connects to /events with no
// cursor query parameter, which the knot servers interpret as
// "stream from the beginning of time." For pipeline records that
// would fire one Buildkite build per historical trigger the knot
// still has retained. Pinning the cursor up-front limits us to
// events that arrive *after* tack learned about the knot.
//
// Get returning 0 means "no cursor stored" across all the cursor
// store implementations we use (memory/sqlite/redis), so it's a
// safe sentinel to gate the write.
func (k *knotConsumer) seedCursorIfMissing(source eventconsumer.Source) {
	if k.cursors.Get(source.Key()) != 0 {
		return
	}
	now := time.Now().UnixNano()
	k.cursors.Set(source.Key(), now)
	k.log.Info("seeded fresh knot cursor to now",
		"knot", source.Host,
		"cursor", now,
	)
}

func (k *knotConsumer) source(knot string) eventconsumer.Source {
	source := eventconsumer.NewKnotSource(knot)
	if k.dev {
		source.NoTLS = true
	}
	return source
}

// RemoveKnot unsubscribes from a knot that no longer has any authorized
// repos pointing at this spindle. Safe to call repeatedly:
// eventconsumer.Consumer.RemoveSource ignores sources that are not present.
func (k *knotConsumer) RemoveKnot(_ context.Context, knot string) {
	if knot == "" {
		return
	}
	k.log.Info("removing knot source", "knot", knot)
	k.c.RemoveSource(k.source(knot))
}

// Stop tears down all knot websocket connections and waits for the
// consumer's goroutines to exit. It must be called exactly once.
func (k *knotConsumer) Stop() {
	k.c.Stop()
}

// process is the ProcessFunc handed to eventconsumer. It runs once per
// inbound message, on a worker goroutine. For now we only care about
// git ref updates; everything else is logged at debug and dropped.
//
// Returning an error only logs it (the consumer keeps reading); the
// cursor is advanced after the ProcessFunc runs even if it returns an error,
// so a returned error does not cause a replay.
func (k *knotConsumer) process(
	ctx context.Context,
	src eventconsumer.Source,
	msg eventstream.Event,
) error {
	switch msg.Nsid {
	case tangled.GitRefUpdateNSID:
		var event tangled.GitRefUpdate
		if err := json.Unmarshal(msg.EventJson, &event); err != nil {
			k.log.Error("decode git ref update",
				"err", err,
				"knot", src.Host,
				"rkey", msg.Rkey,
			)
			return err
		}
		if err := k.pipelines.processRefUpdate(ctx, src, &event); err != nil {
			k.log.Warn("git ref update failed; scheduling retry", "err", err, "repo", event.Repo)
			k.retryPipelineEvent("git ref update", func(ctx context.Context) error {
				return k.pipelines.processRefUpdate(ctx, src, &event)
			})
		}
		return nil
	case knotdb.RepoCollaboratorUpdateNSID:
		var event knotdb.RepoCollaboratorUpdate
		if err := json.Unmarshal(msg.EventJson, &event); err != nil {
			return fmt.Errorf("decode knot collaborator update: %w", err)
		}
		if err := k.pipelines.processKnotCollaborator(ctx, src, &event); err != nil {
			k.log.Warn("collaborator update failed; scheduling retry",
				"err", err, "repo", event.Repo,
			)
			k.retryPipelineEvent("collaborator update", func(ctx context.Context) error {
				return k.pipelines.processKnotCollaborator(ctx, src, &event)
			})
		}
		return nil

	default:
		// Knots may publish other record types over the same stream; we
		// don't care about them yet. Debug-only so it's available when
		// chasing "why isn't my pipeline firing" but doesn't drown out
		// info-level logs.
		k.log.Debug("ignored knot event",
			"knot", src.Host,
			"nsid", msg.Nsid,
			"rkey", msg.Rkey,
		)
	}

	return nil
}

func (k *knotConsumer) retryPipelineEvent(
	kind string,
	process func(context.Context) error,
) {
	go func() {
		delay := 15 * time.Second
		for attempt := 1; attempt <= 5; attempt++ {
			timer := time.NewTimer(delay)
			select {
			case <-k.pipelines.rootCtx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			if err := process(k.pipelines.rootCtx); err == nil {
				k.log.Info("retried knot event successfully", "kind", kind, "attempt", attempt)
				return
			} else {
				k.log.Warn("knot event retry failed",
					"kind", kind, "attempt", attempt, "err", err,
				)
			}
			if delay < 2*time.Minute {
				delay *= 2
			}
		}
	}()
}
