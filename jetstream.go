package main

// This file wires tack into the AT Protocol firehose via Bluesky's
// "jetstream" — a JSON projection of the firehose served over a websocket
// (see https://github.com/bluesky-social/jetstream). Tangled rides on top of
// AT Proto: things like "this user is a spindle member", "this repo wants
// this spindle", etc. are all atproto records published to users' PDSes,
// and jetstream is how a service like a spindle observes them in real time.
//
// As a spindle, the records we care about are:
//
//   - sh.tangled.spindle.member       — owner authorizes a DID to use us
//   - sh.tangled.repo                 — a repo declares us as its spindle
//   - sh.tangled.repo.collaborator    — collaborators on those repos
//
// (Pipeline trigger records, sh.tangled.pipeline, do *not* come over
// jetstream; they're delivered by the knot servers via a separate event
// stream. That is plumbed in separately.)

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/bluesky-social/jetstream/pkg/client"
	"github.com/bluesky-social/jetstream/pkg/client/schedulers/sequential"
	jsmodels "github.com/bluesky-social/jetstream/pkg/models"
	"tangled.org/core/api/tangled"
)

// jetstream operation strings. The jetstream protocol publishes these as
// the Commit.Operation field; pulling them out as constants keeps the
// switch in handleJetstreamEvent honest about typos.
const (
	jsOpCreate = "create"
	jsOpUpdate = "update"
	jsOpDelete = "delete"
)

// jetstreamRewind is how far before the persisted cursor we resume on
// reconnect. Jetstream's docs recommend rewinding a few seconds because
// the cursor is a time-based filter and exact-boundary replay is not
// guaranteed gapless across disconnects. The handler's mutations are
// idempotent (UPSERTs and DELETEs keyed on (did, rkey)) so the small
// amount of duplicate replay this introduces is harmless.
const jetstreamRewind = 5 * time.Second

// startJetstream dials the configured jetstream endpoint and spawns a
// background goroutine that consumes events for the lifetime of ctx. It
// returns once the client is constructed; connection errors surface in
// logs, not return values, because the read loop is expected to reconnect
// on its own.
//
// The store is used for two things: loading the persisted cursor so we
// resume from the last seen event after a restart, and persisting
// observed records so the rest of tack can answer membership questions
// without re-reading the firehose.
//
// The logger is pulled from ctx (see log.go); falls back to slog.Default()
// if none is attached.
func startJetstream(ctx context.Context, cfg config, st *store, knots KnotConsumer) error {
	logger := loggerFrom(ctx).With("component", "jetstream")

	// `wantedCollections` is a server-side filter: jetstream will only send
	// us commit events whose record collection (NSID) is in this list. The
	// NSIDs come from tangled-core's generated lexicon types so they stay
	// in sync with whatever the appview/knots are publishing.
	collections := []string{
		tangled.SpindleMemberNSID,
		tangled.RepoNSID,
		tangled.RepoCollaboratorNSID,
	}

	// Configure our JetStream client.
	clientCfg := client.DefaultClientConfig()
	clientCfg.WebsocketURL = cfg.JetstreamURL
	clientCfg.WantedCollections = collections

	// The handler closes over `st`, `knots` and the spindle hostname so
	// the scheduler signature stays plain `func(ctx, *Event) error` and
	// applyCommit can hand the knot consumer new sources as soon as
	// matching repo records arrive.
	handler := func(ctx context.Context, evt *jsmodels.Event) error {
		return handleJetstreamEvent(ctx, st, knots, cfg.Hostname, evt)
	}

	// Re-attach the component-scoped logger so handler — which the
	// scheduler invokes with the ctx we pass to ConnectAndRead — can
	// pull it back out via loggerFrom.
	ctx = loggerInto(ctx, logger)

	// The sequential scheduler processes events one-at-a-time in arrival
	// order. That's the right default for a spindle: ordering matters
	// (e.g. a member-added event must apply before any record from that
	// member is processed), and our event volume is tiny.
	c, err := client.NewClient(
		clientCfg,
		logger,
		sequential.NewScheduler("tack", logger, handler),
	)
	if err != nil {
		return fmt.Errorf("new jetstream client: %w", err)
	}

	go func() {
		for {
			// We re-read the cursor from the store at every (re)connect so we
			// pick up any progress the previous connection persisted before
			// dying. nil means "start from now", which is the right default on
			// a brand-new install or after a corrupt cursor read.
			cur, err := st.LoadCursor(ctx)
			if err != nil {
				logger.Warn("ignoring unreadable cursor; resuming from now", "err", err)
				cur = nil
			}
			// Rewind a few seconds before the saved cursor on reconnect.
			// Jetstream cursors are time-based and the docs explicitly note
			// that exact-boundary replay is not guaranteed gapless, so a
			// small negative buffer protects against missing events that
			// straddle the disconnect. Duplicates the rewind produces are
			// safe: every applyCommit path is an idempotent upsert/delete
			// keyed on (did, rkey), and SaveCursor only moves forward in
			// practice because TimeUS is monotonic.
			if cur != nil {
				rewound := *cur - int64(jetstreamRewind/time.Microsecond)
				if rewound < 0 {
					rewound = 0
				}
				logger.Info("connecting to jetstream",
					"cursor_us", *cur,
					"rewound_us", rewound,
					"rewind", jetstreamRewind,
				)
				cur = &rewound
			} else {
				logger.Info("connecting to jetstream from now (no cursor)")
			}

			// Reconnect loop. ConnectAndRead blocks on the websocket and returns
			// either when the connection drops (transient network error, server
			// restart, etc.) or when ctx is cancelled. On error we sleep briefly
			// and reconnect; on ctx cancellation we exit cleanly.
			if err := c.ConnectAndRead(ctx, cur); err != nil {
				if ctx.Err() != nil {
					return
				}
				logger.Error("jetstream read loop", "err", err)
				time.Sleep(2 * time.Second)
				continue
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()

	return nil
}

// handleJetstreamEvent is the per-event callback for the JetStream. It
// applies the event to the store and, when appropriate, advances the
// persisted cursor. Any returned error is logged by the scheduler but
// does not tear down the connection.
//
// Cursor-advancement policy:
//
//   - Apply succeeded: advance the cursor.
//   - Apply failed with a badRecordError (malformed record / unusable
//     input): advance anyway, since replaying a permanently-broken
//     event on every reconnect would stall the firehose forever.
//   - Apply failed with anything else (treated as transient: store
//     hiccup, SQLite busy, disk full, shutdown race, ...): do NOT
//     advance. Saving the cursor here would permanently skip the
//     record, which for membership/repo state means we'd silently
//     lose a row that the rest of tack relies on. Leaving the cursor
//     in place gives the next reconnect (which rewinds by
//     jetstreamRewind) a chance to re-deliver and re-apply it.
func handleJetstreamEvent(ctx context.Context, st *store, knots KnotConsumer, hostname string, evt *jsmodels.Event) error {
	// We only care about commits, which are the actual record CRUD
	// operations on a user's PDS. Account/identity events are ignored
	// for now; if we ever care about handle changes we can add them.
	if evt.Kind != jsmodels.EventKindCommit || evt.Commit == nil {
		return nil
	}
	logger := loggerFrom(ctx)

	// Dispatch on collection. Unknown collections shouldn't happen given
	// our wantedCollections filter, but be defensive — jetstream may
	// send schema changes ahead of us updating the filter.
	applyErr := applyCommit(ctx, st, knots, hostname, evt)
	if applyErr != nil {
		logger.Error("apply commit",
			"err", applyErr,
			"did", evt.Did,
			"collection", evt.Commit.Collection,
			"op", evt.Commit.Operation,
			"rkey", evt.Commit.RKey,
			"transient", !isBadRecord(applyErr),
		)

		// Transient failure: bail without advancing the cursor so the
		// next delivery can retry this event. Returning the error
		// surfaces it in the scheduler's logs too.
		if !isBadRecord(applyErr) {
			return applyErr
		}
		// Otherwise (badRecordError) fall through to cursor save: a
		// single bad record shouldn't stall the cursor forever and
		// force us to re-process every subsequent event after a
		// restart.
	}

	// Advance the cursor. TimeUS is the jetstream-assigned microsecond
	// timestamp; saving it after-apply means a crash mid-batch will at
	// worst replay the failing event, never skip past it.
	if err := st.SaveCursor(ctx, evt.TimeUS); err != nil {
		// Returning the error logs it; it doesn't kill the scheduler.
		return fmt.Errorf("save cursor: %w", err)
	}

	return nil
}

// badRecordError marks an applyCommit failure as caused by the *record
// itself* being permanently unusable, e.g. a malformed JSON body, or a
// field that violates an invariant we can't recover from on retry.
//
// We need this distinction because the jetstream cursor is time-based
// and once advanced past an event we will never see it again. So:
//
//   - Permanent bad-input failures should still advance the cursor:
//     replaying the same broken record on every restart accomplishes
//     nothing and would stall progress on every later event behind it.
//   - Transient infrastructure failures (SQLite busy, disk full, store
//     closed mid-shutdown, etc.) must NOT advance the cursor: the
//     record is fine and the next attempt, on reconnect or after the
//     transient condition clears, should reapply it. Skipping past
//     such a failure can permanently lose membership/repo state.
//
// Anything returned from applyCommit that isn't a badRecordError is
// treated as transient by handleJetstreamEvent.
type badRecordError struct{ err error }

func (e *badRecordError) Error() string { return e.err.Error() }
func (e *badRecordError) Unwrap() error { return e.err }

// badRecord wraps err so handleJetstreamEvent recognizes it as a
// permanent, cursor-advancing failure. Use this for any error caused by
// the contents of the record (decode errors, schema violations); never
// for store/IO errors.
func badRecord(err error) error { return &badRecordError{err: err} }

// isBadRecord reports whether err (or anything it wraps) is a
// badRecordError.
func isBadRecord(err error) bool {
	var b *badRecordError
	return errors.As(err, &b)
}

// applyCommit routes a commit to the right store mutation based on its
// collection NSID and operation.
func applyCommit(ctx context.Context, st *store, knots KnotConsumer, hostname string, evt *jsmodels.Event) error {
	c := evt.Commit
	switch c.Collection {
	case tangled.SpindleMemberNSID:
		return applySpindleMember(ctx, st, evt.Did, c)
	case tangled.RepoNSID:
		return applyRepo(ctx, st, knots, hostname, evt.Did, c)
	case tangled.RepoCollaboratorNSID:
		return applyRepoCollaborator(ctx, st, evt.Did, c)
	default:
		// Server-side filter should prevent this, but log so we notice
		// if jetstream ever changes behavior.
		loggerFrom(ctx).Debug("ignoring unexpected collection",
			"collection", c.Collection)
		return nil
	}
}

func applySpindleMember(ctx context.Context, st *store, did string, c *jsmodels.Commit) error {
	switch c.Operation {
	case jsOpCreate, jsOpUpdate:
		var rec tangled.SpindleMember
		if err := json.Unmarshal(c.Record, &rec); err != nil {
			// Decode failures are a permanent property of the record's
			// bytes; mark as bad so the cursor can advance past it.
			return badRecord(fmt.Errorf("decode spindle.member: %w", err))
		}
		return st.UpsertSpindleMember(ctx, did, c.RKey, rec.Instance, rec.Subject, rec.CreatedAt)
	case jsOpDelete:
		return st.DeleteSpindleMember(ctx, did, c.RKey)
	}
	return nil
}

func applyRepo(ctx context.Context, st *store, knots KnotConsumer, hostname string, did string, c *jsmodels.Commit) error {
	switch c.Operation {
	case jsOpCreate, jsOpUpdate:
		var rec tangled.Repo
		if err := json.Unmarshal(c.Record, &rec); err != nil {
			// See applySpindleMember: decode errors are permanent.
			return badRecord(fmt.Errorf("decode repo: %w", err))
		}

		// Capture the prior (knot, spindle) before the upsert so the
		// post-mutation reconcile below can detect transitions like
		// "repo used to point at us, no longer does" — which would
		// otherwise leave a knot subscription dangling.
		oldKnot, oldSpindle, err := st.GetRepo(ctx, did, c.RKey)
		if err != nil {
			return err
		}

		if err := st.UpsertRepo(ctx, did, c.RKey,
			rec.Knot, rec.Name,
			deref(rec.Spindle), deref(rec.RepoDid),
			rec.CreatedAt,
		); err != nil {
			return err
		}

		newSpindle := deref(rec.Spindle)
		return reconcileKnot(ctx, st, knots, hostname,
			oldKnot, oldSpindle,
			rec.Knot, newSpindle,
		)

	case jsOpDelete:
		// Same shape as the update path, just with no "new" side: we
		// have to read the row out before deleting so we can decide
		// whether deletion freed the last hold on a knot we'd been
		// subscribed to.
		oldKnot, oldSpindle, err := st.GetRepo(ctx, did, c.RKey)
		if err != nil {
			return err
		}
		if err := st.DeleteRepo(ctx, did, c.RKey); err != nil {
			return err
		}
		return reconcileKnot(ctx, st, knots, hostname,
			oldKnot, oldSpindle,
			"", "",
		)
	}
	return nil
}

// reconcileKnot brings the knot consumer's subscriptions in line with
// the latest store state after a single repo mutation. It is called
// after the mutation has been applied so IsKnotWanted reflects the
// post-mutation truth.
//
// Logic:
//   - If the new record names us as its spindle, ensure we're
//     subscribed to its knot (AddKnot is idempotent, so calling it on
//     an already-watched knot is cheap).
//   - If the old record named us as its spindle, check whether any
//     other repo still references that knot through us; if not,
//     RemoveKnot it. Skip this when the knot didn't change AND the
//     spindle didn't move away from us, because then nothing actually
//     released our hold.
func reconcileKnot(
	ctx context.Context,
	st *store,
	knots KnotConsumer,
	hostname string,
	oldKnot, oldSpindle string,
	newKnot, newSpindle string,
) error {
	// Tests pass nil for the consumer when they only care about the
	// store mutation half of the handler; tolerate that here so
	// callers don't have to special-case it.
	if knots == nil {
		return nil
	}

	if newSpindle == hostname && newKnot != "" {
		knots.AddKnot(ctx, newKnot)
	}

	// Did we just lose our claim on oldKnot? Two ways that can happen:
	// the spindle field moved off of us, or the knot field moved to a
	// different host. Either is a reason to consider unsubscribing
	// from oldKnot — but only if no *other* repo still has us on it.
	releasedOld := oldSpindle == hostname && oldKnot != "" &&
		(newSpindle != hostname || newKnot != oldKnot)
	if releasedOld {
		stillWanted, err := st.IsKnotWanted(ctx, hostname, oldKnot)
		if err != nil {
			return err
		}
		if !stillWanted {
			knots.RemoveKnot(ctx, oldKnot)
		}
	}
	return nil
}

func applyRepoCollaborator(ctx context.Context, st *store, did string, c *jsmodels.Commit) error {
	switch c.Operation {
	case jsOpCreate, jsOpUpdate:
		var rec tangled.RepoCollaborator
		if err := json.Unmarshal(c.Record, &rec); err != nil {
			// See applySpindleMember: decode errors are permanent.
			return badRecord(fmt.Errorf("decode repo.collaborator: %w", err))
		}
		return st.UpsertRepoCollaborator(ctx, did, c.RKey,
			deref(rec.Repo), deref(rec.RepoDid),
			rec.Subject, rec.CreatedAt,
		)
	case jsOpDelete:
		return st.DeleteRepoCollaborator(ctx, did, c.RKey)
	}
	return nil
}

// deref returns the pointed-to string, or "" for nil. The lexicon types
// model optional fields as *string; the store schema treats absent and
// empty the same, so collapsing the two here keeps callers tidy.
func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
