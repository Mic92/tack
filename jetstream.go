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
	"fmt"

	jsmodels "github.com/bluesky-social/jetstream/pkg/models"
	js "go.mitchellh.com/tack/internal/jetstream"
	"tangled.org/core/api/tangled"
)

// jetstream operation strings. The jetstream protocol publishes these as
// the Commit.Operation field; pulling them out as constants keeps the
// switch in applyCommit honest about typos.
const (
	jsOpCreate = "create"
	jsOpUpdate = "update"
	jsOpDelete = "delete"
)

var _ js.CursorStore = (*store)(nil)

// jetstreamCollections is the server-side and local filter for the Tangled
// records tack mirrors out of jetstream.
var jetstreamCollections = []string{
	tangled.SpindleMemberNSID,
	tangled.RepoNSID,
	tangled.RepoCollaboratorNSID,
}

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

	// The handler closes over `st`, `knots`, the spindle hostname and
	// the owner DID so the scheduler signature stays plain
	// `func(ctx, *Event) error` and applyCommit can hand the knot
	// consumer new sources as soon as matching repo records arrive
	// (gated on the publisher being an authorized actor).
	handler := js.HandlerFunc(func(ctx context.Context, evt *jsmodels.Event) error {
		return applyCommit(ctx, st, knots, cfg.Hostname, cfg.OwnerDID, evt)
	})

	// Re-attach the component-scoped logger so handler — which the
	// consumer invokes with the ctx we pass to ConnectAndRead — can pull
	// it back out via loggerFrom.
	ctx = loggerInto(ctx, logger)

	_, err := js.Start(ctx, js.Config{
		WebsocketURL:   cfg.JetstreamURL,
		Collections:    jetstreamCollections,
		CursorStore:    st,
		Handler:        handler,
		Logger:         logger,
		SchedulerIdent: "tack",
	})
	return err
}

// applyCommit routes a commit to the right store mutation based on its
// collection NSID and operation.
func applyCommit(ctx context.Context, st *store, knots KnotConsumer, hostname, ownerDID string, evt *jsmodels.Event) error {
	c := evt.Commit
	switch c.Collection {
	case tangled.SpindleMemberNSID:
		return applySpindleMember(ctx, st, knots, hostname, ownerDID, evt.Did, c)
	case tangled.RepoNSID:
		return applyRepo(ctx, st, knots, hostname, ownerDID, evt.Did, c)
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

func applySpindleMember(ctx context.Context, st *store, knots KnotConsumer, hostname, ownerDID, did string, c *jsmodels.Commit) error {
	switch c.Operation {
	case jsOpCreate, jsOpUpdate:
		// Capture the previous subject. Necessary because a same-rkey
		// update can move the grant to a different DID, in which case
		// the *old* subject's knots may need to be released even as
		// the new subject's are picked up.
		oldSubject, err := st.GetSpindleMember(ctx, did, c.RKey)
		if err != nil {
			return err
		}

		var rec tangled.SpindleMember
		if err := json.Unmarshal(c.Record, &rec); err != nil {
			// Decode failures are a permanent property of the record's
			// bytes; mark as bad so the cursor can advance past it.
			return js.BadRecord(fmt.Errorf("decode spindle.member: %w", err))
		}
		if err := st.UpsertSpindleMember(ctx, did, c.RKey, rec.Instance, rec.Subject, rec.CreatedAt); err != nil {
			return err
		}

		// Only grants published by the spindle owner actually change
		// authorization (see IsAuthorizedActor). Forged grants are
		// stored but don't move the needle, so don't bother
		// reconciling on them: it would be a no-op at best and
		// add log noise.
		if did != ownerDID {
			return nil
		}
		if oldSubject != "" && oldSubject != rec.Subject {
			if err := reconcileMember(ctx, st, knots, hostname, ownerDID, oldSubject); err != nil {
				return err
			}
		}
		return reconcileMember(ctx, st, knots, hostname, ownerDID, rec.Subject)

	case jsOpDelete:
		oldSubject, err := st.GetSpindleMember(ctx, did, c.RKey)
		if err != nil {
			return err
		}
		if err := st.DeleteSpindleMember(ctx, did, c.RKey); err != nil {
			return err
		}
		// As above: only the owner's grants matter for authorization.
		if did != ownerDID || oldSubject == "" {
			return nil
		}
		return reconcileMember(ctx, st, knots, hostname, ownerDID, oldSubject)
	}
	return nil
}

func applyRepo(ctx context.Context, st *store, knots KnotConsumer, hostname, ownerDID, did string, c *jsmodels.Commit) error {
	switch c.Operation {
	case jsOpCreate, jsOpUpdate:
		var rec tangled.Repo
		if err := json.Unmarshal(c.Record, &rec); err != nil {
			// See applySpindleMember: decode errors are permanent.
			return js.BadRecord(fmt.Errorf("decode repo: %w", err))
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
		return reconcileKnot(ctx, st, knots, hostname, ownerDID, did,
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
		return reconcileKnot(ctx, st, knots, hostname, ownerDID, did,
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
//   - If the new record names us as its spindle AND the publisher is
//     an authorized actor (spindle owner or owner-vouched member),
//     ensure we're subscribed to its knot. Without the membership
//     check, any firehose publisher could pin us to an attacker-chosen
//     knot just by publishing a sh.tangled.repo record naming us.
//   - If the old record named us as its spindle, check whether any
//     other authorized repo still references that knot through us;
//     if not, RemoveKnot it. Skip this when the knot didn't change
//     AND the spindle didn't move away from us, because then nothing
//     actually released our hold.
func reconcileKnot(
	ctx context.Context,
	st *store,
	knots KnotConsumer,
	hostname, ownerDID, publisherDID string,
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
		ok, err := st.IsAuthorizedActor(ctx, ownerDID, publisherDID)
		if err != nil {
			return err
		}
		if ok {
			knots.AddKnot(ctx, newKnot)
		} else {
			loggerFrom(ctx).Warn("ignoring repo from unauthorized publisher",
				"publisher_did", publisherDID,
				"knot", newKnot,
			)
		}
	}

	// Did we just lose our claim on oldKnot? Two ways that can happen:
	// the spindle field moved off of us, or the knot field moved to a
	// different host. Either is a reason to consider unsubscribing
	// from oldKnot, but only if no *other* authorized repo still has
	// us on it.
	releasedOld := oldSpindle == hostname && oldKnot != "" &&
		(newSpindle != hostname || newKnot != oldKnot)
	if releasedOld {
		stillWanted, err := st.IsKnotWanted(ctx, hostname, ownerDID, oldKnot)
		if err != nil {
			return err
		}
		if !stillWanted {
			knots.RemoveKnot(ctx, oldKnot)
		}
	}
	return nil
}

// reconcileMember adjusts knot subscriptions after a membership grant
// or revocation may have changed `subject`'s authorization status.
// For each knot named by subject's repos that point at us:
//
//   - if subject is now authorized, AddKnot (idempotent: already-
//     subscribed knots are no-ops in the consumer);
//   - if subject is now unauthorized, ask IsKnotWanted whether any
//     *other* authorized repo still holds the knot; if not,
//     RemoveKnot.
//
// Without this, a member's repos picked up over the firehose before
// the grant arrived would never get subscribed (the grant doesn't
// re-deliver the older repo events), and a revocation would leave
// the now-unauthorized publisher's knot subscribed until restart.
func reconcileMember(
	ctx context.Context,
	st *store,
	knots KnotConsumer,
	hostname, ownerDID, subject string,
) error {
	if knots == nil || subject == "" {
		return nil
	}
	knotsForSubject, err := st.KnotsForOwner(ctx, hostname, subject)
	if err != nil {
		return err
	}
	if len(knotsForSubject) == 0 {
		return nil
	}
	authorized, err := st.IsAuthorizedActor(ctx, ownerDID, subject)
	if err != nil {
		return err
	}
	for _, k := range knotsForSubject {
		if authorized {
			knots.AddKnot(ctx, k)
			continue
		}
		stillWanted, err := st.IsKnotWanted(ctx, hostname, ownerDID, k)
		if err != nil {
			return err
		}
		if !stillWanted {
			knots.RemoveKnot(ctx, k)
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
			return js.BadRecord(fmt.Errorf("decode repo.collaborator: %w", err))
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
