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
func startJetstream(ctx context.Context, cfg config, st *store) error {
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

	// The handler closes over `st` and `logger` so the scheduler signature
	// stays plain `func(ctx, *Event) error` — no need for a method
	// receiver or a global.
	handler := func(ctx context.Context, evt *jsmodels.Event) error {
		return handleJetstreamEvent(ctx, st, evt)
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
			if cur != nil {
				logger.Info("connecting to jetstream", "cursor_us", *cur)
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
// applies the event to the store and advances the persisted cursor. Any
// returned error is logged by the scheduler but does not tear down the
// connection — the next event will retry the cursor write implicitly.
func handleJetstreamEvent(ctx context.Context, st *store, evt *jsmodels.Event) error {
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
	if err := applyCommit(ctx, st, evt); err != nil {
		logger.Error("apply commit",
			"err", err,
			"did", evt.Did,
			"collection", evt.Commit.Collection,
			"op", evt.Commit.Operation,
			"rkey", evt.Commit.RKey,
		)

		// Fall through to cursor save: a single bad record shouldn't
		// stall the cursor forever and force us to re-process every
		// subsequent event after a restart.
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

// applyCommit routes a commit to the right store mutation based on its
// collection NSID and operation.
func applyCommit(ctx context.Context, st *store, evt *jsmodels.Event) error {
	c := evt.Commit
	switch c.Collection {
	case tangled.SpindleMemberNSID:
		return applySpindleMember(ctx, st, evt.Did, c)
	case tangled.RepoNSID:
		return applyRepo(ctx, st, evt.Did, c)
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
			return fmt.Errorf("decode spindle.member: %w", err)
		}
		return st.UpsertSpindleMember(ctx, did, c.RKey, rec.Instance, rec.Subject, rec.CreatedAt)
	case jsOpDelete:
		return st.DeleteSpindleMember(ctx, did, c.RKey)
	}
	return nil
}

func applyRepo(ctx context.Context, st *store, did string, c *jsmodels.Commit) error {
	switch c.Operation {
	case jsOpCreate, jsOpUpdate:
		var rec tangled.Repo
		if err := json.Unmarshal(c.Record, &rec); err != nil {
			return fmt.Errorf("decode repo: %w", err)
		}
		return st.UpsertRepo(ctx, did, c.RKey,
			rec.Knot, rec.Name,
			deref(rec.Spindle), deref(rec.RepoDid),
			rec.CreatedAt,
		)
	case jsOpDelete:
		return st.DeleteRepo(ctx, did, c.RKey)
	}
	return nil
}

func applyRepoCollaborator(ctx context.Context, st *store, did string, c *jsmodels.Commit) error {
	switch c.Operation {
	case jsOpCreate, jsOpUpdate:
		var rec tangled.RepoCollaborator
		if err := json.Unmarshal(c.Record, &rec); err != nil {
			return fmt.Errorf("decode repo.collaborator: %w", err)
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
