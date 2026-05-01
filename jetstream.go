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
	"fmt"
	"time"

	"github.com/bluesky-social/jetstream/pkg/client"
	"github.com/bluesky-social/jetstream/pkg/client/schedulers/sequential"
	jsmodels "github.com/bluesky-social/jetstream/pkg/models"
	"tangled.org/core/api/tangled"
)

// startJetstream dials the configured jetstream endpoint and spawns a
// background goroutine that consumes events for the lifetime of ctx. It
// returns once the client is constructed; connection errors surface in
// logs, not return values, because the read loop is expected to reconnect
// on its own.
//
// The logger is pulled from ctx (see log.go); falls back to slog.Default()
// if none is attached.
func startJetstream(ctx context.Context, cfg config) error {
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

	// Re-attach the component-scoped logger so handleJetstreamEvent — which
	// the scheduler invokes with the ctx we pass to ConnectAndRead — can
	// pull it back out via loggerFrom.
	ctx = loggerInto(ctx, logger)

	// The sequential scheduler processes events one-at-a-time in arrival
	// order. That's the right default for a spindle: ordering matters
	// (e.g. a member-added event must apply before any record from that
	// member is processed), and our event volume is tiny.
	c, err := client.NewClient(
		clientCfg,
		logger,
		sequential.NewScheduler(
			"tack",
			logger,
			handleJetstreamEvent),
	)
	if err != nil {
		return fmt.Errorf("new jetstream client: %w", err)
	}

	// Reconnect loop. ConnectAndRead blocks on the websocket and returns
	// either when the connection drops (transient network error, server
	// restart, etc.) or when ctx is cancelled. On error we sleep briefly
	// and reconnect; on ctx cancellation we exit cleanly.
	//
	// TODO: pass a *cursor here once we persist one, so we resume from the
	// last seen event instead of "now" after a restart.
	go func() {
		for {
			if err := c.ConnectAndRead(ctx, nil); err != nil {
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

// handleJetstreamEvent is the per-event callback for the JetStream.
func handleJetstreamEvent(ctx context.Context, evt *jsmodels.Event) error {
	// We only care about commits, which are the actual record CRUD operations
	// on a user's PDS.
	if evt.Kind != jsmodels.EventKindCommit || evt.Commit == nil {
		return nil
	}

	loggerFrom(ctx).Debug("event",
		"did", evt.Did,
		"collection", evt.Commit.Collection,
		"op", evt.Commit.Operation,
		"rkey", evt.Commit.RKey,
	)
	return nil
}
