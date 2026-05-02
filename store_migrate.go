package main

// Schema definition and migration logic for the SQLite store. Pulled out
// of store.go so the big SQL block doesn't sit in the middle of the
// runtime API surface.

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// schema is the full set of CREATE statements applied at startup. It is
// idempotent and additive only — no `DROP`s — so future changes can be
// layered on as additional statements without needing a separate
// migration tool until the project actually outgrows that.
const schema = `
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- Records of sh.tangled.spindle.member. The owner of a spindle publishes
-- one of these per authorized member. (did, rkey) is the natural ATProto
-- key — did identifies the publisher's PDS, rkey identifies the record
-- within that PDS's collection.
CREATE TABLE IF NOT EXISTS spindle_members (
    did        TEXT NOT NULL,
    rkey       TEXT NOT NULL,
    instance   TEXT NOT NULL,
    subject    TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (did, rkey)
);

-- Records of sh.tangled.repo. We keep the full set so that when a
-- pipeline trigger arrives we can look up which knot/spindle/repo_did
-- it corresponds to without another round-trip.
CREATE TABLE IF NOT EXISTS repos (
    did         TEXT NOT NULL,
    rkey        TEXT NOT NULL,
    knot        TEXT NOT NULL,
    name        TEXT NOT NULL,
    spindle     TEXT,
    repo_did    TEXT,
    created_at  TEXT NOT NULL,
    PRIMARY KEY (did, rkey)
);

-- Records of sh.tangled.repo.collaborator. Used together with repos to
-- decide whether a triggering DID is allowed to push builds to us.
CREATE TABLE IF NOT EXISTS repo_collaborators (
    did        TEXT NOT NULL,
    rkey       TEXT NOT NULL,
    repo       TEXT,
    repo_did   TEXT,
    subject    TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (did, rkey)
);

-- Outbound event log. Each row is one record we want to fan out to
-- connected /events websocket subscribers (typically the Tangled
-- appview) — today only sh.tangled.pipeline.status.
--
-- We persist instead of pushing through an in-memory channel so that
-- (a) a reconnecting subscriber can resume from a cursor without
--     missing events that happened during the gap, and
-- (b) slow subscribers can't make us drop events for fast ones — they
--     simply lag behind in the rowid space.
--
-- AUTOINCREMENT (vs plain INTEGER PRIMARY KEY) guarantees rowids
-- strictly increase and never get reused if a row is ever deleted, so
-- treating the created column as a monotonic cursor is safe forever.
CREATE TABLE IF NOT EXISTS events (
    created     INTEGER PRIMARY KEY AUTOINCREMENT,
    rkey        TEXT NOT NULL,
    nsid        TEXT NOT NULL,
    event_json  TEXT NOT NULL,
    inserted_at TEXT NOT NULL
);

-- Mapping from a Buildkite build back to the Tangled pipeline that
-- spawned it. The Buildkite webhook receiver only knows the build
-- UUID; everything we need to publish a pipeline.status record
-- (knot, pipeline rkey, workflow name, full pipeline ATURI) lives
-- on this row.
--
-- pipeline_uri is denormalized off (knot, pipeline_rkey) so the
-- webhook handler doesn't have to recompute the at:// string on
-- every event — it's a constant for the lifetime of the build and
-- the webhook is the hot path for status fan-out.
--
-- The (knot, pipeline_rkey, workflow) index supports the /logs
-- handler, which only knows that tuple at request time.
-- org is the Buildkite organisation slug the build was created
-- against. A workflow's YAML can override the spindle's default
-- org via tack.buildkite.org, so we persist whatever was used at
-- Spawn time and read it back when fetching jobs/logs. The empty
-- string means "use the provider's defaultOrg" — that's both the
-- usual single-org case and what every row written before this
-- column existed will scan as.
-- created_unix_ns is the monotonic recency key. created_at is kept
-- (RFC3339Nano text) for human-readable inspection, but it must NOT
-- be used for ordering: text comparison of nanosecond timestamps is
-- not reliable, which used to make /logs occasionally resolve the
-- wrong run. created_unix_ns is the integer the latest-build lookup
-- sorts on instead.
CREATE TABLE IF NOT EXISTS buildkite_builds (
    build_uuid      TEXT PRIMARY KEY,
    build_number    INTEGER NOT NULL,
    pipeline_slug   TEXT NOT NULL,
    org             TEXT NOT NULL DEFAULT '',
    knot            TEXT NOT NULL,
    pipeline_rkey   TEXT NOT NULL,
    workflow        TEXT NOT NULL,
    pipeline_uri    TEXT NOT NULL,
    created_at      TEXT NOT NULL,
    created_unix_ns INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS buildkite_builds_lookup
    ON buildkite_builds (knot, pipeline_rkey, workflow);

-- Mapping from a Tangled workflow tuple to the latest Tekton
-- PipelineRun tack created for it. Unlike Buildkite webhooks, Tekton
-- status observation happens in-process, so the primary read path is
-- /logs resolving (knot, pipeline_rkey, workflow) back to the concrete
-- PipelineRun whose TaskRuns and pods hold output.
CREATE TABLE IF NOT EXISTS tekton_runs (
    knot              TEXT NOT NULL,
    pipeline_rkey     TEXT NOT NULL,
    workflow          TEXT NOT NULL,
    namespace         TEXT NOT NULL,
    pipeline_run_name TEXT NOT NULL,
    pipeline_run_uid  TEXT NOT NULL,
    pipeline_name     TEXT NOT NULL,
    pipeline_uri      TEXT NOT NULL,
    created_at        TEXT NOT NULL,
    created_unix_ns   INTEGER NOT NULL,
    PRIMARY KEY (knot, pipeline_rkey, workflow)
);
CREATE INDEX IF NOT EXISTS tekton_runs_uid
    ON tekton_runs (pipeline_run_uid);
`

// migrate applies the schema. Safe to call repeatedly.
//
// CREATE TABLE IF NOT EXISTS is enough for fresh databases, but it
// won't widen an already-existing table. Columns added after the
// initial release therefore need a parallel ALTER TABLE step here;
// SQLite has no `ADD COLUMN IF NOT EXISTS`, so we run the ALTER and
// swallow the "duplicate column" error that fires on subsequent
// startups. Anything else is fatal.
func (s *store) migrate(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	for _, alter := range []string{
		// Persist the Buildkite org each build was created
		// against so /logs can target the same org the workflow
		// chose. Pre-existing rows scan as empty string, which
		// the provider treats as "use defaultOrg".
		`ALTER TABLE buildkite_builds ADD COLUMN org TEXT NOT NULL DEFAULT ''`,

		// Monotonic integer ordering key for buildkite_builds.
		// Replaces ORDER BY created_at (RFC3339Nano text), whose
		// lexical order isn't reliable across nanosecond precision
		// and could make /logs resolve the wrong run. Default 0
		// covers pre-existing rows; the backfill below promotes
		// them to their parsed timestamp so ordering stays stable
		// across the upgrade.
		`ALTER TABLE buildkite_builds ADD COLUMN created_unix_ns INTEGER NOT NULL DEFAULT 0`,
	} {
		if _, err := s.db.ExecContext(ctx, alter); err != nil {
			if strings.Contains(err.Error(), "duplicate column name") {
				continue
			}
			return fmt.Errorf("apply alter %q: %w", alter, err)
		}
	}

	if err := s.backfillBuildkiteCreatedUnixNS(ctx); err != nil {
		return fmt.Errorf("backfill buildkite created_unix_ns: %w", err)
	}
	return nil
}

// backfillBuildkiteCreatedUnixNS walks every buildkite_builds row whose
// created_unix_ns is still the post-ALTER default (0) and sets it from
// the RFC3339Nano text in created_at. SQLite has no native nanosecond
// parser, so the conversion has to happen in Go.
//
// Rows whose created_at can't be parsed are left at 0; that keeps a
// single corrupt row from blocking startup, and ordering between two
// 0-keyed rows still falls through to created_at as a deterministic
// tiebreaker in the lookup query.
func (s *store) backfillBuildkiteCreatedUnixNS(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx,
		`SELECT build_uuid, created_at FROM buildkite_builds
          WHERE created_unix_ns = 0`,
	)
	if err != nil {
		return fmt.Errorf("query rows to backfill: %w", err)
	}
	type pending struct {
		uuid string
		ns   int64
	}
	var todo []pending
	for rows.Next() {
		var uuid, createdAt string
		if err := rows.Scan(&uuid, &createdAt); err != nil {
			rows.Close()
			return fmt.Errorf("scan row: %w", err)
		}
		t, perr := time.Parse(time.RFC3339Nano, createdAt)
		if perr != nil {
			// Skip unparseable rows rather than failing the whole
			// migration. The lookup query still has a fallback
			// ordering for rows that share the default 0 key.
			continue
		}
		todo = append(todo, pending{uuid: uuid, ns: t.UnixNano()})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate rows: %w", err)
	}
	rows.Close()

	for _, p := range todo {
		if _, err := s.db.ExecContext(ctx,
			`UPDATE buildkite_builds SET created_unix_ns = ?
              WHERE build_uuid = ?`,
			p.ns, p.uuid,
		); err != nil {
			return fmt.Errorf("update row %q: %w", p.uuid, err)
		}
	}
	return nil
}
