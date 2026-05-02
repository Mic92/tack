package main

// SQLite-backed persistence for tack.
//
// Two responsibilities live here:
//
//  1. The jetstream cursor — a microsecond unix timestamp the AT Proto
//     firehose uses to resume from a specific point. Without persistence
//     every restart begins at "now" and we'd silently miss any record
//     published while we were down.
//
//  2. Tangled membership state derived from jetstream commits:
//     sh.tangled.spindle.member, sh.tangled.repo, and
//     sh.tangled.repo.collaborator. We need this to later answer
//     "is DID X allowed to trigger a build on this spindle for repo Y?"
//
// We use mattn/go-sqlite3, which is the most battle-tested SQLite driver
// for Go. It requires CGo, which is fine for tack — the project already
// builds under Nix where a C toolchain is readily available.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// store wraps the SQLite handle and exposes the small set of operations
// the rest of tack needs. Keeping the surface narrow lets the persistence
// layer be swapped or mocked later without rewriting callers.
type store struct {
	db *sql.DB
}

// openStore opens (or creates) the SQLite database at path and applies
// the schema. It also flips on WAL + NORMAL synchronous, which is the
// usual "this is a long-running server, not a one-shot script" config:
// concurrent reads don't block the single writer, and we trade a little
// crash-safety on power loss for a lot of write throughput.
func openStore(path string) (*store, error) {
	// mattn/go-sqlite3 reads pragma-style query parameters (_journal_mode,
	// _synchronous, _foreign_keys) and applies them on each new connection.
	// journal_mode=WAL persists in the database file but setting it here
	// also covers the first-ever open.
	dsn := fmt.Sprintf("file:%s?_journal_mode=WAL&_synchronous=NORMAL&_foreign_keys=on", path)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite %q: %w", path, err)
	}

	// SQLite only supports one writer at a time. Capping max open conns
	// at 1 keeps database/sql from spinning up extra connections that
	// will only ever serialize behind that writer.
	db.SetMaxOpenConns(1)

	s := &store{db: db}
	if err := s.migrate(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

// Close releases the underlying database handle.
func (s *store) Close() error {
	return s.db.Close()
}

// metaCursorKey is the meta-table key under which we persist the
// jetstream cursor. Pulled out as a constant to avoid drift between
// load/save sites.
const metaCursorKey = "jetstream_cursor"

// LoadCursor returns the persisted jetstream cursor, or nil if none has
// been saved yet (signaling "start from now"). The cursor is a unix
// microsecond timestamp — see jetstream.Event.TimeUS.
func (s *store) LoadCursor(ctx context.Context) (*int64, error) {
	var raw string
	err := s.db.QueryRowContext(ctx,
		`SELECT value FROM meta WHERE key = ?`, metaCursorKey,
	).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load cursor: %w", err)
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		// A malformed cursor shouldn't wedge startup — log-and-ignore
		// (return nil) so we just resume from "now". The caller has the
		// logger; we surface the parse error so it can decide.
		return nil, fmt.Errorf("parse cursor %q: %w", raw, err)
	}
	return &v, nil
}

// SaveCursor writes the jetstream cursor. It uses an UPSERT so the meta
// row is created on first call and updated thereafter.
func (s *store) SaveCursor(ctx context.Context, cursor int64) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO meta (key, value) VALUES (?, ?)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
		metaCursorKey, strconv.FormatInt(cursor, 10),
	)
	if err != nil {
		return fmt.Errorf("save cursor: %w", err)
	}
	return nil
}

// UpsertSpindleMember records (or refreshes) a sh.tangled.spindle.member
// observation.
func (s *store) UpsertSpindleMember(ctx context.Context, did, rkey, instance, subject, createdAt string) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO spindle_members (did, rkey, instance, subject, created_at)
         VALUES (?, ?, ?, ?, ?)
         ON CONFLICT(did, rkey) DO UPDATE SET
             instance   = excluded.instance,
             subject    = excluded.subject,
             created_at = excluded.created_at`,
		did, rkey, instance, subject, createdAt,
	)
	if err != nil {
		return fmt.Errorf("upsert spindle_member: %w", err)
	}
	return nil
}

// DeleteSpindleMember removes a member record by its ATProto identity.
func (s *store) DeleteSpindleMember(ctx context.Context, did, rkey string) error {
	_, err := s.db.ExecContext(ctx,
		`DELETE FROM spindle_members WHERE did = ? AND rkey = ?`,
		did, rkey,
	)
	if err != nil {
		return fmt.Errorf("delete spindle_member: %w", err)
	}
	return nil
}

// UpsertRepo records (or refreshes) a sh.tangled.repo observation.
// spindle and repoDid may be empty strings — we store them as such rather
// than as SQL NULLs to keep the upsert path uniform.
func (s *store) UpsertRepo(ctx context.Context, did, rkey, knot, name, spindle, repoDid, createdAt string) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO repos (did, rkey, knot, name, spindle, repo_did, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(did, rkey) DO UPDATE SET
             knot       = excluded.knot,
             name       = excluded.name,
             spindle    = excluded.spindle,
             repo_did   = excluded.repo_did,
             created_at = excluded.created_at`,
		did, rkey, knot, name, spindle, repoDid, createdAt,
	)
	if err != nil {
		return fmt.Errorf("upsert repo: %w", err)
	}
	return nil
}

// DeleteRepo removes a repo record by its ATProto identity.
func (s *store) DeleteRepo(ctx context.Context, did, rkey string) error {
	_, err := s.db.ExecContext(ctx,
		`DELETE FROM repos WHERE did = ? AND rkey = ?`,
		did, rkey,
	)
	if err != nil {
		return fmt.Errorf("delete repo: %w", err)
	}
	return nil
}

// UpsertRepoCollaborator records (or refreshes) a
// sh.tangled.repo.collaborator observation.
func (s *store) UpsertRepoCollaborator(ctx context.Context, did, rkey, repo, repoDid, subject, createdAt string) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO repo_collaborators (did, rkey, repo, repo_did, subject, created_at)
         VALUES (?, ?, ?, ?, ?, ?)
         ON CONFLICT(did, rkey) DO UPDATE SET
             repo       = excluded.repo,
             repo_did   = excluded.repo_did,
             subject    = excluded.subject,
             created_at = excluded.created_at`,
		did, rkey, repo, repoDid, subject, createdAt,
	)
	if err != nil {
		return fmt.Errorf("upsert repo_collaborator: %w", err)
	}
	return nil
}

// GetRepo returns the (knot, spindle) currently stored for a (did, rkey)
// pair. Both are returned as empty strings when no row exists; callers
// that need to distinguish "absent" from "stored but empty" should
// pre-check existence themselves.
//
// This exists so applyRepo can read the *previous* spindle/knot of a
// record before applying a mutation, which is what makes it possible to
// detect transitions like "this repo used to be ours, now it isn't" and
// trigger a knot unsubscribe.
func (s *store) GetRepo(ctx context.Context, did, rkey string) (knot, spindle string, err error) {
	err = s.db.QueryRowContext(ctx,
		`SELECT knot, spindle FROM repos WHERE did = ? AND rkey = ?`,
		did, rkey,
	).Scan(&knot, &spindle)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", nil
	}
	if err != nil {
		return "", "", fmt.Errorf("get repo: %w", err)
	}
	return knot, spindle, nil
}

// IsKnotWanted reports whether any repo currently stored still names the
// given hostname as its spindle and the given knot as its host. After a
// repo update or delete this is the question we ask to decide whether
// to keep watching that knot or unsubscribe from it.
func (s *store) IsKnotWanted(ctx context.Context, hostname, knot string) (bool, error) {
	var n int
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM repos WHERE spindle = ? AND knot = ?`,
		hostname, knot,
	).Scan(&n)
	if err != nil {
		return false, fmt.Errorf("count repos for knot: %w", err)
	}
	return n > 0, nil
}

// KnotsForSpindle returns the distinct knot hostnames of all repos that
// have declared the given spindle hostname as their CI spindle. The knot
// event-stream subscriber uses this to decide which knots to dial.
//
// Returns an empty slice (not nil) when nothing matches, so callers can
// range over the result without a nil check.
func (s *store) KnotsForSpindle(ctx context.Context, hostname string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT DISTINCT knot FROM repos WHERE spindle = ? AND knot <> ''`,
		hostname,
	)
	if err != nil {
		return nil, fmt.Errorf("query knots: %w", err)
	}
	defer rows.Close()

	out := []string{}
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, fmt.Errorf("scan knot: %w", err)
		}
		out = append(out, k)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate knots: %w", err)
	}
	return out, nil
}

// DeleteRepoCollaborator removes a collaborator record by its ATProto
// identity.
func (s *store) DeleteRepoCollaborator(ctx context.Context, did, rkey string) error {
	_, err := s.db.ExecContext(ctx,
		`DELETE FROM repo_collaborators WHERE did = ? AND rkey = ?`,
		did, rkey,
	)
	if err != nil {
		return fmt.Errorf("delete repo_collaborator: %w", err)
	}
	return nil
}

// EventRow is one row of the events table. It represents an outbound
// record we want to deliver to /events websocket subscribers, in the
// shape callers actually need (raw record JSON, not stringly-typed).
type EventRow struct {
	// Created is the assigned monotonic rowid; doubles as the cursor
	// value subscribers use to resume.
	Created int64
	// Rkey is the ATProto record key. For sh.tangled.pipeline.status
	// records this is the rkey we mint when publishing.
	Rkey string
	// Nsid is the lexicon collection (e.g. sh.tangled.pipeline.status).
	Nsid string
	// EventJSON is the record body verbatim — held as RawMessage so
	// the /events handler can splice it into the wire envelope without
	// an unmarshal/remarshal round-trip.
	EventJSON json.RawMessage
}

// InsertEvent appends an event row and returns its assigned `created`
// (rowid) cursor. Storage is the source of truth for fan-out, so we
// write here even if zero subscribers are connected — a subscriber that
// connects later (with an old cursor) will pick the row up via
// EventsAfter.
//
// eventJSON must be a valid JSON object; we store it verbatim. Length
// validation is intentionally absent — the schema accepts arbitrary
// TEXT and SQLite handles huge blobs fine for our scale.
func (s *store) InsertEvent(ctx context.Context, rkey, nsid string, eventJSON []byte) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`INSERT INTO events (rkey, nsid, event_json, inserted_at)
         VALUES (?, ?, ?, ?)`,
		rkey, nsid, string(eventJSON),
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return 0, fmt.Errorf("insert event: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("event last insert id: %w", err)
	}
	return id, nil
}

// BuildkiteBuildRef is the persisted mapping from one Buildkite build
// to the Tangled pipeline tuple that spawned it. It's the row written
// by the Buildkite provider at Spawn time and read back from two
// places: the webhook handler (by build UUID) when an event arrives,
// and the /logs handler (by knot+rkey+workflow) when an appview
// client asks for output.
type BuildkiteBuildRef struct {
	BuildUUID    string
	BuildNumber  int64
	PipelineSlug string
	// Org is the Buildkite organisation slug the build was created
	// against. Persisted at Spawn time so /logs and any other
	// post-creation API call can target the same org the workflow
	// originally chose — see the workflow YAML `tack.buildkite.org`
	// override. Empty means "use the provider's default org", which
	// is what every row written before the org column existed will
	// scan as.
	Org          string
	Knot         string
	PipelineRkey string
	Workflow     string
	PipelineURI  string
}

// InsertBuildkiteBuild records that a Buildkite build was created on
// behalf of the given (knot, pipelineRkey, workflow) tuple. Uses
// INSERT OR REPLACE so that an unlikely build-uuid collision (or a
// Buildkite-side rebuild that re-fires us) just refreshes the row
// instead of failing.
func (s *store) InsertBuildkiteBuild(ctx context.Context, ref BuildkiteBuildRef) error {
	// Capture wall-clock and monotonic-friendly forms once so the two
	// columns agree on the same instant. created_at is the
	// human-readable RFC3339Nano string; created_unix_ns is the
	// integer the lookup orders on (text comparison of nanosecond
	// timestamps isn't reliable, so we sort on the int instead).
	now := time.Now().UTC()
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO buildkite_builds (
            build_uuid, build_number, pipeline_slug, org,
            knot, pipeline_rkey, workflow,
            pipeline_uri, created_at, created_unix_ns
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(build_uuid) DO UPDATE SET
             build_number    = excluded.build_number,
             pipeline_slug   = excluded.pipeline_slug,
             org             = excluded.org,
             knot            = excluded.knot,
             pipeline_rkey   = excluded.pipeline_rkey,
             workflow        = excluded.workflow,
             pipeline_uri    = excluded.pipeline_uri,
             created_at      = excluded.created_at,
             created_unix_ns = excluded.created_unix_ns`,
		ref.BuildUUID, ref.BuildNumber, ref.PipelineSlug, ref.Org,
		ref.Knot, ref.PipelineRkey, ref.Workflow,
		ref.PipelineURI, now.Format(time.RFC3339Nano), now.UnixNano(),
	)
	if err != nil {
		return fmt.Errorf("insert buildkite_build: %w", err)
	}
	return nil
}

// LookupBuildkiteBuildByUUID returns the saved mapping for the given
// Buildkite build UUID, or nil when no such build is recorded.
// Returning a nil pointer rather than a sentinel error keeps the
// webhook handler's "we don't know about this build" branch a simple
// nil check.
func (s *store) LookupBuildkiteBuildByUUID(ctx context.Context, buildUUID string) (*BuildkiteBuildRef, error) {
	var ref BuildkiteBuildRef
	err := s.db.QueryRowContext(ctx,
		`SELECT build_uuid, build_number, pipeline_slug, org,
                knot, pipeline_rkey, workflow, pipeline_uri
           FROM buildkite_builds WHERE build_uuid = ?`,
		buildUUID,
	).Scan(
		&ref.BuildUUID, &ref.BuildNumber, &ref.PipelineSlug, &ref.Org,
		&ref.Knot, &ref.PipelineRkey, &ref.Workflow, &ref.PipelineURI,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("lookup buildkite_build by uuid: %w", err)
	}
	return &ref, nil
}

// LookupBuildkiteBuildByTuple finds the most recently created build
// for (knot, pipelineRkey, workflow). Returns nil when no build has
// been recorded for that tuple — used by /logs to translate the
// appview's path-based identity back into something Buildkite knows.
//
// "Most recent" matters because a workflow may have multiple builds
// over time (rebuilds, re-triggers). We always serve logs for the
// latest run; older runs are still queryable by build UUID directly
// if anyone ever wants that.
//
// Ordering is on created_unix_ns (a monotonic int) rather than
// created_at. Text comparison of RFC3339Nano timestamps is not
// reliable across nanosecond precision, which used to make this
// query occasionally pick the wrong run. created_at and build_number
// are kept as deterministic tiebreakers for legacy rows that pre-date
// the new column and still scan as 0.
func (s *store) LookupBuildkiteBuildByTuple(ctx context.Context, knot, pipelineRkey, workflow string) (*BuildkiteBuildRef, error) {
	var ref BuildkiteBuildRef
	err := s.db.QueryRowContext(ctx,
		`SELECT build_uuid, build_number, pipeline_slug, org,
                knot, pipeline_rkey, workflow, pipeline_uri
           FROM buildkite_builds
          WHERE knot = ? AND pipeline_rkey = ? AND workflow = ?
          ORDER BY created_unix_ns DESC, created_at DESC, build_number DESC
          LIMIT 1`,
		knot, pipelineRkey, workflow,
	).Scan(
		&ref.BuildUUID, &ref.BuildNumber, &ref.PipelineSlug, &ref.Org,
		&ref.Knot, &ref.PipelineRkey, &ref.Workflow, &ref.PipelineURI,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("lookup buildkite_build by tuple: %w", err)
	}
	return &ref, nil
}

// EventsAfter returns every event row with `created` strictly greater
// than cursor, in cursor order. Used by /events to backfill a
// reconnecting subscriber and to drain newly-published rows on each
// broker notification.
//
// Pass cursor=0 to get the full log from the beginning, which is what
// happens when a subscriber connects without a ?cursor= query param.
func (s *store) EventsAfter(ctx context.Context, cursor int64) ([]EventRow, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT created, rkey, nsid, event_json
         FROM events
         WHERE created > ?
         ORDER BY created ASC`,
		cursor,
	)
	if err != nil {
		return nil, fmt.Errorf("query events: %w", err)
	}
	defer rows.Close()

	out := []EventRow{}
	for rows.Next() {
		var (
			ev  EventRow
			raw string
		)
		if err := rows.Scan(&ev.Created, &ev.Rkey, &ev.Nsid, &raw); err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		ev.EventJSON = json.RawMessage(raw)
		out = append(out, ev)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate events: %w", err)
	}
	return out, nil
}
