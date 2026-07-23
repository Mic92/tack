package main

// SQLite-backed persistence for tack. Holds:
//
//   - the jetstream cursor, so restarts resume the firehose where the
//     previous run left off
//   - mirrored Tangled state (spindle members, repos, collaborators)
//     and the authorization helpers built on top of it, used both to
//     gate inbound pipeline triggers and to decide which knots we
//     should be subscribed to
//   - spindle-owned pipeline and status records served by the CI XRPC API
//   - provider run mappings used by webhooks and log subscriptions
//
// Per-method docs go into more detail.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// store wraps the SQLite handle and exposes the small set of operations
// the rest of tack needs. Keeping the surface narrow lets the persistence
// layer be swapped or mocked later without rewriting callers.
type store struct {
	db *sql.DB

	// eventMu makes event cursors strictly monotonic even when two provider
	// callbacks publish within the same clock tick.
	eventMu        sync.Mutex
	lastEventNanos int64
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
	if err := db.QueryRow(`SELECT COALESCE(MAX(created), 0) FROM events`).Scan(&s.lastEventNanos); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("load event clock: %w", err)
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

// RepoRef is the repository identity Tack needs when turning a knot event into
// a spindle-owned pipeline. RepoDID is the stable git identity; OwnerDID and
// Rkey identify the sh.tangled.repo record that assigned the spindle.
type RepoRef struct {
	OwnerDID string
	Rkey     string
	Knot     string
	Name     string
	Spindle  string
	RepoDID  string
}

// RepoByDID resolves the latest repository record for a stable repository DID.
func (s *store) RepoByDID(ctx context.Context, repoDID string) (*RepoRef, error) {
	var repo RepoRef
	err := s.db.QueryRowContext(ctx, `
		SELECT did, rkey, knot, name, spindle, repo_did
		FROM repos
		WHERE repo_did = ?
		ORDER BY created_at DESC
		LIMIT 1`, repoDID,
	).Scan(
		&repo.OwnerDID, &repo.Rkey, &repo.Knot, &repo.Name,
		&repo.Spindle, &repo.RepoDID,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("lookup repo by DID: %w", err)
	}
	return &repo, nil
}

// ManagedRepoByDID resolves a repository assignment only when it agrees with
// the authoritative knot and owner resolved from the repository DID.
func (s *store) ManagedRepoByDID(
	ctx context.Context,
	repoDID, hostname, ownerDID, knot, repoOwnerDID, repoRkey string,
) (*RepoRef, error) {
	var repo RepoRef
	err := s.db.QueryRowContext(ctx, `
		SELECT r.did, r.rkey, r.knot, r.name, r.spindle, r.repo_did
		FROM repos r
		WHERE r.repo_did = ?
		  AND r.spindle = ?
		  AND r.knot = ?
		  AND (? = '' OR r.did = ?)
		  AND (? = '' OR r.rkey = ?)
		  AND (
			r.did = ?
			OR EXISTS (
				SELECT 1 FROM spindle_members m
				WHERE m.did = ? AND m.instance = ? AND m.subject = r.did
			)
		  )
		ORDER BY r.created_at DESC
		LIMIT 1`,
		repoDID, hostname, knot, repoOwnerDID, repoOwnerDID,
		repoRkey, repoRkey,
		ownerDID, ownerDID, hostname,
	).Scan(
		&repo.OwnerDID, &repo.Rkey, &repo.Knot, &repo.Name,
		&repo.Spindle, &repo.RepoDID,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("lookup managed repo by DID: %w", err)
	}
	return &repo, nil
}

func (s *store) HasManagedRepo(
	ctx context.Context,
	repoDID, hostname, ownerDID string,
) (bool, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM repos r
		WHERE r.repo_did = ? AND r.spindle = ?
		  AND (
			r.did = ?
			OR EXISTS (
				SELECT 1 FROM spindle_members m
				WHERE m.did = ? AND m.instance = ? AND m.subject = r.did
			)
		  )`, repoDID, hostname, ownerDID, ownerDID, hostname,
	).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("check managed repo: %w", err)
	}
	return count > 0, nil
}

// IsRepoActorAuthorized reports whether actor owns or collaborates on repoDID.
// New Tangled appviews authenticate manual and retry requests as the user that
// initiated them, so this check is the XRPC equivalent of the push permission
// check performed by the stock spindle.
func (s *store) IsRepoActorAuthorized(
	ctx context.Context,
	repoDID, ownerDID, actor string,
) (bool, error) {
	if actor == ownerDID {
		return true, nil
	}
	var n int
	err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM repo_collaborators c
		WHERE c.did = ?
		  AND c.subject = ?
		  AND (c.repo_did = ? OR c.repo = ?)`,
		ownerDID, actor, repoDID, repoDID,
	).Scan(&n)
	if err != nil {
		return false, fmt.Errorf("authorize repo actor: %w", err)
	}
	return n > 0, nil
}

// AuthorizePipelineActor reports whether a pipeline trigger that
// arrived on knot for repo (repoOwnerDID, repoName) should be allowed
// to spawn work on this spindle. Both halves of the answer are
// derived from the persisted Tangled state in spindle_members and
// repos: the network's own authorization records, mirrored here from
// the firehose by jetstream.go.
//
// The check is two independent gates; both must hold:
//
//  1. **Repo claim**: a sh.tangled.repo record exists naming
//     hostname as its CI spindle AND knot as its host knot,
//     published by repoOwnerDID with the matching name. Without this
//     a knot we already happen to be subscribed to (because *some*
//     repo on it points at us) could otherwise smuggle in pipeline
//     triggers for unrelated repos that never opted into us.
//
//  2. **Actor membership**: repoOwnerDID is either the spindle owner
//     itself, or has been authorized via a sh.tangled.spindle.member
//     record published by the spindle owner. The publisher (`did`)
//     of that membership grant must equal ownerDID, and its instance must
//     equal this spindle hostname. Anyone can publish a member record naming
//     anyone, so trusting unscoped grants would let any DID grant itself
//     access or reuse a grant intended for another spindle.
//
// reason is a short, log-friendly description of why authorization
// failed when ok is false; it is empty when ok is true. Errors
// reported here are SQL/IO failures, never policy denials; those
// surface as ok=false with a populated reason.
func (s *store) AuthorizePipelineActor(
	ctx context.Context,
	hostname, knot, ownerDID, repoOwnerDID, repoName string,
) (ok bool, reason string, err error) {
	// We can't authorize an unknown actor or an unidentified repo:
	// every gate below joins on these. Bail before touching SQLite
	// so a malformed trigger logs cleanly instead of triggering a
	// "no rows" miss that could be confused with a real denial.
	if repoOwnerDID == "" {
		return false, "trigger has no repo did", nil
	}
	if repoName == "" {
		return false, "trigger has no repo name", nil
	}

	// Gate 1: this specific repo opted into us on this specific knot.
	var n int
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM repos
          WHERE did = ? AND (name = ? OR rkey = ?) AND knot = ? AND spindle = ?`,
		repoOwnerDID, repoName, repoName, knot, hostname,
	).Scan(&n); err != nil {
		return false, "", fmt.Errorf("count repo claim: %w", err)
	}
	if n == 0 {
		return false, "no repo record claims this spindle on this knot", nil
	}

	// Gate 2: actor is the spindle owner, or vouched for by them.
	if repoOwnerDID == ownerDID {
		return true, "", nil
	}
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM spindle_members
		  WHERE did = ? AND instance = ? AND subject = ?`,
		ownerDID, hostname, repoOwnerDID,
	).Scan(&n); err != nil {
		return false, "", fmt.Errorf("count membership: %w", err)
	}
	if n == 0 {
		return false, "actor is not a spindle member", nil
	}
	return true, "", nil
}

// IsKnotWanted reports whether any *authorized* repo currently stored
// still names the given hostname as its spindle and the given knot as
// its host. After a repo update or delete this is the question we ask
// to decide whether to keep watching that knot or unsubscribe from it.
//
// "Authorized" means the repo's publisher (the row's `did` column) is
// either the spindle owner or has been vouched for by the spindle owner
// via a sh.tangled.spindle.member record. Without this filter, a non-
// member could pin us to an arbitrary attacker-chosen knot just by
// publishing a sh.tangled.repo record naming us as its spindle. See
// the matching gate in IsAuthorizedActor and AuthorizePipelineActor.
func (s *store) IsKnotWanted(ctx context.Context, hostname, ownerDID, knot string) (bool, error) {
	var n int
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM repos r
          WHERE r.spindle = ? AND r.knot = ?
            AND (
                r.did = ?
				 OR EXISTS (
					 SELECT 1 FROM spindle_members m
					  WHERE m.did = ? AND m.instance = ? AND m.subject = r.did
				 )
			 )`,
		hostname, knot, ownerDID, ownerDID, hostname,
	).Scan(&n)
	if err != nil {
		return false, fmt.Errorf("count repos for knot: %w", err)
	}
	return n > 0, nil
}

// IsAuthorizedActor reports whether did is the spindle owner or has
// been authorized by the spindle owner via a sh.tangled.spindle.member
// record. The membership record's publisher (its `did` column) must
// equal ownerDID; anyone can publish a membership record naming
// anyone, so trusting unsigned grants would let any DID grant itself
// access. This is the same trust rule AuthorizePipelineActor enforces;
// it's pulled out here so knot-subscription decisions in jetstream.go
// gate on the same check as pipeline-spawning decisions in knot.go.
func (s *store) IsAuthorizedActor(
	ctx context.Context,
	hostname, ownerDID, did string,
) (bool, error) {
	if did == "" {
		return false, nil
	}
	if did == ownerDID {
		return true, nil
	}
	var n int
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM spindle_members
		  WHERE did = ? AND instance = ? AND subject = ?`,
		ownerDID, hostname, did,
	).Scan(&n)
	if err != nil {
		return false, fmt.Errorf("count membership: %w", err)
	}
	return n > 0, nil
}

// GetSpindleMember returns the subject DID currently stored for a
// (did, rkey) spindle.member row, or "" when no such row exists.
//
// Used by the jetstream handler to learn whose authorization a
// delete/update of a membership record affects, so it can reconcile
// that subject's knot subscriptions in the same step as the mutation.
func (s *store) GetSpindleMember(ctx context.Context, did, rkey string) (string, error) {
	var subject string
	err := s.db.QueryRowContext(ctx,
		`SELECT subject FROM spindle_members WHERE did = ? AND rkey = ?`,
		did, rkey,
	).Scan(&subject)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("get spindle_member: %w", err)
	}
	return subject, nil
}

// KnotsForOwner returns the distinct knot hostnames of repos published
// by `did` whose spindle field equals hostname. Used after a membership
// change to find which knots that DID's repos want, so we can subscribe
// (newly granted) or potentially unsubscribe (revoked) in lockstep with
// the grant.
//
// Returns an empty slice (not nil) on no matches so callers can range
// over the result without a nil check.
func (s *store) KnotsForOwner(ctx context.Context, hostname, did string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT DISTINCT knot FROM repos
          WHERE did = ? AND spindle = ? AND knot <> ''`,
		did, hostname,
	)
	if err != nil {
		return nil, fmt.Errorf("query knots for owner: %w", err)
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

// KnotsForSpindle returns the distinct knot hostnames of all repos that
// (a) have declared the given spindle hostname as their CI spindle, and
// (b) were published by an *authorized* DID: either the spindle owner
// itself or a member they vouched for via sh.tangled.spindle.member.
//
// The membership filter is critical: without it, any DID that publishes
// a sh.tangled.repo record naming us as its spindle could force us to
// dial an attacker-chosen knot at startup. See IsAuthorizedActor for
// the matching trust check applied at firehose-event time.
//
// Returns an empty slice (not nil) when nothing matches, so callers can
// range over the result without a nil check.
func (s *store) KnotsForSpindle(ctx context.Context, hostname, ownerDID string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT DISTINCT r.knot FROM repos r
          WHERE r.spindle = ? AND r.knot <> ''
            AND (
                r.did = ?
				 OR EXISTS (
					 SELECT 1 FROM spindle_members m
					  WHERE m.did = ? AND m.instance = ? AND m.subject = r.did
				 )
			 )`,
		hostname, ownerDID, ownerDID, hostname,
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

// ApplyKnotCollaborator mirrors the authoritative ACL update emitted by the
// knot that owns repo. These events replaced PDS collaborator records for
// current knots, but use the same table so XRPC authorization has one source.
func (s *store) ApplyKnotCollaborator(
	ctx context.Context,
	repo *RepoRef,
	subject string,
	add bool,
) error {
	if !add {
		_, err := s.db.ExecContext(ctx, `
			DELETE FROM repo_collaborators
			WHERE repo_did = ? AND subject = ?`, repo.RepoDID, subject,
		)
		if err != nil {
			return fmt.Errorf("delete knot collaborator: %w", err)
		}
		return nil
	}
	return s.UpsertRepoCollaborator(
		ctx,
		repo.OwnerDID,
		"knot:"+repo.RepoDID+":"+subject,
		repo.RepoDID,
		repo.RepoDID,
		subject,
		time.Now().UTC().Format(time.RFC3339Nano),
	)
}

// EventRow is one retained spindle-owned pipeline or status record.
type EventRow struct {
	// Created is the assigned monotonic Unix-nanosecond ordering key.
	Created int64
	// Rkey is the ATProto record key. For sh.tangled.pipeline.status
	// records this is the rkey we mint when publishing.
	Rkey string
	// Nsid is the lexicon collection (e.g. sh.tangled.pipeline.status).
	Nsid string
	// EventJSON is the record body verbatim.
	EventJSON json.RawMessage
}

// InsertEvent appends an event row and returns its assigned created value.
// Storage is the source of truth for pipeline queries and status projection.
//
// eventJSON must be a valid JSON object; we store it verbatim. Length
// validation is intentionally absent — the schema accepts arbitrary
// TEXT and SQLite handles huge blobs fine for our scale.
func (s *store) InsertEvent(ctx context.Context, rkey, nsid string, eventJSON []byte) (int64, error) {
	s.eventMu.Lock()
	defer s.eventMu.Unlock()

	now := time.Now().UnixNano()
	if now <= s.lastEventNanos {
		now = s.lastEventNanos + 1
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO events (created, rkey, nsid, event_json, inserted_at)
		 VALUES (?, ?, ?, ?, ?)`,
		now, rkey, nsid, string(eventJSON),
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return 0, fmt.Errorf("insert event: %w", err)
	}
	s.lastEventNanos = now
	return now, nil
}

// BuildkiteBuildRef is the persisted mapping from one Buildkite build
// to the Tangled pipeline tuple that spawned it. It's the row written
// by the Buildkite provider at Spawn time and read by the webhook handler and
// CI log subscription.
type BuildkiteBuildRef struct {
	BuildUUID    string
	BuildNumber  int64
	PipelineSlug string
	// Org is the Buildkite organisation slug the build was created
	// against. Persisted at Spawn time so log subscriptions and any other
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

// TektonRunRef is the persisted link from a Tangled workflow tuple
// to the in-cluster PipelineRun tack created for it. The tuple is the
// user-facing identity the appview knows; namespace/name/uid are the
// Kubernetes identity needed for status watching and log lookup.
type TektonRunRef struct {
	Knot            string
	PipelineRkey    string
	Workflow        string
	Namespace       string
	PipelineRunName string
	PipelineRunUID  string
	PipelineName    string
	PipelineURI     string
}

// SourcehutJobRef is the persisted link from a Tangled workflow tuple
// to the builds.sr.ht job tack submitted for it. Owner is the
// canonical name (with leading "~") returned by the upstream API and
// is the path component log URLs are rooted at — without it we can't
// fetch logs even though the job ID alone is unique. Instance is the
// base URL the job lives on; persisted per-row so post-creation API
// calls always target the same server the workflow originally ran on.
type SourcehutJobRef struct {
	Knot         string
	PipelineRkey string
	Workflow     string
	JobID        int64
	Owner        string
	Instance     string
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
// been recorded for that tuple. Log subscriptions use it to translate the
// spindle pipeline identity back into something Buildkite knows.
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

// InsertTektonRun records the latest PipelineRun created for a Tangled
// workflow tuple. Reusing the tuple as the primary key intentionally
// makes log subscriptions resolve to the newest run for that workflow identity.
func (s *store) InsertTektonRun(ctx context.Context, ref TektonRunRef) error {
	now := time.Now().UTC()
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO tekton_runs (
            knot, pipeline_rkey, workflow,
            namespace, pipeline_run_name, pipeline_run_uid,
            pipeline_name, pipeline_uri, created_at, created_unix_ns
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(knot, pipeline_rkey, workflow) DO UPDATE SET
             namespace         = excluded.namespace,
             pipeline_run_name = excluded.pipeline_run_name,
             pipeline_run_uid  = excluded.pipeline_run_uid,
             pipeline_name     = excluded.pipeline_name,
             pipeline_uri      = excluded.pipeline_uri,
             created_at        = excluded.created_at,
             created_unix_ns   = excluded.created_unix_ns`,
		ref.Knot, ref.PipelineRkey, ref.Workflow,
		ref.Namespace, ref.PipelineRunName, ref.PipelineRunUID,
		ref.PipelineName, ref.PipelineURI, now.Format(time.RFC3339Nano), now.UnixNano(),
	)
	if err != nil {
		return fmt.Errorf("insert tekton_run: %w", err)
	}
	return nil
}

// InsertSourcehutJob records the latest builds.sr.ht job submitted for
// a Tangled workflow tuple. Reusing the tuple as the primary key
// intentionally makes log subscriptions resolve to the newest job for that workflow
// identity — same behaviour as InsertTektonRun.
func (s *store) InsertSourcehutJob(ctx context.Context, ref SourcehutJobRef) error {
	now := time.Now().UTC()
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO sourcehut_jobs (
            knot, pipeline_rkey, workflow,
            job_id, owner, instance,
            pipeline_uri, created_at, created_unix_ns
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(knot, pipeline_rkey, workflow) DO UPDATE SET
             job_id          = excluded.job_id,
             owner           = excluded.owner,
             instance        = excluded.instance,
             pipeline_uri    = excluded.pipeline_uri,
             created_at      = excluded.created_at,
             created_unix_ns = excluded.created_unix_ns`,
		ref.Knot, ref.PipelineRkey, ref.Workflow,
		ref.JobID, ref.Owner, ref.Instance,
		ref.PipelineURI, now.Format(time.RFC3339Nano), now.UnixNano(),
	)
	if err != nil {
		return fmt.Errorf("insert sourcehut_job: %w", err)
	}
	return nil
}

// LookupSourcehutJobByTuple resolves a workflow identity to the concrete
// builds.sr.ht job Tack submitted for it.
func (s *store) LookupSourcehutJobByTuple(ctx context.Context, knot, pipelineRkey, workflow string) (*SourcehutJobRef, error) {
	var ref SourcehutJobRef
	err := s.db.QueryRowContext(ctx,
		`SELECT knot, pipeline_rkey, workflow,
                job_id, owner, instance, pipeline_uri
           FROM sourcehut_jobs
          WHERE knot = ? AND pipeline_rkey = ? AND workflow = ?`,
		knot, pipelineRkey, workflow,
	).Scan(
		&ref.Knot, &ref.PipelineRkey, &ref.Workflow,
		&ref.JobID, &ref.Owner, &ref.Instance, &ref.PipelineURI,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("lookup sourcehut_job by tuple: %w", err)
	}
	return &ref, nil
}

// LookupTektonRunByTuple resolves a workflow identity to the concrete
// PipelineRun Tack created in Kubernetes.
func (s *store) LookupTektonRunByTuple(ctx context.Context, knot, pipelineRkey, workflow string) (*TektonRunRef, error) {
	var ref TektonRunRef
	err := s.db.QueryRowContext(ctx,
		`SELECT knot, pipeline_rkey, workflow,
                namespace, pipeline_run_name, pipeline_run_uid,
                pipeline_name, pipeline_uri
           FROM tekton_runs
          WHERE knot = ? AND pipeline_rkey = ? AND workflow = ?`,
		knot, pipelineRkey, workflow,
	).Scan(
		&ref.Knot, &ref.PipelineRkey, &ref.Workflow,
		&ref.Namespace, &ref.PipelineRunName, &ref.PipelineRunUID,
		&ref.PipelineName, &ref.PipelineURI,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("lookup tekton_run by tuple: %w", err)
	}
	return &ref, nil
}

// EventsAfter returns every event row with created strictly greater than
// cursor. Provider and persistence tests use it to inspect published records.
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
