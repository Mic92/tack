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
	"errors"
	"fmt"
	"strconv"

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
