package main

// Schema definition and migration logic for the SQLite store. Pulled out
// of store.go so the big SQL block doesn't sit in the middle of the
// runtime API surface.

import (
	"context"
	"fmt"
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
`

// migrate applies the schema. Safe to call repeatedly.
func (s *store) migrate(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	return nil
}
