package main

// Migration tests for the SQLite store. These specifically cover the
// upgrade path from a pre-`created_unix_ns` database (the shape every
// production tack.db that pre-dates this commit will have on first
// boot) to the current schema. The fresh-database path is exercised
// implicitly by every other test via newTestStore. These tests are
// about the *transition*.

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// legacyBuildkiteSchema is the buildkite_builds table as it looked
// before created_unix_ns was added. We hand-craft a database in this
// shape so the migration has something realistic to widen and backfill.
//
// Note that org is also absent: the migrate() path adds it via ALTER
// too, so the test doubles as coverage for two stacked column adds
// applying to the same table on the same upgrade.
const legacyBuildkiteSchema = `
CREATE TABLE buildkite_builds (
    build_uuid     TEXT PRIMARY KEY,
    build_number   INTEGER NOT NULL,
    pipeline_slug  TEXT NOT NULL,
    knot           TEXT NOT NULL,
    pipeline_rkey  TEXT NOT NULL,
    workflow       TEXT NOT NULL,
    pipeline_uri   TEXT NOT NULL,
    created_at     TEXT NOT NULL
);
`

// openLegacyStore opens a brand-new sqlite file, hand-installs the
// pre-migration schema, and seeds it with the supplied rows. It
// returns the file path so the caller can re-open it through the
// real openStore (which runs migrate()) and observe the upgrade.
//
// We deliberately don't go through openStore for the seeding step,
// since that would apply the current schema and defeat the point.
func openLegacyStore(t *testing.T, rows []legacyRow) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "tack.db")
	dsn := fmt.Sprintf("file:%s?_journal_mode=WAL&_synchronous=NORMAL&_foreign_keys=on", path)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatalf("open legacy db: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(legacyBuildkiteSchema); err != nil {
		t.Fatalf("install legacy schema: %v", err)
	}
	for _, r := range rows {
		if _, err := db.Exec(
			`INSERT INTO buildkite_builds (
                build_uuid, build_number, pipeline_slug,
                knot, pipeline_rkey, workflow,
                pipeline_uri, created_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			r.uuid, r.buildNumber, "p",
			r.knot, r.rkey, r.workflow,
			"at://x", r.createdAt,
		); err != nil {
			t.Fatalf("seed legacy row %q: %v", r.uuid, err)
		}
	}
	return path
}

// legacyRow is the minimal set of fields we need to seed for the
// migration tests. Anything unspecified in the legacy schema (org,
// created_unix_ns) is filled in by the migration itself.
type legacyRow struct {
	uuid                 string
	knot, rkey, workflow string
	buildNumber          int64
	createdAt            string
}

// TestMigrateAddsAndBackfillsCreatedUnixNS exercises the full upgrade:
// a database in the old schema gets opened through openStore, which
// runs migrate(), which should (a) add the created_unix_ns column and
// (b) populate it from each row's existing created_at text.
func TestMigrateAddsAndBackfillsCreatedUnixNS(t *testing.T) {
	older := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	newer := older.Add(time.Nanosecond) // see store_test.go for why this matters

	path := openLegacyStore(t, []legacyRow{
		{
			uuid: "older", knot: "k", rkey: "r", workflow: "w",
			buildNumber: 1, createdAt: older.Format(time.RFC3339Nano),
		},
		{
			uuid: "newer", knot: "k", rkey: "r", workflow: "w",
			buildNumber: 2, createdAt: newer.Format(time.RFC3339Nano),
		},
	})

	s, err := openStore(path)
	if err != nil {
		t.Fatalf("openStore (migrate): %v", err)
	}
	defer s.Close()

	// Column must now exist and be populated for both seeded rows.
	got := map[string]int64{}
	rows, err := s.db.Query(`SELECT build_uuid, created_unix_ns FROM buildkite_builds`)
	if err != nil {
		t.Fatalf("select after migrate: %v", err)
	}
	for rows.Next() {
		var uuid string
		var ns int64
		if err := rows.Scan(&uuid, &ns); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got[uuid] = ns
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate: %v", err)
	}

	if got["older"] != older.UnixNano() {
		t.Errorf("older: created_unix_ns = %d; want %d", got["older"], older.UnixNano())
	}
	if got["newer"] != newer.UnixNano() {
		t.Errorf("newer: created_unix_ns = %d; want %d", got["newer"], newer.UnixNano())
	}

	// And the lookup query (the actual reason the column exists)
	// must surface the newer row, which is the case the original bug
	// report says was getting it wrong before.
	ref, err := s.LookupBuildkiteBuildByTuple(context.Background(), "k", "r", "w")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if ref == nil || ref.BuildUUID != "newer" {
		t.Fatalf("lookup picked %+v; want uuid=newer", ref)
	}
}

// TestMigrateIdempotent makes sure running migrate() repeatedly is
// safe: re-applying the ALTERs returns "duplicate column name" which
// is swallowed, and the backfill sees no rows with created_unix_ns=0
// the second time around so it must not clobber the values written
// on the first pass.
func TestMigrateIdempotent(t *testing.T) {
	when := time.Date(2026, 5, 1, 12, 34, 56, 789, time.UTC)
	path := openLegacyStore(t, []legacyRow{{
		uuid: "u1", knot: "k", rkey: "r", workflow: "w",
		buildNumber: 1, createdAt: when.Format(time.RFC3339Nano),
	}})

	// First open: migrate runs, column is added and backfilled.
	s, err := openStore(path)
	if err != nil {
		t.Fatalf("first openStore: %v", err)
	}

	// Run migrate() again on the same handle. Should be a no-op and
	// must not return an error from a redundant ALTER.
	if err := s.migrate(context.Background()); err != nil {
		t.Fatalf("second migrate: %v", err)
	}
	s.Close()

	// Re-open from scratch (which also re-runs migrate) and confirm
	// the backfilled value is still intact.
	s2, err := openStore(path)
	if err != nil {
		t.Fatalf("re-open: %v", err)
	}
	defer s2.Close()

	var ns int64
	if err := s2.db.QueryRow(
		`SELECT created_unix_ns FROM buildkite_builds WHERE build_uuid = ?`, "u1",
	).Scan(&ns); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if ns != when.UnixNano() {
		t.Fatalf("created_unix_ns = %d after repeated migrate; want %d", ns, when.UnixNano())
	}
}

// TestMigrateBackfillSkipsUnparseableCreatedAt confirms the backfill's
// "skip and keep going" behavior for malformed timestamps. A single
// corrupt row must not block startup; the row is left at 0 (the
// post-ALTER default) and the lookup's tiebreaker columns then take
// over for it.
func TestMigrateBackfillSkipsUnparseableCreatedAt(t *testing.T) {
	good := time.Date(2026, 7, 8, 9, 10, 11, 12, time.UTC)
	path := openLegacyStore(t, []legacyRow{
		{
			uuid: "good", knot: "k", rkey: "r", workflow: "w",
			buildNumber: 1, createdAt: good.Format(time.RFC3339Nano),
		},
		{
			uuid: "garbage", knot: "k", rkey: "r", workflow: "w",
			buildNumber: 2, createdAt: "not a timestamp",
		},
	})

	s, err := openStore(path)
	if err != nil {
		// The whole point of swallowing parse errors is that
		// migrate() must succeed regardless. If it returns an
		// error here, the swallow logic regressed.
		t.Fatalf("openStore must tolerate unparseable created_at: %v", err)
	}
	defer s.Close()

	values := map[string]int64{}
	rows, err := s.db.Query(`SELECT build_uuid, created_unix_ns FROM buildkite_builds`)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	for rows.Next() {
		var uuid string
		var ns int64
		if err := rows.Scan(&uuid, &ns); err != nil {
			t.Fatalf("scan: %v", err)
		}
		values[uuid] = ns
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate: %v", err)
	}

	if values["good"] != good.UnixNano() {
		t.Errorf("good row: created_unix_ns = %d; want %d", values["good"], good.UnixNano())
	}
	if values["garbage"] != 0 {
		t.Errorf("garbage row: created_unix_ns = %d; want 0 (left at default)", values["garbage"])
	}
}
