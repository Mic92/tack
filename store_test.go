package main

// Tests for the SQLite store. We use t.TempDir() for an isolated database
// per test, which both keeps tests independent and exercises the open +
// migrate path on every run.
//
// Where the store doesn't expose a query method (it's intentionally
// write-mostly today) we drop down to raw SQL via s.db to verify the
// row is what we expect.

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

// newTestStore opens a fresh store in a per-test temp dir and registers
// cleanup. Centralized so test bodies stay focused on behavior.
func newTestStore(t *testing.T) *store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "tack.db")
	s, err := openStore(path)
	if err != nil {
		t.Fatalf("openStore: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})
	return s
}

// TestOpenStoreIdempotent makes sure re-opening an existing database
// (which is what happens on every restart) succeeds and leaves the
// schema intact.
func TestOpenStoreIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tack.db")
	s1, err := openStore(path)
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	s2, err := openStore(path)
	if err != nil {
		t.Fatalf("second open: %v", err)
	}
	defer s2.Close()

	// Sanity check: the schema is in place by writing and reading back
	// a cursor through the freshly re-opened handle.
	ctx := context.Background()
	if err := s2.SaveCursor(ctx, 42); err != nil {
		t.Fatalf("save cursor: %v", err)
	}
	got, err := s2.LoadCursor(ctx)
	if err != nil {
		t.Fatalf("load cursor: %v", err)
	}
	if got == nil || *got != 42 {
		t.Fatalf("cursor = %v, want 42", got)
	}
}

// TestCursorRoundtrip covers the three states a cursor can be in:
// missing (nil), present, and overwritten. Together they exercise the
// INSERT and the ON CONFLICT branch of SaveCursor.
func TestCursorRoundtrip(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	got, err := s.LoadCursor(ctx)
	if err != nil {
		t.Fatalf("load cursor (empty): %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil cursor on fresh store, got %d", *got)
	}

	if err := s.SaveCursor(ctx, 1234567890); err != nil {
		t.Fatalf("save cursor: %v", err)
	}
	got, err = s.LoadCursor(ctx)
	if err != nil {
		t.Fatalf("load cursor: %v", err)
	}
	if got == nil || *got != 1234567890 {
		t.Fatalf("cursor after save = %v, want 1234567890", got)
	}

	// Overwrite path — exercises ON CONFLICT DO UPDATE.
	if err := s.SaveCursor(ctx, 9999); err != nil {
		t.Fatalf("overwrite cursor: %v", err)
	}
	got, err = s.LoadCursor(ctx)
	if err != nil {
		t.Fatalf("load cursor (overwrite): %v", err)
	}
	if got == nil || *got != 9999 {
		t.Fatalf("cursor after overwrite = %v, want 9999", got)
	}
}

// TestSpindleMemberLifecycle covers insert, update-on-conflict, and
// delete for spindle.member rows. We read back via raw SQL since the
// store doesn't expose a query helper.
func TestSpindleMemberLifecycle(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	const did, rkey = "did:plc:owner", "abc123"

	if err := s.UpsertSpindleMember(ctx, did, rkey, "https://spindle.example", "did:plc:alice", "2026-01-01T00:00:00Z"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	var instance, subject, createdAt string
	err := s.db.QueryRowContext(ctx,
		`SELECT instance, subject, created_at FROM spindle_members WHERE did = ? AND rkey = ?`,
		did, rkey,
	).Scan(&instance, &subject, &createdAt)
	if err != nil {
		t.Fatalf("query after insert: %v", err)
	}
	if instance != "https://spindle.example" || subject != "did:plc:alice" || createdAt != "2026-01-01T00:00:00Z" {
		t.Fatalf("after insert got (%q,%q,%q)", instance, subject, createdAt)
	}

	// Update path — same primary key, different fields.
	if err := s.UpsertSpindleMember(ctx, did, rkey, "https://spindle.example", "did:plc:bob", "2026-02-02T00:00:00Z"); err != nil {
		t.Fatalf("update: %v", err)
	}
	err = s.db.QueryRowContext(ctx,
		`SELECT subject, created_at FROM spindle_members WHERE did = ? AND rkey = ?`,
		did, rkey,
	).Scan(&subject, &createdAt)
	if err != nil {
		t.Fatalf("query after update: %v", err)
	}
	if subject != "did:plc:bob" || createdAt != "2026-02-02T00:00:00Z" {
		t.Fatalf("after update got (%q,%q)", subject, createdAt)
	}

	// Delete and verify the row is gone.
	if err := s.DeleteSpindleMember(ctx, did, rkey); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if n := countRows(t, s, "spindle_members"); n != 0 {
		t.Fatalf("after delete, spindle_members has %d rows, want 0", n)
	}

	// Deleting a row that doesn't exist must succeed silently — the
	// jetstream stream can replay deletes after a restart.
	if err := s.DeleteSpindleMember(ctx, did, rkey); err != nil {
		t.Fatalf("delete missing: %v", err)
	}
}

// TestRepoLifecycle exercises Repo upsert/delete and confirms that
// optional fields (spindle, repo_did) round-trip as the empty string
// rather than NULL — the store schema treats them uniformly.
func TestRepoLifecycle(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	const did, rkey = "did:plc:owner", "repo1"

	if err := s.UpsertRepo(ctx, did, rkey, "knot.example", "myrepo", "https://spindle.example", "did:plc:repo", "2026-01-01T00:00:00Z"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	var knot, name, spindle, repoDid string
	err := s.db.QueryRowContext(ctx,
		`SELECT knot, name, spindle, repo_did FROM repos WHERE did = ? AND rkey = ?`,
		did, rkey,
	).Scan(&knot, &name, &spindle, &repoDid)
	if err != nil {
		t.Fatalf("query after insert: %v", err)
	}
	if knot != "knot.example" || name != "myrepo" || spindle != "https://spindle.example" || repoDid != "did:plc:repo" {
		t.Fatalf("after insert got (%q,%q,%q,%q)", knot, name, spindle, repoDid)
	}

	// Upsert with cleared optional fields — verifies UPDATE actually
	// overwrites them rather than preserving the old non-empty values.
	if err := s.UpsertRepo(ctx, did, rkey, "knot.example", "myrepo", "", "", "2026-01-01T00:00:00Z"); err != nil {
		t.Fatalf("update: %v", err)
	}
	err = s.db.QueryRowContext(ctx,
		`SELECT spindle, repo_did FROM repos WHERE did = ? AND rkey = ?`,
		did, rkey,
	).Scan(&spindle, &repoDid)
	if err != nil {
		t.Fatalf("query after update: %v", err)
	}
	if spindle != "" || repoDid != "" {
		t.Fatalf("after update got (%q,%q), want both empty", spindle, repoDid)
	}

	if err := s.DeleteRepo(ctx, did, rkey); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if n := countRows(t, s, "repos"); n != 0 {
		t.Fatalf("after delete, repos has %d rows, want 0", n)
	}
}

// TestRepoCollaboratorLifecycle mirrors the repo and member tests for
// the collaborator table.
func TestRepoCollaboratorLifecycle(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	const did, rkey = "did:plc:owner", "collab1"

	if err := s.UpsertRepoCollaborator(ctx, did, rkey, "myrepo", "did:plc:repo", "did:plc:carol", "2026-01-01T00:00:00Z"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	var repo, repoDid, subject string
	err := s.db.QueryRowContext(ctx,
		`SELECT repo, repo_did, subject FROM repo_collaborators WHERE did = ? AND rkey = ?`,
		did, rkey,
	).Scan(&repo, &repoDid, &subject)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if repo != "myrepo" || repoDid != "did:plc:repo" || subject != "did:plc:carol" {
		t.Fatalf("got (%q,%q,%q)", repo, repoDid, subject)
	}

	if err := s.DeleteRepoCollaborator(ctx, did, rkey); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if n := countRows(t, s, "repo_collaborators"); n != 0 {
		t.Fatalf("after delete, repo_collaborators has %d rows, want 0", n)
	}
}

// TestKnotsForSpindle verifies the query returns only knots from repos
// whose .spindle field matches the given hostname *and* whose publisher
// is an authorized actor (owner or owner-vouched member). Duplicate
// knots collapse to a single entry. Repos from non-members are excluded:
// that's the gate that prevents an attacker-published repo record from
// forcing us to dial an attacker-chosen knot.
func TestKnotsForSpindle(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	const ours = "tack.example"
	const other = "other.example"
	const owner = "did:plc:owner"

	// Vouch for did:plc:a and did:plc:b; did:plc:c is the owner so
	// no membership grant is needed; did:plc:zzz is unvouched.
	if err := s.UpsertSpindleMember(ctx, owner, "mk1", ours, "did:plc:a", "t"); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertSpindleMember(ctx, owner, "mk2", ours, "did:plc:b", "t"); err != nil {
		t.Fatal(err)
	}

	// Two member-published repos on the same knot pointing at us;
	// should collapse to one entry.
	if err := s.UpsertRepo(ctx, "did:plc:a", "rk1", "knot1.example", "repo-a", ours, "", "t"); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertRepo(ctx, "did:plc:b", "rk2", "knot1.example", "repo-b", ours, "", "t"); err != nil {
		t.Fatal(err)
	}
	// Owner-published repo on a second knot. Owner is implicitly
	// authorized and must be included.
	if err := s.UpsertRepo(ctx, owner, "rk3", "knot2.example", "repo-c", ours, "", "t"); err != nil {
		t.Fatal(err)
	}
	// Repo pointing at a different spindle: must be excluded.
	if err := s.UpsertRepo(ctx, "did:plc:a", "rk4", "knot3.example", "repo-d", other, "", "t"); err != nil {
		t.Fatal(err)
	}
	// Repo with no spindle declared: must be excluded.
	if err := s.UpsertRepo(ctx, "did:plc:a", "rk5", "knot4.example", "repo-e", "", "", "t"); err != nil {
		t.Fatal(err)
	}
	// Repo published by an unvouched DID: must be excluded even
	// though it points at us. This is the security-relevant case:
	// without the membership filter, a stranger could pin us to
	// "evil.example" just by publishing a sh.tangled.repo record.
	if err := s.UpsertRepo(ctx, "did:plc:zzz", "rk6", "evil.example", "repo-z", ours, "", "t"); err != nil {
		t.Fatal(err)
	}

	got, err := s.KnotsForSpindle(ctx, ours, owner)
	if err != nil {
		t.Fatalf("KnotsForSpindle: %v", err)
	}
	want := map[string]struct{}{"knot1.example": {}, "knot2.example": {}}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for _, k := range got {
		if _, ok := want[k]; !ok {
			t.Fatalf("unexpected knot %q in %v", k, got)
		}
	}
}

// TestLookupBuildkiteBuildByTuplePicksMostRecent verifies that
// LookupBuildkiteBuildByTuple returns the build with the largest
// created_unix_ns even when text-ordering created_at would lie.
//
// The motivating bug: time.Format(RFC3339Nano) trims trailing zeros,
// so an exact-second instant renders as "...:00Z" while one nanosecond
// later renders as "...:00.000000001Z". Lex ordering puts ".000000001Z"
// *before* "Z" because '.' (0x2E) < 'Z' (0x5A), so ORDER BY created_at
// DESC would surface the *earlier* row as the latest build. Sorting
// on created_unix_ns avoids that and is what /logs depends on to
// resolve the right run.
func TestLookupBuildkiteBuildByTuplePicksMostRecent(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// Two rows for the same (knot, rkey, workflow). The "newer" one
	// is one nanosecond later but its RFC3339Nano text sorts BEFORE
	// the older row's, which is exactly the failure mode we're
	// guarding against.
	older := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	newer := older.Add(time.Nanosecond)

	insert := func(uuid string, ts time.Time) {
		t.Helper()
		_, err := s.db.ExecContext(ctx,
			`INSERT INTO buildkite_builds (
                build_uuid, build_number, pipeline_slug, org,
                knot, pipeline_rkey, workflow,
                pipeline_uri, created_at, created_unix_ns
             ) VALUES (?, ?, ?, '', ?, ?, ?, ?, ?, ?)`,
			uuid, int64(1), "p",
			"k", "r", "w", "at://x",
			ts.Format(time.RFC3339Nano), ts.UnixNano(),
		)
		if err != nil {
			t.Fatalf("insert %s: %v", uuid, err)
		}
	}
	insert("older", older)
	insert("newer", newer)

	ref, err := s.LookupBuildkiteBuildByTuple(ctx, "k", "r", "w")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if ref == nil {
		t.Fatal("lookup returned nil; want a row")
	}
	if ref.BuildUUID != "newer" {
		t.Fatalf("lookup picked %q; want %q (text-ordering bug regression)",
			ref.BuildUUID, "newer")
	}
}

// TestBuildkiteCreatedUnixNSBackfill simulates the upgrade path: rows
// inserted before the column existed (created_unix_ns = 0) get
// promoted to the parsed UnixNano of their created_at by the migration.
// Without the backfill the lookup would tie on 0 across all legacy
// rows and have to fall back to the (still-imperfect) text ordering.
func TestBuildkiteCreatedUnixNSBackfill(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// Pretend this row was written by an older binary: created_at
	// is set, created_unix_ns is the post-ALTER default 0.
	want := time.Date(2026, 3, 4, 5, 6, 7, 8, time.UTC)
	if _, err := s.db.ExecContext(ctx,
		`INSERT INTO buildkite_builds (
            build_uuid, build_number, pipeline_slug, org,
            knot, pipeline_rkey, workflow,
            pipeline_uri, created_at, created_unix_ns
         ) VALUES (?, 1, 'p', '', 'k', 'r', 'w', 'at://x', ?, 0)`,
		"legacy", want.Format(time.RFC3339Nano),
	); err != nil {
		t.Fatalf("seed legacy row: %v", err)
	}

	if err := s.backfillBuildkiteCreatedUnixNS(ctx); err != nil {
		t.Fatalf("backfill: %v", err)
	}

	var got int64
	if err := s.db.QueryRowContext(ctx,
		`SELECT created_unix_ns FROM buildkite_builds WHERE build_uuid = ?`,
		"legacy",
	).Scan(&got); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if got != want.UnixNano() {
		t.Fatalf("created_unix_ns = %d; want %d", got, want.UnixNano())
	}
}

// TestAuthorizePipelineActor exercises every gate of the
// authorization helper end-to-end. The case table covers the matrix
// of {repo claim present, member grant present, actor is owner} and
// pins the negative-path failure reasons, which the knot consumer
// surfaces verbatim into its denial logs.
func TestAuthorizePipelineActor(t *testing.T) {
	const (
		hostname = "spindle.example"
		knot     = "knot.example"
		owner    = "did:plc:owner"
		repoOwn  = "did:plc:alice"
		repoName = "myrepo"
	)
	ctx := context.Background()

	t.Run("missing repo did", func(t *testing.T) {
		s := newTestStore(t)
		ok, reason, err := s.AuthorizePipelineActor(ctx,
			hostname, knot, owner, "", repoName)
		if err != nil || ok {
			t.Fatalf("got ok=%v err=%v; want ok=false err=nil", ok, err)
		}
		if reason == "" {
			t.Fatal("expected non-empty reason")
		}
	})

	t.Run("repo not on this spindle/knot", func(t *testing.T) {
		s := newTestStore(t)
		// Repo exists but with the wrong knot: the trigger
		// arrived on knot.example but the repo opted into a
		// different host. Must be rejected.
		if err := s.UpsertRepo(ctx, repoOwn, "rk1",
			"other.knot", repoName, hostname, "", "t"); err != nil {
			t.Fatal(err)
		}
		ok, reason, err := s.AuthorizePipelineActor(ctx,
			hostname, knot, owner, repoOwn, repoName)
		if err != nil || ok {
			t.Fatalf("got ok=%v err=%v reason=%q", ok, err, reason)
		}
	})

	t.Run("repo on wrong spindle", func(t *testing.T) {
		s := newTestStore(t)
		// Right knot, but spindle is someone else.
		if err := s.UpsertRepo(ctx, repoOwn, "rk1",
			knot, repoName, "other.spindle", "", "t"); err != nil {
			t.Fatal(err)
		}
		ok, _, err := s.AuthorizePipelineActor(ctx,
			hostname, knot, owner, repoOwn, repoName)
		if err != nil || ok {
			t.Fatalf("got ok=%v err=%v", ok, err)
		}
	})

	t.Run("repo claims us but actor not a member", func(t *testing.T) {
		s := newTestStore(t)
		if err := s.UpsertRepo(ctx, repoOwn, "rk1",
			knot, repoName, hostname, "", "t"); err != nil {
			t.Fatal(err)
		}
		ok, reason, err := s.AuthorizePipelineActor(ctx,
			hostname, knot, owner, repoOwn, repoName)
		if err != nil || ok {
			t.Fatalf("got ok=%v err=%v reason=%q", ok, err, reason)
		}
	})

	t.Run("membership granted by non-owner is ignored", func(t *testing.T) {
		s := newTestStore(t)
		if err := s.UpsertRepo(ctx, repoOwn, "rk1",
			knot, repoName, hostname, "", "t"); err != nil {
			t.Fatal(err)
		}
		// A member record published by anyone other than the
		// spindle owner must NOT count, otherwise self-grants
		// would bypass authorization entirely.
		if err := s.UpsertSpindleMember(ctx,
			"did:plc:rando", "mk1", hostname, repoOwn, "t"); err != nil {
			t.Fatal(err)
		}
		ok, _, err := s.AuthorizePipelineActor(ctx,
			hostname, knot, owner, repoOwn, repoName)
		if err != nil || ok {
			t.Fatalf("got ok=%v err=%v", ok, err)
		}
	})

	t.Run("owner-granted member is allowed", func(t *testing.T) {
		s := newTestStore(t)
		if err := s.UpsertRepo(ctx, repoOwn, "rk1",
			knot, repoName, hostname, "", "t"); err != nil {
			t.Fatal(err)
		}
		if err := s.UpsertSpindleMember(ctx,
			owner, "mk1", hostname, repoOwn, "t"); err != nil {
			t.Fatal(err)
		}
		ok, _, err := s.AuthorizePipelineActor(ctx,
			hostname, knot, owner, repoOwn, repoName)
		if err != nil || !ok {
			t.Fatalf("got ok=%v err=%v; want ok=true", ok, err)
		}
	})

	t.Run("spindle owner triggers their own repo", func(t *testing.T) {
		s := newTestStore(t)
		// Owner needs no membership row to act on a repo they
		// themselves published. Just the repo-claim gate.
		if err := s.UpsertRepo(ctx, owner, "rk1",
			knot, repoName, hostname, "", "t"); err != nil {
			t.Fatal(err)
		}
		ok, _, err := s.AuthorizePipelineActor(ctx,
			hostname, knot, owner, owner, repoName)
		if err != nil || !ok {
			t.Fatalf("got ok=%v err=%v; want ok=true", ok, err)
		}
	})

	t.Run("repo record key matches trigger repo when name is empty", func(t *testing.T) {
		s := newTestStore(t)
		const rkeyName = "woodyfm-site"
		if err := s.UpsertRepo(ctx, owner, rkeyName,
			knot, "", hostname, "", "t"); err != nil {
			t.Fatal(err)
		}
		ok, _, err := s.AuthorizePipelineActor(ctx,
			hostname, knot, owner, owner, rkeyName)
		if err != nil || !ok {
			t.Fatalf("got ok=%v err=%v; want ok=true", ok, err)
		}
	})
}

// countRows is a small SELECT COUNT(*) helper used by lifecycle tests
// to verify deletes actually removed the row. Table name is interpolated
// directly because callers pass a constant from the schema, not user
// input — SQLite doesn't allow parameterized table names anyway.
func countRows(t *testing.T, s *store, table string) int {
	t.Helper()
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}
