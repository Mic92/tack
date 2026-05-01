package main

// Tests for handleJetstreamEvent. We exercise the full path — collection
// dispatch, record decoding, store mutation, and cursor advancement —
// using an in-memory store from store_test.go's newTestStore helper.
//
// Events are constructed by hand rather than recorded from a live
// jetstream so the tests don't need network access and stay fast.

import (
	"context"
	"encoding/json"
	"testing"

	jsmodels "github.com/bluesky-social/jetstream/pkg/models"
	"tangled.org/core/api/tangled"
)

// commitEvent builds a jetstream commit event for tests. timeUS is the
// cursor value the handler should persist after applying. record can be
// nil for delete operations, which carry no body.
func commitEvent(timeUS int64, did, collection, op, rkey string, record any) *jsmodels.Event {
	var raw json.RawMessage
	if record != nil {
		b, err := json.Marshal(record)
		if err != nil {
			panic(err) // test helper — callers control the input
		}
		raw = b
	}
	return &jsmodels.Event{
		Did:    did,
		TimeUS: timeUS,
		Kind:   jsmodels.EventKindCommit,
		Commit: &jsmodels.Commit{
			Operation:  op,
			Collection: collection,
			RKey:       rkey,
			Record:     raw,
		},
	}
}

// requireCursor asserts the persisted cursor equals want. Pulled out
// because almost every test in this file checks it.
func requireCursor(t *testing.T, s *store, want int64) {
	t.Helper()
	got, err := s.LoadCursor(context.Background())
	if err != nil {
		t.Fatalf("load cursor: %v", err)
	}
	if got == nil || *got != want {
		t.Fatalf("cursor = %v, want %d", got, want)
	}
}

// TestHandleNonCommitEvent confirms account/identity events are ignored
// without error and without advancing the cursor — they don't have a
// TimeUS we want to commit to.
func TestHandleNonCommitEvent(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	evt := &jsmodels.Event{
		Did:    "did:plc:foo",
		TimeUS: 100,
		Kind:   jsmodels.EventKindAccount,
	}
	if err := handleJetstreamEvent(ctx, s, evt); err != nil {
		t.Fatalf("handle: %v", err)
	}
	got, err := s.LoadCursor(ctx)
	if err != nil {
		t.Fatalf("load cursor: %v", err)
	}
	if got != nil {
		t.Fatalf("expected no cursor for non-commit, got %d", *got)
	}
}

// TestHandleSpindleMemberCreate exercises the happy path: a create
// commit lands a row in spindle_members and advances the cursor.
func TestHandleSpindleMemberCreate(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	rec := tangled.SpindleMember{
		Instance:  "https://spindle.example",
		Subject:   "did:plc:alice",
		CreatedAt: "2026-01-01T00:00:00Z",
	}
	evt := commitEvent(12345, "did:plc:owner", tangled.SpindleMemberNSID, jsOpCreate, "rk1", rec)
	if err := handleJetstreamEvent(ctx, s, evt); err != nil {
		t.Fatalf("handle: %v", err)
	}

	var instance, subject string
	err := s.db.QueryRowContext(ctx,
		`SELECT instance, subject FROM spindle_members WHERE did = ? AND rkey = ?`,
		"did:plc:owner", "rk1",
	).Scan(&instance, &subject)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if instance != "https://spindle.example" || subject != "did:plc:alice" {
		t.Fatalf("got (%q,%q)", instance, subject)
	}
	requireCursor(t, s, 12345)
}

// TestHandleSpindleMemberDelete confirms a delete commit removes the
// previously-persisted row.
func TestHandleSpindleMemberDelete(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	if err := s.UpsertSpindleMember(ctx, "did:plc:owner", "rk1", "i", "s", "t"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	evt := commitEvent(99, "did:plc:owner", tangled.SpindleMemberNSID, jsOpDelete, "rk1", nil)
	if err := handleJetstreamEvent(ctx, s, evt); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if n := countRows(t, s, "spindle_members"); n != 0 {
		t.Fatalf("after delete: %d rows, want 0", n)
	}
	requireCursor(t, s, 99)
}

// TestHandleRepoCreateOptionals verifies pointer-typed optional fields
// on tangled.Repo are derefed correctly (and nils become empty strings)
// when written to the store.
func TestHandleRepoCreateOptionals(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	spindle := "https://spindle.example"
	repoDid := "did:plc:repo"
	rec := tangled.Repo{
		Knot:      "knot.example",
		Name:      "myrepo",
		Spindle:   &spindle,
		RepoDid:   &repoDid,
		CreatedAt: "2026-01-01T00:00:00Z",
	}
	evt := commitEvent(7, "did:plc:owner", tangled.RepoNSID, jsOpCreate, "repo1", rec)
	if err := handleJetstreamEvent(ctx, s, evt); err != nil {
		t.Fatalf("handle: %v", err)
	}

	var gotSpindle, gotRepoDid string
	err := s.db.QueryRowContext(ctx,
		`SELECT spindle, repo_did FROM repos WHERE did = ? AND rkey = ?`,
		"did:plc:owner", "repo1",
	).Scan(&gotSpindle, &gotRepoDid)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if gotSpindle != spindle || gotRepoDid != repoDid {
		t.Fatalf("got (%q,%q)", gotSpindle, gotRepoDid)
	}

	// Now a record with both optionals nil — should land as empty
	// strings, not crash on a nil dereference in deref().
	rec2 := tangled.Repo{
		Knot:      "knot.example",
		Name:      "other",
		CreatedAt: "2026-01-01T00:00:00Z",
	}
	evt2 := commitEvent(8, "did:plc:owner", tangled.RepoNSID, jsOpCreate, "repo2", rec2)
	if err := handleJetstreamEvent(ctx, s, evt2); err != nil {
		t.Fatalf("handle nil-optionals: %v", err)
	}
	err = s.db.QueryRowContext(ctx,
		`SELECT spindle, repo_did FROM repos WHERE did = ? AND rkey = ?`,
		"did:plc:owner", "repo2",
	).Scan(&gotSpindle, &gotRepoDid)
	if err != nil {
		t.Fatalf("query nil-optionals: %v", err)
	}
	if gotSpindle != "" || gotRepoDid != "" {
		t.Fatalf("nil optionals: got (%q,%q), want both empty", gotSpindle, gotRepoDid)
	}
	requireCursor(t, s, 8)
}

// TestHandleRepoCollaboratorCreate covers the third dispatch arm so each
// collection has at least one apply test.
func TestHandleRepoCollaboratorCreate(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	repo := "myrepo"
	rec := tangled.RepoCollaborator{
		Repo:      &repo,
		Subject:   "did:plc:carol",
		CreatedAt: "2026-01-01T00:00:00Z",
	}
	evt := commitEvent(55, "did:plc:owner", tangled.RepoCollaboratorNSID, jsOpCreate, "c1", rec)
	if err := handleJetstreamEvent(ctx, s, evt); err != nil {
		t.Fatalf("handle: %v", err)
	}

	var subject string
	err := s.db.QueryRowContext(ctx,
		`SELECT subject FROM repo_collaborators WHERE did = ? AND rkey = ?`,
		"did:plc:owner", "c1",
	).Scan(&subject)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if subject != "did:plc:carol" {
		t.Fatalf("subject = %q", subject)
	}
	requireCursor(t, s, 55)
}

// TestHandleUnknownCollection makes sure a collection we didn't ask for
// (jetstream filter changes, schema drift) is silently dropped — no
// error, no row, but cursor still advances so we don't replay it.
func TestHandleUnknownCollection(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	evt := commitEvent(42, "did:plc:owner", "app.bsky.feed.post", jsOpCreate, "rk", map[string]string{"text": "hi"})
	if err := handleJetstreamEvent(ctx, s, evt); err != nil {
		t.Fatalf("handle: %v", err)
	}
	requireCursor(t, s, 42)
}

// TestHandleBadRecordAdvancesCursor is the failure-mode counterpart to
// the happy paths: a malformed record body must be logged-and-skipped
// (not returned as an error that pauses the scheduler) and the cursor
// must still advance so we don't loop on the same bad event forever.
func TestHandleBadRecordAdvancesCursor(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	evt := &jsmodels.Event{
		Did:    "did:plc:owner",
		TimeUS: 1000,
		Kind:   jsmodels.EventKindCommit,
		Commit: &jsmodels.Commit{
			Operation:  jsOpCreate,
			Collection: tangled.SpindleMemberNSID,
			RKey:       "broken",
			Record:     json.RawMessage(`{not valid json`),
		},
	}
	if err := handleJetstreamEvent(ctx, s, evt); err != nil {
		t.Fatalf("handle should swallow decode error, got: %v", err)
	}
	if n := countRows(t, s, "spindle_members"); n != 0 {
		t.Fatalf("bad record should not have inserted; got %d rows", n)
	}
	requireCursor(t, s, 1000)
}
