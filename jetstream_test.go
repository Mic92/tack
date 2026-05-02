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
	"path/filepath"
	"testing"

	jsmodels "github.com/bluesky-social/jetstream/pkg/models"
	js "go.mitchellh.com/tack/internal/jetstream"
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

// handleJetstreamEvent is the testable per-event entry point. The generic
// jetstream processor owns cursor advancement; this wrapper only supplies
// Tack's collection list and commit application callback.
func handleJetstreamEvent(
	ctx context.Context,
	st *store,
	knots KnotConsumer,
	hostname, ownerDID string,
	evt *jsmodels.Event,
) error {
	processor := &js.Processor{
		Collections: jetstreamCollections,
		CursorStore: st,
		Handler: js.HandlerFunc(func(ctx context.Context, evt *jsmodels.Event) error {
			return applyCommit(ctx, st, knots, hostname, ownerDID, evt)
		}),
		Logger: loggerFrom(ctx),
	}
	return processor.HandleEvent(ctx, evt)
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
	if err := handleJetstreamEvent(ctx, s, nil, "", "", evt); err != nil {
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
	if err := handleJetstreamEvent(ctx, s, nil, "", "", evt); err != nil {
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
	if err := handleJetstreamEvent(ctx, s, nil, "", "", evt); err != nil {
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
	if err := handleJetstreamEvent(ctx, s, nil, "", "", evt); err != nil {
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
	if err := handleJetstreamEvent(ctx, s, nil, "", "", evt2); err != nil {
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
	if err := handleJetstreamEvent(ctx, s, nil, "", "", evt); err != nil {
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
	if err := handleJetstreamEvent(ctx, s, nil, "", "", evt); err != nil {
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
	if err := handleJetstreamEvent(ctx, s, nil, "", "", evt); err != nil {
		t.Fatalf("handle should swallow decode error, got: %v", err)
	}
	if n := countRows(t, s, "spindle_members"); n != 0 {
		t.Fatalf("bad record should not have inserted; got %d rows", n)
	}
	requireCursor(t, s, 1000)
}

// TestHandleTransientStoreErrorDoesNotAdvanceCursor is the partner to
// TestHandleBadRecordAdvancesCursor: the cursor must hold its previous
// position when applyCommit fails for an *infrastructure* reason
// (here: store closed mid-flight, simulating SQLite busy / shutdown
// races). Saving the cursor through such a failure would permanently
// skip a perfectly good record and silently lose membership state.
func TestHandleTransientStoreErrorDoesNotAdvanceCursor(t *testing.T) {
	// Build the store inline (not via newTestStore) so the cleanup
	// tolerates the explicit Close we do below to provoke errors.
	path := filepath.Join(t.TempDir(), "tack.db")
	s, err := openStore(path)
	if err != nil {
		t.Fatalf("openStore: %v", err)
	}
	ctx := context.Background()

	// Seed a baseline cursor; after the failed apply it must still be
	// 500, not 1000.
	if err := s.SaveCursor(ctx, 500); err != nil {
		t.Fatalf("seed cursor: %v", err)
	}

	// Close the underlying DB. Subsequent store calls will fail with
	// "sql: database is closed", which is a transient-style error from
	// applyCommit's perspective: the record itself is well-formed.
	if err := s.db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	rec := tangled.SpindleMember{
		Instance:  "https://spindle.example",
		Subject:   "did:plc:alice",
		CreatedAt: "2026-01-01T00:00:00Z",
	}
	evt := commitEvent(1000, "did:plc:owner", tangled.SpindleMemberNSID, jsOpCreate, "rk", rec)

	// Expect a non-nil error: handler propagates transient failures.
	if err := handleJetstreamEvent(ctx, s, nil, "", "", evt); err == nil {
		t.Fatalf("handle should return transient error, got nil")
	}

	// Re-open the same DB file to inspect the persisted cursor; the
	// closed handle obviously can't answer queries.
	s2, err := openStore(path)
	if err != nil {
		t.Fatalf("re-open: %v", err)
	}
	defer s2.Close()
	got, err := s2.LoadCursor(ctx)
	if err != nil {
		t.Fatalf("load cursor: %v", err)
	}
	if got == nil || *got != 500 {
		t.Fatalf("cursor = %v, want 500 (must NOT advance on transient error)", got)
	}
}

// TestRepoEventSubscribesKnotForOurSpindle confirms that observing a
// sh.tangled.repo whose .spindle field equals our hostname results in a
// dynamic AddKnot call. This is the hot path for picking up new repos
// without a tack restart.
func TestRepoEventSubscribesKnotForOurSpindle(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	const ours = "tack.example"
	spindle := ours
	rec := tangled.Repo{
		Knot:      "knot.example",
		Name:      "myrepo",
		Spindle:   &spindle,
		CreatedAt: "2026-01-01T00:00:00Z",
	}
	evt := commitEvent(1, "did:plc:owner", tangled.RepoNSID, jsOpCreate, "rk", rec)

	fake := &fakeKnotConsumer{}
	if err := handleJetstreamEvent(ctx, s, fake, ours, "did:plc:owner", evt); err != nil {
		t.Fatalf("handle: %v", err)
	}
	added := fake.Added()
	if len(added) != 1 || added[0] != "knot.example" {
		t.Fatalf("AddKnot calls = %v, want [knot.example]", added)
	}
}

// TestRepoEventIgnoresUnauthorizedPublisher pins the high-severity
// security gate: a sh.tangled.repo record naming us as its spindle
// must NOT cause an outbound knot subscription unless its publisher
// is the spindle owner or an owner-vouched member. Without this gate
// any firehose publisher could force tack to dial an attacker-chosen
// websocket simply by minting a matching repo record.
func TestRepoEventIgnoresUnauthorizedPublisher(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	const ours = "tack.example"
	const owner = "did:plc:owner"

	// did:plc:rando is neither the owner nor a member.
	spindle := ours
	rec := tangled.Repo{
		Knot:      "evil.example",
		Name:      "myrepo",
		Spindle:   &spindle,
		CreatedAt: "2026-01-01T00:00:00Z",
	}
	evt := commitEvent(1, "did:plc:rando", tangled.RepoNSID, jsOpCreate, "rk", rec)

	fake := &fakeKnotConsumer{}
	if err := handleJetstreamEvent(ctx, s, fake, ours, owner, evt); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if added := fake.Added(); len(added) != 0 {
		t.Fatalf("AddKnot calls = %v, want none (publisher is unauthorized)", added)
	}
}

// TestSpindleMemberGrantSubscribesPendingKnots covers the case where
// a member publishes their sh.tangled.repo *before* the owner's
// sh.tangled.spindle.member grant arrives over the firehose. Until
// the grant lands the publisher is unauthorized, so the repo doesn't
// pull a subscription. Once the grant arrives, the same-rkey reconcile
// must catch up by AddKnot-ing every knot that member's pending repos
// already named.
func TestSpindleMemberGrantSubscribesPendingKnots(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	const ours = "tack.example"
	const owner = "did:plc:owner"
	const alice = "did:plc:alice"

	// Step 1: alice publishes a repo claiming us. She isn't a member
	// yet, so no subscription should happen.
	spindle := ours
	repoRec := tangled.Repo{
		Knot:      "knot.example",
		Name:      "myrepo",
		Spindle:   &spindle,
		CreatedAt: "2026-01-01T00:00:00Z",
	}
	repoEvt := commitEvent(1, alice, tangled.RepoNSID, jsOpCreate, "rk", repoRec)

	fake := &fakeKnotConsumer{}
	if err := handleJetstreamEvent(ctx, s, fake, ours, owner, repoEvt); err != nil {
		t.Fatalf("handle repo: %v", err)
	}
	if added := fake.Added(); len(added) != 0 {
		t.Fatalf("AddKnot before grant = %v, want none", added)
	}

	// Step 2: owner publishes a membership grant naming alice. The
	// reconcile triggered by that grant must subscribe to alice's
	// already-stored repo's knot.
	grant := tangled.SpindleMember{
		Instance:  ours,
		Subject:   alice,
		CreatedAt: "2026-01-02T00:00:00Z",
	}
	grantEvt := commitEvent(2, owner, tangled.SpindleMemberNSID, jsOpCreate, "mk", grant)
	if err := handleJetstreamEvent(ctx, s, fake, ours, owner, grantEvt); err != nil {
		t.Fatalf("handle grant: %v", err)
	}
	if added := fake.Added(); len(added) != 1 || added[0] != "knot.example" {
		t.Fatalf("AddKnot after grant = %v, want [knot.example]", added)
	}
}

// TestSpindleMemberRevokeUnsubscribesKnot verifies that revoking a
// previously-granted membership tears down the only-held-by-that-member
// knot subscription. Without this, an attacker who briefly held
// membership could leave us dialing their chosen knot until restart.
func TestSpindleMemberRevokeUnsubscribesKnot(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	const ours = "tack.example"
	const owner = "did:plc:owner"
	const alice = "did:plc:alice"

	// Seed: owner has previously vouched for alice, and alice has
	// already published a repo on knot.example pointing at us.
	if err := s.UpsertSpindleMember(ctx, owner, "mk", ours, alice, "t"); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertRepo(ctx, alice, "rk", "knot.example", "myrepo", ours, "", "t"); err != nil {
		t.Fatal(err)
	}

	// Owner publishes a delete of the membership grant.
	revoke := commitEvent(10, owner, tangled.SpindleMemberNSID, jsOpDelete, "mk", nil)

	fake := &fakeKnotConsumer{}
	if err := handleJetstreamEvent(ctx, s, fake, ours, owner, revoke); err != nil {
		t.Fatalf("handle revoke: %v", err)
	}
	if removed := fake.Removed(); len(removed) != 1 || removed[0] != "knot.example" {
		t.Fatalf("RemoveKnot calls = %v, want [knot.example]", removed)
	}
}

// TestSpindleMemberGrantFromNonOwnerIgnored confirms that a forged
// membership record published by anyone other than the spindle owner
// does NOT trigger a knot subscription, even if a matching repo for
// the named subject exists. This mirrors AuthorizePipelineActor's
// rule that the membership grant's publisher must equal the spindle
// owner.
func TestSpindleMemberGrantFromNonOwnerIgnored(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	const ours = "tack.example"
	const owner = "did:plc:owner"
	const alice = "did:plc:alice"

	if err := s.UpsertRepo(ctx, alice, "rk", "knot.example", "myrepo", ours, "", "t"); err != nil {
		t.Fatal(err)
	}

	// Forged grant: alice "vouches" for herself. Stored, but must
	// not flip her into authorized status.
	forgery := tangled.SpindleMember{
		Instance:  ours,
		Subject:   alice,
		CreatedAt: "2026-01-02T00:00:00Z",
	}
	evt := commitEvent(2, alice, tangled.SpindleMemberNSID, jsOpCreate, "mk", forgery)

	fake := &fakeKnotConsumer{}
	if err := handleJetstreamEvent(ctx, s, fake, ours, owner, evt); err != nil {
		t.Fatalf("handle forged grant: %v", err)
	}
	if added := fake.Added(); len(added) != 0 {
		t.Fatalf("AddKnot calls = %v, want none (grant publisher != owner)", added)
	}
}

// TestRepoEventIgnoresKnotForOtherSpindle confirms repos pointing at a
// *different* spindle do not pull us into watching their knot. Without
// this guard, tack would dial every knot named in any sh.tangled.repo
// it sees over the firehose, which is most of them.
func TestRepoEventIgnoresKnotForOtherSpindle(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	other := "other-spindle.example"
	rec := tangled.Repo{
		Knot:      "knot.example",
		Name:      "myrepo",
		Spindle:   &other,
		CreatedAt: "2026-01-01T00:00:00Z",
	}
	evt := commitEvent(1, "did:plc:owner", tangled.RepoNSID, jsOpCreate, "rk", rec)

	fake := &fakeKnotConsumer{}
	if err := handleJetstreamEvent(ctx, s, fake, "tack.example", "did:plc:owner", evt); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if added := fake.Added(); len(added) != 0 {
		t.Fatalf("AddKnot calls = %v, want none", added)
	}
}

// TestRepoUpdateSpindleAwayFromUsRemovesKnot covers the case where a
// repo we'd previously been watching gets its .spindle field flipped to
// some other spindle. Once that's the only repo we had on that knot,
// the reconciliation must call RemoveKnot.
func TestRepoUpdateSpindleAwayFromUsRemovesKnot(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	const ours = "tack.example"
	const knot = "knot.example"

	// Seed: a repo that names us as its spindle on `knot`.
	if err := s.UpsertRepo(ctx, "did:plc:a", "rk", knot, "repo-a", ours, "", "t"); err != nil {
		t.Fatal(err)
	}

	// Update: same record, now points at a different spindle.
	other := "other.example"
	rec := tangled.Repo{
		Knot:      knot,
		Name:      "repo-a",
		Spindle:   &other,
		CreatedAt: "2026-01-01T00:00:00Z",
	}
	evt := commitEvent(1, "did:plc:a", tangled.RepoNSID, jsOpUpdate, "rk", rec)

	fake := &fakeKnotConsumer{}
	if err := handleJetstreamEvent(ctx, s, fake, ours, "did:plc:owner", evt); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if added := fake.Added(); len(added) != 0 {
		t.Fatalf("AddKnot calls = %v, want none", added)
	}
	if removed := fake.Removed(); len(removed) != 1 || removed[0] != knot {
		t.Fatalf("RemoveKnot calls = %v, want [%s]", removed, knot)
	}
}

// TestRepoUpdateSpindleAwayFromUsKeepsKnotIfShared ensures we don't
// over-eagerly unsubscribe from a knot when one of multiple repos on
// that knot moves away from us. The other repo's subscription must keep
// the knot in our wanted set.
func TestRepoUpdateSpindleAwayFromUsKeepsKnotIfShared(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	const ours = "tack.example"
	const knot = "knot.example"

	// Both publishers must be vouched for; otherwise IsKnotWanted's
	// membership filter would zero them out and we'd unsubscribe
	// regardless. The reconciliation we want to test only matters
	// when there is a real authorized hold to keep.
	if err := s.UpsertSpindleMember(ctx, "did:plc:owner", "mk1", ours, "did:plc:a", "t"); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertSpindleMember(ctx, "did:plc:owner", "mk2", ours, "did:plc:b", "t"); err != nil {
		t.Fatal(err)
	}

	// Two repos sharing one knot, both pointed at us.
	if err := s.UpsertRepo(ctx, "did:plc:a", "rk1", knot, "a", ours, "", "t"); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertRepo(ctx, "did:plc:b", "rk2", knot, "b", ours, "", "t"); err != nil {
		t.Fatal(err)
	}

	// Repo A flips to a different spindle. B still wants us.
	other := "other.example"
	rec := tangled.Repo{
		Knot:      knot,
		Name:      "a",
		Spindle:   &other,
		CreatedAt: "2026-01-01T00:00:00Z",
	}
	evt := commitEvent(1, "did:plc:a", tangled.RepoNSID, jsOpUpdate, "rk1", rec)

	fake := &fakeKnotConsumer{}
	if err := handleJetstreamEvent(ctx, s, fake, ours, "did:plc:owner", evt); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if removed := fake.Removed(); len(removed) != 0 {
		t.Fatalf("RemoveKnot calls = %v, want none (B still wants us on %s)", removed, knot)
	}
}

// TestRepoUpdateChangingKnotSwapsSubscription verifies that a repo
// staying with us but changing its .knot field unsubscribes the old
// knot (if no other repo holds it) and subscribes the new one.
func TestRepoUpdateChangingKnotSwapsSubscription(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	const ours = "tack.example"
	const oldKnot = "old.example"
	const newKnot = "new.example"

	// Vouch for did:plc:a so reconcileKnot will actually call
	// AddKnot for the new host. Without the grant the publisher
	// is unauthorized and the AddKnot half is (correctly) skipped.
	if err := s.UpsertSpindleMember(ctx, "did:plc:owner", "mk1", ours, "did:plc:a", "t"); err != nil {
		t.Fatal(err)
	}

	if err := s.UpsertRepo(ctx, "did:plc:a", "rk", oldKnot, "a", ours, "", "t"); err != nil {
		t.Fatal(err)
	}

	spindle := ours
	rec := tangled.Repo{
		Knot:      newKnot,
		Name:      "a",
		Spindle:   &spindle,
		CreatedAt: "2026-01-01T00:00:00Z",
	}
	evt := commitEvent(1, "did:plc:a", tangled.RepoNSID, jsOpUpdate, "rk", rec)

	fake := &fakeKnotConsumer{}
	if err := handleJetstreamEvent(ctx, s, fake, ours, "did:plc:owner", evt); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if added := fake.Added(); len(added) != 1 || added[0] != newKnot {
		t.Fatalf("AddKnot calls = %v, want [%s]", added, newKnot)
	}
	if removed := fake.Removed(); len(removed) != 1 || removed[0] != oldKnot {
		t.Fatalf("RemoveKnot calls = %v, want [%s]", removed, oldKnot)
	}
}

// TestRepoDeleteRemovesKnotWhenLast confirms deleting the last repo on
// a knot we cared about triggers RemoveKnot.
func TestRepoDeleteRemovesKnotWhenLast(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	const ours = "tack.example"
	const knot = "knot.example"

	if err := s.UpsertRepo(ctx, "did:plc:a", "rk", knot, "a", ours, "", "t"); err != nil {
		t.Fatal(err)
	}
	evt := commitEvent(1, "did:plc:a", tangled.RepoNSID, jsOpDelete, "rk", nil)

	fake := &fakeKnotConsumer{}
	if err := handleJetstreamEvent(ctx, s, fake, ours, "did:plc:owner", evt); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if removed := fake.Removed(); len(removed) != 1 || removed[0] != knot {
		t.Fatalf("RemoveKnot calls = %v, want [%s]", removed, knot)
	}
}

// TestRepoDeleteKeepsKnotIfShared ensures deleting one of multiple
// repos on a knot does not unsubscribe — the survivors still want it.
func TestRepoDeleteKeepsKnotIfShared(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	const ours = "tack.example"
	const knot = "knot.example"

	// Both publishers vouched for; otherwise IsKnotWanted would
	// (correctly) report neither holds the knot and we'd unsubscribe.
	if err := s.UpsertSpindleMember(ctx, "did:plc:owner", "mk1", ours, "did:plc:a", "t"); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertSpindleMember(ctx, "did:plc:owner", "mk2", ours, "did:plc:b", "t"); err != nil {
		t.Fatal(err)
	}

	if err := s.UpsertRepo(ctx, "did:plc:a", "rk1", knot, "a", ours, "", "t"); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertRepo(ctx, "did:plc:b", "rk2", knot, "b", ours, "", "t"); err != nil {
		t.Fatal(err)
	}
	evt := commitEvent(1, "did:plc:a", tangled.RepoNSID, jsOpDelete, "rk1", nil)

	fake := &fakeKnotConsumer{}
	if err := handleJetstreamEvent(ctx, s, fake, ours, "did:plc:owner", evt); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if removed := fake.Removed(); len(removed) != 0 {
		t.Fatalf("RemoveKnot calls = %v, want none", removed)
	}
}
