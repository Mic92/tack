package main

// Tests for the post-1.13 knot protocol. A knot emits git.refUpdate and Tack
// must authorize the repository before creating and dispatching a pipeline.

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/url"
	"testing"

	"tangled.org/core/api/tangled"
	"tangled.org/core/eventconsumer"
	"tangled.org/core/eventstream"
	knotdb "tangled.org/core/knotserver/db"
	"tangled.org/core/repoident"
	"tangled.org/core/repoverify"
)

// newTestKnotConsumer replaces network verification and workflow loading while
// retaining the real authorization, persistence, and dispatch paths.
func newTestKnotConsumer(st *store, provider Provider) *knotConsumer {
	cfg := config{
		Hostname: "spindle.example",
		OwnerDID: "did:plc:owner",
	}
	service := newPipelineService(context.Background(), cfg, st, newBroker(st), provider)
	service.verify = testRepoVerifier(st)
	service.load = func(
		context.Context,
		*RepoRef,
		string,
		*tangled.Pipeline_TriggerMetadata,
		[]string,
	) ([]*tangled.Pipeline_Workflow, error) {
		return []*tangled.Pipeline_Workflow{
			{Name: "ci.yml", Engine: "tack", Raw: "tack:\n  fake: {}\n"},
		}, nil
	}
	return &knotConsumer{log: slog.Default(), pipelines: service}
}

// testRepoVerifier treats the mirrored test row as the canonical DID result.
func testRepoVerifier(st *store) repoverify.Verifier {
	return func(ctx context.Context, repoDID repoident.RepoDid) (repoverify.Result, error) {
		repo, err := st.RepoByDID(ctx, repoDID.String())
		if err != nil {
			return repoverify.Result{}, err
		}
		if repo == nil {
			knot, _ := url.Parse("https://knot.example")
			return repoverify.Result{
				RepoDid: repoDID, OwnerDid: repoident.OwnerDid("did:plc:owner"), KnotURL: knot,
			}, nil
		}
		knot, _ := url.Parse("https://" + repo.Knot)
		return repoverify.Result{
			RepoDid:  repoDID,
			OwnerDid: repoident.OwnerDid(repo.OwnerDID),
			KnotURL:  knot,
			Rkey:     repo.Rkey,
		}, nil
	}
}

// refUpdateMessage constructs the current knot-side push trigger envelope.
func refUpdateMessage(t *testing.T, repoDID string) eventstream.Event {
	t.Helper()
	record := tangled.GitRefUpdate{
		LexiconTypeID: tangled.GitRefUpdateNSID,
		Repo:          repoDID,
		Ref:           "refs/heads/main",
		OldSha:        "1111111111111111111111111111111111111111",
		NewSha:        "2222222222222222222222222222222222222222",
	}
	body, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("marshal ref update: %v", err)
	}
	return eventstream.Event{Rkey: "ref-1", Nsid: tangled.GitRefUpdateNSID, EventJson: body}
}

// knotSource is the authoritative source expected by the seeded repo records.
func knotSource() eventconsumer.Source {
	return eventconsumer.Source{Kind: eventconsumer.KindKnot, Host: "knot.example"}
}

// TestKnotProcessAuthorized proves an owner-managed repo creates and dispatches
// one spindle-owned pipeline.
func TestKnotProcessAuthorized(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	if err := st.UpsertRepo(
		ctx,
		"did:plc:owner",
		"rk1",
		"knot.example",
		"myrepo",
		"spindle.example",
		"did:plc:repo",
		"t",
	); err != nil {
		t.Fatal(err)
	}

	stub := &stubProvider{}
	kc := newTestKnotConsumer(st, stub)
	if err := kc.process(ctx, knotSource(), refUpdateMessage(t, "did:plc:repo")); err != nil {
		t.Fatalf("process: %v", err)
	}
	if got, want := stub.names(), []string{"ci.yml"}; !equalStrings(got, want) {
		t.Fatalf("Spawn workflows = %v; want %v", got, want)
	}

	rows, err := st.EventsAfter(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Nsid != tangled.PipelineNSID {
		t.Fatalf("pipeline events = %+v; want one %s event", rows, tangled.PipelineNSID)
	}
}

// TestKnotProcessRejectsUnknownRepo ensures global knot traffic cannot force
// verification or provider work for a repo Tack does not serve.
func TestKnotProcessRejectsUnknownRepo(t *testing.T) {
	st := newTestStore(t)
	stub := &stubProvider{}
	kc := newTestKnotConsumer(st, stub)
	if err := kc.process(
		context.Background(), knotSource(), refUpdateMessage(t, "did:plc:unknown"),
	); err != nil {
		t.Fatalf("process: %v", err)
	}
	if got := stub.names(); len(got) != 0 {
		t.Fatalf("Spawn called for unknown repo: %v", got)
	}
}

// TestKnotProcessRejectsNonMember pins spindle membership as a live gate and
// verifies an owner-issued grant takes effect without restarting the consumer.
func TestKnotProcessRejectsNonMember(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	if err := st.UpsertRepo(
		ctx,
		"did:plc:alice",
		"rk1",
		"knot.example",
		"myrepo",
		"spindle.example",
		"did:plc:repo",
		"t",
	); err != nil {
		t.Fatal(err)
	}

	stub := &stubProvider{}
	kc := newTestKnotConsumer(st, stub)
	message := refUpdateMessage(t, "did:plc:repo")
	if err := kc.process(ctx, knotSource(), message); err != nil {
		t.Fatalf("process: %v", err)
	}
	if got := stub.names(); len(got) != 0 {
		t.Fatalf("Spawn called for non-member: %v", got)
	}

	if err := st.UpsertSpindleMember(
		ctx,
		"did:plc:owner",
		"mk1",
		"spindle.example",
		"did:plc:alice",
		"t",
	); err != nil {
		t.Fatal(err)
	}
	if err := kc.process(ctx, knotSource(), message); err != nil {
		t.Fatalf("process after grant: %v", err)
	}
	if got, want := stub.names(), []string{"ci.yml"}; !equalStrings(got, want) {
		t.Fatalf("post-grant Spawn = %v; want %v", got, want)
	}
}

// TestKnotProcessHonorsSkipCI confirms knot push options suppress compilation
// before any provider work is created.
func TestKnotProcessHonorsSkipCI(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	if err := st.UpsertRepo(
		ctx,
		"did:plc:owner",
		"rk1",
		"knot.example",
		"myrepo",
		"spindle.example",
		"did:plc:repo",
		"t",
	); err != nil {
		t.Fatal(err)
	}
	message := refUpdateMessage(t, "did:plc:repo")
	var record tangled.GitRefUpdate
	if err := json.Unmarshal(message.EventJson, &record); err != nil {
		t.Fatal(err)
	}
	record.PushOptions = []string{"skip-ci"}
	message.EventJson, _ = json.Marshal(record)

	stub := &stubProvider{}
	if err := newTestKnotConsumer(st, stub).process(ctx, knotSource(), message); err != nil {
		t.Fatal(err)
	}
	if got := stub.names(); len(got) != 0 {
		t.Fatalf("Spawn called for skip-ci push: %v", got)
	}
}

// TestKnotProcessCollaboratorUpdate covers authoritative knot ACL add/remove
// events and their effect on manual-trigger authorization.
func TestKnotProcessCollaboratorUpdate(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	if err := st.UpsertRepo(
		ctx,
		"did:plc:owner",
		"rk1",
		"knot.example",
		"myrepo",
		"spindle.example",
		"did:plc:repo",
		"t",
	); err != nil {
		t.Fatal(err)
	}
	kc := newTestKnotConsumer(st, &stubProvider{})
	record := knotdb.RepoCollaboratorUpdate{
		Op: knotdb.AclOpAdd, Repo: "did:plc:repo", Subject: "did:plc:alice",
	}
	body, _ := json.Marshal(record)
	message := eventstream.Event{
		Nsid: knotdb.RepoCollaboratorUpdateNSID, EventJson: body,
	}
	if err := kc.process(ctx, knotSource(), message); err != nil {
		t.Fatal(err)
	}
	authorized, err := st.IsRepoActorAuthorized(
		ctx, "did:plc:repo", "did:plc:owner", "did:plc:alice",
	)
	if err != nil {
		t.Fatal(err)
	}
	if !authorized {
		t.Fatal("knot collaborator was not authorized")
	}

	record.Op = knotdb.AclOpRemove
	message.EventJson, _ = json.Marshal(record)
	if err := kc.process(ctx, knotSource(), message); err != nil {
		t.Fatal(err)
	}
	authorized, err = st.IsRepoActorAuthorized(
		ctx, "did:plc:repo", "did:plc:owner", "did:plc:alice",
	)
	if err != nil {
		t.Fatal(err)
	}
	if authorized {
		t.Fatal("removed knot collaborator remains authorized")
	}
}
