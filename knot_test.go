package main

// Tests for the knotConsumer.process() authorization gate. The knot
// /events stream hands us every pipeline record published on a knot
// we're subscribed to, but only those matching the persisted Tangled
// state should reach a Provider. This file pins both halves: a
// trigger that satisfies AuthorizePipelineActor must reach Spawn,
// and one that doesn't must be silently dropped.

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/url"
	"testing"

	"tangled.org/core/api/tangled"
	"tangled.org/core/eventconsumer"
)

// fakeSource is a minimal eventconsumer.Source: process() only ever
// reads Source.Key(), so a string-keyed stub keeps tests independent
// of the eventconsumer package's URL plumbing.
type fakeSource struct{ key string }

func (f fakeSource) Key() string                                { return f.key }
func (f fakeSource) Url(int64, bool) (*url.URL, error)          { return nil, nil }

// newTestKnotConsumer wires a knotConsumer against the provided
// store/provider with the fixed (hostname, owner) used across the
// tests below. cursors and the underlying *eventconsumer.Consumer
// are intentionally left nil; process() never touches them.
func newTestKnotConsumer(st *store, provider Provider) *knotConsumer {
	return &knotConsumer{
		log:      slog.Default(),
		provider: provider,
		store:    st,
		hostname: "spindle.example",
		ownerDID: "did:plc:owner",
	}
}

// pipelineMessage builds an eventconsumer.Message for a pipeline
// trigger pointing at (repoOwn, repoName) on the named knot. Output
// matches what eventconsumer feeds process() at runtime.
func pipelineMessage(t *testing.T, repoOwn, repoName string) eventconsumer.Message {
	t.Helper()
	repo := repoName
	rec := tangled.Pipeline{
		LexiconTypeID: tangled.PipelineNSID,
		TriggerMetadata: &tangled.Pipeline_TriggerMetadata{
			Kind: "push",
			Repo: &tangled.Pipeline_TriggerRepo{
				Did:  repoOwn,
				Repo: &repo,
			},
		},
		Workflows: []*tangled.Pipeline_Workflow{
			{Name: "ci.yml", Raw: "tack:\n  fake: {}\n"},
		},
	}
	body, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("marshal pipeline: %v", err)
	}
	return eventconsumer.Message{
		Rkey:      "pl-1",
		Nsid:      tangled.PipelineNSID,
		EventJson: body,
	}
}

// TestKnotProcessAuthorized confirms a trigger whose repo opted into
// us on the right knot AND whose actor is the spindle owner reaches
// Spawn with the actor DID populated.
func TestKnotProcessAuthorized(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	// The repo owner is the spindle owner, so no spindle.member
	// row is needed. Repo claims us as its spindle on the right
	// knot, so both gates should pass.
	if err := st.UpsertRepo(ctx,
		"did:plc:owner", "rk1",
		"knot.example", "myrepo",
		"spindle.example", "", "t",
	); err != nil {
		t.Fatal(err)
	}

	stub := &stubProvider{}
	kc := newTestKnotConsumer(st, stub)

	if err := kc.process(ctx,
		fakeSource{key: "knot.example"},
		pipelineMessage(t, "did:plc:owner", "myrepo"),
	); err != nil {
		t.Fatalf("process: %v", err)
	}

	if got, want := stub.names(), []string{"ci.yml"}; !equalStrings(got, want) {
		t.Fatalf("Spawn workflows = %v; want %v", got, want)
	}
}

// TestKnotProcessRejectsUnauthorized covers the bug this commit
// fixes: a knot we're already subscribed to publishes a pipeline
// trigger for a repo that never opted into us. The store has no
// matching repos row, so AuthorizePipelineActor must deny and
// Spawn must NOT be invoked.
func TestKnotProcessRejectsUnauthorized(t *testing.T) {
	st := newTestStore(t)

	// Note: NO repos row inserted. The knot may publish whatever
	// pipeline records it likes, but absent a sh.tangled.repo
	// declaring us the spindle, we must drop the event.
	stub := &stubProvider{}
	kc := newTestKnotConsumer(st, stub)

	if err := kc.process(context.Background(),
		fakeSource{key: "knot.example"},
		pipelineMessage(t, "did:plc:rando", "evilrepo"),
	); err != nil {
		t.Fatalf("process: %v", err)
	}

	if got := stub.names(); len(got) != 0 {
		t.Fatalf("Spawn called for unauthorized trigger: %v", got)
	}
}

// TestKnotProcessRejectsNonMember confirms that even when a repo
// declares us as its spindle, a trigger from a publisher who is not
// the spindle owner and has no owner-vouched membership is dropped.
// This is the gate that makes spindle_members load-bearing.
func TestKnotProcessRejectsNonMember(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	// Repo claim is fine, but the owner of that repo isn't us,
	// and no membership grant has been published.
	if err := st.UpsertRepo(ctx,
		"did:plc:alice", "rk1",
		"knot.example", "myrepo",
		"spindle.example", "", "t",
	); err != nil {
		t.Fatal(err)
	}

	stub := &stubProvider{}
	kc := newTestKnotConsumer(st, stub)

	if err := kc.process(ctx,
		fakeSource{key: "knot.example"},
		pipelineMessage(t, "did:plc:alice", "myrepo"),
	); err != nil {
		t.Fatalf("process: %v", err)
	}

	if got := stub.names(); len(got) != 0 {
		t.Fatalf("Spawn called for non-member: %v", got)
	}

	// Now grant alice membership and re-run; this time it must
	// pass. Verifies the gate is actually consulting the live
	// store rather than caching a denial.
	if err := st.UpsertSpindleMember(ctx,
		"did:plc:owner", "mk1", "spindle.example", "did:plc:alice", "t",
	); err != nil {
		t.Fatal(err)
	}
	if err := kc.process(ctx,
		fakeSource{key: "knot.example"},
		pipelineMessage(t, "did:plc:alice", "myrepo"),
	); err != nil {
		t.Fatalf("process (after grant): %v", err)
	}
	if got, want := stub.names(), []string{"ci.yml"}; !equalStrings(got, want) {
		t.Fatalf("post-grant Spawn = %v; want %v", got, want)
	}
}
