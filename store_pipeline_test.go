package main

import (
	"context"
	"encoding/json"
	"testing"

	"tangled.org/core/api/tangled"
)

// TestPipelineQueriesMapStatus covers legacy-record projection, status/timestamp
// aggregation, trigger filtering, and newest-retry-per-commit semantics.
func TestPipelineQueriesMapStatus(t *testing.T) {
	st := newTestStore(t)
	br := newBroker(st)
	ctx := context.Background()
	repoDID := "did:plc:repo"
	repoRkey := "repo"
	pipelineRkey := "3m5x7k2abcde2"
	pipelineURI := pipelineATURI("knot.example", pipelineRkey)

	pipeline := tangled.Pipeline{
		LexiconTypeID: tangled.PipelineNSID,
		TriggerMetadata: &tangled.Pipeline_TriggerMetadata{
			Kind: "push",
			Push: &tangled.Pipeline_PushTriggerData{
				Ref:    "refs/heads/main",
				OldSha: "1111111111111111111111111111111111111111",
				NewSha: "2222222222222222222222222222222222222222",
			},
			Repo: &tangled.Pipeline_TriggerRepo{
				Did:     "did:plc:owner",
				Knot:    "knot.example",
				Repo:    &repoRkey,
				RepoDid: &repoDID,
			},
		},
		Workflows: []*tangled.Pipeline_Workflow{{Name: "ci.yml", Engine: "tack"}},
	}
	publishRecord(t, ctx, br, pipelineRkey, tangled.PipelineNSID, pipeline)
	publishRecord(t, ctx, br, "status-running", tangled.PipelineStatusNSID, tangled.PipelineStatus{
		Pipeline: pipelineURI, Workflow: "ci.yml", Status: "running",
		CreatedAt: "2026-07-22T10:00:00Z",
	})
	publishRecord(t, ctx, br, "status-success", tangled.PipelineStatusNSID, tangled.PipelineStatus{
		Pipeline: pipelineURI, Workflow: "ci.yml", Status: "success",
		CreatedAt: "2026-07-22T10:01:00Z",
	})

	got, err := st.GetPipeline(ctx, pipelineRkey)
	if err != nil {
		t.Fatalf("GetPipeline: %v", err)
	}
	if got.Id != pipelineRkey || got.Repo == nil || *got.Repo != repoDID {
		t.Fatalf("GetPipeline identity = %+v", got)
	}
	if got.Commit != pipeline.TriggerMetadata.Push.NewSha {
		t.Fatalf("commit = %q", got.Commit)
	}
	if len(got.Workflows) != 1 || got.Workflows[0].Status != "success" {
		t.Fatalf("workflows = %+v", got.Workflows)
	}
	if got.Workflows[0].StartedAt == nil || *got.Workflows[0].StartedAt != "2026-07-22T10:00:00Z" {
		t.Fatalf("startedAt = %v", got.Workflows[0].StartedAt)
	}
	if got.Workflows[0].FinishedAt == nil || *got.Workflows[0].FinishedAt != "2026-07-22T10:01:00Z" {
		t.Fatalf("finishedAt = %v", got.Workflows[0].FinishedAt)
	}
	newestRkey := "3m5x7k2abcdf2"
	publishRecord(t, ctx, br, newestRkey, tangled.PipelineNSID, pipeline)

	list, cursor, total, err := st.QueryPipelines(
		ctx, repoDID, []string{got.Commit}, "", []string{"push"}, 30,
	)
	if err != nil {
		t.Fatalf("QueryPipelines: %v", err)
	}
	if len(list) != 1 || list[0].Id != newestRkey || cursor != "" || total != 1 {
		t.Fatalf("QueryPipelines = (%+v, %q, %d)", list, cursor, total)
	}
}

// publishRecord keeps test setup on the same marshal/persist boundary used by
// providers without making each case repeat error handling.
func publishRecord(t *testing.T, ctx context.Context, br *broker, rkey, nsid string, record any) {
	t.Helper()
	encoded, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := br.Publish(ctx, rkey, nsid, encoded); err != nil {
		t.Fatal(err)
	}
}
