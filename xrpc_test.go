package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"tangled.org/core/api/tangled"
)

func TestCiLogEventCBORRoundTrip(t *testing.T) {
	original := ciLogEvent("ci.yml", LogLine{
		Kind:    LogKindData,
		Content: "hello\n",
		Time:    time.Date(2026, 7, 22, 10, 0, 0, 0, time.UTC),
		StepId:  2,
		Stream:  "stderr",
	})
	var encoded bytes.Buffer
	if err := original.Serialize(&encoded); err != nil {
		t.Fatalf("Serialize: %v", err)
	}
	var decoded tangled.CiSubscribePipelineLogs_Event
	if err := decoded.Deserialize(&encoded); err != nil {
		t.Fatalf("Deserialize: %v", err)
	}
	if decoded.Data == nil || decoded.Data.Workflow != "ci.yml" ||
		decoded.Data.Content != "hello\n" || decoded.Data.Stream != "stderr" ||
		decoded.Data.Step != 2 {
		t.Fatalf("decoded event = %+v", decoded.Data)
	}
}

type delayedLogsProvider struct {
	broker *broker
	calls  int
	err    error
}

func (p *delayedLogsProvider) Spawn(
	context.Context,
	string,
	string,
	string,
	*tangled.Pipeline_TriggerMetadata,
	[]*tangled.Pipeline_Workflow,
) {
}

func (p *delayedLogsProvider) Logs(
	ctx context.Context,
	knot, pipeline, workflow string,
) (<-chan LogLine, error) {
	p.calls++
	if p.calls == 1 {
		return nil, ErrLogsNotFound
	}
	record, err := json.Marshal(tangled.PipelineStatus{
		Pipeline:  pipelineATURI(knot, pipeline),
		Workflow:  workflow,
		Status:    "success",
		CreatedAt: "2026-07-22T10:00:00Z",
	})
	if err == nil {
		_, err = p.broker.Publish(ctx, "done", tangled.PipelineStatusNSID, record)
	}
	p.err = err
	lines := make(chan LogLine, 1)
	lines <- LogLine{Kind: LogKindData, Content: "ready\n", Stream: "stdout"}
	close(lines)
	return lines, nil
}

func TestFollowProviderLogsWaitsForProviderMapping(t *testing.T) {
	st := newTestStore(t)
	provider := &delayedLogsProvider{broker: newBroker(st)}
	events := make(chan *tangled.CiSubscribePipelineLogs_Event, 1)
	followProviderLogsWithInterval(
		context.Background(), slog.Default(), st, provider,
		"knot.example", "3m5x7k2abcde2", "ci.yml", events, 5*time.Millisecond,
	)
	if provider.err != nil {
		t.Fatal(provider.err)
	}
	if provider.calls != 2 {
		t.Fatalf("Logs calls = %d; want 2", provider.calls)
	}
	select {
	case event := <-events:
		if event.Data == nil || event.Data.Content != "ready\n" {
			t.Fatalf("event = %+v", event)
		}
	default:
		t.Fatal("no log event emitted")
	}
}

func TestLogPollBackoff(t *testing.T) {
	for _, test := range []struct {
		failures int
		want     time.Duration
	}{
		{1, 15 * time.Second},
		{2, 30 * time.Second},
		{3, time.Minute},
		{4, time.Minute},
	} {
		if got := logPollBackoff(15*time.Second, test.failures, time.Minute); got != test.want {
			t.Fatalf("failures %d: got %s, want %s", test.failures, got, test.want)
		}
	}
}

func TestTriggerPipelineCreatesSpindleOwnedRecord(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	if err := st.UpsertRepo(
		ctx,
		"did:plc:owner",
		"repo",
		"knot.example",
		"repo",
		"spindle.example",
		"did:plc:repo",
		"2026-07-22T10:00:00Z",
	); err != nil {
		t.Fatal(err)
	}

	provider := &stubProvider{}
	service := newPipelineService(
		ctx, config{Hostname: "spindle.example", OwnerDID: "did:plc:owner"},
		st, newBroker(st), provider,
	)
	service.verify = testRepoVerifier(st)
	service.load = func(
		context.Context,
		*RepoRef,
		string,
		*tangled.Pipeline_TriggerMetadata,
		[]string,
	) ([]*tangled.Pipeline_Workflow, error) {
		return []*tangled.Pipeline_Workflow{{
			Name: "ci.yml", Engine: "tack", Raw: "tack:\n  fake: {}\n",
		}}, nil
	}
	service.resolveDefaultBranch = func(context.Context, *RepoRef) string { return "main" }

	uri, err := service.triggerPipeline(ctx, "did:plc:owner", &tangled.CiTriggerPipeline_Input{
		Repo: "did:plc:repo",
		Trigger: &tangled.CiTriggerPipeline_Input_Trigger{
			CiTrigger_Manual: &tangled.CiTrigger_Manual{
				Sha: "2222222222222222222222222222222222222222",
			},
		},
	})
	if err != nil {
		t.Fatalf("triggerPipeline: %v", err)
	}
	if !strings.HasPrefix(uri, "at://did:web:knot.example/sh.tangled.pipeline/") {
		t.Fatalf("pipeline URI = %q", uri)
	}
	if got, want := provider.names(), []string{"ci.yml"}; !equalStrings(got, want) {
		t.Fatalf("Spawn workflows = %v; want %v", got, want)
	}

	rkey := uri[strings.LastIndex(uri, "/")+1:]
	pipeline, err := st.GetPipeline(ctx, rkey)
	if err != nil {
		t.Fatalf("GetPipeline: %v", err)
	}
	if pipeline.Trigger == nil || pipeline.Trigger.CiTrigger_Manual == nil {
		t.Fatalf("trigger = %+v", pipeline.Trigger)
	}
}

func TestPullRecordCreatesPipeline(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	if err := st.UpsertRepo(
		ctx,
		"did:plc:owner",
		"repo",
		"knot.example",
		"repo",
		"spindle.example",
		"did:plc:repo",
		"2026-07-22T10:00:00Z",
	); err != nil {
		t.Fatal(err)
	}

	provider := &stubProvider{}
	service := newPipelineService(
		ctx, config{Hostname: "spindle.example", OwnerDID: "did:plc:owner"},
		st, newBroker(st), provider,
	)
	service.verify = testRepoVerifier(st)
	service.load = func(
		context.Context,
		*RepoRef,
		string,
		*tangled.Pipeline_TriggerMetadata,
		[]string,
	) ([]*tangled.Pipeline_Workflow, error) {
		return []*tangled.Pipeline_Workflow{{Name: "pr.yml", Engine: "tack"}}, nil
	}
	service.fetchPullRevision = func(
		context.Context, string, string, *tangled.RepoPull,
	) (string, error) {
		return "2222222222222222222222222222222222222222", nil
	}

	err := service.processPull(ctx, "did:plc:alice", "pull-1", &tangled.RepoPull{
		Source: &tangled.RepoPull_Source{Branch: "feature"},
		Target: &tangled.RepoPull_Target{Repo: "did:plc:repo", Branch: "main"},
	})
	if err != nil {
		t.Fatalf("processPull: %v", err)
	}
	if got, want := provider.names(), []string{"pr.yml"}; !equalStrings(got, want) {
		t.Fatalf("Spawn workflows = %v; want %v", got, want)
	}
	if err := service.processPull(ctx, "did:plc:alice", "pull-1", &tangled.RepoPull{
		Source: &tangled.RepoPull_Source{Branch: "feature"},
		Target: &tangled.RepoPull_Target{Repo: "did:plc:repo", Branch: "main"},
	}); err != nil {
		t.Fatalf("replayed processPull: %v", err)
	}
	if got := provider.names(); len(got) != 1 {
		t.Fatalf("replayed pull spawned %d workflows; want 1", len(got))
	}

	rows, err := st.EventsAfter(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("events = %d; want 1", len(rows))
	}
	var pipeline tangled.Pipeline
	if err := json.Unmarshal(rows[0].EventJSON, &pipeline); err != nil {
		t.Fatal(err)
	}
	if pipeline.TriggerMetadata == nil || pipeline.TriggerMetadata.PullRequest == nil ||
		pipeline.TriggerMetadata.PullRequest.SourceBranch != "feature" {
		t.Fatalf("pull trigger = %+v", pipeline.TriggerMetadata)
	}
}
