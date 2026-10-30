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

// TestCiLogEventCBORRoundTrip verifies our provider-neutral log conversion is
// consumable by Tangled's generated subscription decoder.
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

// delayedLogsProvider models the real startup race where a pipeline exists
// before an asynchronous provider has persisted its run-to-log mapping.
type delayedLogsProvider struct {
	broker *broker
	calls  int
	err    error
}

var _ Provider = (*delayedLogsProvider)(nil)

// Spawn is unused by log-follower tests.
func (p *delayedLogsProvider) Spawn(
	context.Context,
	string,
	string,
	string,
	*tangled.Pipeline_TriggerMetadata,
	[]*tangled.Pipeline_Workflow,
) {
}

// Logs fails its first lookup, then publishes a terminal status and returns one
// line. That is the shortest faithful model of asynchronous provider startup.
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

// TestFollowProviderLogsWaitsForProviderMapping pins that ErrLogsNotFound is retried while the
// workflow status is not terminal, instead of closing the log subscription. Providers record the
// mapping from a pipeline to its external run asynchronously after Spawn, so the first lookup can
// find no logs yet. The test also checks that the line reaches the caller with its kind and
// content intact.
func TestFollowProviderLogsWaitsForProviderMapping(t *testing.T) {
	st := newTestStore(t)
	provider := &delayedLogsProvider{broker: newBroker(st)}
	events := make(chan workflowLogLine, 1)
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
		if event.line.Kind != LogKindData || event.line.Content != "ready\n" {
			t.Fatalf("event = %+v", event)
		}
	default:
		t.Fatal("no log event emitted")
	}
}

// TestLogPollBackoff pins the rate-limit-friendly capped delay sequence.
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

// TestTriggerPipelineCreatesSpindleOwnedRecord covers authenticated manual
// trigger conversion, durable pipeline creation, and provider dispatch.
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

// TestPullRecordCreatesPipeline covers branch-based pull conversion and proves
// replaying the same pull/SHA cannot create a duplicate provider run.
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

// TestOrgLogEventCarriesStepOutcome pins how orgLogEvent builds current (Tangled v1.20) log
// frames. End markers carry the step outcome because the Tangled web app uses it to mark the failed
// step. When no step reports an outcome and the workflow ends without succeeding, the web app marks
// the last step as failed instead. Start markers never carry an outcome, even when the provider
// set one. Data lines with an unknown stream fall back to stdout, the same default the legacy
// frames use.
func TestOrgLogEventCarriesStepOutcome(t *testing.T) {
	end := LogLine{
		Kind: LogKindControl, StepStatus: StepStatusEnd, StepOutcome: StepOutcomeFailed,
		Content: "test", StepId: 1, Time: time.Date(2026, 10, 30, 10, 0, 0, 0, time.UTC),
	}
	event := orgLogEvent("ci.yml", end)
	if event.Control == nil || pointerValue(event.Control.Outcome) != "failed" ||
		pointerValue(event.Control.Status) != "end" || event.Control.Time != "2026-10-30T10:00:00Z" {
		t.Fatalf("end marker = %+v", event.Control)
	}
	// start keeps the failed outcome, so a nil Outcome proves orgLogEvent drops it.
	start := end
	start.StepStatus = StepStatusStart
	if event := orgLogEvent("ci.yml", start); event.Control.Outcome != nil {
		t.Fatalf("start marker carries outcome %q", *event.Control.Outcome)
	}
	data := orgLogEvent("ci.yml", LogLine{Kind: LogKindData, Content: "x\n", Stream: "bogus"})
	if data.Data == nil || data.Data.Stream != "stdout" {
		t.Fatalf("data = %+v", data.Data)
	}
}
