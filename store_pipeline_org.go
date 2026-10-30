package main

// This file serves stored pipelines in the current (Tangled v1.20) org.tangled.ci.defs shapes.
//
// Tack still stores each pipeline as the legacy sh.tangled.pipeline record, because that is what
// Tangled's workflow compiler produces and what Tack's providers consume. Converting at read time
// means every tack.db row, including rows written before the current protocol existed, is served
// in the current shape without a migration. No stored data is rewritten.

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"tangled.org/core/api/tangled"
	"tangled.org/core/workflow"

	"go.mitchellh.com/tack/internal/orgtangled"
)

// triggerKindByNSID maps the trigger NSIDs a queryPipelines caller filters by to the
// triggerMetadata.kind values stored in legacy pipeline records. The workflow package Tack
// builds against has no constant for schedules, so that kind is spelled out.
var triggerKindByNSID = map[string]string{
	orgtangled.NSIDPushEvent:        string(workflow.TriggerKindPush),
	orgtangled.NSIDPullRequestEvent: string(workflow.TriggerKindPullRequest),
	orgtangled.NSIDManualTrigger:    string(workflow.TriggerKindManual),
	orgtangled.NSIDScheduleTrigger:  "schedule",
}

// triggerNSIDByEvent is the inverse of triggerKindByNSID. It maps the event names a workflow
// lists under `when.event` to the trigger NSIDs reported in a workflow definition.
var triggerNSIDByEvent = map[string]string{
	string(workflow.TriggerKindPush):        orgtangled.NSIDPushEvent,
	string(workflow.TriggerKindPullRequest): orgtangled.NSIDPullRequestEvent,
	string(workflow.TriggerKindManual):      orgtangled.NSIDManualTrigger,
	"schedule":                              orgtangled.NSIDScheduleTrigger,
}

// orgWorkflowStatuses is the closed set of workflow statuses the current protocol allows. The
// Tangled web app looks up status icons, labels, and counts in tables keyed by status, so a value
// outside this set breaks rendering. mapOrgPipeline reports any other value as pending.
var orgWorkflowStatuses = map[string]struct{}{
	"pending": {}, "blocked": {}, "running": {}, "failed": {},
	"timeout": {}, "cancelled": {}, "success": {},
}

// orgTriggerKindFilter converts the trigger NSIDs from a queryPipelines request into stored kind
// values, one output per input. The trigger union is open, so newer clients may ask for triggers
// Tack cannot produce. Like the upstream spindle, an unknown NSID becomes a NUL-prefixed value
// that no stored kind can equal. It matches nothing instead of failing the request, and a filter
// made only of unknown NSIDs returns an empty list rather than every pipeline.
func orgTriggerKindFilter(triggers []string) []string {
	kinds := make([]string, 0, len(triggers))
	for _, trigger := range triggers {
		if kind, ok := triggerKindByNSID[trigger]; ok {
			kinds = append(kinds, kind)
		} else {
			kinds = append(kinds, "\x00unknown")
		}
	}
	return kinds
}

// QueryOrgPipelines implements org.tangled.ci.queryPipelines for one repository DID. It returns
// up to limit pipelines newest first, the cursor for the next page, and the total match count.
//
// Unlike the legacy [store.QueryPipelines], it does not collapse a commit's pipelines into one.
// The total counts every match for the repo, commits, and triggers filters and ignores the
// cursor, because the Tangled web app derives its page count from it. The cursor is the created
// timestamp (Unix nanoseconds, in decimal) of the last pipeline returned. A full page always
// yields a cursor, so the final page may be empty.
func (s *store) QueryOrgPipelines(
	ctx context.Context,
	repoDID string,
	commits, triggers []string,
	cursor *int64,
	limit int,
) ([]*orgtangled.Pipeline, *string, int64, error) {
	query := `
		SELECT rkey, event_json, created
		FROM events
		WHERE nsid = ?
		  AND COALESCE(
			json_extract(event_json, '$.triggerMetadata.repo.repoDid'),
			json_extract(event_json, '$.triggerMetadata.repo.did')
		  ) = ?`
	args := []any{tangled.PipelineNSID, repoDID}
	if len(commits) > 0 {
		query += ` AND COALESCE(
			json_extract(event_json, '$.triggerMetadata.push.newSha'),
			json_extract(event_json, '$.triggerMetadata.pullRequest.sourceSha'),
			json_extract(event_json, '$.triggerMetadata.manual.sha')
		) IN (` + sqlPlaceholders(len(commits)) + `)`
		for _, commit := range commits {
			args = append(args, commit)
		}
	}
	if len(triggers) > 0 {
		kinds := orgTriggerKindFilter(triggers)
		query += ` AND json_extract(event_json, '$.triggerMetadata.kind') IN (` +
			sqlPlaceholders(len(kinds)) + `)`
		for _, kind := range kinds {
			args = append(args, kind)
		}
	}

	var total int64
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM (`+query+`)`, args...).Scan(&total); err != nil {
		return nil, nil, 0, fmt.Errorf("count pipelines: %w", err)
	}
	if cursor != nil {
		query += ` AND created < ?`
		args = append(args, *cursor)
	}
	stored, err := s.queryStoredPipelines(ctx, query+` ORDER BY created DESC LIMIT ?`, append(args, limit)...)
	if err != nil {
		return nil, nil, 0, err
	}

	pipelines := make([]*orgtangled.Pipeline, 0, len(stored))
	for i := range stored {
		pipeline, err := s.mapOrgPipeline(ctx, stored[i].rkey, stored[i].created, &stored[i].raw)
		if err != nil {
			return nil, nil, 0, err
		}
		pipelines = append(pipelines, pipeline)
	}
	var next *string
	if len(stored) == limit {
		value := strconv.FormatInt(stored[len(stored)-1].created, 10)
		next = &value
	}
	return pipelines, next, total, nil
}

// storedPipeline is one pipeline row, decoded and held in memory until its workflow statuses
// are loaded. created is the insertion time in Unix nanoseconds.
type storedPipeline struct {
	rkey    string
	raw     tangled.Pipeline
	created int64
}

// queryStoredPipelines runs a query that selects rkey, event_json, and created from pipeline
// rows and decodes every row before returning. Buffering is required: the store holds a single
// SQLite connection, so the status lookups that follow would block while the rows were open.
func (s *store) queryStoredPipelines(ctx context.Context, query string, args ...any) ([]storedPipeline, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query pipelines: %w", err)
	}
	defer rows.Close()
	var stored []storedPipeline
	for rows.Next() {
		var row storedPipeline
		var rawJSON []byte
		if err := rows.Scan(&row.rkey, &rawJSON, &row.created); err != nil {
			return nil, fmt.Errorf("scan pipeline: %w", err)
		}
		if err := json.Unmarshal(rawJSON, &row.raw); err != nil {
			return nil, fmt.Errorf("decode pipeline %q: %w", row.rkey, err)
		}
		stored = append(stored, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pipelines: %w", err)
	}
	return stored, nil
}

// GetOrgPipeline returns the pipeline with record key rkey in the org.tangled.ci.defs shape. A
// missing pipeline returns sql.ErrNoRows. It does not check the repository, so callers compare
// the result's Repo against the requested one.
func (s *store) GetOrgPipeline(ctx context.Context, rkey string) (*orgtangled.Pipeline, error) {
	raw, created, err := s.GetRawPipeline(ctx, rkey)
	if err != nil {
		return nil, err
	}
	return s.mapOrgPipeline(ctx, rkey, created, raw)
}

// pipelineRepoDID returns the DID of the repository a stored pipeline belongs to, or "" when the
// record names no repository. It prefers the repository's own DID and falls back to the owner DID
// that older records carry instead. Handlers compare this value with the requested repository to
// keep one repository from reading or acting on another's pipelines.
func pipelineRepoDID(raw *tangled.Pipeline) string {
	if raw.TriggerMetadata == nil || raw.TriggerMetadata.Repo == nil {
		return ""
	}
	if repoDID := raw.TriggerMetadata.Repo.RepoDid; repoDID != nil && *repoDID != "" {
		return *repoDID
	}
	return raw.TriggerMetadata.Repo.Did
}

// mapOrgPipeline converts one stored pipeline, plus the latest status of each workflow, into the
// org.tangled.ci.defs#pipeline shape. created is the row's insertion time in Unix nanoseconds.
// Workflows without a name are skipped. An error means a status lookup failed.
func (s *store) mapOrgPipeline(
	ctx context.Context,
	rkey string,
	created int64,
	raw *tangled.Pipeline,
) (*orgtangled.Pipeline, error) {
	createdAt := time.Unix(0, created).UTC().Format(time.RFC3339)
	repoDID := pipelineRepoDID(raw)
	result := &orgtangled.Pipeline{
		ID:        rkey,
		Repo:      repoDID,
		CreatedAt: &createdAt,
		Workflows: make([]*orgtangled.WorkflowRun, 0, len(raw.Workflows)),
	}

	var knot string
	if metadata := raw.TriggerMetadata; metadata != nil {
		result.SourceRepo = metadata.SourceRepo
		if metadata.Repo != nil {
			knot = metadata.Repo.Knot
		}
		result.Trigger, result.Commit = orgTrigger(metadata, repoDID)
	}

	// Workflow files are read at the commit the pipeline ran. For a pull request from a fork, that
	// commit lives in the source repository, so the definition points there.
	sourceRepo := repoDID
	if result.SourceRepo != nil && *result.SourceRepo != "" {
		sourceRepo = *result.SourceRepo
	}
	for _, wf := range raw.Workflows {
		if wf == nil || wf.Name == "" {
			continue
		}
		status, startedAt, finishedAt, statusErr, err := s.workflowStatus(ctx, knot, rkey, wf.Name)
		if err != nil {
			return nil, fmt.Errorf("load status for pipeline %q workflow %q: %w", rkey, wf.Name, err)
		}
		// Stored statuses come from providers and older records, so clamp them to the closed enum.
		if _, ok := orgWorkflowStatuses[status]; !ok {
			status = "pending"
		}
		name := wf.Name
		result.Workflows = append(result.Workflows, &orgtangled.WorkflowRun{
			// The Tangled web app keys runs, URLs, and log frames by id, and Tangled's SSH log
			// viewer (cilogs) keys logs by name. The upstream spindle uses the workflow file name for
			// both, so Tack does too.
			ID:         wf.Name,
			Name:       &name,
			Status:     status,
			StartedAt:  startedAt,
			FinishedAt: finishedAt,
			Error:      statusErr,
			Definition: &orgtangled.Workflow{
				ID:   wf.Name,
				Name: &name,
				Source: &orgtangled.WorkflowSource{File: &orgtangled.FileSource{
					Repo:   sourceRepo,
					Commit: result.Commit,
					Path:   workflow.WorkflowDir + "/" + wf.Name,
				}},
				Triggers: workflowTriggerNSIDs(wf),
			},
		})
	}
	return result, nil
}

// orgTrigger converts stored trigger metadata into the current open trigger union and returns
// the commit the pipeline ran. repoDID fills the repo field that push and manual triggers carry.
// An unknown kind, or a known kind whose details are missing, yields a nil trigger and an empty
// commit rather than invented metadata.
func orgTrigger(
	metadata *tangled.Pipeline_TriggerMetadata,
	repoDID string,
) (*orgtangled.Trigger, string) {
	switch workflow.TriggerKind(metadata.Kind) {
	case workflow.TriggerKindPush:
		if value := metadata.Push; value != nil {
			return &orgtangled.Trigger{Push: &orgtangled.PushEvent{
				Repo:   repoDID,
				Ref:    value.Ref,
				OldSHA: value.OldSha,
				NewSHA: value.NewSha,
			}}, value.NewSha
		}
	case workflow.TriggerKindPullRequest:
		if value := metadata.PullRequest; value != nil {
			sha := value.SourceSha
			targetBranch := value.TargetBranch
			trigger := &orgtangled.PullRequestEvent{
				Pull:         value.Pull,
				SourceRepo:   metadata.SourceRepo,
				SourceSHA:    &sha,
				TargetBranch: &targetBranch,
			}
			if value.SourceBranch != "" {
				sourceBranch := value.SourceBranch
				trigger.SourceBranch = &sourceBranch
			}
			return &orgtangled.Trigger{PullRequest: trigger}, value.SourceSha
		}
	case workflow.TriggerKindManual:
		if value := metadata.Manual; value != nil {
			manual := &orgtangled.ManualTrigger{Repo: repoDID, Commit: value.Sha}
			if value.Ref != nil && *value.Ref != "" {
				manual.Ref = value.Ref
			}
			for _, pair := range value.Inputs {
				if pair != nil {
					manual.Inputs = append(manual.Inputs, &orgtangled.ManualInput{
						Key: pair.Key, Value: pair.Value,
					})
				}
			}
			return &orgtangled.Trigger{Manual: manual}, value.Sha
		}
	}
	return nil, ""
}

// workflowTriggerNSIDs lists, in order of first appearance and without duplicates, the trigger
// NSIDs a stored workflow declares in its `when` block. Event names with no known NSID are
// dropped. The lexicon requires the field, so YAML that fails to parse yields an empty list,
// which encodes as [] rather than null.
func workflowTriggerNSIDs(wf *tangled.Pipeline_Workflow) []string {
	triggers := make([]string, 0, 2)
	parsed, err := workflow.FromFile(wf.Name, []byte(wf.Raw))
	if err != nil {
		return triggers
	}
	seen := make(map[string]struct{})
	for _, constraint := range parsed.When {
		for _, event := range constraint.Event {
			nsid, ok := triggerNSIDByEvent[event]
			if !ok {
				continue
			}
			if _, dup := seen[nsid]; !dup {
				seen[nsid] = struct{}{}
				triggers = append(triggers, nsid)
			}
		}
	}
	return triggers
}
