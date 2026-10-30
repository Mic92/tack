package main

// Pipeline queries over the events table: the legacy sh.tangled.ci reads, plus the lookups that
// push deduplication and the current org.tangled.ci reads depend on.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"tangled.org/core/api/tangled"
	"tangled.org/core/workflow"
)

// QueryPipelines returns newest-first pipeline summaries for one repository.
// The filtering and cursor semantics mirror the stock Spindle implementation.
func (s *store) QueryPipelines(
	ctx context.Context,
	repoDID string,
	commits []string,
	cursor string,
	kinds []string,
	limit int,
) ([]*tangled.CiPipeline, string, int64, error) {
	if limit <= 0 {
		limit = 30
	}
	if limit > 250 {
		limit = 250
	}

	// Build one parameterized base query, then wrap it for the commit-specific
	// deduplication contract below. RepoDid is preferred, with Did retained only
	// for pipeline records created before stable repository DIDs were universal.
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
		// Commit identity lives in a different union branch for each trigger kind.
		// COALESCE gives filtering and later partitioning one common SQL key.
		query += ` AND COALESCE(
			json_extract(event_json, '$.triggerMetadata.push.newSha'),
			json_extract(event_json, '$.triggerMetadata.pullRequest.sourceSha'),
			json_extract(event_json, '$.triggerMetadata.manual.sha')
		) IN (` + sqlPlaceholders(len(commits)) + `)`
		for _, commit := range commits {
			args = append(args, commit)
		}
	}
	if len(kinds) > 0 {
		query += ` AND json_extract(event_json, '$.triggerMetadata.kind') IN (` +
			sqlPlaceholders(len(kinds)) + `)`
		for _, kind := range kinds {
			args = append(args, kind)
		}
	}
	var cursorValue *int64
	if cursor != "" {
		value, err := strconv.ParseInt(cursor, 10, 64)
		if err != nil {
			return nil, "", 0, fmt.Errorf("invalid cursor %q: %w", cursor, err)
		}
		cursorValue = &value
		if len(commits) == 0 {
			query += ` AND created < ?`
			args = append(args, value)
		}
	}
	if len(commits) > 0 {
		// The query contract permits at most one pipeline per requested
		// commit. Keep the newest retry so callers that index by SHA do not
		// accidentally replace it with an older run.
		query = `
			SELECT rkey, event_json, created
			FROM (
				SELECT rkey, event_json, created,
					ROW_NUMBER() OVER (
						PARTITION BY COALESCE(
							json_extract(event_json, '$.triggerMetadata.push.newSha'),
							json_extract(event_json, '$.triggerMetadata.pullRequest.sourceSha'),
							json_extract(event_json, '$.triggerMetadata.manual.sha')
						)
						ORDER BY created DESC
					) AS commit_rank
				FROM (` + query + `)
			)
			WHERE commit_rank = 1`
		if cursorValue != nil {
			query += ` AND created < ?`
			args = append(args, *cursorValue)
		}
	}

	var total int64
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM (`+query+`)`, args...).Scan(&total); err != nil {
		return nil, "", 0, fmt.Errorf("count pipelines: %w", err)
	}

	rows, err := s.db.QueryContext(ctx, query+` ORDER BY created DESC LIMIT ?`, append(args, limit)...)
	if err != nil {
		return nil, "", 0, fmt.Errorf("query pipelines: %w", err)
	}
	defer rows.Close()
	// Buffer raw rows before status projection. store deliberately permits only
	// one SQLite connection; querying statuses while rows is open would deadlock
	// waiting for that same connection.
	type storedPipeline struct {
		rkey    string
		raw     tangled.Pipeline
		created int64
	}
	stored := make([]storedPipeline, 0, limit)
	for rows.Next() {
		var rkey string
		var rawJSON []byte
		var created int64
		if err := rows.Scan(&rkey, &rawJSON, &created); err != nil {
			return nil, "", 0, fmt.Errorf("scan pipeline: %w", err)
		}
		var raw tangled.Pipeline
		if err := json.Unmarshal(rawJSON, &raw); err != nil {
			return nil, "", 0, fmt.Errorf("decode pipeline %q: %w", rkey, err)
		}
		stored = append(stored, storedPipeline{rkey: rkey, raw: raw, created: created})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, "", 0, fmt.Errorf("iterate pipelines: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, "", 0, fmt.Errorf("close pipeline rows: %w", err)
	}

	// mapPipeline performs status lookups. Close the outer rows first because
	// Tack intentionally uses one SQLite connection to serialize writers.
	pipelines := make([]*tangled.CiPipeline, 0, len(stored))
	var lastCreated int64
	for i := range stored {
		pipeline, err := s.mapPipeline(ctx, stored[i].rkey, stored[i].created, &stored[i].raw)
		if err != nil {
			return nil, "", 0, err
		}
		pipelines = append(pipelines, pipeline)
		lastCreated = stored[i].created
	}

	nextCursor := ""
	if len(pipelines) == limit {
		nextCursor = strconv.FormatInt(lastCreated, 10)
	}
	return pipelines, nextCursor, total, nil
}

// HasPullPipeline makes live repo.pull processing idempotent across
// Jetstream's replay window and harmless metadata-only pull updates.
func (s *store) HasPullPipeline(ctx context.Context, pullURI, sourceSHA string) (bool, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM events
		WHERE nsid = ?
		  AND json_extract(event_json, '$.triggerMetadata.pullRequest.pull') = ?
		  AND json_extract(event_json, '$.triggerMetadata.pullRequest.sourceSha') = ?`,
		tangled.PipelineNSID, pullURI, sourceSHA,
	).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("check pull pipeline: %w", err)
	}
	return count > 0, nil
}

// PipelineForPush returns the record key of the newest pipeline already created for the push of
// ref from oldSHA to newSHA in repoDID, or "" when there is none. It is the durable half of push
// deduplication (the in-memory half is pipelineService.coalescePush). knot2 retries any delivery it
// does not count as answered (see the notify.go file comment), signing every attempt with a fresh
// service auth token. The same push can therefore arrive more than once,
// and neither the request nor its token identifies it. The push is identified by its content
// instead: repository, ref, old SHA, and new SHA.
func (s *store) PipelineForPush(ctx context.Context, repoDID, ref, oldSHA, newSHA string) (string, error) {
	var rkey string
	err := s.db.QueryRowContext(ctx, `
		SELECT rkey
		FROM events
		WHERE nsid = ?
		  AND json_extract(event_json, '$.triggerMetadata.kind') = 'push'
		  AND json_extract(event_json, '$.triggerMetadata.repo.repoDid') = ?
		  AND json_extract(event_json, '$.triggerMetadata.push.ref') = ?
		  AND json_extract(event_json, '$.triggerMetadata.push.oldSha') = ?
		  AND json_extract(event_json, '$.triggerMetadata.push.newSha') = ?
		ORDER BY created DESC
		LIMIT 1`,
		tangled.PipelineNSID, repoDID, ref, oldSHA, newSHA,
	).Scan(&rkey)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("check push pipeline: %w", err)
	}
	return rkey, nil
}

// HasRepoPipelines reports whether any stored pipeline belongs to repoDID. It matches repositories
// the same way QueryPipelines and QueryOrgPipelines do: by the trigger's repo.repoDid field, or by
// repo.did for records written before stable repository DIDs. Retained pipeline history shows
// Tack has served the repository, which is enough for isRepoIndexed to answer without its later
// checks, the last of which is a network lookup of the repo manifest on the repository's knot.
func (s *store) HasRepoPipelines(ctx context.Context, repoDID string) (bool, error) {
	var exists int
	err := s.db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM events
			WHERE nsid = ?
			  AND COALESCE(
				json_extract(event_json, '$.triggerMetadata.repo.repoDid'),
				json_extract(event_json, '$.triggerMetadata.repo.did')
			  ) = ?
		)`, tangled.PipelineNSID, repoDID,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check repo pipelines: %w", err)
	}
	return exists == 1, nil
}

// sqlPlaceholders returns a comma-separated placeholder list for a non-empty
// filter. Callers append values separately, so user input never enters SQL text.
func sqlPlaceholders(count int) string {
	return strings.TrimSuffix(strings.Repeat("?,", count), ",")
}

// GetPipeline returns one spindle-local pipeline summary by TID.
func (s *store) GetPipeline(ctx context.Context, rkey string) (*tangled.CiPipeline, error) {
	raw, created, err := s.GetRawPipeline(ctx, rkey)
	if err != nil {
		return nil, err
	}
	return s.mapPipeline(ctx, rkey, created, raw)
}

// GetRawPipeline returns the retained legacy record used internally to launch
// providers. The public API maps it to sh.tangled.ci.pipeline.
func (s *store) GetRawPipeline(ctx context.Context, rkey string) (*tangled.Pipeline, int64, error) {
	var rawJSON []byte
	var created int64
	err := s.db.QueryRowContext(ctx, `
		SELECT event_json, created
		FROM events
		WHERE nsid = ? AND rkey = ?
		LIMIT 1`, tangled.PipelineNSID, rkey,
	).Scan(&rawJSON, &created)
	if err != nil {
		return nil, 0, err
	}
	var raw tangled.Pipeline
	if err := json.Unmarshal(rawJSON, &raw); err != nil {
		return nil, 0, fmt.Errorf("decode pipeline %q: %w", rkey, err)
	}
	return &raw, created, nil
}

// mapPipeline projects the internal sh.tangled.pipeline record and retained
// workflow statuses into the spindle-owned sh.tangled.ci.pipeline response.
// Keeping the legacy record internally avoids duplicating Tangled's compiler
// and provider trigger types at write time.
func (s *store) mapPipeline(
	ctx context.Context,
	rkey string,
	created int64,
	raw *tangled.Pipeline,
) (*tangled.CiPipeline, error) {
	createdAt := time.Unix(0, created).UTC().Format(time.RFC3339)
	result := &tangled.CiPipeline{
		Id:        rkey,
		CreatedAt: &createdAt,
	}

	var knot string
	if raw.TriggerMetadata != nil {
		result.SourceRepo = raw.TriggerMetadata.SourceRepo
		if repo := raw.TriggerMetadata.Repo; repo != nil {
			knot = repo.Knot
			repoDID := repo.Did
			if repo.RepoDid != nil {
				repoDID = *repo.RepoDid
			}
			if repoDID != "" {
				result.Repo = &repoDID
			}
		}
		result.Trigger, result.Commit = ciTrigger(raw.TriggerMetadata)
	}

	for _, wf := range raw.Workflows {
		if wf == nil || wf.Name == "" {
			continue
		}
		status, startedAt, finishedAt, statusErr, err := s.workflowStatus(
			ctx, knot, rkey, wf.Name,
		)
		if err != nil {
			return nil, fmt.Errorf("load status for pipeline %q workflow %q: %w", rkey, wf.Name, err)
		}
		result.Workflows = append(result.Workflows, &tangled.CiPipeline_Workflow{
			Id:         wf.Name,
			Name:       wf.Name,
			Status:     status,
			StartedAt:  startedAt,
			FinishedAt: finishedAt,
			Error:      statusErr,
		})
	}
	return result, nil
}

// ciTrigger converts legacy Pipeline_TriggerMetadata into the public CI trigger
// union and returns the commit SHA used by CiPipeline.Commit. Unknown or
// structurally incomplete trigger variants produce a nil trigger and empty SHA
// rather than inventing metadata.
func ciTrigger(metadata *tangled.Pipeline_TriggerMetadata) (*tangled.CiPipeline_Trigger, string) {
	trigger := &tangled.CiPipeline_Trigger{}
	switch workflow.TriggerKind(metadata.Kind) {
	case workflow.TriggerKindPush:
		if value := metadata.Push; value != nil {
			trigger.CiTrigger_Push = &tangled.CiTrigger_Push{
				NewSha: value.NewSha,
				OldSha: value.OldSha,
				Ref:    value.Ref,
			}
			return trigger, value.NewSha
		}
	case workflow.TriggerKindPullRequest:
		if value := metadata.PullRequest; value != nil {
			sourceBranch := value.SourceBranch
			trigger.CiTrigger_PullRequest = &tangled.CiTrigger_PullRequest{
				Pull:         value.Pull,
				SourceBranch: &sourceBranch,
				SourceRepo:   metadata.SourceRepo,
				SourceSha:    value.SourceSha,
				TargetBranch: value.TargetBranch,
			}
			return trigger, value.SourceSha
		}
	case workflow.TriggerKindManual:
		if value := metadata.Manual; value != nil {
			inputs := make([]*tangled.CiTrigger_Pair, 0, len(value.Inputs))
			for _, pair := range value.Inputs {
				if pair != nil {
					inputs = append(inputs, &tangled.CiTrigger_Pair{Key: pair.Key, Value: pair.Value})
				}
			}
			trigger.CiTrigger_Manual = &tangled.CiTrigger_Manual{
				Inputs:     inputs,
				Ref:        value.Ref,
				Sha:        value.Sha,
				SourceRepo: metadata.SourceRepo,
			}
			return trigger, value.Sha
		}
	}
	return nil, ""
}

// workflowStatus returns the latest status/error plus the first running and
// last terminal timestamps for one workflow. Missing status rows mean pending:
// a pipeline is persisted before asynchronous provider setup publishes its
// first explicit status.
func (s *store) workflowStatus(
	ctx context.Context,
	knot, pipelineRkey, workflowName string,
) (string, *string, *string, *string, error) {
	status := "pending"
	if knot == "" {
		return status, nil, nil, nil, nil
	}
	pipelineURI := pipelineATURI(knot, pipelineRkey)

	var rawJSON []byte
	// Status is an append-only event stream. Ordering by the monotonic created
	// value gives the current state without maintaining a second mutable table.
	err := s.db.QueryRowContext(ctx, `
		SELECT event_json
		FROM events
		WHERE nsid = ?
		  AND json_extract(event_json, '$.pipeline') = ?
		  AND json_extract(event_json, '$.workflow') = ?
		ORDER BY created DESC
		LIMIT 1`, tangled.PipelineStatusNSID, pipelineURI, workflowName,
	).Scan(&rawJSON)
	var statusErr *string
	if err == nil {
		var record tangled.PipelineStatus
		if err := json.Unmarshal(rawJSON, &record); err != nil {
			return "", nil, nil, nil, err
		}
		status = record.Status
		statusErr = record.Error
	} else if !errors.Is(err, sql.ErrNoRows) {
		return "", nil, nil, nil, err
	}

	// Timestamps come from record payloads, not insertion time: provider webhook
	// delivery may lag the actual transition we want to display.
	var started, finished sql.NullString
	err = s.db.QueryRowContext(ctx, `
		SELECT
			MIN(CASE
				WHEN json_extract(event_json, '$.status') = 'running'
				THEN json_extract(event_json, '$.createdAt')
			END),
			MAX(CASE
				WHEN json_extract(event_json, '$.status') IN ('success', 'failed', 'timeout', 'cancelled')
				THEN json_extract(event_json, '$.createdAt')
			END)
		FROM events
		WHERE nsid = ?
		  AND json_extract(event_json, '$.pipeline') = ?
		  AND json_extract(event_json, '$.workflow') = ?`,
		tangled.PipelineStatusNSID, pipelineURI, workflowName,
	).Scan(&started, &finished)
	if err != nil {
		return "", nil, nil, nil, err
	}
	return status, nullStringPointer(started), nullStringPointer(finished), statusErr, nil
}

// nullStringPointer converts SQLite aggregate NULLs to optional lexicon fields.
func nullStringPointer(value sql.NullString) *string {
	if !value.Valid {
		return nil
	}
	return &value.String
}
