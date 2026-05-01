package main

// buildkiteProvider implements Provider against a real Buildkite
// account. Spawn translates a Tangled pipeline trigger into one
// Buildkite build per workflow; status updates flow back asynchronously
// through the /webhooks/buildkite handler (see http.go), which looks
// the build UUID up in the buildkite_builds table to recover the
// (knot, pipelineRkey, workflow) tuple this provider persisted at
// Spawn time and publishes a sh.tangled.pipeline.status record on
// the in-process broker.
//
// Only one Buildkite pipeline is used per spindle (TACK_BUILDKITE_PIPELINE).
// Every Tangled workflow runs as a build on that single pipeline, with
// the workflow identity plumbed through env + meta_data. The operator
// configures their Buildkite pipeline to read those env vars and
// dispatch accordingly (e.g. via `pipeline upload`). Mapping every
// Tangled workflow to its own Buildkite pipeline would force operators
// to provision Buildkite resources for each workflow file in every
// repo that points at the spindle — friction we don't want to impose.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"tangled.org/core/api/tangled"

	"github.com/mitchellh/tack/internal/buildkite"
)

// Buildkite-side meta_data keys carrying the Tangled identity of a
// build. Mirrored into env vars (see envFromTuple) so an operator's
// Buildkite pipeline script can also reach them via $TACK_*. They
// stay tightly namespaced so a coexisting Buildkite job that uses
// meta_data for its own purposes won't collide.
const (
	bkMetaKnot         = "tack:knot"
	bkMetaPipelineRkey = "tack:pipeline_rkey"
	bkMetaWorkflow     = "tack:workflow"
)

// buildkiteProvider implements Provider.
//
// webhookSecret + webhookMode live on the provider rather than on
// the HTTP server because the provider is the single owner of
// "everything Buildkite-y": colocating the auth knob with the API
// client and the state translator keeps configuration drift to one
// place and makes the http.go side pure transport.
type buildkiteProvider struct {
	br            *broker
	st            *store
	log           *slog.Logger
	client        *buildkite.Client
	pipelineSlug  string
	webhookSecret string
	webhookMode   buildkite.WebhookMode
}

// Compile-time interface conformance check.
var _ Provider = (*buildkiteProvider)(nil)

// newBuildkiteProvider wires a provider to its Buildkite client and
// to the broker it publishes pipeline.status records on. pipelineSlug
// is the Buildkite pipeline that all builds get fired on (see file
// header for why there's only one). webhookSecret/webhookMode govern
// inbound /webhooks/buildkite request authentication.
func newBuildkiteProvider(
	br *broker,
	st *store,
	client *buildkite.Client,
	pipelineSlug string,
	webhookSecret string,
	webhookMode buildkite.WebhookMode,
	log *slog.Logger,
) *buildkiteProvider {
	return &buildkiteProvider{
		br:            br,
		st:            st,
		log:           log.With("component", "provider", "kind", "buildkite"),
		client:        client,
		pipelineSlug:  pipelineSlug,
		webhookSecret: webhookSecret,
		webhookMode:   webhookMode,
	}
}

// VerifyWebhook authenticates an inbound webhook request using
// whichever mode the provider was configured with. Returns nil on
// success; the HTTP handler maps any returned error to 401.
func (p *buildkiteProvider) VerifyWebhook(headers http.Header, body []byte) error {
	switch p.webhookMode {
	case buildkite.WebhookModeSignature:
		return buildkite.VerifySignature(
			headers.Get("X-Buildkite-Signature"),
			p.webhookSecret, body,
		)
	default:
		// Token mode is the Buildkite default and our default, so
		// any unrecognised value falls through to it rather than
		// fail-closed at startup.
		return buildkite.VerifyToken(
			headers.Get("X-Buildkite-Token"),
			p.webhookSecret,
		)
	}
}

// Spawn satisfies Provider. For each workflow it fires a separate
// Buildkite build off the configured pipeline so each workflow gets
// its own status timeline. The actual API call runs on a goroutine —
// CreateBuild is one HTTP round-trip, but we still want Spawn to be
// non-blocking per the interface contract.
//
// On a successful create we persist the build UUID → (knot, rkey,
// workflow) mapping and publish a "pending" pipeline.status so the
// appview sees activity immediately, instead of waiting for the
// first webhook to land.
func (p *buildkiteProvider) Spawn(
	ctx context.Context,
	knot string,
	pipelineRkey string,
	trigger *tangled.Pipeline_TriggerMetadata,
	workflows []*tangled.Pipeline_Workflow,
) {
	if len(workflows) == 0 {
		p.log.Warn("pipeline has no workflows; nothing to spawn",
			"knot", knot, "rkey", pipelineRkey,
		)
		return
	}

	// Derive build inputs once. Every workflow on this trigger
	// targets the same commit/branch — only the workflow name
	// varies between the per-workflow goroutines below.
	commit, branch := triggerCommitAndBranch(trigger)
	if commit == "" {
		// Buildkite's create-build API requires a commit; we'd
		// rather log loudly and skip than fire builds on "HEAD"
		// and silently get whatever main happens to look like.
		p.log.Error("trigger has no commit; refusing to spawn",
			"knot", knot, "rkey", pipelineRkey,
		)
		return
	}

	for _, wf := range workflows {
		if wf == nil || wf.Name == "" {
			continue
		}
		wf := wf
		go p.spawnWorkflow(ctx, knot, pipelineRkey, commit, branch, wf)
	}
}

// spawnWorkflow does the per-workflow API + persistence work for
// Spawn. Errors are logged with full context but not returned —
// nothing in tack consumes the result, and a failed Spawn just
// surfaces as the absence of any status update for the affected
// workflow.
func (p *buildkiteProvider) spawnWorkflow(
	ctx context.Context,
	knot string,
	pipelineRkey string,
	commit string,
	branch string,
	wf *tangled.Pipeline_Workflow,
) {
	logger := p.log.With(
		"knot", knot,
		"pipeline_rkey", pipelineRkey,
		"workflow", wf.Name,
	)

	pipelineURI := pipelineATURI(knot, pipelineRkey)
	meta := map[string]string{
		bkMetaKnot:         knot,
		bkMetaPipelineRkey: pipelineRkey,
		bkMetaWorkflow:     wf.Name,
	}
	env := envFromTuple(knot, pipelineRkey, wf)

	req := buildkite.CreateBuildRequest{
		Commit:                      commit,
		Branch:                      branch,
		Message:                     fmt.Sprintf("tangled: %s", wf.Name),
		Env:                         env,
		MetaData:                    meta,
		IgnorePipelineBranchFilters: true,
	}

	build, err := p.client.CreateBuild(ctx, p.pipelineSlug, req)
	if err != nil {
		logger.Error("create buildkite build", "err", err)
		return
	}
	logger.Info("buildkite build created",
		"build_uuid", build.ID,
		"build_number", build.Number,
		"web_url", build.WebURL,
	)

	if err := p.st.InsertBuildkiteBuild(ctx, BuildkiteBuildRef{
		BuildUUID:    build.ID,
		BuildNumber:  build.Number,
		PipelineSlug: p.pipelineSlug,
		Knot:         knot,
		PipelineRkey: pipelineRkey,
		Workflow:     wf.Name,
		PipelineURI:  pipelineURI,
	}); err != nil {
		// Webhook handlers will fail to translate this build's
		// events because they can't recover the tuple. Surface
		// loudly and bail; we don't want a half-tracked build
		// silently leaking status into the broker.
		logger.Error("persist buildkite build mapping", "err", err,
			"build_uuid", build.ID,
		)
		return
	}

	// Initial status publish so the appview shows the build as
	// queued without waiting for the first webhook. This mirrors
	// the upstream spindle's "schedule then run" cadence.
	if err := p.publishStatus(
		ctx, pipelineURI, wf.Name, "pending", build.ID,
		nil, nil,
	); err != nil {
		logger.Error("publish initial pending status", "err", err)
	}
}

// Logs satisfies Provider. We resolve the (knot, rkey, workflow)
// tuple to a Buildkite build via the store, fetch the current jobs
// list, then drain each job's plain-text log into the channel as one
// LogLine per output line.
//
// Per-job control frames bracket each job so the appview's renderer
// has start/end markers to lay out timing — same shape as the fake
// provider and the upstream spindle.
//
// This is a snapshot read, not a tail — finished or in-progress, we
// fetch what's there and close. Live tailing would require Buildkite
// agent log streaming, which the public REST API doesn't expose; the
// appview's repeated /logs calls during a running build give us
// "good enough" liveness without that complexity.
func (p *buildkiteProvider) Logs(
	ctx context.Context,
	knot string,
	pipelineRkey string,
	workflow string,
) (<-chan LogLine, error) {
	ref, err := p.st.LookupBuildkiteBuildByTuple(ctx, knot, pipelineRkey, workflow)
	if err != nil {
		return nil, fmt.Errorf("lookup build for logs: %w", err)
	}
	if ref == nil {
		return nil, ErrLogsNotFound
	}

	// Fresh fetch so we get the current job set, not whatever was
	// returned at create time (when most jobs are still nil). The
	// upstream's not-found is mapped to the Provider-shaped one
	// here because the /logs handler only knows about ErrLogsNotFound.
	build, err := p.client.GetBuild(ctx, ref.PipelineSlug, ref.BuildNumber)
	if err != nil {
		if errors.Is(err, buildkite.ErrNotFound) {
			return nil, ErrLogsNotFound
		}
		return nil, fmt.Errorf("get build for logs: %w", err)
	}

	out := make(chan LogLine, 64)
	go func() {
		defer close(out)
		stepID := 0
		for _, job := range build.Jobs {
			// Only "script" jobs have agent-produced logs.
			// Waiter / manual / trigger jobs have no body to
			// fetch; skip them so we don't hit Buildkite with
			// 404-bound requests.
			if job.Type != "" && job.Type != "script" {
				continue
			}

			name := job.Name
			if name == "" {
				name = job.ID
			}

			// Job-level start frame so the appview can bound
			// timing per job.
			if !sendLine(ctx, out, LogLine{
				Kind:       LogKindControl,
				Time:       time.Now(),
				Content:    name,
				StepId:     stepID,
				StepStatus: StepStatusStart,
			}) {
				return
			}

			body, err := p.client.GetJobLog(ctx, ref.PipelineSlug, ref.BuildNumber, job.ID)
			if err != nil {
				p.log.Debug("fetch job log",
					"err", err,
					"build_uuid", ref.BuildUUID,
					"job_id", job.ID,
				)
				// Don't fail the whole stream on one job;
				// emit the end frame and move on so the
				// appview at least sees what other jobs
				// produced.
				body = ""
			}

			for _, line := range strings.Split(strings.TrimRight(body, "\n"), "\n") {
				if line == "" {
					// Skip the leading empty entry that
					// Split produces for empty bodies.
					continue
				}
				if !sendLine(ctx, out, LogLine{
					Kind:    LogKindData,
					Time:    time.Now(),
					Content: line + "\n",
					StepId:  stepID,
					Stream:  "stdout",
				}) {
					return
				}
			}

			if !sendLine(ctx, out, LogLine{
				Kind:       LogKindControl,
				Time:       time.Now(),
				Content:    name,
				StepId:     stepID,
				StepStatus: StepStatusEnd,
			}) {
				return
			}
			stepID++
		}
	}()
	return out, nil
}

// publishStatus assembles a tangled.PipelineStatus record and pushes
// it through the broker. buildUUID is mixed into the rkey so multiple
// status events for the same workflow don't collide on the events
// table's (rkey) uniqueness — and so an operator grepping the log
// can find every record that pertains to a given Buildkite build.
//
// errMsg/exitCode are optional; pass nil for non-failure transitions.
func (p *buildkiteProvider) publishStatus(
	ctx context.Context,
	pipelineURI, workflow, status, buildUUID string,
	errMsg *string,
	exitCode *int64,
) error {
	rec := tangled.PipelineStatus{
		LexiconTypeID: tangled.PipelineStatusNSID,
		Pipeline:      pipelineURI,
		Workflow:      workflow,
		Status:        status,
		CreatedAt:     time.Now().UTC().Format(time.RFC3339),
		Error:         errMsg,
		ExitCode:      exitCode,
	}
	body, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("marshal pipeline.status: %w", err)
	}
	rkey := fmt.Sprintf("bk-%s-%s-%d", buildUUID, status, time.Now().UnixNano())
	if _, err := p.br.Publish(ctx, rkey, tangled.PipelineStatusNSID, body); err != nil {
		return fmt.Errorf("publish pipeline.status: %w", err)
	}
	return nil
}

// HandleWebhook applies a decoded Buildkite webhook payload: looks
// the build up in the store, translates the Buildkite state into a
// Tangled StatusKind, and publishes a pipeline.status record. Used
// by the HTTP webhook handler so both the ingress logic and the
// translation logic live next to each other.
//
// Returns nil for events we intentionally ignore (job.* events,
// build.scheduled which we already publish locally on Spawn, builds
// we don't have a mapping for) so the handler can 200 them — webhook
// retries from Buildkite on a 4xx/5xx are noisy and not what we want
// for "we just don't care about this event".
func (p *buildkiteProvider) HandleWebhook(
	ctx context.Context,
	payload buildkite.WebhookPayload,
) error {
	// Only build.* events drive pipeline.status today. Everything
	// else (job.*, agent.*, ping) is acknowledged silently.
	if !strings.HasPrefix(payload.Event, "build.") {
		return nil
	}

	ref, err := p.st.LookupBuildkiteBuildByUUID(ctx, payload.Build.ID)
	if err != nil {
		return fmt.Errorf("lookup build by uuid: %w", err)
	}
	if ref == nil {
		// Most likely: this build was triggered outside tack and
		// just happens to share our webhook URL. Nothing to do.
		p.log.Debug("webhook for unknown build; ignoring",
			"event", payload.Event,
			"build_uuid", payload.Build.ID,
		)
		return nil
	}

	status, ok := mapBuildkiteState(payload.Build.State)
	if !ok {
		// Unknown / transient state ("blocked", "skipped",
		// "not_run", "waiting"…) — log so we can extend the map
		// later, but don't error out the webhook.
		p.log.Debug("unmapped buildkite state; ignoring",
			"event", payload.Event,
			"state", payload.Build.State,
			"build_uuid", payload.Build.ID,
		)
		return nil
	}

	if err := p.publishStatus(ctx, ref.PipelineURI, ref.Workflow,
		status, ref.BuildUUID, nil, nil); err != nil {
		return fmt.Errorf("publish webhook status: %w", err)
	}
	p.log.Info("buildkite webhook → pipeline.status",
		"event", payload.Event,
		"state", payload.Build.State,
		"status", status,
		"build_uuid", payload.Build.ID,
		"workflow", ref.Workflow,
	)
	return nil
}

// mapBuildkiteState translates Buildkite's build state strings into
// the Tangled spindle StatusKind enum. The mapping aligns with the
// upstream constants (StatusKindRunning/Failed/Cancelled/Success);
// states that don't have a direct analogue (blocked, skipped,
// not_run) are reported as not-mapped so the caller can decide
// whether to ignore them.
func mapBuildkiteState(state string) (string, bool) {
	switch state {
	case "scheduled":
		return "pending", true
	case "running", "failing":
		return "running", true
	case "passed":
		return "success", true
	case "failed":
		return "failed", true
	case "canceled", "canceling":
		return "cancelled", true
	default:
		return "", false
	}
}

// envFromTuple builds the env block forwarded into the Buildkite
// build. These are the only handle a user's Buildkite pipeline has
// on the originating Tangled trigger: their pipeline.yml typically
// reads $TACK_WORKFLOW and dispatches based on it (e.g. running a
// `pipeline upload` against a workflow-specific YAML file).
//
// TACK_WORKFLOW_RAW carries the entire YAML body of the workflow as
// captured in the Tangled record. It can be empty if the workflow
// definition omitted it; consumers should defend.
func envFromTuple(knot, pipelineRkey string, wf *tangled.Pipeline_Workflow) map[string]string {
	return map[string]string{
		"TACK_KNOT":          knot,
		"TACK_PIPELINE_RKEY": pipelineRkey,
		"TACK_WORKFLOW":      wf.Name,
		"TACK_WORKFLOW_RAW":  wf.Raw,
	}
}

// pipelineATURI returns the at-uri the appview joins pipeline.status
// records back to their originating pipeline on. Format mirrors the
// upstream spindle; the appview strips the `did:web:` prefix and
// treats the remainder as the knot identifier.
func pipelineATURI(knot, pipelineRkey string) string {
	return fmt.Sprintf("at://did:web:%s/%s/%s",
		knot, tangled.PipelineNSID, pipelineRkey,
	)
}

// triggerCommitAndBranch extracts (commit, branch) from a Tangled
// pipeline trigger, regardless of whether it was a push, a pull
// request, or a manual run. Returns empty strings on a fully-empty
// trigger so the caller can decide whether that's fatal.
func triggerCommitAndBranch(trigger *tangled.Pipeline_TriggerMetadata) (string, string) {
	if trigger == nil {
		return "", ""
	}
	switch {
	case trigger.Push != nil:
		// For push events, NewSha is the commit being built and
		// Ref is the full ref (e.g. "refs/heads/main") — strip
		// the prefix so Buildkite's branch-aware features work.
		return trigger.Push.NewSha, refToBranch(trigger.Push.Ref)
	case trigger.PullRequest != nil:
		// PRs build the source commit on the source branch.
		// Buildkite's pipeline can opt into PR-aware behaviour
		// via pull_request_id (not currently plumbed through).
		return trigger.PullRequest.SourceSha, trigger.PullRequest.SourceBranch
	default:
		// Manual triggers and any future kinds: fall back to the
		// repo default branch with no commit, which the caller
		// will treat as fatal — manual triggers will need
		// additional plumbing to pick a commit.
		if trigger.Repo != nil {
			return "", trigger.Repo.DefaultBranch
		}
		return "", ""
	}
}

// refToBranch strips the conventional refs/heads/ prefix from a git
// ref. Refs that don't match the prefix (tags, refs/pull/N/head) are
// returned as-is so downstream tooling can decide what to do with
// them — Buildkite happily accepts either form in `branch`.
func refToBranch(ref string) string {
	const prefix = "refs/heads/"
	if strings.HasPrefix(ref, prefix) {
		return strings.TrimPrefix(ref, prefix)
	}
	return ref
}

// sendLine pushes one LogLine into out, returning false if ctx
// fired first. Centralised so the per-job loop in Logs stays
// focused on the wire-shape decisions.
func sendLine(ctx context.Context, out chan<- LogLine, line LogLine) bool {
	select {
	case <-ctx.Done():
		return false
	case out <- line:
		return true
	}
}
