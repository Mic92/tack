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
// The Buildkite *pipeline slug* a workflow targets is carried inside
// the workflow's YAML body (Pipeline_Workflow.Raw), not configured
// globally on the spindle. That keeps tack a thin translator: the
// repo author decides which Buildkite pipeline runs each Tangled
// workflow without an operator round-trip. See workflowConfig below
// for the supported YAML schema.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"go.yaml.in/yaml/v2"
	"tangled.org/core/api/tangled"

	"go.mitchellh.com/tack/internal/buildkite"
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

// workflowConfig is the tack-flavoured schema we expect inside each
// Tangled workflow's Raw YAML body. Only the Buildkite `pipeline`
// slug is required; everything else is optional. Fields nest under
// `tack: { buildkite: ... }` so the workflow YAML can grow other
// top-level keys (Tangled's own scheduling fields, future provider
// blocks) without colliding with our namespace.
//
// Fields map onto the Buildkite REST "Create a build" request
// properties documented at
// https://buildkite.com/docs/apis/rest-api/builds#create-a-build
// (see also the comment block on buildkite.CreateBuildRequest). We
// expose only the small subset users genuinely need to override —
// trigger metadata supplies commit/branch, and tack supplies the
// identity env+meta the webhook handler relies on, so there's no
// reason to let users re-specify those.
type workflowConfig struct {
	Tack tackConfig `yaml:"tack"`
}

// tackConfig is the per-provider block under the top-level `tack:`
// key. Right now the only nested provider is Buildkite.
type tackConfig struct {
	Buildkite buildkiteConfig `yaml:"buildkite"`
}

// buildkiteConfig is the Buildkite-specific subset of workflowConfig.
//
// `org` lets a workflow target a Buildkite organisation other than
// the spindle's default — useful when one tack instance fronts
// multiple orgs. The configured API token must have access to that
// org or the build creation request will 401/403; we surface that
// error verbatim rather than guessing.
//
// `clean_checkout` is forwarded verbatim to Buildkite. CleanCheckout
// is a *bool so omitting it leaves Buildkite's own default in place
// instead of always shipping `false`.
type buildkiteConfig struct {
	Pipeline      string `yaml:"pipeline"`
	Org           string `yaml:"org"`
	CleanCheckout *bool  `yaml:"clean_checkout"`
}

// parseWorkflowConfig decodes a workflow YAML body into workflowConfig.
// An empty body is treated as a structural error so spawnWorkflow can
// short-circuit cleanly: a workflow with no body has nothing for tack
// to do anyway.
func parseWorkflowConfig(raw string) (*buildkiteConfig, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, errors.New("workflow body is empty")
	}
	var cfg workflowConfig
	if err := yaml.Unmarshal([]byte(raw), &cfg); err != nil {
		return nil, fmt.Errorf("parse workflow yaml: %w", err)
	}
	bk := cfg.Tack.Buildkite
	if bk.Pipeline == "" {
		return nil, errors.New("workflow yaml: `tack.buildkite.pipeline` is required")
	}
	return &bk, nil
}

// buildkiteProvider implements Provider.
//
// webhookSecret + webhookMode live on the provider rather than on
// the HTTP server because the provider is the single owner of
// "everything Buildkite-y": colocating the auth knob with the API
// client and the state translator keeps configuration drift to one
// place and makes the http.go side pure transport.
//
// defaultOrg is the Buildkite organisation the configured API token
// belongs to. Workflows may opt into a different org via their YAML
// `org` field; the API token then needs to be authorised against it.
type buildkiteProvider struct {
	br            *broker
	st            *store
	log           *slog.Logger
	client        *buildkite.Client
	defaultOrg    string
	webhookSecret string
	webhookMode   buildkite.WebhookMode
}

// Compile-time interface conformance check.
var _ Provider = (*buildkiteProvider)(nil)

// newBuildkiteProvider wires a provider to its Buildkite client and
// to the broker it publishes pipeline.status records on. defaultOrg
// is the org the API token authenticates against and the org used
// when a workflow doesn't specify its own. webhookSecret/webhookMode
// govern inbound /webhooks/buildkite request authentication.
func newBuildkiteProvider(
	br *broker,
	st *store,
	client *buildkite.Client,
	defaultOrg string,
	webhookSecret string,
	webhookMode buildkite.WebhookMode,
	log *slog.Logger,
) *buildkiteProvider {
	return &buildkiteProvider{
		br:            br,
		st:            st,
		log:           log.With("component", "provider", "kind", "buildkite"),
		client:        client,
		defaultOrg:    defaultOrg,
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
// Buildkite build off the pipeline named in that workflow's YAML so
// each workflow gets its own status timeline. The actual API call
// runs on a goroutine — CreateBuild is one HTTP round-trip, but we
// still want Spawn to be non-blocking per the interface contract.
//
// On a successful create we persist the build UUID → (knot, rkey,
// workflow) mapping and publish a "pending" pipeline.status so the
// appview sees activity immediately, instead of waiting for the
// first webhook to land.
func (p *buildkiteProvider) Spawn(
	ctx context.Context,
	knot string,
	pipelineRkey string,
	actor string,
	trigger *tangled.Pipeline_TriggerMetadata,
	workflows []*tangled.Pipeline_Workflow,
) {
	if len(workflows) == 0 {
		p.log.Warn("pipeline has no workflows; nothing to spawn",
			"knot", knot, "rkey", pipelineRkey,
		)
		return
	}

	for _, wf := range workflows {
		if wf == nil || wf.Name == "" {
			continue
		}
		wf := wf
		go p.spawnWorkflow(ctx, knot, pipelineRkey, actor, trigger, wf)
	}
}

// spawnWorkflow does the per-workflow API + persistence work for
// Spawn. Errors are logged with full context but not returned —
// nothing in tack consumes the result, and a failed Spawn just
// surfaces as the absence of any status update for the affected
// workflow.
//
// actor is the DID the knot consumer authorized to spawn this work
// (the publishing repo owner). It's surfaced in the per-workflow
// logger so operator-side audits can join Buildkite logs back to the
// triggering Tangled identity even before the build itself is up.
func (p *buildkiteProvider) spawnWorkflow(
	ctx context.Context,
	knot string,
	pipelineRkey string,
	actor string,
	trigger *tangled.Pipeline_TriggerMetadata,
	wf *tangled.Pipeline_Workflow,
) {
	logger := p.log.With(
		"knot", knot,
		"pipeline_rkey", pipelineRkey,
		"workflow", wf.Name,
		"actor", actor,
	)

	cfg, err := parseWorkflowConfig(wf.Raw)
	if err != nil {
		// Bad workflow YAML is a user-facing config error: log it
		// loudly and skip. Firing a build off some default would
		// be more confusing than doing nothing.
		logger.Error("invalid workflow config; refusing to spawn", "err", err)
		return
	}
	logger = logger.With("pipeline", cfg.Pipeline)

	req, err := p.buildCreateRequest(cfg, trigger, knot, pipelineRkey, wf)
	if err != nil {
		logger.Error("build create request", "err", err)
		return
	}

	org := cfg.Org
	if org == "" {
		org = p.defaultOrg
	}

	build, err := p.client.CreateBuild(ctx, org, cfg.Pipeline, req)
	if err != nil {
		logger.Error("create buildkite build", "err", err, "org", org)
		return
	}
	logger.Info("buildkite build created",
		"build_uuid", build.ID,
		"build_number", build.Number,
		"web_url", build.WebURL,
		"org", org,
	)

	pipelineURI := pipelineATURI(knot, pipelineRkey)
	// Persist the *resolved* org — the one we actually issued the
	// CreateBuild against — rather than cfg.Org. If we stored only
	// cfg.Org, a later change to the provider's defaultOrg would
	// silently retarget historical lookups (logs, webhook joins) at
	// the wrong organisation. Legacy rows written before this fix
	// may still have an empty Org; the read path keeps the
	// defaultOrg fallback for those (see Logs).
	if err := p.st.InsertBuildkiteBuild(ctx, BuildkiteBuildRef{
		BuildUUID:    build.ID,
		BuildNumber:  build.Number,
		PipelineSlug: cfg.Pipeline,
		Org:          org,
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

// buildCreateRequest folds the parsed workflow config and the
// Tangled trigger metadata into a single Buildkite create-build
// payload. Trigger metadata supplies commit/branch; the workflow
// YAML supplies the Buildkite routing knobs (pipeline/org) and the
// small handful of build options we expose.
//
// `ignore_pipeline_branch_filters` is hard-coded to true: Tangled
// refs frequently don't match arbitrary Buildkite pipeline branch
// filters, and a build silently dropped at create time is a worse
// failure mode than running one we shouldn't have. Users wanting
// the filter back are expected to drop the filter on the Buildkite
// pipeline itself.
//
// Returns an error when the trigger lacks a commit — Buildkite's
// API requires one and we'd rather log+skip than fire a build that
// resolves to "whatever main happens to be".
func (p *buildkiteProvider) buildCreateRequest(
	cfg *buildkiteConfig,
	trigger *tangled.Pipeline_TriggerMetadata,
	knot, pipelineRkey string,
	wf *tangled.Pipeline_Workflow,
) (buildkite.CreateBuildRequest, error) {
	commit, branch := triggerCommitAndBranch(trigger)
	if commit == "" {
		return buildkite.CreateBuildRequest{}, errors.New(
			"trigger has no commit",
		)
	}

	cleanCheckout := false
	if cfg.CleanCheckout != nil {
		cleanCheckout = *cfg.CleanCheckout
	}

	req := buildkite.CreateBuildRequest{
		Commit:  commit,
		Branch:  branch,
		Message: fmt.Sprintf("tangled: %s", wf.Name),
		Env:     envFromTuple(knot, pipelineRkey, wf),
		MetaData: map[string]string{
			bkMetaKnot:         knot,
			bkMetaPipelineRkey: pipelineRkey,
			bkMetaWorkflow:     wf.Name,
		},
		CleanCheckout:               cleanCheckout,
		IgnorePipelineBranchFilters: true,
	}

	// Auto-populate Buildkite's PR fields from the Tangled PR
	// trigger when present. Buildkite doesn't get a PR number from
	// us (Tangled doesn't surface one through the trigger), but
	// the base branch alone is enough for `pull_request_base_branch`-
	// gated step filters to work.
	if trigger != nil && trigger.PullRequest != nil {
		req.PullRequestBaseBranch = trigger.PullRequest.TargetBranch
	}

	return req, nil
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
// repeated log subscription snapshots during a running build give us
// "good enough" liveness without that complexity.
func (p *buildkiteProvider) Logs(
	ctx context.Context,
	knot string,
	pipelineRkey string,
	workflow string,
) (<-chan LogLine, error) {
	ref, err := p.st.LookupBuildkiteBuildByTuple(ctx, knot, pipelineRkey, workflow)
	if err != nil {
		return nil, fmt.Errorf("lookup build mapping: %w", err)
	}
	if ref == nil {
		return nil, ErrLogsNotFound
	}

	// Resolve the org against which we should pull jobs/logs.
	// Spawn now persists the *resolved* org used at create time, so
	// for any row written by current code ref.Org is authoritative
	// and we use it verbatim. The empty-string fallback to
	// defaultOrg only exists for legacy rows on disk that predate
	// persisting the resolved org; new rows should never hit it.
	org := ref.Org
	if org == "" {
		org = p.defaultOrg
	}

	build, err := p.client.GetBuild(ctx, org, ref.PipelineSlug, ref.BuildNumber)
	if err != nil {
		if errors.Is(err, buildkite.ErrNotFound) {
			return nil, ErrLogsNotFound
		}
		return nil, fmt.Errorf("get build: %w", err)
	}

	out := make(chan LogLine, 32)
	go func() {
		defer close(out)
		stepID := 0
		for _, job := range build.Jobs {
			if job.Type != "" && job.Type != "script" {
				// Skip non-script jobs (waiter, manual,
				// trigger). They have no log to fetch and
				// surfacing empty steps just clutters the
				// appview.
				continue
			}
			name := job.Name
			if name == "" {
				name = fmt.Sprintf("job %s", job.ID)
			}

			if !sendLine(ctx, out, LogLine{
				Kind:       LogKindControl,
				Time:       time.Now(),
				Content:    name,
				StepId:     stepID,
				StepStatus: StepStatusStart,
			}) {
				return
			}

			body, err := p.client.GetJobLog(ctx, org, ref.PipelineSlug, ref.BuildNumber, job.ID)
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

			// Buildkite injects per-line timestamp metadata as
			// ANSI APC sequences (ESC "_" "bk;t=<ms>" BEL) and
			// some renderers downstream don't recognise the APC
			// envelope, leaking the inner "_bk;t=…" payload into
			// the displayed text. Strip them here so consumers
			// only ever see the actual log content.
			body = stripTerminal(body)

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
		// Cache miss. Two plausible causes:
		//
		//   1. Genuinely-foreign build: a Buildkite job triggered
		//      outside tack that just happens to share this
		//      webhook URL. Nothing to do.
		//   2. Race: Spawn's goroutine fired CreateBuild but
		//      hasn't yet written the UUID→tuple row. A fast
		//      build.scheduled webhook can land in that window
		//      and would otherwise be dropped forever.
		//
		// We disambiguate using the Buildkite meta_data we set at
		// CreateBuild time. If the tack:* keys are present the
		// build is ours; we reconstruct the ref and opportunistically
		// persist it so subsequent webhooks (and any Logs call)
		// hit the cache rather than re-doing this work.
		ref = refFromWebhook(payload)
		if ref == nil {
			p.log.Debug("webhook for unknown build; ignoring",
				"event", payload.Event,
				"build_uuid", payload.Build.ID,
			)
			return nil
		}
		// Opportunistic cache fill. Failure here is non-fatal:
		// Spawn's authoritative insert will land shortly (or has
		// already, in which case our INSERT … ON CONFLICT just
		// refreshes the row). We continue with the reconstructed
		// ref either way so a status publish isn't lost.
		if err := p.st.InsertBuildkiteBuild(ctx, *ref); err != nil {
			p.log.Warn("opportunistic persist of buildkite build mapping",
				"err", err, "build_uuid", ref.BuildUUID,
			)
		}
		p.log.Info("buildkite webhook reconstructed from meta_data",
			"event", payload.Event,
			"build_uuid", ref.BuildUUID,
			"workflow", ref.Workflow,
		)
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

// refFromWebhook reconstructs a BuildkiteBuildRef directly from a
// webhook payload, using the tack:* meta_data we attach at Spawn
// time as the source of truth for (knot, pipeline_rkey, workflow).
// Org and pipeline slug come from the payload's organization and
// embedded pipeline objects, both of which Buildkite populates on
// every build.* event.
//
// Returns nil when the payload doesn't carry our meta_data: that's
// the signal the build was triggered outside tack and we should
// keep ignoring it. A partial set (one or two of the three keys)
// also returns nil; we don't want to half-reconstruct a row.
//
// This exists so HandleWebhook can recover the tuple when the
// CreateBuild→InsertBuildkiteBuild race drops a webhook on the
// floor. The caller is expected to opportunistically persist the
// returned ref so subsequent lookups hit the cache.
func refFromWebhook(payload buildkite.WebhookPayload) *BuildkiteBuildRef {
	md := payload.Build.MetaData
	knot := md[bkMetaKnot]
	rkey := md[bkMetaPipelineRkey]
	wf := md[bkMetaWorkflow]
	if knot == "" || rkey == "" || wf == "" {
		return nil
	}

	// Pipeline slug lives on the build's embedded pipeline object.
	// Decoding it as map[string]interface{} keeps the buildkite
	// package's Build struct from sprouting fields we only ever
	// touch on this fallback path.
	pipelineSlug, _ := payload.Build.Pipeline["slug"].(string)

	return &BuildkiteBuildRef{
		BuildUUID:    payload.Build.ID,
		BuildNumber:  payload.Build.Number,
		PipelineSlug: pipelineSlug,
		Org:          payload.Organization.Slug,
		Knot:         knot,
		PipelineRkey: rkey,
		Workflow:     wf,
		PipelineURI:  pipelineATURI(knot, rkey),
	}
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
		return trigger.PullRequest.SourceSha, trigger.PullRequest.SourceBranch
	case trigger.Manual != nil:
		branch := ""
		if trigger.Manual.Ref != nil {
			branch = refToBranch(*trigger.Manual.Ref)
		} else if trigger.Repo != nil {
			branch = trigger.Repo.DefaultBranch
		}
		return trigger.Manual.Sha, branch
	default:
		// Future trigger kinds can still use the repository's default
		// branch, but have no commit until their metadata is supported.
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

// stripTerminal removes ANSI/ECMA-48 escape sequences from a log
// payload, leaving only the displayable text. We need this because
// Buildkite ships its plain-text log API with the agent's full
// terminal output — per-line timestamp APC envelopes
// (`ESC _ "bk;t=<unix-ms>" BEL`), CSI colour codes, clear-to-EOL
// (`ESC [ K`), OSC title sets, etc. — and our consumers are not
// terminal emulators; they render the bytes verbatim.
//
// We recognise the standard escape families described by ECMA-48:
//
//   - CSI:        ESC '['  parameters intermediates final
//   - OSC/DCS/APC/SOS/PM: ESC (']'|'P'|'_'|'X'|'^') … (BEL | ESC '\')
//   - everything else: ESC <single byte>
//
// As a safety belt we also strip the bare "_bk;t=<digits>" residue
// that appears when an upstream processor has stripped the ESC/BEL
// framing without understanding the APC envelope inside it.
//
// We should use libghostty for obvious reasons.
func stripTerminal(s string) string {
	if !strings.ContainsAny(s, "\x1b_") {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); {
		if s[i] == 0x1b && i+1 < len(s) {
			switch s[i+1] {
			case '[':
				// CSI: parameter bytes 0x30-0x3F, then
				// intermediate bytes 0x20-0x2F, then a
				// single final byte 0x40-0x7E. Anything
				// that doesn't conform we drop minimally
				// (just the ESC) so we don't swallow
				// legitimate text.
				j := i + 2
				for j < len(s) && s[j] >= 0x30 && s[j] <= 0x3F {
					j++
				}
				for j < len(s) && s[j] >= 0x20 && s[j] <= 0x2F {
					j++
				}
				if j < len(s) && s[j] >= 0x40 && s[j] <= 0x7E {
					i = j + 1
					continue
				}
				i += 2
				continue
			case ']', 'P', '_', 'X', '^':
				// OSC/DCS/APC/SOS/PM: terminated by BEL or
				// ST (ESC '\'). Drop the entire envelope.
				j := i + 2
				for j < len(s) {
					if s[j] == 0x07 {
						j++
						break
					}
					if s[j] == 0x1b && j+1 < len(s) && s[j+1] == '\\' {
						j += 2
						break
					}
					j++
				}
				i = j
				continue
			default:
				// Two-byte escape (RIS, DECSC, charset
				// selection, …). Drop both bytes.
				i += 2
				continue
			}
		}
		// Bare residue: "_bk;t=<digits>" with no ESC/BEL framing.
		if s[i] == '_' && strings.HasPrefix(s[i:], "_bk;t=") {
			j := i + len("_bk;t=")
			for j < len(s) && s[j] >= '0' && s[j] <= '9' {
				j++
			}
			if j > i+len("_bk;t=") {
				i = j
				continue
			}
		}
		b.WriteByte(s[i])
		i++
	}
	return b.String()
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
