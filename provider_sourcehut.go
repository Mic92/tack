package main

// sourcehutProvider implements Provider against a builds.sr.ht
// instance. Spawn submits one job per workflow through GraphQL's
// `submit` mutation; status is observed via an in-process poll loop
// — same shape as the Tekton provider.
//
// The build manifest a workflow targets is carried inline in the
// workflow's YAML body under `tack.sourcehut.manifest`. Tack injects
// a small set of TACK_* environment variables into the manifest so
// the user's tasks can dispatch on the originating Tangled trigger.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"go.yaml.in/yaml/v2"
	"tangled.org/core/api/tangled"

	"go.mitchellh.com/tack/internal/sourcehut"
)

// defaultSourcehutPollInterval is how often the watch loop re-fetches
// upstream job state.
const defaultSourcehutPollInterval = 5 * time.Second

// workflowSourcehutDoc is the tack-flavoured schema we expect inside
// each Tangled workflow's Raw YAML body. Same nesting convention as
// the other providers (`tack: { sourcehut: ... }`) so a workflow can
// grow other top-level keys without colliding with our namespace.
type workflowSourcehutDoc struct {
	Tack struct {
		Sourcehut sourcehutWorkflowConfig `yaml:"sourcehut"`
	} `yaml:"tack"`
}

// sourcehutWorkflowConfig is the sourcehut-specific subset of a
// workflow YAML.
//
// Manifest is the YAML build manifest the user wants builds.sr.ht to
// run, exactly as they'd paste it into the web UI. We treat it as an
// opaque string until Spawn time, then decode/inject env vars/re-emit
// just before submitting so users keep authorial control of their
// manifests.
//
// Instance optionally overrides the provider's default sourcehut host
// (e.g. for users running their own builds.sr.ht). It must be a full
// URL including scheme so we don't have to guess http vs https.
//
// Tags/Note flow straight through to the submit API — they're useful
// for the user's own filtering on builds.sr.ht's job list view.
//
// Secrets controls whether the runner mounts the user's configured
// sourcehut secrets. Default is false because secrets injection is a
// blast-radius decision that should be opt-in per workflow.
type sourcehutWorkflowConfig struct {
	Manifest string   `yaml:"manifest"`
	Instance string   `yaml:"instance"`
	Tags     []string `yaml:"tags"`
	Note     string   `yaml:"note"`
	Secrets  bool     `yaml:"secrets"`
}

// parseSourcehutWorkflowConfig decodes `tack.sourcehut` from a workflow
// body and validates the small set of fields we require. An empty body
// or a missing manifest is a structural error so spawnWorkflow can
// short-circuit cleanly instead of submitting a malformed job that
// builds.sr.ht would reject.
func parseSourcehutWorkflowConfig(raw string) (*sourcehutWorkflowConfig, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, errors.New("workflow body is empty")
	}
	var doc workflowSourcehutDoc
	if err := yaml.Unmarshal([]byte(raw), &doc); err != nil {
		return nil, fmt.Errorf("parse workflow yaml: %w", err)
	}
	cfg := doc.Tack.Sourcehut
	if strings.TrimSpace(cfg.Manifest) == "" {
		return nil, errors.New("workflow yaml: `tack.sourcehut.manifest` is required")
	}
	return &cfg, nil
}

// sourcehutProvider implements Provider.
//
// defaultInstance is the builds.sr.ht instance Spawn submits to when
// a workflow doesn't override it; the configured token must
// authenticate against that instance. defaultClient is the matching
// Client; per-workflow `instance` overrides cause Spawn to mint a
// short-lived sibling Client carrying the same token, since one token
// can address only one sourcehut deployment in practice.
type sourcehutProvider struct {
	br              *broker
	st              *store
	log             *slog.Logger
	token           string
	defaultClient   *sourcehut.Client
	defaultInstance string
	pollInterval    time.Duration

	// instanceClients caches a Client per non-default instance URL so a
	// workflow that targets a custom builds.sr.ht doesn't mint (and
	// leak) a fresh http.Transport + connection pool on every Spawn,
	// watch tick, and Logs call. The default instance always uses
	// defaultClient and never lands here.
	mu              sync.Mutex
	instanceClients map[string]*sourcehut.Client
}

var _ Provider = (*sourcehutProvider)(nil)

// newSourcehutProvider wires a provider to its sourcehut client and
// to the broker it publishes pipeline.status records on. instance is
// the default builds.sr.ht base URL; pass empty to use
// sourcehut.DefaultBaseURL.
func newSourcehutProvider(
	br *broker,
	st *store,
	token string,
	instance string,
	log *slog.Logger,
) *sourcehutProvider {
	if instance == "" {
		instance = sourcehut.DefaultBaseURL
	}
	return &sourcehutProvider{
		br:              br,
		st:              st,
		log:             log.With("component", "provider", "kind", "sourcehut"),
		token:           token,
		defaultClient:   sourcehut.NewClient(instance, token),
		defaultInstance: instance,
		pollInterval:    defaultSourcehutPollInterval,
		instanceClients: map[string]*sourcehut.Client{},
	}
}

// Spawn satisfies Provider. For each workflow it submits a separate
// builds.sr.ht job so each workflow gets its own status timeline. The
// actual API call runs on a goroutine — submission is one HTTP round
// trip, but Spawn is contractually non-blocking.
func (p *sourcehutProvider) Spawn(
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

func (p *sourcehutProvider) spawnWorkflow(
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

	cfg, err := parseSourcehutWorkflowConfig(wf.Raw)
	if err != nil {
		logger.Error("invalid workflow config; refusing to spawn", "err", err)
		return
	}

	commit, branch := triggerCommitAndBranch(trigger)
	manifest, err := injectSourcehutEnvironment(cfg.Manifest, map[string]string{
		"TACK_KNOT":          knot,
		"TACK_PIPELINE_RKEY": pipelineRkey,
		"TACK_WORKFLOW":      wf.Name,
		"TACK_WORKFLOW_RAW":  wf.Raw,
		"TACK_ACTOR":         actor,
		"TACK_COMMIT":        commit,
		"TACK_BRANCH":        branch,
	})
	if err != nil {
		logger.Error("inject TACK_* env into manifest", "err", err)
		return
	}

	client, instance := p.clientFor(cfg.Instance)
	logger = logger.With("instance", instance)

	tags := cfg.Tags
	if len(tags) == 0 {
		// Auto-tag with "tack" so an operator browsing the builds.sr.ht
		// job list can filter to the jobs originating from this spindle
		// without users having to remember to set tags themselves.
		tags = []string{"tack"}
	}
	note := cfg.Note
	if note == "" {
		note = fmt.Sprintf("tangled: %s @ %s", wf.Name, shortCommit(commit))
	}

	job, err := client.SubmitJob(ctx, sourcehut.SubmitRequest{
		Manifest: manifest,
		Tags:     tags,
		Note:     note,
		Secrets:  cfg.Secrets,
		Execute:  true,
	})
	if err != nil {
		logger.Error("submit sourcehut job", "err", err)
		return
	}
	logger.Info("sourcehut job submitted",
		"job_id", job.ID,
		"owner", job.Owner.CanonicalName,
		"web_url", client.JobWebURL(job.Owner.CanonicalName, job.ID),
	)

	pipelineURI := pipelineATURI(knot, pipelineRkey)
	ref := SourcehutJobRef{
		Knot:         knot,
		PipelineRkey: pipelineRkey,
		Workflow:     wf.Name,
		JobID:        job.ID,
		Owner:        job.Owner.CanonicalName,
		Instance:     instance,
		PipelineURI:  pipelineURI,
	}
	if err := p.st.InsertSourcehutJob(ctx, ref); err != nil {
		// Without the row the watch goroutine and any /logs lookup
		// can't recover the job. Surface loudly and bail.
		logger.Error("persist sourcehut job mapping", "err", err,
			"job_id", job.ID,
		)
		return
	}

	if err := p.publishStatus(ctx, pipelineURI, wf.Name,
		"pending", job.ID, nil, nil); err != nil {
		logger.Error("publish initial pending status", "err", err)
	}

	go p.watchJob(ctx, ref, client)
}

// watchJob polls the job until it reaches a terminal state, publishing
// pipeline.status records on every distinct transition. We mirror the
// Tekton provider's structure: a single goroutine per job, suppressed
// duplicate publishes via `last`, exit on terminal or ctx cancellation.
func (p *sourcehutProvider) watchJob(
	ctx context.Context,
	ref SourcehutJobRef,
	client *sourcehut.Client,
) {
	logger := p.log.With(
		"knot", ref.Knot,
		"pipeline_rkey", ref.PipelineRkey,
		"workflow", ref.Workflow,
		"job_id", ref.JobID,
		"instance", ref.Instance,
	)
	logger.Debug("watchJob: starting")

	last := ""
	ticker := time.NewTicker(p.pollInterval)
	defer ticker.Stop()

	// Issue one immediate poll so a fast-completing job (e.g. a tiny
	// manifest the runner finished before the first tick) doesn't sit
	// at "pending" for the whole interval.
	for {
		job, err := client.GetJob(ctx, ref.JobID)
		if errors.Is(err, sourcehut.ErrNotFound) {
			logger.Warn("job disappeared while watching")
			return
		}
		if err != nil {
			logger.Debug("get sourcehut job", "err", err)
		} else {
			status, terminal, ok := mapSourcehutStatus(job.Status)
			logger.Debug("watchJob: poll",
				"upstream_status", job.Status,
				"status", status, "terminal", terminal, "ok", ok, "last", last,
			)
			if !ok {
				// Keep polling on an unrecognised status, but warn
				// so a silent infinite loop is at least visible.
				logger.Warn("unmapped sourcehut status; continuing to poll",
					"upstream_status", job.Status,
				)
			} else if status != last {
				last = status
				if err := p.publishStatus(ctx, ref.PipelineURI, ref.Workflow,
					status, ref.JobID, nil, nil); err != nil {
					logger.Error("publish sourcehut status", "err", err, "status", status)
				}
			}
			if ok && terminal {
				logger.Debug("watchJob: terminal status reached; exiting", "status", status)
				return
			}
		}

		select {
		case <-ctx.Done():
			logger.Debug("watchJob: context cancelled")
			return
		case <-ticker.C:
		}
	}
}

// mapSourcehutStatus translates builds.sr.ht's job status into the
// Tangled spindle StatusKind strings the appview consumes. The
// upstream enum is uppercase over GraphQL; we lowercase first so a
// surface change between cases doesn't fall to the not-mapped branch.
func mapSourcehutStatus(state string) (status string, terminal bool, ok bool) {
	switch strings.ToLower(state) {
	case "pending", "queued":
		return "pending", false, true
	case "running":
		return "running", false, true
	case "success":
		return "success", true, true
	case "failed", "timeout":
		return "failed", true, true
	case "cancelled":
		return "cancelled", true, true
	default:
		return "", false, false
	}
}

// Logs satisfies Provider. We resolve the (knot, rkey, workflow) tuple
// to a sourcehut job, fetch its tasks, and stream the per-task logs as
// LogLine frames bracketed by per-task control frames.
//
// Snapshot read, not a tail — for an in-progress job each task's log
// is fetched at its current length and the channel closes. The
// appview's repeated /logs calls during a running build give us
// "good enough" liveness.
func (p *sourcehutProvider) Logs(
	ctx context.Context,
	knot string,
	pipelineRkey string,
	workflow string,
) (<-chan LogLine, error) {
	ref, err := p.st.LookupSourcehutJobByTuple(ctx, knot, pipelineRkey, workflow)
	if err != nil {
		return nil, fmt.Errorf("lookup sourcehut job: %w", err)
	}
	if ref == nil {
		return nil, ErrLogsNotFound
	}

	client, _ := p.clientFor(ref.Instance)

	job, err := client.GetJob(ctx, ref.JobID)
	if err != nil {
		if errors.Is(err, sourcehut.ErrNotFound) {
			return nil, ErrLogsNotFound
		}
		return nil, fmt.Errorf("get sourcehut job: %w", err)
	}

	// Probe the master log up front to detect a systemic
	// authorization failure (typically a token that lacks
	// `builds.sr.ht/LOGS:RO`). Without this check, every per-task
	// fetch in the goroutine below would 403 and `streamStep` would
	// emit empty steps — leaving the operator with builds whose
	// status updates correctly but whose logs are silently blank.
	// ErrNotFound here is fine: the runner just hasn't produced any
	// setup output yet.
	if _, err := client.GetTaskLog(ctx, ref.JobID, ""); err != nil &&
		!errors.Is(err, sourcehut.ErrNotFound) {
		if errors.Is(err, sourcehut.ErrUnauthorized) {
			return nil, fmt.Errorf(
				"sourcehut log fetch unauthorized; "+
					"token likely missing `builds.sr.ht/LOGS:RO`: %w", err,
			)
		}
		return nil, fmt.Errorf("probe sourcehut task log: %w", err)
	}

	out := make(chan LogLine, 32)
	go func() {
		defer close(out)
		stepID := 0
		// "setup" is the master log: setup output (sources clone,
		// package install) before any task runs. Always emit it as
		// step 0 even if empty so the renderer has a stable timeline.
		if !p.streamStep(ctx, out, client, ref, "setup", "", stepID) {
			return
		}
		stepID++
		for _, task := range job.Tasks {
			name := task.Name
			if name == "" {
				name = fmt.Sprintf("task %d", stepID)
			}
			if !p.streamStep(ctx, out, client, ref, name, task.Name, stepID) {
				return
			}
			stepID++
		}
	}()
	return out, nil
}

// streamStep emits one step's worth of frames into out: a start
// control, one data frame per non-empty log line, and an end control.
// stepName is the human-visible label; logTask is the name passed to
// GetTaskLog (empty string fetches the master log). Returns false if
// the context fired and the caller should bail.
func (p *sourcehutProvider) streamStep(
	ctx context.Context,
	out chan<- LogLine,
	client *sourcehut.Client,
	ref *SourcehutJobRef,
	stepName, logTask string,
	stepID int,
) bool {
	if !sendLine(ctx, out, LogLine{
		Kind: LogKindControl, Time: time.Now(),
		Content: stepName, StepId: stepID, StepStatus: StepStatusStart,
	}) {
		return false
	}
	body, err := client.GetTaskLog(ctx, ref.JobID, logTask)
	if err != nil && !errors.Is(err, sourcehut.ErrNotFound) {
		if errors.Is(err, sourcehut.ErrUnauthorized) {
			// Auth errors are systemic — every subsequent task fetch
			// will fail the same way. Log loudly and abort the stream
			// so the operator notices instead of getting a build with
			// every step rendered empty. The HTTP layer can't change
			// status mid-stream, but at least the server log will
			// point straight at the misconfigured token.
			p.log.Error("sourcehut log fetch unauthorized; aborting stream",
				"err", err, "job_id", ref.JobID, "task", logTask,
			)
			return false
		}
		// Don't fail the whole stream on one task; emit the end frame
		// and move on so the renderer at least sees what other tasks
		// produced. ErrNotFound (no log yet) is treated as an empty body.
		p.log.Debug("fetch sourcehut task log",
			"err", err, "job_id", ref.JobID, "task", logTask,
		)
		body = ""
	}
	if !emitLogBody(ctx, out, body, stepID) {
		return false
	}
	return sendLine(ctx, out, LogLine{
		Kind: LogKindControl, Time: time.Now(),
		Content: stepName, StepId: stepID, StepStatus: StepStatusEnd,
	})
}

// emitLogBody pushes one LogLine per non-empty line of body into out.
// Returns false if the context fired so the caller can stop draining.
func emitLogBody(ctx context.Context, out chan<- LogLine, body string, stepID int) bool {
	if body == "" {
		return true
	}
	for _, line := range strings.Split(strings.TrimRight(body, "\n"), "\n") {
		if line == "" {
			continue
		}
		if !sendLine(ctx, out, LogLine{
			Kind:    LogKindData,
			Time:    time.Now(),
			Content: line + "\n",
			StepId:  stepID,
			Stream:  "stdout",
		}) {
			return false
		}
	}
	return true
}

// publishStatus assembles a tangled.PipelineStatus record and pushes
// it through the broker. jobID is mixed into the rkey so multiple
// status records for the same job don't collide on the events table's
// (rkey) uniqueness, and so an operator grepping the log can find
// every record pertaining to a given sourcehut job.
func (p *sourcehutProvider) publishStatus(
	ctx context.Context,
	pipelineURI, workflow, status string,
	jobID int64,
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
	rkey := fmt.Sprintf("sh-%d-%s-%d", jobID, status, time.Now().UnixNano())
	if _, err := p.br.Publish(ctx, rkey, tangled.PipelineStatusNSID, body); err != nil {
		return fmt.Errorf("publish pipeline.status: %w", err)
	}
	return nil
}

// injectSourcehutEnvironment decodes the user-supplied manifest,
// merges env into its top-level `environment` map, and re-emits it
// as YAML. We round-trip via map[string]any rather than a typed
// schema so unknown fields aren't dropped; comments and key ordering
// are not preserved.
//
// Existing user-set entries win over our injected vars — an explicit
// `TACK_FOO: ...` is a deliberate override.
//
// `environment:` must be a map. A sequence form is rejected loudly
// rather than silently replaced with our map.
func injectSourcehutEnvironment(manifest string, env map[string]string) (string, error) {
	var doc map[string]any
	if err := yaml.Unmarshal([]byte(manifest), &doc); err != nil {
		return "", fmt.Errorf("parse manifest: %w", err)
	}
	if doc == nil {
		doc = map[string]any{}
	}
	var current map[any]any
	switch existing := doc["environment"].(type) {
	case nil:
		current = map[any]any{}
	case map[any]any:
		current = existing
	default:
		return "", fmt.Errorf(
			"manifest `environment` must be a map, got %T", existing,
		)
	}
	for k, v := range env {
		if _, exists := current[k]; exists {
			continue
		}
		current[k] = v
	}
	doc["environment"] = current

	out, err := yaml.Marshal(doc)
	if err != nil {
		return "", fmt.Errorf("emit manifest: %w", err)
	}
	return string(out), nil
}

// clientFor resolves a workflow's optional `instance` override to the
// client that should issue API calls for that workflow, plus the
// concrete instance URL the row was/should-be persisted as. The
// no-override (or matches-default) case reuses the long-lived default
// client. Custom instances are memoised so repeated calls (per-poll,
// per-Logs) reuse one Client — and therefore one connection pool —
// per distinct upstream.
func (p *sourcehutProvider) clientFor(instance string) (*sourcehut.Client, string) {
	if instance == "" || instance == p.defaultInstance {
		return p.defaultClient, p.defaultInstance
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.instanceClients[instance]; ok {
		return c, instance
	}
	c := sourcehut.NewClient(instance, p.token)
	p.instanceClients[instance] = c
	return c, instance
}

// shortCommit returns the first 12 hex chars of a commit SHA, or "?"
// when commit is empty.
func shortCommit(commit string) string {
	if commit == "" {
		return "?"
	}
	if len(commit) > 12 {
		return commit[:12]
	}
	return commit
}
