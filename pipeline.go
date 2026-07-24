package main

// Pipeline creation for the post-1.13 Tangled protocol. Knots now emit git
// ref updates; the spindle owns workflow discovery, pipeline IDs, and retained
// pipeline records.

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
	indigoxrpc "github.com/bluesky-social/indigo/xrpc"
	"tangled.org/core/api/tangled"
	"tangled.org/core/eventconsumer"
	"tangled.org/core/hostutil"
	"tangled.org/core/idresolver"
	knotdb "tangled.org/core/knotserver/db"
	"tangled.org/core/patchutil"
	"tangled.org/core/repoident"
	"tangled.org/core/repoverify"
	"tangled.org/core/tid"
	"tangled.org/core/workflow"
)

// errNoMatchingWorkflows distinguishes a valid trigger that selected nothing
// from an operational failure. Push and pull consumers silently skip the former;
// explicit XRPC triggers surface it to the caller as a rejected request.
var errNoMatchingWorkflows = errors.New("no workflows matched the trigger")

// workflowLoader is replaceable in tests so trigger/auth behavior can be
// exercised without cloning a repository. Production always uses
// pipelineService.loadWorkflows.
type workflowLoader func(
	context.Context,
	*RepoRef,
	string,
	*tangled.Pipeline_TriggerMetadata,
	[]string,
) ([]*tangled.Pipeline_Workflow, error)

// pullRevisionFetcher isolates the network/blob work needed to recover the
// source commit from a pull round. Keeping it injectable makes pull-trigger
// tests deterministic and avoids constructing signed ATProto blobs there.
type pullRevisionFetcher func(
	context.Context,
	string,
	string,
	*tangled.RepoPull,
) (string, error)

// verifiedRepoCache holds canonical repository identity data resolved from the
// repo DID. Entries are short-lived because repository DIDs can migrate knots;
// repo-record mutations also invalidate the entire cache immediately.
type verifiedRepoCache struct {
	repo    *RepoRef
	expires time.Time
}

// pipelineService owns the post-1.13 Spindle pipeline lifecycle. It verifies
// trigger provenance, checks out workflow definitions, persists the
// spindle-owned pipeline record, and only then dispatches work to a Provider.
//
// The function-valued fields are production dependencies with test seams. The
// two mutex domains deliberately stay separate: repoLocks serialize mutable Git
// worktrees, while verifiedMu protects the short-lived identity cache.
type pipelineService struct {
	// rootCtx outlives request contexts and scopes asynchronous provider work.
	rootCtx context.Context
	// httpClient rejects private-network PDS/knot targets outside dev mode.
	httpClient           *http.Client
	cfg                  config
	store                *store
	broker               *broker
	provider             Provider
	load                 workflowLoader
	fetchPullRevision    pullRevisionFetcher
	resolveDefaultBranch func(context.Context, *RepoRef) string
	directory            identity.Directory
	verify               repoverify.Verifier
	repoLocksMu          sync.Mutex
	repoLocks            map[string]*sync.Mutex
	verifiedMu           sync.Mutex
	verifiedRepos        map[string]verifiedRepoCache
}

// newPipelineService wires production identity, HTTP, checkout, and compiler
// dependencies. The service is shared by knot events, Jetstream pulls, and CI
// XRPC procedures so every trigger path applies the same authorization rules.
func newPipelineService(
	rootCtx context.Context,
	cfg config,
	st *store,
	br *broker,
	provider Provider,
) *pipelineService {
	resolver := idresolver.DefaultResolver(identity.DefaultPLCURL)
	p := &pipelineService{
		rootCtx:       rootCtx,
		httpClient:    newPipelineHTTPClient(cfg.Dev),
		cfg:           cfg,
		store:         st,
		broker:        br,
		provider:      provider,
		directory:     identity.DefaultDirectory(),
		verify:        repoverify.New(resolver, cfg.Dev),
		repoLocks:     make(map[string]*sync.Mutex),
		verifiedRepos: make(map[string]verifiedRepoCache),
	}
	p.load = p.loadWorkflows
	p.fetchPullRevision = p.fetchLatestPullRevision
	p.resolveDefaultBranch = p.defaultBranch
	return p
}

// processRefUpdate validates the knot-owned event against the repository state
// mirrored from ATProto, compiles matching workflows, and creates a new
// spindle-local pipeline.
func (p *pipelineService) processRefUpdate(
	ctx context.Context,
	source eventconsumer.Source,
	event *tangled.GitRefUpdate,
) error {
	logger := loggerFrom(ctx).With(
		"component", "pipeline",
		"knot", source.Host,
		"repo_did", event.Repo,
		"ref", event.Ref,
		"commit", event.NewSha,
	)

	// A refUpdate names only a repo DID. Resolve that DID through our mirrored
	// assignment plus the canonical knot before trusting any event payload.
	if _, err := syntax.ParseDID(event.Repo); err != nil {
		return fmt.Errorf("invalid ref update repo DID %q: %w", event.Repo, err)
	}
	repo, err := p.managedRepo(ctx, event.Repo)
	if err != nil {
		return err
	}
	if repo == nil {
		logger.Debug("ignoring ref update for unmanaged repository")
		return nil
	}
	if repo.Knot != source.Host {
		logger.Warn("ignoring ref update from non-owning knot", "repo_knot", repo.Knot)
		return nil
	}
	if skipsCI(event.PushOptions) {
		logger.Info("push requested CI skip")
		return nil
	}

	// The knot already tells us whether this is the default ref, avoiding an
	// extra XRPC round trip on the high-volume push path.
	defaultBranch := ""
	if event.Meta != nil && event.Meta.IsDefaultRef {
		defaultBranch = strings.TrimPrefix(event.Ref, "refs/heads/")
	}
	triggerRepo := pipelineTriggerRepo(repo, defaultBranch)
	trigger := &tangled.Pipeline_TriggerMetadata{
		Kind: string(workflow.TriggerKindPush),
		Push: &tangled.Pipeline_PushTriggerData{
			Ref:    event.Ref,
			OldSha: event.OldSha,
			NewSha: event.NewSha,
		},
		Repo: triggerRepo,
	}

	rkey, err := p.run(ctx, repo, repo, event.NewSha, trigger, event.ChangedFiles, nil)
	if errors.Is(err, errNoMatchingWorkflows) {
		logger.Info("no workflow matched push trigger")
		return nil
	}
	if err != nil {
		return err
	}
	logger.Info("pipeline triggered", "pipeline", pipelineATURI(repo.Knot, rkey))
	return nil
}

// processPull turns live branch-based pull records into pull_request
// pipelines. Fork-based pulls are dispatched explicitly through
// sh.tangled.ci.triggerPipeline because their source repository differs.
func (p *pipelineService) processPull(
	ctx context.Context,
	actor, rkey string,
	record *tangled.RepoPull,
) error {
	// Branch-based pulls check out the target repository directly. Fork-based
	// pulls carry Source.Repo and must go through authenticated triggerPipeline,
	// where the foreign source DID is independently verified.
	if record.Target == nil || record.Source == nil || record.Source.Repo != nil {
		return nil
	}
	if _, err := syntax.ParseDID(record.Target.Repo); err != nil {
		return fmt.Errorf("invalid pull target repo DID %q: %w", record.Target.Repo, err)
	}
	target, err := p.managedRepo(ctx, record.Target.Repo)
	if err != nil {
		return err
	}
	if target == nil {
		return nil
	}

	sha, err := p.fetchPullRevision(ctx, actor, rkey, record)
	if err != nil {
		return fmt.Errorf("load pull request revision: %w", err)
	}
	if len(sha) != 40 {
		return fmt.Errorf("pull request source revision must be a 40-character commit hash")
	}
	pullURI := fmt.Sprintf("at://%s/%s/%s", actor, tangled.RepoPullNSID, rkey)
	// Jetstream can replay a short window after reconnects, and metadata-only
	// pull updates retain the same source SHA. Do not turn either into another
	// externally billed CI run.
	exists, err := p.store.HasPullPipeline(ctx, pullURI, sha)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	trigger := &tangled.Pipeline_TriggerMetadata{
		Kind: string(workflow.TriggerKindPullRequest),
		PullRequest: &tangled.Pipeline_PullRequestTriggerData{
			Pull:         &pullURI,
			SourceBranch: record.Source.Branch,
			SourceSha:    sha,
			TargetBranch: record.Target.Branch,
		},
		Repo: pipelineTriggerRepo(target, ""),
	}
	_, err = p.run(ctx, target, target, sha, trigger, nil, nil)
	if errors.Is(err, errNoMatchingWorkflows) {
		return nil
	}
	return err
}

// processKnotCollaborator mirrors authoritative knot ACL changes into the
// local authorization table. The source-host comparison is load-bearing: a
// different knot must not be able to grant access to a repository it does not
// host.
func (p *pipelineService) processKnotCollaborator(
	ctx context.Context,
	source eventconsumer.Source,
	event *knotdb.RepoCollaboratorUpdate,
) error {
	if _, err := syntax.ParseDID(event.Repo); err != nil {
		return fmt.Errorf("invalid collaborator repo DID %q: %w", event.Repo, err)
	}
	if _, err := syntax.ParseDID(event.Subject); err != nil {
		return fmt.Errorf("invalid collaborator subject DID %q: %w", event.Subject, err)
	}
	repo, err := p.managedRepo(ctx, event.Repo)
	if err != nil {
		return err
	}
	if repo == nil || repo.Knot != source.Host {
		return nil
	}
	switch event.Op {
	case knotdb.AclOpAdd:
		return p.store.ApplyKnotCollaborator(ctx, repo, event.Subject, true)
	case knotdb.AclOpRemove:
		return p.store.ApplyKnotCollaborator(ctx, repo, event.Subject, false)
	default:
		return fmt.Errorf("unknown collaborator operation %q", event.Op)
	}
}

// triggerPipeline implements the authenticated sh.tangled.ci.triggerPipeline
// procedure used for retries, manual runs, and fork pull requests.
func (p *pipelineService) triggerPipeline(
	ctx context.Context,
	actor string,
	input *tangled.CiTriggerPipeline_Input,
) (string, error) {
	if _, err := syntax.ParseDID(input.Repo); err != nil {
		return "", fmt.Errorf("invalid repository DID %q: %w", input.Repo, err)
	}
	target, err := p.managedRepo(ctx, input.Repo)
	if err != nil {
		return "", err
	}
	if target == nil {
		return "", fmt.Errorf("repository %q is not managed by this spindle", input.Repo)
	}
	authorized, err := p.authorizeRepoActor(ctx, target, actor)
	if err != nil {
		return "", err
	}
	if !authorized {
		return "", fmt.Errorf("actor %q cannot trigger repository %q", actor, input.Repo)
	}
	if input.Trigger == nil {
		return "", errors.New("trigger is required")
	}

	// Manual/retry payloads use the public ci.trigger union, while providers and
	// the retained event format still consume Pipeline_TriggerMetadata. Convert
	// between them once here so providers remain protocol-agnostic.
	defaultBranch := p.resolveDefaultBranch(ctx, target)
	trigger := &tangled.Pipeline_TriggerMetadata{
		Repo: pipelineTriggerRepo(target, defaultBranch),
	}
	sha := ""
	sourceRepoDID := ""
	switch {
	case input.Trigger.CiTrigger_Manual != nil:
		manual := input.Trigger.CiTrigger_Manual
		sha = manual.Sha
		sourceRepoDID = pointerValue(manual.SourceRepo)
		inputs := make([]*tangled.Pipeline_Pair, 0, len(manual.Inputs))
		for _, pair := range manual.Inputs {
			if pair != nil {
				inputs = append(inputs, &tangled.Pipeline_Pair{Key: pair.Key, Value: pair.Value})
			}
		}
		trigger.Kind = string(workflow.TriggerKindManual)
		trigger.Manual = &tangled.Pipeline_ManualTriggerData{
			Sha:    manual.Sha,
			Ref:    manual.Ref,
			Inputs: inputs,
		}
	case input.Trigger.CiTrigger_PullRequest != nil:
		pull := input.Trigger.CiTrigger_PullRequest
		if pull.TargetBranch == "" {
			return "", errors.New("pull request targetBranch is required")
		}
		sha = pull.SourceSha
		sourceRepoDID = pointerValue(pull.SourceRepo)
		trigger.Kind = string(workflow.TriggerKindPullRequest)
		trigger.PullRequest = &tangled.Pipeline_PullRequestTriggerData{
			Pull:         pull.Pull,
			SourceBranch: pointerValue(pull.SourceBranch),
			SourceSha:    pull.SourceSha,
			TargetBranch: pull.TargetBranch,
		}
	default:
		return "", errors.New("unsupported trigger variant")
	}
	if len(sha) != 40 {
		return "", errors.New("sha must be a 40-character commit hash")
	}

	source := target
	if sourceRepoDID != "" && sourceRepoDID != target.RepoDID {
		// A fork source is not required to assign itself to this spindle, but its
		// DID must still resolve canonically before we construct a clone URL.
		if _, err := syntax.ParseDID(sourceRepoDID); err != nil {
			return "", fmt.Errorf("invalid source repository DID %q: %w", sourceRepoDID, err)
		}
		source, err = p.verifiedRepo(ctx, sourceRepoDID)
		if err != nil {
			return "", err
		}
		trigger.SourceRepo = &sourceRepoDID
	}

	rkey, err := p.run(ctx, target, source, sha, trigger, nil, input.Workflows)
	if err != nil {
		return "", err
	}
	return pipelineATURI(target.Knot, rkey), nil
}

// run is the common commit point for every trigger source. It compiles and
// filters workflows, persists the immutable pipeline definition, and dispatches
// the provider only after persistence succeeds so query/log requests can never
// observe provider work without a corresponding pipeline.
func (p *pipelineService) run(
	ctx context.Context,
	target, source *RepoRef,
	revision string,
	trigger *tangled.Pipeline_TriggerMetadata,
	changedFiles, only []string,
) (string, error) {
	workflows, err := p.load(ctx, source, revision, trigger, changedFiles)
	if err != nil {
		return "", fmt.Errorf("load workflows: %w", err)
	}
	if len(only) > 0 {
		// Explicit retries may select one workflow from the original pipeline.
		// Filter after compilation so ordinary trigger constraints and diagnostics
		// still behave exactly like a full run.
		allowed := make(map[string]struct{}, len(only))
		for _, name := range only {
			allowed[name] = struct{}{}
		}
		filtered := workflows[:0]
		for _, wf := range workflows {
			if wf != nil {
				if _, ok := allowed[wf.Name]; ok {
					filtered = append(filtered, wf)
				}
			}
		}
		workflows = filtered
	}
	if len(workflows) == 0 {
		return "", errNoMatchingWorkflows
	}
	if err := p.rootCtx.Err(); err != nil {
		return "", fmt.Errorf("spindle is shutting down: %w", err)
	}

	rkey := tid.TID()
	record := tangled.Pipeline{
		LexiconTypeID:   tangled.PipelineNSID,
		TriggerMetadata: trigger,
		Workflows:       workflows,
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		return "", fmt.Errorf("encode pipeline: %w", err)
	}
	if _, err := p.broker.Publish(ctx, rkey, tangled.PipelineNSID, encoded); err != nil {
		return "", fmt.Errorf("persist pipeline: %w", err)
	}
	// Manual triggers arrive on an HTTP request context, which is cancelled as
	// soon as the XRPC response is written. Provider work must instead follow
	// the process lifetime so it survives the request but still stops cleanly
	// during Tack shutdown.
	p.provider.Spawn(p.rootCtx, target.Knot, rkey, target.OwnerDID, trigger, workflows)
	return rkey, nil
}

// loadWorkflows checks out only .tangled/workflows at revision and runs the
// upstream compiler with the trigger metadata and changed-file set. The lock is
// held through both checkout and file reads: releasing it after checkout would
// let a concurrent push replace the worktree before we finish compiling it.
func (p *pipelineService) loadWorkflows(
	ctx context.Context,
	source *RepoRef,
	revision string,
	trigger *tangled.Pipeline_TriggerMetadata,
	changedFiles []string,
) ([]*tangled.Pipeline_Workflow, error) {
	host, noTLS, err := hostutil.ParseHostname(source.Knot)
	if err != nil {
		return nil, fmt.Errorf("parse knot hostname: %w", err)
	}
	scheme := "https"
	if noTLS || p.cfg.Dev {
		scheme = "http"
	}
	cloneURL := fmt.Sprintf("%s://%s/%s", scheme, host, source.RepoDID)
	// Hash the DID instead of using it as a path component. Besides keeping
	// paths short, this prevents malformed or future DID methods from escaping
	// the configured cache directory.
	repoHash := sha256.Sum256([]byte(source.RepoDID))
	repoPath := filepath.Join(p.cfg.RepoDir, fmt.Sprintf("%x", repoHash[:]))
	if err := os.MkdirAll(p.cfg.RepoDir, 0o700); err != nil {
		return nil, fmt.Errorf("create repository cache: %w", err)
	}
	unlock := p.lockRepo(source.RepoDID)
	defer unlock()
	if err := syncWorkflowRepo(ctx, cloneURL, repoPath, revision); err != nil {
		return nil, fmt.Errorf("sync repository: %w", err)
	}

	directory := filepath.Join(repoPath, workflow.WorkflowDir)
	entries, err := os.ReadDir(directory)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read workflow directory: %w", err)
	}
	raw := make(workflow.RawPipeline, 0, len(entries))
	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			continue
		}
		contents, err := os.ReadFile(filepath.Join(directory, entry.Name()))
		if err != nil {
			return nil, fmt.Errorf("read workflow %q: %w", entry.Name(), err)
		}
		raw = append(raw, workflow.RawWorkflow{Name: entry.Name(), Contents: contents})
	}

	compiler := workflow.Compiler{Trigger: *trigger, ChangedFiles: changedFiles}
	compiled := compiler.Compile(compiler.Parse(raw))
	logger := loggerFrom(ctx).With("component", "pipeline", "repo_did", source.RepoDID)
	for _, diagnostic := range compiler.Diagnostics.Errors {
		logger.Error("invalid workflow", "error", diagnostic.String())
	}
	for _, diagnostic := range compiler.Diagnostics.Warnings {
		logger.Warn("workflow skipped", "warning", diagnostic.String())
	}
	return compiled.Workflows, nil
}

// lockRepo acquires the mutex for one repository cache and returns its unlock
// function. Locks are keyed by stable repo DID, so unrelated repositories can
// compile concurrently while revisions of one repository remain serialized.
func (p *pipelineService) lockRepo(repoDID string) func() {
	p.repoLocksMu.Lock()
	lock := p.repoLocks[repoDID]
	if lock == nil {
		lock = &sync.Mutex{}
		p.repoLocks[repoDID] = lock
	}
	p.repoLocksMu.Unlock()
	lock.Lock()
	return lock.Unlock
}

// syncWorkflowRepo performs the same sparse, shallow checkout as the stock
// spindle without git clone --revision, which requires Git 2.49 while Debian
// trixie currently ships 2.47.
func syncWorkflowRepo(ctx context.Context, cloneURL, repoPath, revision string) error {
	gitDir := filepath.Join(repoPath, ".git")
	// Initialize manually rather than cloning so Debian's pre-2.49 Git can use
	// the same workflow. A non-Git cache directory is always disposable because
	// repoPath is a Tack-owned hash beneath TACK_REPO_DIR.
	if _, err := os.Stat(gitDir); errors.Is(err, os.ErrNotExist) {
		if err := os.RemoveAll(repoPath); err != nil {
			return fmt.Errorf("remove invalid repository cache: %w", err)
		}
		if err := runGit(ctx, "init", repoPath); err != nil {
			return err
		}
		if err := runGit(ctx, "-C", repoPath, "remote", "add", "origin", cloneURL); err != nil {
			return err
		}
		if err := runGit(
			ctx, "-C", repoPath, "sparse-checkout", "set", "--no-cone", "/.tangled/workflows",
		); err != nil {
			return err
		}
	} else if err != nil {
		return fmt.Errorf("inspect repository cache: %w", err)
	}
	// Repository DIDs can migrate between knots. Refresh origin on every sync
	// rather than letting a warm cache silently keep fetching the old host.
	if err := runGit(ctx, "-C", repoPath, "remote", "set-url", "origin", cloneURL); err != nil {
		return err
	}
	if err := runGit(
		ctx, "-C", repoPath, "fetch", "--depth=1", "--filter=tree:0", "origin", revision,
	); err != nil {
		return err
	}
	return runGit(ctx, "-C", repoPath, "checkout", "--detach", "FETCH_HEAD")
}

// runGit executes one bounded-by-context Git operation and includes stderr in
// failures. The latter is essential for diagnosing protocol/auth failures from
// remote knots; os/exec's bare ExitError is otherwise nearly content-free.
func runGit(ctx context.Context, args ...string) error {
	command := exec.CommandContext(ctx, "git", args...)
	output, err := command.CombinedOutput()
	if err != nil {
		return fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(output)))
	}
	return nil
}

// fetchLatestPullRevision downloads the newest pull round's format-patch blob
// from its author's PDS and returns the final commit SHA. Both compressed and
// expanded sizes are bounded because pull records are untrusted network input.
func (p *pipelineService) fetchLatestPullRevision(
	ctx context.Context,
	did, rkey string,
	record *tangled.RepoPull,
) (string, error) {
	if len(record.Rounds) == 0 {
		return "", errors.New("pull request has no rounds")
	}
	round := record.Rounds[len(record.Rounds)-1]
	if round == nil || round.PatchBlob == nil {
		return "", errors.New("latest pull request round has no patch blob")
	}

	parsedDID, err := syntax.ParseDID(did)
	if err != nil {
		return "", fmt.Errorf("parse pull request owner DID: %w", err)
	}
	// Automatic pull processing accepts PLC identities only. did:web
	// resolution starts from an author-controlled host, before we can apply
	// the protected HTTP transport used for the PDS blob itself.
	if !strings.HasPrefix(parsedDID.String(), "did:plc:") {
		return "", fmt.Errorf("unsupported pull request owner DID method %q", parsedDID)
	}
	ident, err := p.directory.LookupDID(ctx, parsedDID)
	if err != nil {
		return "", fmt.Errorf("resolve pull request owner: %w", err)
	}
	pds := ident.PDSEndpoint()
	if pds == "" {
		return "", errors.New("pull request owner has no PDS endpoint")
	}
	blobURL, err := url.Parse(pds)
	if err != nil {
		return "", fmt.Errorf("parse PDS endpoint: %w", err)
	}
	if blobURL.Host == "" ||
		(blobURL.Scheme != "https" && !(p.cfg.Dev && blobURL.Scheme == "http")) {
		return "", fmt.Errorf("unsafe PDS endpoint %q", pds)
	}
	blobURL.Path = strings.TrimRight(blobURL.Path, "/") + "/xrpc/com.atproto.sync.getBlob"
	query := blobURL.Query()
	query.Set("did", did)
	query.Set("cid", round.PatchBlob.Ref.String())
	blobURL.RawQuery = query.Encode()

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, blobURL.String(), nil)
	if err != nil {
		return "", fmt.Errorf("create blob request: %w", err)
	}
	response, err := p.httpClient.Do(request)
	if err != nil {
		return "", fmt.Errorf("fetch pull request blob: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("fetch pull request blob: unexpected HTTP status %s", response.Status)
	}

	// Limit the compressed stream and the expanded patch independently: a small
	// gzip body can otherwise expand enough to exhaust the spindle.
	reader, err := gzip.NewReader(io.LimitReader(response.Body, 16<<20))
	if err != nil {
		return "", fmt.Errorf("open pull request patch: %w", err)
	}
	defer reader.Close()
	patch, err := io.ReadAll(io.LimitReader(reader, (64<<20)+1))
	if err != nil {
		return "", fmt.Errorf("decompress pull request patch: %w", err)
	}
	if len(patch) > 64<<20 {
		return "", errors.New("pull request patch exceeds 64 MiB")
	}
	if !patchutil.IsFormatPatch(string(patch)) {
		return "", errors.New("pull request patch is not a format-patch")
	}
	patches, err := patchutil.ExtractPatches(string(patch))
	if err != nil {
		return "", fmt.Errorf("parse pull request patch: %w", err)
	}
	if len(patches) == 0 {
		return "", errors.New("pull request patch contains no commits")
	}
	return patches[len(patches)-1].SHA, nil
}

// defaultBranch asks the authoritative knot for display/provider metadata used
// by manual runs. Failure is intentionally soft: the requested SHA remains
// sufficient to run, and providers can tolerate an empty branch.
func (p *pipelineService) defaultBranch(ctx context.Context, repo *RepoRef) string {
	host, noTLS, err := hostutil.ParseHostname(repo.Knot)
	if err != nil {
		return ""
	}
	scheme := "https"
	if noTLS || p.cfg.Dev {
		scheme = "http"
	}
	client := &indigoxrpc.Client{Host: fmt.Sprintf("%s://%s", scheme, host)}
	result, err := tangled.RepoGetDefaultBranch(ctx, client, repo.RepoDID)
	if err != nil {
		return ""
	}
	return result.Name
}

// newPipelineHTTPClient builds the client used for author-controlled PDS and
// knot URLs. Production dials reject loopback, private, link-local, multicast,
// and unspecified addresses after DNS resolution; dev mode deliberately allows
// them for local-infrastructure testing. Redirects receive the same scheme and
// address checks through this transport.
func newPipelineHTTPClient(dev bool) *http.Client {
	dialer := &net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}
	if !dev {
		dialer.Control = func(_, address string, _ syscall.RawConn) error {
			host, _, err := net.SplitHostPort(address)
			if err != nil {
				return fmt.Errorf("invalid dial address %q: %w", address, err)
			}
			ip := net.ParseIP(host)
			if ip == nil || ip.IsLoopback() || ip.IsPrivate() ||
				ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
				ip.IsMulticast() || ip.IsUnspecified() {
				return fmt.Errorf("refusing to dial reserved address %q", host)
			}
			return nil
		}
	}
	client := &http.Client{
		Timeout:   15 * time.Second,
		Transport: &http.Transport{DialContext: dialer.DialContext},
	}
	client.CheckRedirect = func(request *http.Request, via []*http.Request) error {
		if len(via) >= 10 {
			return errors.New("too many redirects")
		}
		if request.URL.Host == "" ||
			(request.URL.Scheme != "https" && !(dev && request.URL.Scheme == "http")) {
			return fmt.Errorf("refusing unsafe redirect to %q", request.URL)
		}
		return nil
	}
	return client
}

// authorizeRepoActor first checks the local collaborator mirror, then queries
// the authoritative knot on a miss. The fallback makes pre-existing knot ACLs
// usable after a fresh Tack install, whose event cursor only sees later changes;
// a successful lookup is mirrored locally for subsequent requests.
func (p *pipelineService) authorizeRepoActor(
	ctx context.Context,
	repo *RepoRef,
	actor string,
) (bool, error) {
	authorized, err := p.store.IsRepoActorAuthorized(
		ctx, repo.RepoDID, repo.OwnerDID, actor,
	)
	if err != nil || authorized {
		return authorized, err
	}

	host, noTLS, err := hostutil.ParseHostname(repo.Knot)
	if err != nil {
		return false, err
	}
	scheme := "https"
	if noTLS || p.cfg.Dev {
		scheme = "http"
	}
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	client := &indigoxrpc.Client{
		Host: fmt.Sprintf("%s://%s", scheme, host), Client: p.httpClient,
	}
	cursor := ""
	for page := 0; page < 10; page++ {
		result, err := tangled.RepoListCollaborators(
			queryCtx, client, cursor, 100, "asc", repo.RepoDID,
		)
		if err != nil {
			return false, fmt.Errorf("query knot collaborators: %w", err)
		}
		for _, item := range result.Items {
			if item != nil && item.Subject == actor {
				if err := p.store.ApplyKnotCollaborator(ctx, repo, actor, true); err != nil {
					return false, err
				}
				return true, nil
			}
		}
		if result.Cursor == nil || *result.Cursor == "" {
			return false, nil
		}
		cursor = *result.Cursor
	}
	return false, errors.New("knot collaborator list exceeded 1000 entries")
}

// managedRepo performs a cheap local assignment check before network
// verification, then requires the mirrored record to agree with the canonical
// knot, owner, and record key resolved from the repo DID. This two-phase shape
// prevents unrelated global events from forcing verifier traffic and prevents
// a forged sibling record from shadowing the real repository.
func (p *pipelineService) managedRepo(ctx context.Context, repoDID string) (*RepoRef, error) {
	managed, err := p.store.HasManagedRepo(
		ctx, repoDID, p.cfg.Hostname, p.cfg.OwnerDID,
	)
	if err != nil || !managed {
		return nil, err
	}
	verified, err := p.verifiedRepo(ctx, repoDID)
	if err != nil {
		return nil, err
	}
	return p.store.ManagedRepoByDID(
		ctx,
		repoDID,
		p.cfg.Hostname,
		p.cfg.OwnerDID,
		verified.Knot,
		verified.OwnerDID,
		verified.Rkey,
	)
}

// verifiedRepo resolves canonical repository identity with a five-minute cache.
// Verification gets one short-delay retry because eventconsumer advances its
// cursor even when processing returns an error; retrying here avoids losing a
// push on a single transient resolver failure.
func (p *pipelineService) verifiedRepo(ctx context.Context, value string) (*RepoRef, error) {
	p.verifiedMu.Lock()
	cached, ok := p.verifiedRepos[value]
	p.verifiedMu.Unlock()
	if ok && time.Now().Before(cached.expires) {
		copy := *cached.repo
		return &copy, nil
	}

	repoDID, err := repoident.NewRepoDid(value)
	if err != nil {
		return nil, err
	}
	verified, err := p.verify(ctx, repoDID)
	if err != nil {
		timer := time.NewTimer(250 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
		verified, err = p.verify(ctx, repoDID)
		if err != nil {
			return nil, fmt.Errorf("verify repository %q: %w", value, err)
		}
	}
	repo := &RepoRef{
		OwnerDID: verified.OwnerDid.String(),
		Rkey:     verified.Rkey,
		Knot:     verified.KnotURL.Host,
		RepoDID:  verified.RepoDid.String(),
	}
	p.verifiedMu.Lock()
	p.verifiedRepos[value] = verifiedRepoCache{repo: repo, expires: time.Now().Add(5 * time.Minute)}
	p.verifiedMu.Unlock()
	copy := *repo
	return &copy, nil
}

// invalidateRepoVerificationCache drops canonical identities after any mirrored
// repo mutation. The cache is small and repo updates are rare, so clearing it is
// simpler and safer than trying to infer which DID an update/delete displaced.
func (p *pipelineService) invalidateRepoVerificationCache() {
	p.verifiedMu.Lock()
	clear(p.verifiedRepos)
	p.verifiedMu.Unlock()
}

// pipelineTriggerRepo converts the persisted repository identity into the
// deprecated trigger shape still used internally by Tangled's compiler and
// provider environment helpers.
func pipelineTriggerRepo(repo *RepoRef, defaultBranch string) *tangled.Pipeline_TriggerRepo {
	rkey := repo.Rkey
	repoDID := repo.RepoDID
	return &tangled.Pipeline_TriggerRepo{
		DefaultBranch: defaultBranch,
		Did:           repo.OwnerDID,
		Knot:          repo.Knot,
		Repo:          &rkey,
		RepoDid:       &repoDID,
	}
}

// pointerValue collapses an optional lexicon string to the store/compiler's
// empty-string convention.
func pointerValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

// skipsCI recognizes both push-option spellings supported by current knots.
func skipsCI(options []string) bool {
	for _, option := range options {
		if option == "skip-ci" || option == "ci-skip" {
			return true
		}
	}
	return false
}
