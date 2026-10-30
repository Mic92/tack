package main

// This file decides whether Tack serves a repository, meaning Tack is the spindle (Tangled CI
// service) the repository has chosen and the repository's owner is allowed to use this Tack.
//
// The two generations of Tangled assign a spindle in different ways:
//
//   - Legacy (Go knot, Tangled v1.16 and earlier): the owner's PDS publishes a sh.tangled.repo
//     record whose spindle field is Tack's hostname. Jetstream (a service that re-serves the
//     atproto firehose as JSON record events) delivers those records, and Tack mirrors them into
//     the repos table.
//   - Current (knot2, Tangled v1.20): the knot publishes an org.tangled.repo.manifest record,
//     the repo manifest, inside the repository DID's own atproto repo. Its spindle field is
//     Tack's did:web. Nothing about the assignment reaches Tack in advance. knot2 starts calling
//     org.tangled.event.notifyEvent on pushes, and the Tangled web app starts querying Tack.
//
// servedRepo handles both by resolving the repository DID and reading from its knot. When the
// knot publishes a manifest, the manifest wins, because it is the same record knot2 reads to
// decide which spindle receives a push.

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/bluesky-social/indigo/atproto/syntax"
	"tangled.org/core/hostutil"

	"go.mitchellh.com/tack/internal/orgtangled"
)

// errLookupBusy reports that every slot in the public lookup pool is taken. XRPC handlers map it
// to 503 Service Unavailable rather than 500 so a retrying client backs off instead of treating
// the repository as broken.
var errLookupBusy = errors.New("too many repository lookups in flight")

// servedRepoCache is one cached served-repo decision. A nil repo records "not served".
type servedRepoCache struct {
	repo    *RepoRef
	expires time.Time
}

// Cache lifetimes and sizes for served-repo decisions.
const (
	// servedRepoTTL is how long a positive decision is reused.
	servedRepoTTL = 5 * time.Minute
	// notServedRepoTTL is shorter so a refusal is reconsidered sooner. The identity behind a
	// manifest decision (including the spindle it names) is cached separately by verifiedRepo
	// for five minutes, so a repository whose manifest just switched to Tack can take up to
	// that long to be served.
	notServedRepoTTL = time.Minute
	// repoCacheCapacity bounds each repository cache, because public routes let anyone choose
	// the DIDs that get cached.
	repoCacheCapacity = 4096
)

// servedRepo returns the canonical identity of repoDID when Tack is its spindle and its owner is
// admitted, or nil when Tack does not serve it. A returned error means no decision was reached:
// the DID is malformed, or a resolver, knot, PDS, or database call failed. An error is never a
// policy denial, so callers refuse on nil and treat an error as a failure to decide. Errors are
// not cached.
//
// Callers must be trusted trigger paths: authenticated procedures, and legacy inputs already
// gated on a local assignment (see managedRepo). Unauthenticated routes use lookupPublicRepo,
// whose small bounded pool keeps public traffic from starving these callers.
func (p *pipelineService) servedRepo(ctx context.Context, repoDID string) (*RepoRef, error) {
	if repo, ok := p.cachedServedRepo(repoDID); ok {
		return repo, nil
	}
	repo, err := p.decideServedRepo(ctx, repoDID)
	if err != nil {
		return nil, err
	}
	p.cacheServedRepo(repoDID, repo)
	return cloneRepoRef(repo), nil
}

// lookupPublicRepo is servedRepo for unauthenticated callers. It only looks up did:plc
// repositories. Resolving a did:web fetches its DID document from the host named in the DID
// through the identity directory, which does not use Tack's SSRF-guarded HTTP client, so an
// anonymous caller could aim Tack at any host. (The knot named in a did:plc document is also
// caller-chosen, but Tack fetches from it with the guarded client.) Uncached lookups take a slot
// from a small dedicated pool and return errLookupBusy instead of waiting when the pool is full.
//
// Failures are logged and cached as "not served" so one bad DID cannot hold a pool slot over and
// over. The cache is shared with servedRepo, so such a failure also hides the repository from
// trusted callers until the entry expires.
func (p *pipelineService) lookupPublicRepo(ctx context.Context, repoDID string) (*RepoRef, error) {
	if repo, ok := p.cachedServedRepo(repoDID); ok {
		return repo, nil
	}
	if !strings.HasPrefix(repoDID, "did:plc:") {
		return nil, nil
	}
	select {
	case p.lookupSlots <- struct{}{}:
		defer func() { <-p.lookupSlots }()
	default:
		return nil, errLookupBusy
	}
	repo, err := p.decideServedRepo(ctx, repoDID)
	if err != nil {
		loggerFrom(ctx).Debug("public repository lookup failed", "repo_did", repoDID, "err", err)
		repo = nil
	}
	p.cacheServedRepo(repoDID, repo)
	return cloneRepoRef(repo), nil
}

// cachedServedRepo returns the cached decision for repoDID and true, or false when there is no
// unexpired entry. The returned identity is a copy.
func (p *pipelineService) cachedServedRepo(repoDID string) (*RepoRef, bool) {
	p.repoCacheMu.Lock()
	defer p.repoCacheMu.Unlock()
	cached, ok := p.servedRepos[repoDID]
	if !ok || !time.Now().Before(cached.expires) {
		return nil, false
	}
	return cloneRepoRef(cached.repo), true
}

// cacheServedRepo records a decision for repoDID. Refusals (nil repo) get the shorter
// notServedRepoTTL (see its comment for why that bounds pickup only loosely). When the cache is
// full of live entries, the decision is dropped rather than stored.
func (p *pipelineService) cacheServedRepo(repoDID string, repo *RepoRef) {
	ttl := servedRepoTTL
	if repo == nil {
		ttl = notServedRepoTTL
	}
	p.repoCacheMu.Lock()
	defer p.repoCacheMu.Unlock()
	if !makeCacheRoom(p.servedRepos, repoCacheCapacity, func(entry servedRepoCache) time.Time {
		return entry.expires
	}) {
		return
	}
	p.servedRepos[repoDID] = servedRepoCache{repo: cloneRepoRef(repo), expires: time.Now().Add(ttl)}
}

// makeCacheRoom reports whether one more entry fits in cache. Once cache reaches capacity it
// first deletes expired entries, as reported by expires. A cache full of live entries is left
// alone instead of evicting one, which costs only repeated lookups and keeps an attacker from
// flushing useful entries. The caller must hold the lock that guards cache.
func makeCacheRoom[V any](cache map[string]V, capacity int, expires func(V) time.Time) bool {
	if len(cache) < capacity {
		return true
	}
	now := time.Now()
	for key, entry := range cache {
		if !now.Before(expires(entry)) {
			delete(cache, key)
		}
	}
	return len(cache) < capacity
}

// cloneRepoRef returns a shallow copy of repo, or nil for nil. Cached identities are copied on
// the way in and out so callers cannot mutate the cache. A shallow copy is enough because
// RepoRef holds only strings and a bool.
func cloneRepoRef(repo *RepoRef) *RepoRef {
	if repo == nil {
		return nil
	}
	clone := *repo
	return &clone
}

// decideServedRepo makes the uncached served-repo decision behind servedRepo and
// lookupPublicRepo. It resolves canonical identity with verifiedRepo, then sends knot2
// repositories (identity read from a repo manifest) through admitManifestRepo and Go knot
// repositories through the mirrored legacy assignment.
func (p *pipelineService) decideServedRepo(ctx context.Context, repoDID string) (*RepoRef, error) {
	if _, err := syntax.ParseDID(repoDID); err != nil {
		return nil, fmt.Errorf("invalid repository DID %q: %w", repoDID, err)
	}
	verified, err := p.verifiedRepo(ctx, repoDID)
	if err != nil {
		return nil, err
	}
	if verified.FromManifest {
		return p.admitManifestRepo(ctx, verified)
	}
	// Go knot: the mirrored assignment must match the canonical knot, owner, and record key
	// resolved from the DID, so a forged sibling record cannot shadow the real repository.
	// The query also requires the owner to be the spindle owner or a member.
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

// admitManifestRepo applies Tack's admission rules to a knot2 repository whose identity came
// from its repo manifest. It returns verified when the manifest names Tack's did:web, the owner
// is admitted, and the owner confirms the repository. It returns nil for any refusal.
//
// The knot writes the manifest, and a knot controls the DID documents of the repositories it
// creates. Trusting the declared owner alone would let any knot claim that the spindle owner
// owns an arbitrary repository and run CI on the operator's provider accounts. So the owner must
// (1) be the spindle owner or a member (IsAuthorizedActor, the same rule the ManagedRepoByDID
// query applies to legacy repositories) and (2) confirm the repository from their own PDS
// (ownerDeclares).
func (p *pipelineService) admitManifestRepo(ctx context.Context, verified *RepoRef) (*RepoRef, error) {
	logger := loggerFrom(ctx).With("component", "pipeline", "repo_did", verified.RepoDID)
	if verified.Spindle != p.cfg.DID().String() {
		logger.Debug("repository manifest names another spindle", "spindle", verified.Spindle)
		return nil, nil
	}
	admitted, err := p.store.IsAuthorizedActor(
		ctx, p.cfg.Hostname, p.cfg.OwnerDID, verified.OwnerDID,
	)
	if err != nil {
		return nil, err
	}
	if !admitted {
		logger.Warn("ignoring repository from non-member owner", "owner", verified.OwnerDID)
		return nil, nil
	}
	declared, err := p.ownerDeclares(ctx, verified)
	if err != nil {
		return nil, fmt.Errorf("confirm repository owner: %w", err)
	}
	if !declared {
		logger.Warn("repository owner does not declare this repository", "owner", verified.OwnerDID)
		return nil, nil
	}
	return verified, nil
}

// ownerDeclaresRepo reports whether repo's owner confirms, from their own side, that the
// repository belongs to them. It is the production implementation of pipelineService.ownerDeclares.
//
// A mirrored legacy sh.tangled.repo record already proves this, because Jetstream rows are keyed
// by the DID that published them (see store.OwnerClaimsRepo). Otherwise the owner's PDS must
// publish org.tangled.repo.declaration/<rkey> whose repo field is the repository DID. A record
// the PDS positively reports as missing is a plain false. Other fetch failures are errors.
//
// The owner DID is resolved with p.directory, which follows did:web to any host without Tack's
// SSRF guard. That is acceptable here, unlike for pull request authors (see
// fetchLatestPullRevision, which accepts only did:plc), because admitManifestRepo has already
// admitted the owner (spindle owner or member) before this runs.
func (p *pipelineService) ownerDeclaresRepo(ctx context.Context, repo *RepoRef) (bool, error) {
	claimed, err := p.store.OwnerClaimsRepo(ctx, repo.OwnerDID, repo.RepoDID)
	if err != nil || claimed {
		return claimed, err
	}
	owner, err := syntax.ParseDID(repo.OwnerDID)
	if err != nil {
		return false, err
	}
	ident, err := p.directory.LookupDID(ctx, owner)
	if err != nil {
		return false, fmt.Errorf("resolve owner %s: %w", owner, err)
	}
	pds, err := url.Parse(ident.PDSEndpoint())
	if err != nil || pds.Host == "" ||
		(pds.Scheme != "https" && !(p.cfg.Dev && pds.Scheme == "http")) {
		return false, fmt.Errorf("owner %s has no usable PDS endpoint", owner)
	}
	var declaration orgtangled.RepoDeclaration
	err = getRecord(
		ctx, p.httpClient, pds.String(), repo.OwnerDID,
		orgtangled.NSIDRepoDeclaration, repo.Rkey, &declaration,
	)
	if errors.Is(err, errRecordNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return declaration.Repo == repo.RepoDID, nil
}

// knotCollaborators returns the DIDs of repo's knot2 collaborators. It is the production
// implementation of pipelineService.listCollaborators. knot2 stores each grant as an
// org.tangled.graph.collaborator record in the repository DID's repo, keyed by the collaborator
// DID. The body holds only a createdAt timestamp, so only the record keys are read.
func (p *pipelineService) knotCollaborators(ctx context.Context, repo *RepoRef) ([]string, error) {
	base, err := p.knotBaseURL(repo.Knot)
	if err != nil {
		return nil, err
	}
	return listRecordKeys(ctx, p.httpClient, base, repo.RepoDID, orgtangled.NSIDCollaborator)
}

// knotBaseURL turns a stored knot host into the origin used for XRPC calls to that knot. It uses
// https unless the host is marked insecure or Tack runs in dev mode, so local infrastructure keeps
// working. hostutil.ParseHostname marks a host insecure when it has an explicit http or ws scheme,
// or is a bare localhost:port.
func (p *pipelineService) knotBaseURL(knot string) (string, error) {
	host, noTLS, err := hostutil.ParseHostname(knot)
	if err != nil {
		return "", fmt.Errorf("parse knot hostname: %w", err)
	}
	scheme := "https"
	if noTLS || p.cfg.Dev {
		scheme = "http"
	}
	return fmt.Sprintf("%s://%s", scheme, host), nil
}
