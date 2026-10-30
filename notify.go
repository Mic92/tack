package main

// This file implements org.tangled.event.notifyEvent, the XRPC procedure that triggers CI in
// Tangled v1.20. It carries two kinds of event: pushes reported by knot2 and manual runs
// that a person requests, usually through the Tangled web app.
//
// knot2 has no /events stream for spindles to subscribe to. After a push it POSTs one
// notifyEvent per updated ref to the spindle named in the repo manifest, signed with service
// auth from the knot's did:web. Delivery is at least once, from a persistent outbox that sends
// one request at a time per spindle, in order. knot2 reads the response status as a verdict:
//
//   - 2xx: delivered. {} means nothing ran, and {"ci":{"pipelines":[id]}} reports a run.
//   - 4xx other than 408 and 429: rejected for good. knot2 drops the event.
//   - Anything else: retried with backoff for up to three days. While it waits, every later
//     event from that knot to this spindle waits behind it.
//
// Two rules follow. Only problems a retry can never fix may answer 4xx, because a 4xx loses
// the push. Transient problems should answer 5xx, but must clear quickly, because a stuck 5xx
// stalls the whole queue. A push for a repository Tack does not serve is a 200 {}, which is
// also how the upstream spindle answers.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/bluesky-social/indigo/atproto/syntax"
	"tangled.org/core/api/tangled"
	"tangled.org/core/workflow"
	"tangled.org/core/xrpc/serviceauth"

	"go.mitchellh.com/tack/internal/orgtangled"
)

// pushWorkTimeout bounds the detached work for one push: cloning, compiling, persisting the
// pipeline, and handing it to the provider. knot2 abandons a request after 60 seconds and
// retries it later. The work keeps running past that point so the retry can join it and
// collect the result, instead of starting over and timing out again.
const pushWorkTimeout = 5 * time.Minute

// pushCall is one push being processed, shared by every concurrent delivery of that same
// push. done is closed once rkey and err are final. Readers must wait on done before reading
// either field. An empty rkey with a nil err means the push ran nothing.
type pushCall struct {
	done chan struct{}
	rkey string
	err  error
}

// notifyEventHandler serves org.tangled.event.notifyEvent. It must be mounted behind the
// [serviceAuth] middleware, which verifies the token signature, audience, method, and expiry
// and records the caller. A request that reaches it without a caller is refused with 403.
//
// The handler checks the shared envelope (body, repository DID, event kind) and then hands
// pushes to servePushEvent and manual runs to serveManualTrigger. replay is applied to pushes
// only. For pushes the checks run in the same order as in the upstream spindle, so a knot gets
// the same status code from Tack as from a stock spindle for the same bad request.
func notifyEventHandler(
	logger *slog.Logger,
	pipelines *pipelineService,
	replay *replayGuard,
) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, ok := principalFrom(r.Context())
		if !ok {
			writeXRPCError(w, http.StatusForbidden, "AuthRequired", "service auth is required")
			return
		}
		var input orgtangled.NotifyEventInput
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&input); err != nil {
			writeXRPCError(w, http.StatusBadRequest, "InvalidRequest", "invalid request body")
			return
		}
		if _, err := syntax.ParseDID(input.Repo); err != nil {
			writeXRPCError(w, http.StatusBadRequest, "InvalidRequest",
				fmt.Sprintf("invalid repo DID %q", input.Repo))
			return
		}
		logger := logger.With("repo_did", input.Repo, "iss", principal.Issuer)
		ctx := loggerInto(r.Context(), logger)
		switch {
		case input.Event != nil && input.Event.Push != nil:
			servePushEvent(ctx, w, logger, pipelines, replay, principal, input.Repo, input.Event.Push)
		case input.Event != nil && input.Event.Manual != nil:
			serveManualTrigger(ctx, w, logger, pipelines, principal, input.Repo, input.Event.Manual)
		default:
			// The event union has no pull request member on purpose. In v1.20, pull request
			// pipelines start from PDS records, never from a knot notification.
			writeXRPCError(w, http.StatusBadRequest, "InvalidRequest",
				"event is not a push or manual trigger")
		}
	})
}

// servePushEvent checks that a push was reported by the knot that hosts the repository, then
// runs it. Every status it writes is chosen for its effect on the knot2 outbox: 400 and 403
// for requests that will never become valid, 422 for pushes that can never build, and 503 or
// 500 for failures a later delivery may get past.
func servePushEvent(
	ctx context.Context,
	w http.ResponseWriter,
	logger *slog.Logger,
	pipelines *pipelineService,
	replay *replayGuard,
	principal *serviceAuthPrincipal,
	repoDID string,
	push *orgtangled.PushEvent,
) {
	if push.Repo != repoDID {
		writeXRPCError(w, http.StatusBadRequest, "InvalidRequest", "push event names a different repo")
		return
	}
	if push.Actor == nil || *push.Actor == "" {
		writeXRPCError(w, http.StatusBadRequest, "InvalidRequest", "push event names no actor")
		return
	}
	if _, err := syntax.ParseDID(*push.Actor); err != nil {
		writeXRPCError(w, http.StatusBadRequest, "InvalidRequest", "push actor is not a DID")
		return
	}

	// Only the knot that hosts the repository may report its pushes. Knots are did:web
	// services. The hosting knot is whatever the repository's DID document says, never
	// something the request body or the calling knot claims. This check runs before any
	// knot-supplied record is read, so a foreign knot cannot make Tack act on its data.
	if principal.Issuer.Method() != "web" {
		writeXRPCError(w, http.StatusForbidden, "Auth", fmt.Sprintf("%s is not a knot", principal.Issuer))
		return
	}
	knot, err := pipelines.resolveKnot(ctx, repoDID)
	if err != nil {
		// This is usually a resolver outage or a DID document in the middle of a migration.
		// A 4xx would make the knot drop the push, so answer 503 and let it retry.
		logger.Warn("resolve repository knot for push", "err", err)
		writeXRPCError(w, http.StatusServiceUnavailable, "RepoKnotUnavailable",
			"could not resolve the repository's knot")
		return
	}
	if principal.Issuer != serviceauth.DidWeb(knot) {
		writeXRPCError(w, http.StatusForbidden, "Auth",
			fmt.Sprintf("%s does not host %s", principal.Issuer, repoDID))
		return
	}
	// A replayed token would run the push a second time. knot2 mints a fresh token for each
	// delivery attempt, so honest retries pass this check. A full guard is load, not a
	// judgment on the token, so it answers 503 to keep the push queued.
	if err := replay.Admit(principal); err != nil {
		if errors.Is(err, errReplayFull) {
			writeXRPCError(w, http.StatusServiceUnavailable, "Busy", err.Error())
			return
		}
		writeXRPCError(w, http.StatusForbidden, "Auth", err.Error())
		return
	}

	// 422 is reserved for permanentError so knot2 drops a push that can never build instead of
	// retrying it for days at the head of its queue. Everything else is answered as retryable.
	rkey, err := pipelines.triggerPush(ctx, push)
	var permanent *permanentError
	switch {
	case errors.As(err, &permanent):
		logger.Warn("push refused", "err", err, "ref", push.Ref, "commit", push.NewSHA)
		writeXRPCError(w, http.StatusUnprocessableEntity, "PushRefused", err.Error())
		return
	case errors.Is(err, errLookupBusy):
		writeXRPCError(w, http.StatusServiceUnavailable, "Busy", err.Error())
		return
	case err != nil:
		logger.Error("push failed; knot will retry", "err", err, "ref", push.Ref, "commit", push.NewSHA)
		writeXRPCError(w, http.StatusInternalServerError, "InternalError", "failed to process push")
		return
	}
	writeNotifyOutput(w, rkey)
}

// serveManualTrigger starts a manual run requested through the Tangled web app. The caller is
// a person or a repo token, so the knot and replay checks for pushes do not apply.
// pipelineService.authorizeRepo decides whether the caller may run CI on the repository, and
// its doc comment explains repo tokens. Unlike a push, a manual run is compiled on the request
// context and is not deduplicated: its caller waits for the answer and is not an outbox that
// redelivers.
func serveManualTrigger(
	ctx context.Context,
	w http.ResponseWriter,
	logger *slog.Logger,
	pipelines *pipelineService,
	principal *serviceAuthPrincipal,
	repoDID string,
	manual *orgtangled.ManualTrigger,
) {
	if manual.Repo != repoDID {
		writeXRPCError(w, http.StatusBadRequest, "InvalidRequest", "manual trigger names a different repo")
		return
	}
	if !isObjectID(manual.Commit) {
		writeXRPCError(w, http.StatusBadRequest, "InvalidRequest", "commit must be a 40 or 64 character hex SHA")
		return
	}
	target, err := pipelines.authorizeRepo(ctx, repoDID, principal.Issuer.String())
	if err != nil {
		writeError(w, logger, err)
		return
	}
	rkey, err := pipelines.runManual(ctx, target, manual)
	if errors.Is(err, errNoMatchingWorkflows) {
		writeXRPCError(w, http.StatusBadRequest, "InvalidRequest", err.Error())
		return
	}
	if err != nil {
		logger.Error("manual trigger failed", "err", err)
		writeXRPCError(w, http.StatusInternalServerError, "InternalError", "failed to start pipeline")
		return
	}
	writeNotifyOutput(w, rkey)
}

// writeNotifyOutput answers 200 with the started pipeline's record key, or with {} when rkey
// is empty because nothing ran. Both are 2xx, so a knot treats the event as delivered.
func writeNotifyOutput(w http.ResponseWriter, rkey string) {
	output := orgtangled.NotifyEventOutput{}
	if rkey != "" {
		output.CI = &orgtangled.NotifyEventCIOutput{Pipelines: []string{rkey}}
	}
	writeJSON(w, http.StatusOK, output)
}

// triggerPush decides whether a push should build and, if so, runs it. It returns the
// pipeline record key, or an empty key with a nil error when nothing ran. Pushes to refs other
// than branches and tags, ref deletions, pushes with a CI skip push option, and pushes for
// repositories Tack does not serve all run nothing and succeed, so knot2 counts them as
// delivered and moves on to the next event. Malformed SHAs, and commits the knot refuses to
// serve (reported by loadWorkflows through runPush), return a permanentError because no retry
// can repair them.
//
// The cheap checks run before servedRepo so that ignored pushes cost no network lookups.
func (p *pipelineService) triggerPush(ctx context.Context, push *orgtangled.PushEvent) (string, error) {
	logger := loggerFrom(ctx).With("ref", push.Ref, "commit", push.NewSHA)
	if !strings.HasPrefix(push.Ref, "refs/heads/") && !strings.HasPrefix(push.Ref, "refs/tags/") {
		logger.Debug("ignoring push to a non-branch, non-tag ref")
		return "", nil
	}
	if !isObjectID(push.OldSHA) || !isObjectID(push.NewSHA) {
		return "", &permanentError{fmt.Errorf("push SHAs must be 40 or 64 character hex object IDs")}
	}
	if isZeroSHA(push.NewSHA) {
		logger.Debug("ignoring ref deletion")
		return "", nil
	}
	if skipsCI(push.PushOptions) {
		logger.Info("push requested CI skip")
		return "", nil
	}

	repo, err := p.servedRepo(ctx, push.Repo)
	if err != nil {
		return "", err
	}
	if repo == nil {
		logger.Debug("ignoring push for a repository Tack does not serve")
		return "", nil
	}
	return p.coalescePush(ctx, repo, push)
}

// coalescePush runs a push once no matter how many deliveries of it arrive while it is in
// progress. A push is identified by its content (repository, ref, old SHA, new SHA), not by
// its token, because knot2 signs each delivery afresh.
//
// The work runs on its own goroutine under rootCtx and pushWorkTimeout, not on the request
// context, so a knot2 retry after its 60 second request timeout joins the running work instead
// of starting a clone that would time out again (see pushWorkTimeout). If ctx ends first,
// coalescePush returns ctx.Err() and the work continues.
//
// This only deduplicates pushes that overlap in time. runPush covers redeliveries that arrive
// after the work has finished.
func (p *pipelineService) coalescePush(
	ctx context.Context,
	repo *RepoRef,
	push *orgtangled.PushEvent,
) (string, error) {
	key := strings.Join([]string{repo.RepoDID, push.Ref, push.OldSHA, push.NewSHA}, " ")
	p.inflightPushesMu.Lock()
	call, running := p.inflightPushes[key]
	if !running {
		call = &pushCall{done: make(chan struct{})}
		p.inflightPushes[key] = call
		logger := loggerFrom(ctx)
		go func() {
			workCtx, cancel := context.WithTimeout(loggerInto(p.rootCtx, logger), pushWorkTimeout)
			call.rkey, call.err = p.runPush(workCtx, repo, push)
			cancel()
			p.inflightPushesMu.Lock()
			delete(p.inflightPushes, key)
			p.inflightPushesMu.Unlock()
			close(call.done)
		}()
	}
	p.inflightPushesMu.Unlock()

	select {
	case <-call.done:
		return call.rkey, call.err
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// runPush builds the push trigger and starts a pipeline for it. It is the durable half of
// push deduplication (coalescePush is the in-memory half): when the store already holds a
// pipeline for exactly this push, runPush reports that pipeline instead of starting a second
// provider build. That covers knot2 redelivering a push after a timeout or 5xx answer that
// came after the pipeline had already been created.
//
// A push that matches no workflow returns an empty key and a nil error.
func (p *pipelineService) runPush(
	ctx context.Context,
	repo *RepoRef,
	push *orgtangled.PushEvent,
) (string, error) {
	logger := loggerFrom(ctx).With("component", "pipeline", "ref", push.Ref, "commit", push.NewSHA)
	existing, err := p.store.PipelineForPush(ctx, repo.RepoDID, push.Ref, push.OldSHA, push.NewSHA)
	if err != nil {
		return "", err
	}
	if existing != "" {
		logger.Info("push already has a pipeline", "pipeline", existing)
		return existing, nil
	}

	defaultBranch := p.resolveDefaultBranch(ctx, repo)
	trigger := &tangled.Pipeline_TriggerMetadata{
		Kind: string(workflow.TriggerKindPush),
		Push: &tangled.Pipeline_PushTriggerData{
			Ref:    push.Ref,
			OldSha: push.OldSHA,
			NewSha: push.NewSHA,
		},
		Repo: pipelineTriggerRepo(repo, defaultBranch),
	}
	// A nil changed-file list tells the loader to compute the push diff itself.
	rkey, err := p.run(ctx, repo, repo, push.NewSHA, trigger, nil, nil)
	if errors.Is(err, errNoMatchingWorkflows) {
		logger.Info("no workflow matched push trigger")
		return "", nil
	}
	if err != nil {
		return "", err
	}
	logger.Info("pipeline triggered", "pipeline", rkey)
	return rkey, nil
}

// runManual starts a pipeline for an org.tangled.ci.trigger.manual event on target. The
// upstream compiler matches every workflow for a manual trigger and ignores `when`
// constraints, so manual.Workflows is the only way to narrow the run. It names the workflow
// files to keep, and an empty list runs them all. Nil entries in manual.Inputs are skipped.
func (p *pipelineService) runManual(
	ctx context.Context,
	target *RepoRef,
	manual *orgtangled.ManualTrigger,
) (string, error) {
	inputs := make([]*tangled.Pipeline_Pair, 0, len(manual.Inputs))
	for _, pair := range manual.Inputs {
		if pair != nil {
			inputs = append(inputs, &tangled.Pipeline_Pair{Key: pair.Key, Value: pair.Value})
		}
	}
	trigger := &tangled.Pipeline_TriggerMetadata{
		Kind: string(workflow.TriggerKindManual),
		Manual: &tangled.Pipeline_ManualTriggerData{
			Sha:    manual.Commit,
			Ref:    manual.Ref,
			Inputs: inputs,
		},
		Repo: pipelineTriggerRepo(target, p.resolveDefaultBranch(ctx, target)),
	}
	return p.run(ctx, target, target, manual.Commit, trigger, nil, manual.Workflows)
}

// isObjectID reports whether sha is a full Git object ID in hex: 40 characters for SHA-1 or
// 64 for SHA-256, in either letter case. Abbreviated SHAs are rejected.
func isObjectID(sha string) bool {
	if len(sha) != 40 && len(sha) != 64 {
		return false
	}
	for _, c := range sha {
		if !('0' <= c && c <= '9' || 'a' <= c && c <= 'f' || 'A' <= c && c <= 'F') {
			return false
		}
	}
	return true
}
