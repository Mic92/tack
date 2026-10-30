package main

// The current org.tangled.ci.* XRPC methods: the CI API that the Tangled web app calls on a
// spindle in Tangled v1.20. XRPC is atproto's HTTP RPC protocol, and each method is named by
// an NSID such as org.tangled.ci.queryPipelines.
//
// These handlers sit beside the legacy sh.tangled.ci.* handlers in xrpc.go and differ from them
// in the ways the v1.20 lexicons require. Every call names its repository with an explicit repo
// DID parameter. queryPipelines filters by trigger kind. Results use the org.tangled.ci.defs
// shapes. retryPipeline and approvePipeline replace the legacy triggerPipeline procedure.
// Validation limits and error names follow the upstream spindle at v1.20.0
// (spindle/xrpc/org_tangled_ci_*.go) and the org.tangled.ci lexicons, so the web app sees the
// same answers from Tack as from the reference implementation.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strconv"

	"github.com/bluesky-social/indigo/atproto/syntax"
	"tangled.org/core/api/tangled"

	"go.mitchellh.com/tack/internal/orgtangled"
)

// xrpcError is a failure from the pipeline service that already carries the XRPC response it
// should produce. It lets service methods such as [pipelineService.authorizeRepo] decide the
// HTTP status and error name close to the check that failed, while handlers stay a thin layer
// that passes any error to [writeError].
type xrpcError struct {
	// status is the HTTP status code written for this error.
	status int
	// name is the XRPC error name (the "error" field of the response body), which clients
	// match on. The web app, for example, recognizes RepoNotFound.
	name string
	// message is the human-readable "message" field. It is sent to the caller, so it must
	// not contain internal details.
	message string
}

// Error returns the client-facing message so an xrpcError can travel as an ordinary error.
func (e *xrpcError) Error() string { return e.message }

// writeError writes err as an XRPC error response. An [xrpcError] anywhere in the chain is
// written with its own status, name, and message. Any other error is unexpected: it is logged
// and answered with a generic 500 so internal details never reach the caller.
func writeError(w http.ResponseWriter, logger *slog.Logger, err error) {
	var known *xrpcError
	if errors.As(err, &known) {
		writeXRPCError(w, known.status, known.name, known.message)
		return
	}
	logger.Error("xrpc procedure failed", "err", err)
	writeXRPCError(w, http.StatusInternalServerError, "InternalError", "internal error")
}

// authorizeRepo resolves repoDID to a repository Tack serves and checks that actor, the
// verified service auth issuer, may run CI on it. It returns the repository on success.
//
// Failures come back as an [xrpcError] where the caller should see a specific answer: 404
// RepoNotFound when Tack does not serve the repository (it is not the repository's spindle, or
// its owner is not admitted), and 403 AccessControl when actor is neither the owner nor a
// collaborator. Resolver, knot, and PDS failures are returned as plain errors and become a 500.
// servedRepo does not use the public lookup pool, so the 503 Busy mapping for errLookupBusy is
// defensive and does not fire today.
//
// An actor equal to repoDID means the caller presented a repo token: service auth issued in
// the repository's own name. The hosting knot mints one (org.tangled.repo.mintToken) only after
// the same permission check it applies to a push, so no further collaborator check is needed.
// The Tangled web app uses repo tokens whenever the knot advertises minting, which knot2 does.
func (p *pipelineService) authorizeRepo(ctx context.Context, repoDID, actor string) (*RepoRef, error) {
	target, err := p.servedRepo(ctx, repoDID)
	if errors.Is(err, errLookupBusy) {
		return nil, &xrpcError{http.StatusServiceUnavailable, "Busy", err.Error()}
	}
	if err != nil {
		return nil, fmt.Errorf("resolve repository %s: %w", repoDID, err)
	}
	if target == nil {
		return nil, &xrpcError{http.StatusNotFound, "RepoNotFound", "repository is not served by this spindle"}
	}
	if actor == repoDID {
		return target, nil
	}
	authorized, err := p.authorizeRepoActor(ctx, target, actor)
	if err != nil {
		return nil, err
	}
	if !authorized {
		return nil, &xrpcError{http.StatusForbidden, "AccessControl",
			fmt.Sprintf("%s cannot run CI for %s", actor, repoDID)}
	}
	return target, nil
}

// retryPipeline reruns a retained pipeline for actor and returns the record key of the new
// pipeline. When input.Workflows is non-empty only those workflows rerun, and naming none
// that exist is a 400.
//
// The rerun reuses the original trigger and compiled workflows instead of compiling the
// branch again. A retry should repeat what ran, and the web app relies on that: after a retry
// it navigates to the same workflow name in the new pipeline.
//
// The pipeline must belong to input.Repo. The actor is only authorized against that
// repository, so without this check a collaborator on one repository could rerun another
// repository's pipeline by passing its record key.
func (p *pipelineService) retryPipeline(
	ctx context.Context,
	actor string,
	input *orgtangled.RetryPipelineInput,
) (string, error) {
	if _, err := syntax.ParseDID(input.Repo); err != nil {
		return "", &xrpcError{http.StatusBadRequest, "InvalidRequest", fmt.Sprintf("invalid repo DID %q", input.Repo)}
	}
	if input.Pipeline == "" {
		return "", &xrpcError{http.StatusBadRequest, "InvalidRequest", "pipeline is required"}
	}
	target, err := p.authorizeRepo(ctx, input.Repo, actor)
	if err != nil {
		return "", err
	}
	raw, _, err := p.store.GetRawPipeline(ctx, input.Pipeline)
	if errors.Is(err, sql.ErrNoRows) {
		return "", &xrpcError{http.StatusNotFound, "PipelineNotFound", "pipeline not found"}
	}
	if err != nil {
		return "", err
	}
	if pipelineRepoDID(raw) != input.Repo {
		return "", &xrpcError{http.StatusForbidden, "AccessControl", "pipeline belongs to another repository"}
	}
	if raw.TriggerMetadata == nil || len(raw.Workflows) == 0 {
		return "", &xrpcError{http.StatusBadRequest, "PipelineNotRetryable",
			"pipeline was stored without a trigger or workflows to rerun"}
	}

	selected := raw.Workflows
	if len(input.Workflows) > 0 {
		selected = make([]*tangled.Pipeline_Workflow, 0, len(input.Workflows))
		for _, wf := range raw.Workflows {
			if wf != nil && slices.Contains(input.Workflows, wf.Name) {
				selected = append(selected, wf)
			}
		}
		if len(selected) == 0 {
			return "", &xrpcError{http.StatusBadRequest, "InvalidRequest", "no requested workflow is in the pipeline"}
		}
	}

	trigger := *raw.TriggerMetadata
	defaultBranch := ""
	if raw.TriggerMetadata.Repo != nil {
		defaultBranch = raw.TriggerMetadata.Repo.DefaultBranch
	}
	// The repository may have moved to another knot since the original run. The rerun is
	// attributed to where it lives now, as just resolved by authorizeRepo.
	trigger.Repo = pipelineTriggerRepo(target, defaultBranch)
	return p.dispatch(ctx, target, &trigger, selected)
}

// orgQueryPipelinesHandler implements org.tangled.ci.queryPipelines, the paged pipeline list
// for one repository, newest first. The route is public and needs no service auth, because
// the web app reads pipeline history for any visitor.
//
// The limit range (1 to 250, default 50) matches the upstream spindle. The 64-character cap on
// each commits value comes from the queryPipelines lexicon (maxLength 64), which the upstream
// handler does not enforce itself. A repository Tack does not serve is a 404 RepoNotFound rather
// than an empty list, so the web app can tell "no runs yet" from "wrong spindle".
func orgQueryPipelinesHandler(logger *slog.Logger, pipelines *pipelineService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		repo := query.Get("repo")
		if _, err := syntax.ParseDID(repo); err != nil {
			writeXRPCError(w, http.StatusBadRequest, "InvalidRequest", "repo must be a DID")
			return
		}
		limit := 50
		if raw := query.Get("limit"); raw != "" {
			parsed, err := strconv.Atoi(raw)
			if err != nil || parsed < 1 || parsed > 250 {
				writeXRPCError(w, http.StatusBadRequest, "InvalidRequest", "limit must be between 1 and 250")
				return
			}
			limit = parsed
		}
		commits := query["commits"]
		for _, commit := range commits {
			if len(commit) > 64 {
				writeXRPCError(w, http.StatusBadRequest, "InvalidRequest", "commits must be at most 64 characters")
				return
			}
		}
		// The cursor is opaque to clients. As in the upstream spindle, a malformed cursor is
		// treated as absent (the first page) rather than rejected.
		var cursor *int64
		if raw := query.Get("cursor"); raw != "" {
			if parsed, err := strconv.ParseInt(raw, 10, 64); err == nil {
				cursor = &parsed
			}
		}

		indexed, err := isRepoIndexed(r.Context(), pipelines, repo)
		if errors.Is(err, errLookupBusy) {
			writeXRPCError(w, http.StatusServiceUnavailable, "Busy", err.Error())
			return
		}
		if err != nil {
			logger.Error("check repository", "err", err, "repo", repo)
			writeXRPCError(w, http.StatusInternalServerError, "InternalError", "failed to check repository")
			return
		}
		if !indexed {
			// The web app turns this error name into "<spindle> has not indexed this
			// repository yet", so the name must stay RepoNotFound.
			writeXRPCError(w, http.StatusNotFound, "RepoNotFound", "repository is not served by this spindle")
			return
		}

		result, next, total, err := pipelines.store.QueryOrgPipelines(
			r.Context(), repo, commits, query["triggers"], cursor, limit,
		)
		if err != nil {
			logger.Error("query pipelines", "err", err, "repo", repo)
			writeXRPCError(w, http.StatusInternalServerError, "InternalError", "failed to query pipelines")
			return
		}
		writeJSON(w, http.StatusOK, orgtangled.QueryPipelinesOutput{
			Cursor: next, Total: total, Pipelines: result,
		})
	}
}

// isRepoIndexed reports whether queryPipelines should answer for repo instead of returning
// RepoNotFound. The checks run from cheapest to most expensive. Retained pipeline history is
// enough, and so is a legacy sh.tangled.repo record from an admitted owner that names Tack as
// its spindle, mirrored from Jetstream (a service that re-serves the atproto firehose as JSON).
// Only when both miss does it fall back to [pipelineService.lookupPublicRepo], which resolves
// the repo DID and reads the repo manifest from its knot. That fallback is how a knot2
// repository with no runs yet gets an empty list.
//
// The route is unauthenticated, so the fallback must be lookupPublicRepo and not servedRepo.
// The public lookup only resolves did:plc DIDs and runs in its own bounded pool, which returns
// errLookupBusy when full instead of letting anonymous traffic starve trigger paths.
func isRepoIndexed(ctx context.Context, pipelines *pipelineService, repo string) (bool, error) {
	if has, err := pipelines.store.HasRepoPipelines(ctx, repo); err != nil || has {
		return has, err
	}
	managed, err := pipelines.store.HasManagedRepo(
		ctx, repo, pipelines.cfg.Hostname, pipelines.cfg.OwnerDID,
	)
	if err != nil || managed {
		return managed, err
	}
	served, err := pipelines.lookupPublicRepo(ctx, repo)
	if err != nil {
		return false, err
	}
	return served != nil, nil
}

// orgGetPipelineHandler implements org.tangled.ci.getPipeline, which returns one pipeline and
// its workflow statuses. Like queryPipelines it is public. A pipeline that exists but belongs
// to a different repository is reported as PipelineNotFound, the same as a missing one. A
// pipeline record key is only meaningful within its repository, and the web app builds
// pipeline pages under a repository path that must agree with the data shown.
func orgGetPipelineHandler(logger *slog.Logger, st *store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		repo, pipeline, ok := orgPipelineParams(w, r)
		if !ok {
			return
		}
		result, err := st.GetOrgPipeline(r.Context(), pipeline)
		if errors.Is(err, sql.ErrNoRows) || (err == nil && result.Repo != repo) {
			writeXRPCError(w, http.StatusNotFound, "PipelineNotFound", "pipeline not found")
			return
		}
		if err != nil {
			logger.Error("get pipeline", "err", err, "pipeline", pipeline)
			writeXRPCError(w, http.StatusInternalServerError, "InternalError", "failed to get pipeline")
			return
		}
		writeJSON(w, http.StatusOK, result)
	}
}

// orgPipelineParams reads and validates the repo and pipeline query parameters of a current
// (org.tangled.*) read. It returns the repo DID and the pipeline record key. When either is
// invalid it has already written a 400 InvalidRequest and returns ok as false, so the caller
// only needs to return.
func orgPipelineParams(w http.ResponseWriter, r *http.Request) (string, string, bool) {
	repo := r.URL.Query().Get("repo")
	if _, err := syntax.ParseDID(repo); err != nil {
		writeXRPCError(w, http.StatusBadRequest, "InvalidRequest", "repo must be a DID")
		return "", "", false
	}
	pipeline := r.URL.Query().Get("pipeline")
	if pipeline == "" {
		writeXRPCError(w, http.StatusBadRequest, "InvalidRequest", "pipeline is required")
		return "", "", false
	}
	if _, err := syntax.ParseRecordKey(pipeline); err != nil {
		writeXRPCError(w, http.StatusBadRequest, "InvalidRequest", "pipeline must be a record key")
		return "", "", false
	}
	return repo, pipeline, true
}

// orgRetryPipelineHandler implements the org.tangled.ci.retryPipeline procedure. It must be
// wrapped in the service auth middleware, which attaches the verified caller to the request
// context. The middleware already answers 403 to a request with no token, so the 401
// MissingActorDid here only fires if the route is mounted without it (upstream answers the same
// way). The request body is capped at 1 MiB. Validation, authorization, and the rerun itself
// live in [pipelineService.retryPipeline].
func orgRetryPipelineHandler(logger *slog.Logger, pipelines *pipelineService) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, ok := principalFrom(r.Context())
		if !ok {
			writeXRPCError(w, http.StatusUnauthorized, "MissingActorDid", "service auth is required")
			return
		}
		var input orgtangled.RetryPipelineInput
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&input); err != nil {
			writeXRPCError(w, http.StatusBadRequest, "InvalidRequest", "invalid request body")
			return
		}
		rkey, err := pipelines.retryPipeline(r.Context(), principal.Issuer.String(), &input)
		if err != nil {
			writeError(w, logger, err)
			return
		}
		writeJSON(w, http.StatusOK, orgtangled.RetryPipelineOutput{Pipeline: rkey})
	})
}

// orgApprovePipelineHandler implements the org.tangled.ci.approvePipeline procedure, which
// releases a pipeline that a spindle held for manual approval. Tack never holds pipelines, so
// every request that passes the checks ends in 400 PipelineNotBlocked, the answer the upstream
// spindle gives for a pipeline that is not waiting.
//
// The authorization and lookup still run first, in the upstream order. That way the caller
// gets the same RepoNotFound, AccessControl, or PipelineNotFound answer it would get from the
// upstream spindle, and an unauthorized caller cannot use the endpoint to learn which
// pipeline record keys exist. Like retryPipeline it requires service auth.
func orgApprovePipelineHandler(logger *slog.Logger, pipelines *pipelineService) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, ok := principalFrom(r.Context())
		if !ok {
			writeXRPCError(w, http.StatusUnauthorized, "MissingActorDid", "service auth is required")
			return
		}
		var input orgtangled.ApprovePipelineInput
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&input); err != nil {
			writeXRPCError(w, http.StatusBadRequest, "InvalidRequest", "invalid request body")
			return
		}
		if _, err := syntax.ParseDID(input.Repo); err != nil || input.Pipeline == "" {
			writeXRPCError(w, http.StatusBadRequest, "InvalidRequest", "repo and pipeline are required")
			return
		}
		if _, err := pipelines.authorizeRepo(r.Context(), input.Repo, principal.Issuer.String()); err != nil {
			writeError(w, logger, err)
			return
		}
		raw, _, err := pipelines.store.GetRawPipeline(r.Context(), input.Pipeline)
		if errors.Is(err, sql.ErrNoRows) {
			writeXRPCError(w, http.StatusNotFound, "PipelineNotFound", "pipeline not found")
			return
		}
		if err != nil {
			writeError(w, logger, err)
			return
		}
		if pipelineRepoDID(raw) != input.Repo {
			writeXRPCError(w, http.StatusForbidden, "AccessControl", "pipeline belongs to another repository")
			return
		}
		writeXRPCError(w, http.StatusBadRequest, "PipelineNotBlocked", "pipeline is not awaiting approval")
	})
}

// orgCancelPipelineHandler answers the org.tangled.ci.cancelPipeline procedure with 501
// MethodNotImplemented. Tack's providers cannot cancel external builds yet. Registering the
// route anyway gives clients a well-formed XRPC error with a reason they can show, instead of
// the router's plain-text 404. The answer is the same for every caller, so the route skips
// service auth.
func orgCancelPipelineHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		writeXRPCError(w, http.StatusNotImplemented, "MethodNotImplemented",
			"this spindle cannot cancel pipelines")
	}
}
