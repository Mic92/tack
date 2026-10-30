package main

// This file holds the shared fixture for tests of the current (Tangled v1.20, org.tangled.*)
// XRPC routes. The fixture drives the real HTTP handler and the real service auth middleware,
// so routing, token checks, and admission rules run as in production. Only the seams that would
// reach the network are replaced. A seam is a function field on pipelineService that tests may
// swap out. The replaced seams cover repository resolution, the owner and collaborator records
// stored on PDSes and knots, workflow checkout, and default branch lookup. DIDs resolve through
// an in-memory directory, so tests can mint tokens for any issuer they register.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/bluesky-social/indigo/atproto/auth"
	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"tangled.org/core/api/tangled"
	"tangled.org/core/repoident"

	"go.mitchellh.com/tack/internal/orgtangled"
)

// These constants name the fixture's fixed world. testSpindleDID is the did:web that
// config.DID derives from testSpindleHost, so it is the audience every service auth token must
// name. testKnotDID is likewise the did:web of testKnotHost, which is what notifyEvent requires
// of a push token's issuer. The SHAs are placeholder 40-character commit IDs, and testZeroSHA is
// the all-zero ID git uses for the missing side of a ref creation or deletion.
const (
	testSpindleHost = "spindle.example"
	testSpindleDID  = "did:web:spindle.example"
	testOwnerDID    = "did:plc:owner"
	testRepoDID     = "did:plc:repo"
	testKnotHost    = "knot.example"
	testKnotDID     = "did:web:knot.example"
	testOldSHA      = "1111111111111111111111111111111111111111"
	testNewSHA      = "2222222222222222222222222222222222222222"
	testZeroSHA     = "0000000000000000000000000000000000000000"
)

// orgFixture is one isolated spindle instance with its own store, provider, and HTTP handler.
// Each test builds its own fixture, so no state leaks between tests.
type orgFixture struct {
	t  *testing.T
	st *store
	// provider records the builds the pipeline service hands off, so tests can assert that a
	// trigger did or did not reach CI.
	provider *stubProvider
	service  *pipelineService
	// dir is the DID directory the service auth middleware verifies token signatures against.
	// addIdentity and newSigner register issuers in it.
	dir     *identity.MockDirectory
	handler http.Handler
	// knotKey is the signing key published for testKnotDID, used by knotToken.
	knotKey *atcrypto.PrivateKeyK256

	// mu guards the fields below. The replaced service seams read them while handling a
	// request, and tests change them in setup to steer the outcome.
	mu sync.Mutex
	// repos maps a repository DID to the RepoRef the verify seam returns for it. The resolveKnot
	// seam returns only that entry's Knot. Both seams return an error for a DID missing from the
	// map, so deleting an entry makes the repository fail to resolve.
	repos map[string]*RepoRef
	// ownerDeclares is the answer of the pipelineService.ownerDeclares seam, which stands in
	// for the owner's PDS confirming the repository.
	ownerDeclares bool
	// collaborators is the list of knot2 collaborator DIDs the listCollaborators seam returns.
	collaborators []string
}

// newOrgFixture returns a fixture that serves testRepoDID as a knot2 repository. Its repo
// manifest names this spindle, it is hosted on testKnotHost, and it is owned by the spindle
// owner, who also declares it from their PDS. That is the happy path through
// admitManifestRepo, so a test changes one field (for example the manifest's spindle or the
// owner) to exercise one refusal. The CORS origin is the Tangled web app so browser-facing
// routes behave as they do in a default deployment.
func newOrgFixture(t *testing.T) *orgFixture {
	t.Helper()
	st := newTestStore(t)
	provider := &stubProvider{}
	cfg := config{
		Hostname:    testSpindleHost,
		OwnerDID:    testOwnerDID,
		CORSOrigins: []string{"https://tangled.org"},
	}
	service := newPipelineService(context.Background(), cfg, st, newBroker(st), provider)
	f := &orgFixture{
		t:             t,
		st:            st,
		provider:      provider,
		service:       service,
		dir:           identity.NewMockDirectory(),
		ownerDeclares: true,
		repos: map[string]*RepoRef{testRepoDID: {
			OwnerDID:     testOwnerDID,
			Rkey:         "repo",
			Name:         "repo",
			Knot:         testKnotHost,
			Spindle:      testSpindleDID,
			RepoDID:      testRepoDID,
			FromManifest: true,
		}},
	}
	// The seams read fixture state under mu and return copies, so code under test cannot
	// mutate the fixture's repos or collaborators through a returned value.
	service.verify = func(_ context.Context, repoDID repoident.RepoDid) (*RepoRef, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		repo, ok := f.repos[repoDID.String()]
		if !ok {
			return nil, fmt.Errorf("repo %s does not resolve", repoDID)
		}
		clone := *repo
		return &clone, nil
	}
	service.resolveKnot = func(_ context.Context, repoDID string) (string, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		repo, ok := f.repos[repoDID]
		if !ok {
			return "", fmt.Errorf("repo %s does not resolve", repoDID)
		}
		return repo.Knot, nil
	}
	service.ownerDeclares = func(context.Context, *RepoRef) (bool, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.ownerDeclares, nil
	}
	service.listCollaborators = func(context.Context, *RepoRef) ([]string, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		return append([]string(nil), f.collaborators...), nil
	}
	// The workflow stands in for a checkout of .tangled/workflows. It matches push and manual
	// triggers and has a fake provider block, so a push or manual trigger that passes admission
	// has a workflow to run without any real checkout.
	service.load = func(
		context.Context, *RepoRef, string, *tangled.Pipeline_TriggerMetadata, []string,
	) ([]*tangled.Pipeline_Workflow, error) {
		return []*tangled.Pipeline_Workflow{{
			Name:   "ci.yml",
			Engine: "tack",
			Raw:    "when:\n  - event: [\"push\", \"manual\"]\nengine: tack\ntack:\n  fake: {}\n",
		}}, nil
	}
	service.resolveDefaultBranch = func(context.Context, *RepoRef) string { return "main" }

	key, err := atcrypto.GeneratePrivateKeyK256()
	if err != nil {
		t.Fatal(err)
	}
	f.knotKey = key
	f.addIdentity(testKnotDID, key)
	// No Buildkite provider and no DID document key: these tests do not exercise the Buildkite
	// webhook, and the did:web document is valid without a verification method.
	f.handler = newHTTPHandler(slog.Default(), cfg, st, provider, nil, service, f.dir, nil)
	return f
}

// addIdentity registers did in the fixture's directory with key's public half as its "atproto"
// verification method, which is the key atproto service auth checks signatures against. It
// replaces any identity already registered for did.
func (f *orgFixture) addIdentity(did string, key atcrypto.PrivateKey) {
	f.t.Helper()
	public, err := key.PublicKey()
	if err != nil {
		f.t.Fatal(err)
	}
	f.dir.Insert(identity.Identity{
		DID: syntax.DID(did),
		Keys: map[string]identity.VerificationMethod{
			"atproto": {Type: "Multikey", PublicKeyMultibase: public.Multibase()},
		},
	})
}

// newSigner generates a fresh signing key, registers it for did with addIdentity, and returns
// it so a test can mint tokens for a caller other than the hosting knot.
func (f *orgFixture) newSigner(did string) *atcrypto.PrivateKeyK256 {
	f.t.Helper()
	key, err := atcrypto.GeneratePrivateKeyK256()
	if err != nil {
		f.t.Fatal(err)
	}
	f.addIdentity(did, key)
	return key
}

// token mints a service auth JWT (the signed token one atproto service sends another to prove
// who is calling) the way knots and PDSes do. The audience is always testSpindleDID and lxm
// is the NSID of the one XRPC method the token may call. Tests pass a different issuer, key,
// method, or lifetime to produce tokens the spindle must reject.
func (f *orgFixture) token(iss string, key atcrypto.PrivateKey, lxm string, ttl time.Duration) string {
	f.t.Helper()
	method := syntax.NSID(lxm)
	token, err := auth.SignServiceAuth(syntax.DID(iss), testSpindleDID, ttl, &method, key)
	if err != nil {
		f.t.Fatal(err)
	}
	return token
}

// knotToken returns a valid notifyEvent token issued by testKnotDID, the knot that hosts
// testRepoDID. Each call mints a token with a new random jti (token ID), so repeated calls
// are not rejected by the replay guard.
func (f *orgFixture) knotToken() string {
	return f.token(testKnotDID, f.knotKey, orgtangled.NSIDNotifyEvent, time.Minute)
}

// post sends an XRPC procedure call for nsid through the full handler and returns the
// recorded response. An empty token sends no Authorization header. A nil body sends an empty
// body, a []byte body is sent verbatim so tests can send malformed JSON, and any other value
// is JSON-encoded.
func (f *orgFixture) post(nsid, token string, body any) *httptest.ResponseRecorder {
	f.t.Helper()
	var payload []byte
	switch value := body.(type) {
	case nil:
	case []byte:
		payload = value
	default:
		var err error
		if payload, err = json.Marshal(value); err != nil {
			f.t.Fatal(err)
		}
	}
	request := httptest.NewRequest(http.MethodPost, "/xrpc/"+nsid, bytes.NewReader(payload))
	request.Header.Set("Content-Type", "application/json")
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	recorder := httptest.NewRecorder()
	f.handler.ServeHTTP(recorder, request)
	return recorder
}

// get sends an unauthenticated GET for target, a path with any query string, through the full
// handler. The routes tests reach this way are public reads (XRPC queries and the log
// subscription), so no token is attached.
func (f *orgFixture) get(target string) *httptest.ResponseRecorder {
	f.t.Helper()
	request := httptest.NewRequest(http.MethodGet, target, nil)
	recorder := httptest.NewRecorder()
	f.handler.ServeHTTP(recorder, request)
	return recorder
}

// pushEvent builds the notifyEvent input knot2 sends for one updated ref. knot2 sends one call
// per ref, so a multi-ref push becomes several calls. The actor is a fixed pusher DID, and
// options carries git push options (git push -o).
func pushEvent(repo, ref, oldSHA, newSHA string, options ...string) orgtangled.NotifyEventInput {
	actor := "did:plc:pusher"
	return orgtangled.NotifyEventInput{
		Repo: repo,
		Event: &orgtangled.Event{Push: &orgtangled.PushEvent{
			Repo: repo, Ref: ref, OldSHA: oldSHA, NewSHA: newSHA,
			Actor: &actor, PushOptions: options,
		}},
	}
}

// decodeXRPCError returns the error field of an XRPC error body, for example "RepoNotFound".
// Tests compare it alongside the status code because several refusals share a status and only
// the error name tells them apart. A body that is not JSON fails the test.
func decodeXRPCError(t *testing.T, recorder *httptest.ResponseRecorder) string {
	t.Helper()
	var body struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode error body %q: %v", recorder.Body.String(), err)
	}
	return body.Error
}

// seedPipeline stores record as the pipeline event with record key rkey, skipping triggers,
// admission, and the provider. Query tests use it to set up exact history. It sets the
// record's $type so the stored JSON matches what the trigger path writes.
func (f *orgFixture) seedPipeline(rkey string, record tangled.Pipeline) {
	f.t.Helper()
	record.LexiconTypeID = tangled.PipelineNSID
	encoded, err := json.Marshal(record)
	if err != nil {
		f.t.Fatal(err)
	}
	if _, err := f.st.InsertEvent(context.Background(), rkey, tangled.PipelineNSID, encoded); err != nil {
		f.t.Fatal(err)
	}
}

// pushRecord returns a stored push pipeline for repoDID that moved refs/heads/main from
// testOldSHA to sha. Tests change the trigger kind or data on the result to build manual and
// pull records, and pass another repoDID to check that queries filter by repository.
func pushRecord(repoDID, sha string) tangled.Pipeline {
	rkey := "repo"
	return tangled.Pipeline{
		TriggerMetadata: &tangled.Pipeline_TriggerMetadata{
			Kind: "push",
			Push: &tangled.Pipeline_PushTriggerData{Ref: "refs/heads/main", OldSha: testOldSHA, NewSha: sha},
			Repo: &tangled.Pipeline_TriggerRepo{
				Did: testOwnerDID, Knot: testKnotHost, Repo: &rkey, RepoDid: &repoDID,
				DefaultBranch: "main",
			},
		},
		Workflows: []*tangled.Pipeline_Workflow{{
			Name: "ci.yml", Engine: "tack",
			Raw: "when:\n  - event: push\nengine: tack\ntack:\n  fake: {}\n",
		}},
	}
}
