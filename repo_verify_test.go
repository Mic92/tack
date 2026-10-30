package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"tangled.org/core/api/tangled"
	"tangled.org/core/repoident"
)

// TestRepoVerifierSupportsTangledKnotService pins the Go knot path. The fake knot answers only
// sh.tangled.repo.describeRepo, so the manifest read fails and the verifier must fall back to
// describeRepo. The result must carry the knot host without a scheme (the form RepoRef.Knot
// stores) and leave FromManifest unset, because later authorization treats the two kinds of
// repository differently.
func TestRepoVerifierSupportsTangledKnotService(t *testing.T) {
	const (
		repoDID  = "did:plc:forkrepo"
		ownerDID = "did:plc:forkowner"
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/xrpc/"+tangled.RepoDescribeRepoNSID {
			http.NotFound(w, r)
			return
		}
		if got := r.URL.Query().Get("repoDid"); got != repoDID {
			t.Errorf("repoDid query = %q; want %q", got, repoDID)
		}
		_ = json.NewEncoder(w).Encode(tangled.RepoDescribeRepo_Output{
			RepoDid: repoDID, OwnerDid: ownerDID, Rkey: "fork-repo",
		})
	}))
	defer server.Close()

	directory := identity.NewMockDirectory()
	directory.Insert(identity.Identity{
		DID: syntax.DID(repoDID),
		Services: map[string]identity.ServiceEndpoint{
			knotServiceID: {
				Type: knotServiceType,
				URL:  server.URL + "/repo/fork-repo",
			},
		},
	})

	verify := newRepoVerifier(directory, newPipelineHTTPClient(true), true)
	result, err := verify(context.Background(), repoident.RepoDid(repoDID))
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if want := strings.TrimPrefix(server.URL, "http://"); result.Knot != want {
		t.Errorf("knot = %q; want %q", result.Knot, want)
	}
	if result.OwnerDID != ownerDID || result.Rkey != "fork-repo" || result.FromManifest {
		t.Errorf("result = %+v; want legacy owner %q and rkey fork-repo", result, ownerDID)
	}
}

// TestKnotURLFromIdentitySupportsLegacyService pins the fallback to the older PDS-shaped knot
// declaration (an atproto_pds service of type AtprotoPersonalDataServer) when a DID document has
// no TangledKnot service. It also pins origin normalization: the host is lowercased, the default
// https port is dropped, and the repository path is removed, because XRPC and git requests are
// rooted at the knot origin.
func TestKnotURLFromIdentitySupportsLegacyService(t *testing.T) {
	ident := &identity.Identity{Services: map[string]identity.ServiceEndpoint{
		legacyKnotServiceID: {
			Type: legacyKnotServiceType,
			URL:  "https://KNOT.EXAMPLE:443/repository/path",
		},
	}}
	knot, err := knotURLFromIdentity(ident, false)
	if err != nil {
		t.Fatalf("knotURLFromIdentity: %v", err)
	}
	if got, want := knot.String(), "https://knot.example"; got != want {
		t.Fatalf("knot URL = %q; want %q", got, want)
	}
}

// newFakeKnot2 starts a server for the com.atproto.repo.* reads that knot2 offers in place of the
// sh.tangled.repo.* methods it dropped. records maps "repo/collection/rkey" to a getRecord value,
// and collaborators lists the record keys that listRecords returns. A missing record answers 400
// with a RecordNotFound error body and an unknown method answers a bare 404, as knot2 does. The
// difference matters because only the first counts as a positive "absent" answer. A PDS serves
// getRecord the same way, so tests also use this server as a fake PDS.
func newFakeKnot2(t *testing.T, records map[string]any, collaborators []string) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		switch r.URL.Path {
		case "/xrpc/com.atproto.repo.getRecord":
			value, ok := records[query.Get("repo")+"/"+query.Get("collection")+"/"+query.Get("rkey")]
			if !ok {
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]string{"error": "RecordNotFound", "message": "no record"})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"uri": "at://x", "value": value})
		case "/xrpc/com.atproto.repo.listRecords":
			// With more than one collaborator, split the list across two pages so callers
			// must follow the cursor to see everyone.
			page := collaborators
			cursor := ""
			if query.Get("cursor") == "" && len(collaborators) > 1 {
				page, cursor = collaborators[:1], "next"
			} else if query.Get("cursor") == "next" {
				page = collaborators[1:]
			}
			records := make([]map[string]string, 0, len(page))
			for _, did := range page {
				records = append(records, map[string]string{
					"uri": "at://" + query.Get("repo") + "/" + query.Get("collection") + "/" + did,
				})
			}
			body := map[string]any{"records": records}
			if cursor != "" {
				body["cursor"] = cursor
			}
			_ = json.NewEncoder(w).Encode(body)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	return server
}

// newRepoDirectory returns a mock DID directory in which repoDID's document names knotURL as its
// TangledKnot service. The endpoint carries a path suffix, like real repository DID documents,
// so tests also check that the verifier reduces it to the knot's origin.
func newRepoDirectory(repoDID, knotURL string) *identity.MockDirectory {
	directory := identity.NewMockDirectory()
	directory.Insert(identity.Identity{
		DID: syntax.DID(repoDID),
		Services: map[string]identity.ServiceEndpoint{
			knotServiceID: {Type: knotServiceType, URL: knotURL + "/" + repoDID},
		},
	})
	return directory
}

// TestRepoVerifierReadsKnot2Manifest pins the knot2 path. knot2 does not answer
// sh.tangled.repo.describeRepo, so the repository's owner, name, and spindle must come from its
// repo manifest. It also checks that a knot serving neither the manifest nor describeRepo yields
// an error. Reporting that case as "not served by this spindle" would hide a broken or
// unreachable knot behind a normal-looking answer.
func TestRepoVerifierReadsKnot2Manifest(t *testing.T) {
	spindle := "did:web:spindle.example"
	knot := newFakeKnot2(t, map[string]any{
		"did:plc:repo/org.tangled.repo.manifest/self": map[string]any{
			"$type":       "org.tangled.repo.manifest",
			"spindle":     spindle,
			"declaration": map[string]string{"owner": "did:plc:owner", "name": "MyRepo"},
			"createdAt":   "2026-10-30T10:00:00Z",
		},
	}, nil)
	verify := newRepoVerifier(newRepoDirectory("did:plc:repo", knot.URL), newPipelineHTTPClient(true), true)
	repo, err := verify(context.Background(), repoident.RepoDid("did:plc:repo"))
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	// Rkey is the lowercased name because that is the key of the owner's org.tangled.repo.declaration
	// record, which ownerDeclaresRepo later fetches.
	if !repo.FromManifest || repo.Spindle != spindle || repo.OwnerDID != "did:plc:owner" ||
		repo.Name != "MyRepo" || repo.Rkey != "myrepo" ||
		repo.Knot != strings.TrimPrefix(knot.URL, "http://") {
		t.Fatalf("repo = %+v", repo)
	}

	// A second call must succeed the same way. This pins that the verifier caches nothing between
	// calls, so one lookup cannot change the result of the next.
	if _, err := verify(context.Background(), repoident.RepoDid("did:plc:repo")); err != nil {
		t.Fatalf("second verify: %v", err)
	}

	// A knot that knows neither method is an error, never "not served".
	empty := newFakeKnot2(t, nil, nil)
	verify = newRepoVerifier(newRepoDirectory("did:plc:gone", empty.URL), newPipelineHTTPClient(true), true)
	if _, err := verify(context.Background(), repoident.RepoDid("did:plc:gone")); err == nil {
		t.Fatal("verify succeeded without a manifest or describeRepo")
	}
}

// newKnot2Service returns a pipelineService and its test store for knot2 tests. The service keeps
// its production network code (knotCollaborators, ownerDeclaresRepo, defaultBranch) so the tests
// exercise the real requests. Dev mode is what lets that code use plain http and dial the
// loopback addresses of the httptest fakes.
func newKnot2Service(t *testing.T) (*pipelineService, *store) {
	t.Helper()
	st := newTestStore(t)
	cfg := config{Hostname: "spindle.example", OwnerDID: "did:plc:owner", Dev: true}
	return newPipelineService(context.Background(), cfg, st, newBroker(st), &stubProvider{}), st
}

// TestOwnerDeclaresRepo pins the owner-side check for knot2 repositories. A knot writes the repo
// manifest and controls the DID documents of the repositories it creates, so on its own word it
// could claim that the spindle owner owns any repository. ownerDeclaresRepo closes that gap by
// requiring the owner's PDS to publish an org.tangled.repo.declaration record, under the
// repository's rkey, that names the same repository DID. A declaration for a different DID and a
// missing declaration must both report false without an error.
func TestOwnerDeclaresRepo(t *testing.T) {
	pds := newFakeKnot2(t, map[string]any{
		"did:plc:owner/org.tangled.repo.declaration/myrepo": map[string]string{"repo": "did:plc:repo"},
		"did:plc:owner/org.tangled.repo.declaration/other":  map[string]string{"repo": "did:plc:else"},
	}, nil)
	service, st := newKnot2Service(t)
	directory := identity.NewMockDirectory()
	directory.Insert(identity.Identity{
		DID: "did:plc:owner",
		Services: map[string]identity.ServiceEndpoint{
			"atproto_pds": {Type: "AtprotoPersonalDataServer", URL: pds.URL},
		},
	})
	service.directory = directory

	for _, test := range []struct {
		rkey, repoDID string
		want          bool
	}{
		{"myrepo", "did:plc:repo", true},
		{"other", "did:plc:repo", false},
		{"missing", "did:plc:repo", false},
	} {
		got, err := service.ownerDeclaresRepo(context.Background(), &RepoRef{
			OwnerDID: "did:plc:owner", Rkey: test.rkey, RepoDID: test.repoDID,
		})
		if err != nil || got != test.want {
			t.Fatalf("%s: declares = %v, %v; want %v", test.rkey, got, err, test.want)
		}
	}

	// A mirrored legacy sh.tangled.repo record from the owner is equivalent first-party proof
	// and needs no PDS request. The empty directory makes any PDS lookup fail, and the rkey
	// matches no declaration, so only the store shortcut can produce true.
	if err := st.UpsertRepo(context.Background(), "did:plc:owner", "legacy", "knot.example",
		"legacy", "spindle.example", "did:plc:legacy", "t"); err != nil {
		t.Fatal(err)
	}
	service.directory = identity.NewMockDirectory()
	got, err := service.ownerDeclaresRepo(context.Background(), &RepoRef{
		OwnerDID: "did:plc:owner", Rkey: "unrelated", RepoDID: "did:plc:legacy",
	})
	if err != nil || !got {
		t.Fatalf("legacy claim = %v, %v; want true", got, err)
	}
}

// TestKnot2CollaboratorsAndDefaultBranch pins the knot2 reads that replace
// sh.tangled.repo.listCollaborators and sh.tangled.repo.getDefaultBranch. Collaborators come from
// the keys of org.tangled.graph.collaborator records, across every listRecords page, and drive
// authorizeRepoActor: the owner and listed collaborators pass, anyone else is refused. The
// default branch comes from the org.tangled.git.refs record.
func TestKnot2CollaboratorsAndDefaultBranch(t *testing.T) {
	knot := newFakeKnot2(t, map[string]any{
		"did:plc:repo/org.tangled.git.refs/self": map[string]string{"defaultBranch": "trunk"},
	}, []string{"did:plc:alice", "did:plc:bob"})
	service, _ := newKnot2Service(t)
	// hostutil.ParseHostname rejects an IP address with a port but accepts localhost:port, so
	// the httptest address is rewritten into a host that knotBaseURL will accept.
	repo := &RepoRef{
		RepoDID: "did:plc:repo", OwnerDID: "did:plc:owner",
		Knot:         strings.Replace(strings.TrimPrefix(knot.URL, "http://"), "127.0.0.1", "localhost", 1),
		FromManifest: true,
	}

	collaborators, err := service.knotCollaborators(context.Background(), repo)
	if err != nil || !equalStrings(collaborators, []string{"did:plc:alice", "did:plc:bob"}) {
		t.Fatalf("collaborators = %v, %v", collaborators, err)
	}
	for actor, want := range map[string]bool{
		"did:plc:owner": true, "did:plc:bob": true, "did:plc:mallory": false,
	} {
		got, err := service.authorizeRepoActor(context.Background(), repo, actor)
		if err != nil || got != want {
			t.Fatalf("authorize %s = %v, %v; want %v", actor, got, err, want)
		}
	}
	if branch := service.defaultBranch(context.Background(), repo); branch != "trunk" {
		t.Fatalf("default branch = %q; want trunk", branch)
	}
}

// TestKnot2CollaboratorRemovalRevokesAccess pins that for knot2 repositories the knot's current
// collaborator list is the only source of truth. The store holds a grant for a collaborator the
// knot no longer lists, and authorization must refuse them. Tack follows no knot2 event stream,
// so a locally mirrored grant would otherwise never learn of its revocation.
func TestKnot2CollaboratorRemovalRevokesAccess(t *testing.T) {
	service, st := newKnot2Service(t)
	repo := &RepoRef{RepoDID: "did:plc:repo", OwnerDID: "did:plc:owner", Knot: "knot.example", FromManifest: true}
	if err := st.ApplyKnotCollaborator(context.Background(), repo, "did:plc:former", true); err != nil {
		t.Fatal(err)
	}
	service.listCollaborators = func(context.Context, *RepoRef) ([]string, error) { return nil, nil }
	authorized, err := service.authorizeRepoActor(context.Background(), repo, "did:plc:former")
	if err != nil || authorized {
		t.Fatalf("removed collaborator authorized = %v, %v; want false", authorized, err)
	}
}
