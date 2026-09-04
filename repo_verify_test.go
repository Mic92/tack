package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"tangled.org/core/api/tangled"
	"tangled.org/core/repoident"
)

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
	if result.KnotURL.String() != server.URL {
		t.Errorf("knot URL = %q; want %q", result.KnotURL, server.URL)
	}
	if result.OwnerDid.String() != ownerDID || result.Rkey != "fork-repo" {
		t.Errorf("result = %+v; want owner %q and rkey fork-repo", result, ownerDID)
	}
}

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
