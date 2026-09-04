package main

// Repository DID verification compatibility for both legacy and current knots.

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
	indigoxrpc "github.com/bluesky-social/indigo/xrpc"
	"tangled.org/core/api/tangled"
	"tangled.org/core/repoident"
	"tangled.org/core/repoverify"
)

const repoVerifyTimeout = 10 * time.Second

const (
	knotServiceID         = "tangled_knot"
	knotServiceType       = "TangledKnot"
	legacyKnotServiceID   = "atproto_pds"
	legacyKnotServiceType = "AtprotoPersonalDataServer"
)

// newRepoVerifier backports the current Tangled repository DID lookup rules.
// core v1.16.1 only recognizes the legacy atproto_pds service, while newly
// created repository DIDs advertise a dedicated TangledKnot endpoint instead.
// Keeping the compatibility here lets Tack consume both DID forms until the
// upstream behavior is available in a tagged core release.
func newRepoVerifier(
	directory identity.Directory,
	httpClient *http.Client,
	dev bool,
) repoverify.Verifier {
	return func(ctx context.Context, repoDID repoident.RepoDid) (repoverify.Result, error) {
		ctx, cancel := context.WithTimeout(ctx, repoVerifyTimeout)
		defer cancel()

		ident, err := directory.LookupDID(ctx, syntax.DID(repoDID))
		if err != nil {
			return repoverify.Result{}, fmt.Errorf("resolve repoDid %s: %w", repoDID, err)
		}
		knot, err := knotURLFromIdentity(ident, dev)
		if err != nil {
			return repoverify.Result{}, fmt.Errorf("repoDid %s: %w", repoDID, err)
		}

		client := &indigoxrpc.Client{Host: knot.String(), Client: httpClient}
		result, err := tangled.RepoDescribeRepo(ctx, client, repoDID.String())
		if err != nil {
			return repoverify.Result{}, fmt.Errorf("describeRepo on %s: %w", knot, err)
		}
		if result.RepoDid != repoDID.String() {
			return repoverify.Result{}, fmt.Errorf(
				"knot %s returned repoDid %q, want %q", knot, result.RepoDid, repoDID,
			)
		}
		ownerDID, err := repoident.NewOwnerDid(result.OwnerDid)
		if err != nil {
			return repoverify.Result{}, fmt.Errorf(
				"describeRepo on %s returned invalid ownerDid: %w", knot, err,
			)
		}
		if _, err := syntax.ParseRecordKey(result.Rkey); err != nil {
			return repoverify.Result{}, fmt.Errorf(
				"describeRepo on %s returned invalid rkey %q: %w", knot, result.Rkey, err,
			)
		}

		return repoverify.Result{
			RepoDid:  repoDID,
			OwnerDid: ownerDID,
			KnotURL:  knot,
			Rkey:     result.Rkey,
		}, nil
	}
}

// knotURLFromIdentity prefers the purpose-specific endpoint and falls back to
// the old PDS-shaped declaration. A TangledKnot endpoint contains a repository
// path, but XRPC and Git-by-DID requests are rooted at the knot origin.
func knotURLFromIdentity(ident *identity.Identity, dev bool) (*url.URL, error) {
	if ident == nil {
		return nil, errors.New("nil identity has no knot service endpoint")
	}
	endpoint := ident.Services[knotServiceID]
	if endpoint.Type != knotServiceType {
		endpoint = ident.Services[legacyKnotServiceID]
		if endpoint.Type != legacyKnotServiceType {
			return nil, errors.New("DID document declares no Tangled knot service endpoint")
		}
	}

	knot, err := url.Parse(endpoint.URL)
	if err != nil {
		return nil, fmt.Errorf("invalid knot URL %q: %w", endpoint.URL, err)
	}
	if knot.Hostname() == "" {
		return nil, fmt.Errorf("knot URL %q has no host", endpoint.URL)
	}
	if knot.User != nil || knot.RawQuery != "" || knot.Fragment != "" {
		return nil, fmt.Errorf("knot URL %q has userinfo, a query, or a fragment", endpoint.URL)
	}
	switch knot.Scheme {
	case "https":
	case "http":
		if !dev {
			return nil, fmt.Errorf("knot URL %q must use https outside dev mode", endpoint.URL)
		}
	default:
		return nil, fmt.Errorf("knot URL %q has unsupported scheme %q", endpoint.URL, knot.Scheme)
	}

	// Service paths identify the repository for DID document readers, not an
	// HTTP base path. Preserve only the canonical origin for XRPC and Git.
	knot.Path = ""
	knot.RawPath = ""
	host := strings.ToLower(knot.Host)
	switch port := knot.Port(); port {
	case "":
		knot.Host = strings.TrimSuffix(host, ":")
	case "443":
		if knot.Scheme == "https" {
			knot.Host = strings.TrimSuffix(host, ":"+port)
		} else {
			knot.Host = host
		}
	case "80":
		if knot.Scheme == "http" {
			knot.Host = strings.TrimSuffix(host, ":"+port)
		} else {
			knot.Host = host
		}
	default:
		knot.Host = host
	}
	return knot, nil
}
