package main

// This file resolves a repository DID to its hosting knot and canonical identity for both Go knots
// and knot2, and holds the unauthenticated XRPC read helpers (getRecord, listRecordKeys, xrpcGet)
// that knot2 and PDS lookups share.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
	indigoxrpc "github.com/bluesky-social/indigo/xrpc"
	"tangled.org/core/api/tangled"
	"tangled.org/core/repoident"

	"go.mitchellh.com/tack/internal/orgtangled"
)

const repoVerifyTimeout = 10 * time.Second

// These are the DID document services that can name a repository's knot. Current repository DIDs
// declare tangled_knot (type TangledKnot). Older ones reuse the PDS-shaped atproto_pds service,
// which is the only one the repoverify package in core v1.16.1 recognizes.
const (
	knotServiceID         = "tangled_knot"
	knotServiceType       = "TangledKnot"
	legacyKnotServiceID   = "atproto_pds"
	legacyKnotServiceType = "AtprotoPersonalDataServer"
)

// repoVerifier resolves the canonical identity of a repository DID. The hosting knot comes from
// the DID document, and the owner and record key come from that knot. A result read from a knot2
// repo manifest has RepoRef.FromManifest set, and its RepoRef.Spindle holds the spindle DID the
// manifest names (empty when it names none) rather than a legacy spindle hostname. Callers must
// still decide whether that identity is trustworthy enough to act on (see
// pipelineService.admitManifestRepo).
type repoVerifier func(context.Context, repoident.RepoDid) (*RepoRef, error)

// newRepoVerifier returns a repoVerifier that works with both knot generations. knot2 no longer
// serves sh.tangled.repo.describeRepo, so the verifier first reads the repo manifest
// (org.tangled.repo.manifest/self). If that read fails for any reason, including a record whose
// JSON does not decode or a response with no value, it falls back to describeRepo, which Go knots
// still answer. When both fail, the error carries both causes so an operator can see why each path
// failed. A manifest that decodes but fails validation in repoRefFromManifest is an error with no
// fallback.
//
// Tack does this itself because the repoverify package in Tangled core v1.16.1 (the version
// go.mod pins) knows neither the TangledKnot DID service nor manifests. Upstream core has since
// added both, so this can move to repoverify once a tagged core release ships them.
//
// Each call is bounded by repoVerifyTimeout. httpClient should be the SSRF-guarded client from
// newPipelineHTTPClient, because the knot URL comes from a DID document that anyone can publish.
func newRepoVerifier(
	directory identity.Directory,
	httpClient *http.Client,
	dev bool,
) repoVerifier {
	return func(ctx context.Context, repoDID repoident.RepoDid) (*RepoRef, error) {
		ctx, cancel := context.WithTimeout(ctx, repoVerifyTimeout)
		defer cancel()

		knot, err := resolveRepoKnot(ctx, directory, repoDID.String(), dev)
		if err != nil {
			return nil, err
		}

		manifest, manifestErr := fetchRepoManifest(ctx, httpClient, knot.String(), repoDID.String())
		if manifestErr == nil {
			return repoRefFromManifest(repoDID.String(), knot.Host, manifest)
		}

		client := &indigoxrpc.Client{Host: knot.String(), Client: httpClient}
		result, err := tangled.RepoDescribeRepo(ctx, client, repoDID.String())
		if err != nil {
			return nil, fmt.Errorf("repoDid %s on %s: manifest: %v; describeRepo: %w",
				repoDID, knot, manifestErr, err)
		}
		if result.RepoDid != repoDID.String() {
			return nil, fmt.Errorf(
				"knot %s returned repoDid %q, want %q", knot, result.RepoDid, repoDID,
			)
		}
		ownerDID, err := repoident.NewOwnerDid(result.OwnerDid)
		if err != nil {
			return nil, fmt.Errorf(
				"describeRepo on %s returned invalid ownerDid: %w", knot, err,
			)
		}
		if _, err := syntax.ParseRecordKey(result.Rkey); err != nil {
			return nil, fmt.Errorf(
				"describeRepo on %s returned invalid rkey %q: %w", knot, result.Rkey, err,
			)
		}
		return &RepoRef{
			OwnerDID: ownerDID.String(),
			Rkey:     result.Rkey,
			Knot:     knot.Host,
			RepoDID:  repoDID.String(),
		}, nil
	}
}

// resolveRepoKnot returns the origin of the knot that a repository DID document declares, as
// normalized by knotURLFromIdentity. It is split out from newRepoVerifier because notifyEvent
// authorization needs only the hosting knot: that knot is the one issuer allowed to report pushes
// for the repository, and resolving it must not depend on any record the knot serves.
func resolveRepoKnot(
	ctx context.Context,
	directory identity.Directory,
	repoDID string,
	dev bool,
) (*url.URL, error) {
	ident, err := directory.LookupDID(ctx, syntax.DID(repoDID))
	if err != nil {
		return nil, fmt.Errorf("resolve repoDid %s: %w", repoDID, err)
	}
	knot, err := knotURLFromIdentity(ident, dev)
	if err != nil {
		return nil, fmt.Errorf("repoDid %s: %w", repoDID, err)
	}
	return knot, nil
}

// repoRefFromManifest converts a knot2 repo manifest into a RepoRef with FromManifest set. It
// rejects a manifest with no declaration, an owner that is not a valid DID, or a name that is not
// a valid record key. The owner and name are asserted by the knot alone, and a knot controls the
// DID documents of the repositories it creates. Callers that grant anything on the strength of this
// result must confirm it against the owner's own PDS (see pipelineService.ownerDeclaresRepo).
func repoRefFromManifest(repoDID, knot string, manifest *orgtangled.RepoManifest) (*RepoRef, error) {
	if manifest.Declaration == nil {
		return nil, fmt.Errorf("manifest for %s has no declaration", repoDID)
	}
	owner, err := repoident.NewOwnerDid(manifest.Declaration.Owner)
	if err != nil {
		return nil, fmt.Errorf("manifest for %s has invalid owner: %w", repoDID, err)
	}
	if _, err := syntax.ParseRecordKey(manifest.Declaration.Name); err != nil {
		return nil, fmt.Errorf("manifest for %s has invalid name %q: %w",
			repoDID, manifest.Declaration.Name, err)
	}
	spindle := ""
	if manifest.Spindle != nil {
		spindle = *manifest.Spindle
	}
	return &RepoRef{
		OwnerDID: owner.String(),
		// The manifest lexicon defines the owner's org.tangled.repo.declaration record key as
		// the lowercased name, and ownerDeclaresRepo looks the declaration up by this key.
		Rkey:         strings.ToLower(manifest.Declaration.Name),
		Name:         manifest.Declaration.Name,
		Knot:         knot,
		Spindle:      spindle,
		RepoDID:      repoDID,
		FromManifest: true,
	}, nil
}

// errRecordNotFound reports that a knot or PDS positively answered that a record or repository
// does not exist. Only an XRPC error body naming RecordNotFound or RepoNotFound produces it. A bare
// 404 from a proxy, or from a server that does not implement the method, is an ordinary error,
// because treating it as "absent" would let an outage look like a revoked declaration.
var errRecordNotFound = errors.New("record not found")

// fetchRepoManifest reads a repository's manifest (org.tangled.repo.manifest/self) from the knot
// at base. Only knot2 serves this record, so an error here usually means a Go knot.
func fetchRepoManifest(
	ctx context.Context,
	client *http.Client,
	base, repoDID string,
) (*orgtangled.RepoManifest, error) {
	var manifest orgtangled.RepoManifest
	if err := getRecord(ctx, client, base, repoDID, orgtangled.NSIDRepoManifest, "self", &manifest); err != nil {
		return nil, err
	}
	return &manifest, nil
}

// getRecord calls com.atproto.repo.getRecord without authentication against base and decodes the
// record value into out. knot2 and PDSes both serve this method, so one helper reads manifests,
// git refs, and owner declarations. A response with no value is an error rather than a zero
// record. A positive not-found answer returns errRecordNotFound, wrapped so errors.Is matches it.
func getRecord(
	ctx context.Context,
	client *http.Client,
	base, repo, collection, rkey string,
	out any,
) error {
	query := url.Values{"repo": {repo}, "collection": {collection}, "rkey": {rkey}}
	var envelope struct {
		Value json.RawMessage `json:"value"`
	}
	if err := xrpcGet(ctx, client, base, "com.atproto.repo.getRecord", query, &envelope); err != nil {
		return fmt.Errorf("getRecord %s/%s/%s: %w", repo, collection, rkey, err)
	}
	if len(envelope.Value) == 0 {
		return fmt.Errorf("getRecord %s/%s/%s: response has no value", repo, collection, rkey)
	}
	if err := json.Unmarshal(envelope.Value, out); err != nil {
		return fmt.Errorf("decode %s/%s/%s: %w", repo, collection, rkey, err)
	}
	return nil
}

// listRecordKeys pages through com.atproto.repo.listRecords for one collection and returns the
// record key of each record. Records whose URI does not parse are skipped. Paging stops at 32
// pages of 100 and then fails, so a hostile knot cannot keep a request alive with endless cursors.
func listRecordKeys(
	ctx context.Context,
	client *http.Client,
	base, repo, collection string,
) ([]string, error) {
	var keys []string
	cursor := ""
	for range 32 {
		query := url.Values{"repo": {repo}, "collection": {collection}, "limit": {"100"}}
		if cursor != "" {
			query.Set("cursor", cursor)
		}
		var page struct {
			Cursor  *string `json:"cursor"`
			Records []struct {
				URI string `json:"uri"`
			} `json:"records"`
		}
		if err := xrpcGet(ctx, client, base, "com.atproto.repo.listRecords", query, &page); err != nil {
			return nil, fmt.Errorf("listRecords %s/%s: %w", repo, collection, err)
		}
		for _, record := range page.Records {
			uri, err := syntax.ParseATURI(record.URI)
			if err != nil {
				continue
			}
			keys = append(keys, uri.RecordKey().String())
		}
		if page.Cursor == nil || *page.Cursor == "" || len(page.Records) == 0 {
			return keys, nil
		}
		cursor = *page.Cursor
	}
	return nil, fmt.Errorf("listRecords %s/%s: too many pages", repo, collection)
}

// xrpcGet issues one XRPC query (an HTTP GET) and decodes a 200 response into out. Response
// bodies are capped at 1 MiB. It exists alongside indigo's client for two reasons. First, Tack must
// tell a definitive RecordNotFound or RepoNotFound (see errRecordNotFound) apart from every other
// failure. Second, indigo's generated getRecord decodes values through its lexicon type registry,
// which does not know the hand-ported orgtangled types. Callers pass the SSRF-guarded client.
func xrpcGet(
	ctx context.Context,
	client *http.Client,
	base, method string,
	query url.Values,
	out any,
) error {
	endpoint := strings.TrimRight(base, "/") + "/xrpc/" + method + "?" + query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	request.Header.Set("Accept", "application/json")
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusOK {
		var xrpcErr struct {
			Error   string `json:"error"`
			Message string `json:"message"`
		}
		if json.Unmarshal(body, &xrpcErr) == nil &&
			(xrpcErr.Error == "RecordNotFound" || xrpcErr.Error == "RepoNotFound") {
			return errRecordNotFound
		}
		return fmt.Errorf("%s: HTTP %d %s %s", method, response.StatusCode, xrpcErr.Error, xrpcErr.Message)
	}
	return json.Unmarshal(body, out)
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
