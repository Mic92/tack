// Package buildkite is a small Buildkite REST + webhook client tack
// uses to drive its Buildkite-backed Provider implementation.
//
// The package deliberately covers a tiny slice of the upstream API
// (create build, get build, fetch job log, decode + authenticate
// webhook payloads). It exists as its own package so the rest of
// tack — particularly the Provider implementation that translates
// Tangled triggers into Buildkite builds — can stay focused on
// translation rather than HTTP plumbing.
//
// Naming convention: types here are *not* prefixed with "Buildkite".
// Imported as `buildkite.Client`, `buildkite.Build`, etc., the package
// path supplies the disambiguation already.
package buildkite

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// APIBase is the public Buildkite REST API root. Exported as a var
// (not a const) so tests can swap it for an httptest server URL
// without hooking up a real Buildkite account.
var APIBase = "https://api.buildkite.com"

// ErrNotFound is returned by Get* methods when the upstream returns
// 404. Callers translate it to whatever shape they need (the Provider
// maps it onto its own ErrLogsNotFound for the /logs handler).
var ErrNotFound = errors.New("buildkite: not found")

// Client is a thin wrapper around net/http carrying API credentials.
// Organisation slug is *not* held on the client — every call takes
// its target org explicitly so a single client can address multiple
// orgs the token has access to. Safe for concurrent use; the
// embedded http.Client is goroutine-safe.
type Client struct {
	http  *http.Client
	token string
}

// NewClient builds a Client with sensible defaults. The 30s timeout
// covers individual requests, not the whole client lifetime —
// long-poll-style endpoints aren't used here, so a generous-but-bounded
// per-request timeout is the right default.
func NewClient(token string) *Client {
	return &Client{
		http:  &http.Client{Timeout: 30 * time.Second},
		token: token,
	}
}

// Job is the subset of a Buildkite job object we care about: the ID
// and name we need to fetch logs for it, plus its state for
// surfacing in webhook handling.
//
// Buildkite jobs come in several "type" values (script, waiter,
// manual) — only "script" jobs have logs, but we decode the slice
// as-is and let the caller decide whether to skip non-script entries.
type Job struct {
	ID    string `json:"id"`
	Type  string `json:"type"`
	Name  string `json:"name"`
	State string `json:"state"`
}

// Build is the subset of a Buildkite build object we care about.
// Fields not present here are dropped silently by the JSON decoder —
// keep this list lean so additions to the upstream schema don't
// force us to touch this file.
type Build struct {
	ID       string                 `json:"id"`
	Number   int64                  `json:"number"`
	State    string                 `json:"state"`
	WebURL   string                 `json:"web_url"`
	Commit   string                 `json:"commit"`
	Branch   string                 `json:"branch"`
	Message  string                 `json:"message"`
	MetaData map[string]string      `json:"meta_data"`
	Jobs     []Job                  `json:"jobs"`
	Pipeline map[string]interface{} `json:"pipeline"`
}

// CreateBuildRequest is the request body for POST /builds. Only the
// fields callers actively use are exposed — the upstream API accepts
// many more, but we'd just be passing through dead options.
//
// IgnorePipelineBranchFilters / CleanCheckout default to false on the
// wire (omitempty elides the zero value); callers that want them
// turned on should set the field explicitly.
//
// PullRequestBaseBranch surfaces a Tangled PR trigger's target
// branch so Buildkite step filters keyed on the PR base behave the
// way users expect.
type CreateBuildRequest struct {
	Commit                      string            `json:"commit"`
	Branch                      string            `json:"branch"`
	Message                     string            `json:"message,omitempty"`
	Env                         map[string]string `json:"env,omitempty"`
	MetaData                    map[string]string `json:"meta_data,omitempty"`
	CleanCheckout               bool              `json:"clean_checkout,omitempty"`
	IgnorePipelineBranchFilters bool              `json:"ignore_pipeline_branch_filters,omitempty"`
	PullRequestBaseBranch       string            `json:"pull_request_base_branch,omitempty"`
}

// CreateBuild fires a build on the named pipeline. Returns the
// decoded response so the caller can persist build_uuid + number for
// later webhook lookup.
//
// Buildkite returns 201 on success; anything else is wrapped into an
// error that includes the response body so a misconfigured pipeline
// (e.g. wrong slug, missing branch) surfaces useful diagnostics into
// the caller's log.
func (c *Client) CreateBuild(
	ctx context.Context,
	org string,
	pipelineSlug string,
	req CreateBuildRequest,
) (*Build, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal build request: %w", err)
	}

	url := fmt.Sprintf("%s/v2/organizations/%s/pipelines/%s/builds",
		APIBase, org, pipelineSlug,
	)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	httpReq.Header.Set("Authorization", "Bearer "+c.token)
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("create build: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("create build: status %d: %s",
			resp.StatusCode, strings.TrimSpace(string(raw)),
		)
	}

	var out Build
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode build response: %w", err)
	}
	return &out, nil
}

// GetBuild fetches the full build record by number, including the
// jobs slice. Used by callers that need the current set of jobs for
// a known (pipelineSlug, buildNumber) pair.
//
// Returns ErrNotFound when Buildkite responds 404.
func (c *Client) GetBuild(
	ctx context.Context,
	org string,
	pipelineSlug string,
	buildNumber int64,
) (*Build, error) {
	url := fmt.Sprintf("%s/v2/organizations/%s/pipelines/%s/builds/%d",
		APIBase, org, pipelineSlug, buildNumber,
	)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	httpReq.Header.Set("Authorization", "Bearer "+c.token)

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("get build: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("get build: status %d: %s",
			resp.StatusCode, strings.TrimSpace(string(raw)),
		)
	}

	var out Build
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode build: %w", err)
	}
	return &out, nil
}

// GetJobLog fetches the plain-text log for a single job. Buildkite
// supports several formats; we ask for text/plain explicitly so the
// response is one big string the caller can split on newlines.
//
// Returns ErrNotFound when Buildkite responds 404 (typical for a
// job that hasn't started yet — it has no log to serve).
func (c *Client) GetJobLog(
	ctx context.Context,
	org string,
	pipelineSlug string,
	buildNumber int64,
	jobID string,
) (string, error) {
	url := fmt.Sprintf("%s/v2/organizations/%s/pipelines/%s/builds/%d/jobs/%s/log",
		APIBase, org, pipelineSlug, buildNumber, jobID,
	)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", fmt.Errorf("build request: %w", err)
	}
	httpReq.Header.Set("Authorization", "Bearer "+c.token)
	httpReq.Header.Set("Accept", "text/plain")

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return "", fmt.Errorf("get job log: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return "", ErrNotFound
	}
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return "", fmt.Errorf("get job log: status %d: %s",
			resp.StatusCode, strings.TrimSpace(string(raw)),
		)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read job log: %w", err)
	}
	return string(body), nil
}

// WebhookPayload is the small slice of the webhook body callers
// actually decode. Build events all wrap a "build" object; job
// events wrap a "job" — keeping both around lets callers add
// job-level mapping later without changing the decoder.
//
// Organization is the top-level organization object Buildkite mirrors
// onto every webhook. We decode just its slug so the provider can
// reconstruct (org, pipeline_slug, build_number) from a webhook alone
// when the local UUID→tuple mapping hasn't been persisted yet (the
// race between CreateBuild returning and InsertBuildkiteBuild landing).
type WebhookPayload struct {
	Event        string       `json:"event"`
	Build        Build        `json:"build"`
	Job          Job          `json:"job"`
	Organization Organization `json:"organization"`
}

// Organization is the small slice of Buildkite's organization object
// we care about on webhook payloads: just the slug, used as the org
// component of REST URLs in subsequent calls.
type Organization struct {
	Slug string `json:"slug"`
}

// WebhookMode selects how an inbound webhook request is
// authenticated. The two values correspond directly to the two
// settings Buildkite's notification service exposes for this:
// WebhookModeToken sends the secret in the X-Buildkite-Token header
// in plain text; WebhookModeSignature sends an HMAC-SHA256 of the
// body in X-Buildkite-Signature.
type WebhookMode string

const (
	WebhookModeToken     WebhookMode = "token"
	WebhookModeSignature WebhookMode = "signature"
)

// MaxSignatureAge bounds how far the signed timestamp may be from
// the local clock before VerifySignature rejects the request as
// stale (or implausibly future-dated). Without this, an attacker who
// captures one valid signed delivery can replay it indefinitely,
// generating duplicate status events and unbounded growth in the
// events table. Five minutes matches Buildkite's own published
// guidance for verifying their signatures and is the same window
// Stripe et al. use; it absorbs realistic clock skew while keeping
// the replay surface tight.
const MaxSignatureAge = 5 * time.Minute

// timeNow is the clock VerifySignature consults for freshness. A
// package-level var so tests can pin "now" deterministically without
// touching the system clock or threading a clock argument through
// every caller.
var timeNow = time.Now

// VerifySignature validates the X-Buildkite-Signature header against
// secret using the documented "<timestamp>.<body>" HMAC-SHA256
// scheme. Returns nil when the header is well-formed, the digest
// matches, and the signed timestamp is within MaxSignatureAge of
// now; any other condition returns an error.
//
// The header format is "timestamp=<unix>,signature=<hex>".
func VerifySignature(header, secret string, body []byte) error {
	if header == "" {
		return errors.New("missing X-Buildkite-Signature header")
	}
	if secret == "" {
		// A misconfigured server is a programmer bug, but we'd
		// rather fail closed than silently accept any signature.
		return errors.New("server has no webhook secret configured")
	}

	var ts, sig string
	for _, part := range strings.Split(header, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			continue
		}
		switch k {
		case "timestamp":
			ts = v
		case "signature":
			sig = v
		}
	}
	if ts == "" || sig == "" {
		return errors.New("malformed signature header")
	}
	tsInt, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid timestamp: %w", err)
	}
	// Freshness check: reject signatures whose timestamp is more
	// than MaxSignatureAge away from now in either direction. The
	// symmetric bound also rejects implausibly future-dated stamps,
	// which would otherwise let an attacker mint a replay window
	// well into the future.
	skew := timeNow().Sub(time.Unix(tsInt, 0))
	if skew < 0 {
		skew = -skew
	}
	if skew > MaxSignatureAge {
		return fmt.Errorf("signature timestamp outside freshness window (skew %s)", skew)
	}

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(ts))
	mac.Write([]byte("."))
	mac.Write(body)
	expected := hex.EncodeToString(mac.Sum(nil))

	// Compare in constant time to keep the verifier from leaking
	// the expected digest through timing.
	if !hmac.Equal([]byte(expected), []byte(sig)) {
		return errors.New("signature mismatch")
	}
	return nil
}

// VerifyToken handles the simpler X-Buildkite-Token mode: the
// configured secret is sent verbatim in the header. Constant-time
// comparison keeps a brute-forcing attacker from learning the token
// one byte at a time.
//
// Returns nil on match. Two error cases:
//   - missing header on the request (caller should 401),
//   - server has no expected token configured (caller should 500;
//     fail-closed, never accept-anything).
func VerifyToken(header, expected string) error {
	if header == "" {
		return errors.New("missing X-Buildkite-Token header")
	}
	if expected == "" {
		return errors.New("server has no webhook token configured")
	}
	if !hmac.Equal([]byte(header), []byte(expected)) {
		return errors.New("token mismatch")
	}
	return nil
}
