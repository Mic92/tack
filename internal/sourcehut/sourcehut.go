// Package sourcehut is a minimal builds.sr.ht client used by the
// sourcehut Provider implementation in tack. Job submission and
// state queries go to GraphQL at /query; plain-text task logs are
// fetched from /query/log/<id>[/<task>]/log.
package sourcehut

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// DefaultBaseURL is the public builds.sr.ht endpoint. Operators
// running their own sourcehut instance can override it on the Client.
const DefaultBaseURL = "https://builds.sr.ht"

// ErrNotFound is returned by Get* methods when the upstream returns
// 404 (or, for GraphQL, when the requested job's `job` field comes
// back null). The Provider maps this onto its own ErrLogsNotFound
// for the log subscription handler.
var ErrNotFound = errors.New("sourcehut: not found")

// ErrUnauthorized is returned by Get* methods when the upstream rejects
// the request with 401/403 — most commonly because the configured
// personal access token is missing the requisite scope (e.g. the log
// endpoints require `builds.sr.ht/LOGS:RO`). It's distinct from
// ErrNotFound so callers can surface a systemic auth misconfiguration
// instead of silently treating it as "no logs yet".
var ErrUnauthorized = errors.New("sourcehut: unauthorized")

// Client is a thin wrapper around net/http carrying API credentials
// and the target instance base URL. Safe for concurrent use; the
// embedded http.Client is goroutine-safe.
type Client struct {
	http    *http.Client
	baseURL string
	token   string
}

// NewClient builds a Client targeting baseURL using the given OAuth2
// personal access token. baseURL should be the scheme+host of the
// builds.sr.ht instance (e.g. "https://builds.sr.ht"); pass empty to
// fall back to DefaultBaseURL.
func NewClient(baseURL, token string) *Client {
	if baseURL == "" {
		baseURL = DefaultBaseURL
	}
	return &Client{
		http:    &http.Client{Timeout: 30 * time.Second},
		baseURL: strings.TrimRight(baseURL, "/"),
		token:   token,
	}
}

// Owner is the small slice of the upstream owner object we care
// about. CanonicalName carries the leading "~" (e.g. "~someone").
type Owner struct {
	CanonicalName string `json:"canonicalName"`
}

// Task is one step inside a sourcehut job.
type Task struct {
	Name   string `json:"name"`
	Status string `json:"status"`
}

// Job is the subset of the upstream job object we care about.
type Job struct {
	ID     int64  `json:"id"`
	Status string `json:"status"`
	Note   string `json:"note"`
	Owner  Owner  `json:"owner"`
	Tasks  []Task `json:"tasks"`
}

// SubmitRequest mirrors the arguments of the GraphQL `submit`
// mutation. Manifest is the YAML build manifest exactly as the user
// would submit it via the web UI. Execute=true tells builds.sr.ht to
// start the job immediately; Secrets controls whether the runner
// mounts the user's configured secrets.
type SubmitRequest struct {
	Manifest string
	Tags     []string
	Note     string
	Secrets  bool
	Execute  bool
}

// gqlRequest / gqlResponse are the on-the-wire shapes for a single
// GraphQL POST. We hand-roll them because the entire query surface we
// touch is two operations — pulling in a real GraphQL library would
// dwarf the call sites.
type gqlRequest struct {
	Query     string         `json:"query"`
	Variables map[string]any `json:"variables,omitempty"`
}

type gqlError struct {
	Message string `json:"message"`
}

type gqlResponse struct {
	Data   json.RawMessage `json:"data"`
	Errors []gqlError      `json:"errors"`
}

// SubmitJob fires a job via the GraphQL `submit` mutation and returns
// the freshly created Job.
//
// The submit mutation's response is shallow — it doesn't include the
// per-task list — so callers wanting a populated Tasks slice should
// follow up with GetJob once the runner has scheduled the job. That
// matches how the Provider already drives its watch loop.
func (c *Client) SubmitJob(ctx context.Context, req SubmitRequest) (*Job, error) {
	const query = `mutation($manifest:String!, $tags:[String!], $note:String, $execute:Boolean, $secrets:Boolean){
		submit(manifest:$manifest, tags:$tags, note:$note, execute:$execute, secrets:$secrets){
			id status note owner{ canonicalName }
		}
	}`
	vars := map[string]any{
		"manifest": req.Manifest,
		"execute":  req.Execute,
		"secrets":  req.Secrets,
	}
	if len(req.Tags) > 0 {
		vars["tags"] = req.Tags
	}
	if req.Note != "" {
		vars["note"] = req.Note
	}

	raw, err := c.gql(ctx, query, vars)
	if err != nil {
		return nil, err
	}
	var wrap struct {
		Submit *Job `json:"submit"`
	}
	if err := json.Unmarshal(raw, &wrap); err != nil {
		return nil, fmt.Errorf("decode submit response: %w", err)
	}
	if wrap.Submit == nil {
		// Sourcehut returns `submit: null` with no errors when the
		// manifest fails server-side validation in some edge cases;
		// guard so the caller doesn't dereference nil.
		return nil, errors.New("sourcehut: submit returned null job")
	}
	return wrap.Submit, nil
}

// GetJob fetches the current state of a job via GraphQL. Returns
// ErrNotFound when the upstream `job(id:)` field resolves to null —
// the usual signal for an unknown or deleted job.
func (c *Client) GetJob(ctx context.Context, id int64) (*Job, error) {
	const query = `query($id:Int!){
		job(id:$id){
			id status note owner{ canonicalName } tasks{ name status }
		}
	}`
	raw, err := c.gql(ctx, query, map[string]any{"id": id})
	if err != nil {
		return nil, err
	}
	var wrap struct {
		Job *Job `json:"job"`
	}
	if err := json.Unmarshal(raw, &wrap); err != nil {
		return nil, fmt.Errorf("decode job response: %w", err)
	}
	if wrap.Job == nil {
		return nil, ErrNotFound
	}
	return wrap.Job, nil
}

// GetTaskLog fetches the plain-text log for a single task inside a
// job. taskName must match the task's `name` exactly; pass an empty
// string to fetch the master log (the wrapper output that contains
// setup steps before any task runs). Returns ErrNotFound on 404 —
// common for a task that hasn't started yet, or a master log fetched
// before the runner has produced output.
func (c *Client) GetTaskLog(ctx context.Context, jobID int64, taskName string) (string, error) {
	path := fmt.Sprintf("%s/query/log/%d/log", c.baseURL, jobID)
	if taskName != "" {
		path = fmt.Sprintf("%s/query/log/%d/%s/log",
			c.baseURL, jobID, url.PathEscape(taskName),
		)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, path, nil)
	if err != nil {
		return "", fmt.Errorf("build request: %w", err)
	}
	c.setAuth(httpReq)

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return "", fmt.Errorf("get task log: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return "", ErrNotFound
	}
	if resp.StatusCode == http.StatusUnauthorized ||
		resp.StatusCode == http.StatusForbidden {
		// Surface auth failures distinctly so callers can stop the
		// log stream loudly rather than silently emitting empty
		// steps for every task. The most common cause is a token
		// missing `builds.sr.ht/LOGS:RO`.
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return "", fmt.Errorf("get task log: status %d: %s: %w",
			resp.StatusCode, strings.TrimSpace(string(raw)),
			ErrUnauthorized,
		)
	}
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return "", fmt.Errorf("get task log: status %d: %s",
			resp.StatusCode, strings.TrimSpace(string(raw)),
		)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read task log: %w", err)
	}
	return string(body), nil
}

// JobWebURL renders the human-visible URL for a job.
func (c *Client) JobWebURL(owner string, jobID int64) string {
	return fmt.Sprintf("%s/%s/job/%d", c.baseURL, owner, jobID)
}

// gql posts a single GraphQL request and returns the raw `data`
// payload. Errors from the upstream "errors" array are surfaced as a
// single Go error so callers don't have to hand-decode them.
func (c *Client) gql(ctx context.Context, query string, vars map[string]any) (json.RawMessage, error) {
	body, err := json.Marshal(gqlRequest{Query: query, Variables: vars})
	if err != nil {
		return nil, fmt.Errorf("marshal gql request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.baseURL+"/query", bytes.NewReader(body),
	)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	c.setAuth(httpReq)
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("post gql: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("gql: status %d: %s",
			resp.StatusCode, strings.TrimSpace(string(raw)),
		)
	}
	var out gqlResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode gql response: %w", err)
	}
	if len(out.Errors) > 0 {
		msgs := make([]string, 0, len(out.Errors))
		for _, e := range out.Errors {
			msgs = append(msgs, e.Message)
		}
		return nil, fmt.Errorf("gql errors: %s", strings.Join(msgs, "; "))
	}
	return out.Data, nil
}

// setAuth applies the OAuth2 Bearer header builds.sr.ht expects.
func (c *Client) setAuth(req *http.Request) {
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
}
