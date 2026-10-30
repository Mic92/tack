package main

// Tests for the current org.tangled.ci.* XRPC routes that the Tangled web app calls from the
// browser in Tangled v1.20, the CORS and WebSocket origin rules those browser calls depend on,
// the legacy routes kept beside them, and the bounds on public repository lookups. Most run
// against the full HTTP handler through orgFixture (org_fixture_test.go), so routing, service
// auth, and middleware are exercised along with the handlers.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"tangled.org/core/api/tangled"
	"tangled.org/core/repoident"

	"go.mitchellh.com/tack/internal/orgtangled"
)

// queryOrgPipelines calls org.tangled.ci.queryPipelines with params and returns the decoded
// output. Any status other than 200 fails the test.
func queryOrgPipelines(t *testing.T, f *orgFixture, params url.Values) orgtangled.QueryPipelinesOutput {
	t.Helper()
	response := f.get("/xrpc/" + orgtangled.NSIDQueryPipelines + "?" + params.Encode())
	if response.Code != http.StatusOK {
		t.Fatalf("queryPipelines %v = %d %s", params, response.Code, response.Body)
	}
	var output orgtangled.QueryPipelinesOutput
	if err := json.Unmarshal(response.Body.Bytes(), &output); err != nil {
		t.Fatal(err)
	}
	return output
}

// publishStatus stores a status event for one workflow of pipeline rkey, the same row a provider
// status update writes. Pipeline reads derive a workflow's status, startedAt, and finishedAt
// from these rows, so tests use it to move a workflow through its lifecycle.
func (f *orgFixture) publishStatus(rkey, workflow, status, createdAt string) {
	f.t.Helper()
	encoded, err := json.Marshal(tangled.PipelineStatus{
		LexiconTypeID: tangled.PipelineStatusNSID,
		Pipeline:      pipelineATURI(testKnotHost, rkey),
		Workflow:      workflow,
		Status:        status,
		CreatedAt:     createdAt,
	})
	if err != nil {
		f.t.Fatal(err)
	}
	if _, err := f.st.InsertEvent(context.Background(), status, tangled.PipelineStatusNSID, encoded); err != nil {
		f.t.Fatal(err)
	}
}

// seedMixedHistory stores one pipeline of each trigger kind for testRepoDID, oldest first: a push
// (3m5x7k2aaaaa2), a manual run (3m5x7k2bbbbb2), and a pull request (3m5x7k2ccccc2). It also
// stores a push for did:plc:elsewhere (3m5x7k2ddddd2) so tests can check that reads and
// procedures stay inside the requested repository. The records use the stored sh.tangled.pipeline
// form, as existing databases hold them.
func seedMixedHistory(f *orgFixture) {
	f.seedPipeline("3m5x7k2aaaaa2", pushRecord(testRepoDID, testNewSHA))
	manual := pushRecord(testRepoDID, "")
	manual.TriggerMetadata.Kind, manual.TriggerMetadata.Push = "manual", nil
	ref := "refs/heads/feature"
	manual.TriggerMetadata.Manual = &tangled.Pipeline_ManualTriggerData{
		Sha: "3333333333333333333333333333333333333333", Ref: &ref,
		Inputs: []*tangled.Pipeline_Pair{{Key: "k", Value: "v"}},
	}
	f.seedPipeline("3m5x7k2bbbbb2", manual)
	pull := pushRecord(testRepoDID, "")
	pullURI := "at://did:plc:repo/org.tangled.track.ticket/1"
	pull.TriggerMetadata.Kind, pull.TriggerMetadata.Push = "pull_request", nil
	pull.TriggerMetadata.PullRequest = &tangled.Pipeline_PullRequestTriggerData{
		Pull: &pullURI, SourceBranch: "feature",
		SourceSha: "4444444444444444444444444444444444444444", TargetBranch: "main",
	}
	f.seedPipeline("3m5x7k2ccccc2", pull)
	f.seedPipeline("3m5x7k2ddddd2", pushRecord("did:plc:elsewhere", testNewSHA))
}

// TestOrgQueryPipelinesProjectsRetainedHistory pins the org.tangled.ci.defs pipeline shape for
// push, manual, and pull request triggers. The rows are stored in the legacy form that existing
// tack.db databases already hold, because Tack converts history when it reads it and never
// rewrites it. The test also checks workflow timestamps taken from status events, the pending
// status of a workflow with no events, and the workflow definition's file source.
func TestOrgQueryPipelinesProjectsRetainedHistory(t *testing.T) {
	f := newOrgFixture(t)
	seedMixedHistory(f)
	f.publishStatus("3m5x7k2aaaaa2", "ci.yml", "running", "2026-10-30T10:00:00Z")
	f.publishStatus("3m5x7k2aaaaa2", "ci.yml", "success", "2026-10-30T10:05:00Z")

	output := queryOrgPipelines(t, f, url.Values{"repo": {testRepoDID}})
	if output.Total != 3 || len(output.Pipelines) != 3 || output.Cursor != nil {
		t.Fatalf("total=%d pipelines=%d cursor=%v; want 3/3/nil", output.Total, len(output.Pipelines), output.Cursor)
	}
	// Newest first. The web app keeps the first pipeline it sees for each commit, so the order
	// decides which run a commit's status shows.
	pull, manual, push := output.Pipelines[0], output.Pipelines[1], output.Pipelines[2]
	if pull.ID != "3m5x7k2ccccc2" || manual.ID != "3m5x7k2bbbbb2" || push.ID != "3m5x7k2aaaaa2" {
		t.Fatalf("order = %s, %s, %s", pull.ID, manual.ID, push.ID)
	}
	for _, pipeline := range output.Pipelines {
		if pipeline.Repo != testRepoDID || pipeline.CreatedAt == nil || len(pipeline.Workflows) != 1 {
			t.Fatalf("pipeline = %+v", pipeline)
		}
	}

	if push.Trigger.Push == nil || push.Trigger.Push.Repo != testRepoDID ||
		push.Trigger.Push.Ref != "refs/heads/main" || push.Commit != testNewSHA {
		t.Fatalf("push trigger = %+v commit %s", push.Trigger.Push, push.Commit)
	}
	run := push.Workflows[0]
	if run.ID != "ci.yml" || run.Name == nil || *run.Name != "ci.yml" || run.Status != "success" ||
		pointerValue(run.StartedAt) != "2026-10-30T10:00:00Z" ||
		pointerValue(run.FinishedAt) != "2026-10-30T10:05:00Z" {
		t.Fatalf("workflow run = %+v", run)
	}
	definition := run.Definition
	if definition == nil || definition.ID != "ci.yml" || definition.Source.File == nil ||
		definition.Source.File.Path != ".tangled/workflows/ci.yml" ||
		definition.Source.File.Commit != testNewSHA ||
		!equalStrings(definition.Triggers, []string{orgtangled.NSIDPushEvent}) {
		t.Fatalf("definition = %+v", definition)
	}

	if manual.Trigger.Manual == nil || manual.Commit != "3333333333333333333333333333333333333333" ||
		manual.Trigger.Manual.Commit != manual.Commit ||
		manual.Trigger.Manual.Repo != testRepoDID ||
		len(manual.Trigger.Manual.Inputs) != 1 || manual.Workflows[0].Status != "pending" {
		t.Fatalf("manual = %+v trigger %+v", manual, manual.Trigger.Manual)
	}
	pr := pull.Trigger.PullRequest
	if pr == nil || pointerValue(pr.Pull) != "at://did:plc:repo/org.tangled.track.ticket/1" ||
		pointerValue(pr.SourceBranch) != "feature" || pointerValue(pr.TargetBranch) != "main" ||
		pull.Commit != "4444444444444444444444444444444444444444" {
		t.Fatalf("pull trigger = %+v", pr)
	}

	// The web app picks the trigger and source variant by its $type tag, so check the encoded
	// JSON and not only the decoded structs.
	raw := f.get("/xrpc/" + orgtangled.NSIDQueryPipelines + "?repo=" + url.QueryEscape(testRepoDID)).Body.String()
	for _, want := range []string{
		`"$type":"org.tangled.event.push"`,
		`"$type":"org.tangled.event.pullRequest"`,
		`"$type":"org.tangled.ci.trigger.manual"`,
		`"$type":"org.tangled.ci.defs#fileSource"`,
	} {
		if !strings.Contains(raw, want) {
			t.Errorf("response lacks %s", want)
		}
	}
}

// TestOrgQueryPipelinesFilters pins the triggers, commits, limit, and cursor parameters of
// queryPipelines. Triggers are trigger NSIDs such as org.tangled.event.push and
// org.tangled.ci.trigger.manual. total counts every match regardless of the page, and an unknown
// trigger NSID yields an empty, non-null list rather than an error. The last loop pins the 400
// InvalidRequest answers for a malformed repo DID, a limit outside 1 to 250 (the upstream
// spindle's range), and a commits value over the lexicon's 64-character cap.
func TestOrgQueryPipelinesFilters(t *testing.T) {
	f := newOrgFixture(t)
	seedMixedHistory(f)

	push := queryOrgPipelines(t, f, url.Values{"repo": {testRepoDID}, "triggers": {orgtangled.NSIDPushEvent}})
	if push.Total != 1 || len(push.Pipelines) != 1 || push.Pipelines[0].Trigger.Push == nil {
		t.Fatalf("push filter = %+v", push)
	}
	several := queryOrgPipelines(t, f, url.Values{"repo": {testRepoDID},
		"triggers": {orgtangled.NSIDPullRequestEvent, orgtangled.NSIDManualTrigger}})
	if several.Total != 2 {
		t.Fatalf("pull+manual total = %d; want 2", several.Total)
	}
	unknown := queryOrgPipelines(t, f, url.Values{"repo": {testRepoDID}, "triggers": {"org.example.future"}})
	if unknown.Total != 0 || unknown.Pipelines == nil || len(unknown.Pipelines) != 0 {
		t.Fatalf("unknown trigger = %+v; want an empty, non-null list", unknown)
	}
	commit := queryOrgPipelines(t, f, url.Values{"repo": {testRepoDID},
		"commits": {"4444444444444444444444444444444444444444", testNewSHA}})
	if commit.Total != 2 {
		t.Fatalf("commit filter total = %d; want 2", commit.Total)
	}

	first := queryOrgPipelines(t, f, url.Values{"repo": {testRepoDID}, "limit": {"2"}})
	if first.Total != 3 || len(first.Pipelines) != 2 || first.Cursor == nil {
		t.Fatalf("page 1 = total %d, %d pipelines, cursor %v", first.Total, len(first.Pipelines), first.Cursor)
	}
	second := queryOrgPipelines(t, f, url.Values{"repo": {testRepoDID}, "limit": {"2"}, "cursor": {*first.Cursor}})
	if second.Total != 3 || len(second.Pipelines) != 1 || second.Pipelines[0].ID != "3m5x7k2aaaaa2" {
		t.Fatalf("page 2 = %+v", second)
	}
	// Cursors are opaque. As in the upstream spindle, a malformed one means the first page
	// rather than an error.
	if bad := queryOrgPipelines(t, f, url.Values{"repo": {testRepoDID}, "cursor": {"garbage"}}); len(bad.Pipelines) != 3 {
		t.Fatalf("bad cursor returned %d pipelines", len(bad.Pipelines))
	}

	for _, params := range []url.Values{
		{"repo": {"not-a-did"}},
		{"repo": {testRepoDID}, "limit": {"0"}},
		{"repo": {testRepoDID}, "limit": {"251"}},
		{"repo": {testRepoDID}, "commits": {strings.Repeat("a", 65)}},
	} {
		response := f.get("/xrpc/" + orgtangled.NSIDQueryPipelines + "?" + params.Encode())
		if response.Code != http.StatusBadRequest || decodeXRPCError(t, response) != "InvalidRequest" {
			t.Fatalf("%v = %d %s; want 400 InvalidRequest", params, response.Code, response.Body)
		}
	}
}

// TestOrgQueryPipelinesRepoDiscovery pins how queryPipelines separates a served repository with
// no runs yet from one Tack does not serve. The first must get an empty list and the second a 404
// RepoNotFound, because the web app shows "no runs" for one and "has not indexed this
// repository" for the other. An unresolvable DID and a repo manifest that names another spindle
// both count as not served.
func TestOrgQueryPipelinesRepoDiscovery(t *testing.T) {
	f := newOrgFixture(t)
	empty := queryOrgPipelines(t, f, url.Values{"repo": {testRepoDID}})
	if empty.Total != 0 || empty.Pipelines == nil {
		t.Fatalf("served repo without runs = %+v; want empty list", empty)
	}
	for _, repo := range []string{"did:plc:unknown", "did:plc:foreign"} {
		if repo == "did:plc:foreign" {
			// This repository resolves, but its manifest names a different spindle.
			f.repos[repo] = &RepoRef{
				RepoDID: repo, OwnerDID: testOwnerDID, Rkey: "x", Knot: testKnotHost,
				Spindle: "did:web:other-spindle.example", FromManifest: true,
			}
		}
		response := f.get("/xrpc/" + orgtangled.NSIDQueryPipelines + "?repo=" + url.QueryEscape(repo))
		if response.Code != http.StatusNotFound || decodeXRPCError(t, response) != "RepoNotFound" {
			t.Fatalf("%s = %d %s; want 404 RepoNotFound", repo, response.Code, response.Body)
		}
	}
}

// TestOrgGetPipeline pins that getPipeline answers only within the requested repository. A record
// key that exists under another repository is PipelineNotFound, the same as a missing one, so the
// web app never shows one repository's pipeline under another's path. It also pins the 400
// InvalidRequest answers for a missing record key and a malformed repo DID.
func TestOrgGetPipeline(t *testing.T) {
	f := newOrgFixture(t)
	seedMixedHistory(f)
	get := func(repo, pipeline string) *httptest.ResponseRecorder {
		return f.get("/xrpc/" + orgtangled.NSIDGetPipeline + "?" +
			url.Values{"repo": {repo}, "pipeline": {pipeline}}.Encode())
	}

	response := get(testRepoDID, "3m5x7k2aaaaa2")
	if response.Code != http.StatusOK {
		t.Fatalf("getPipeline = %d %s", response.Code, response.Body)
	}
	var pipeline orgtangled.Pipeline
	if err := json.Unmarshal(response.Body.Bytes(), &pipeline); err != nil {
		t.Fatal(err)
	}
	if pipeline.ID != "3m5x7k2aaaaa2" || pipeline.Repo != testRepoDID || pipeline.Trigger.Push == nil {
		t.Fatalf("pipeline = %+v", pipeline)
	}

	for _, test := range []struct {
		repo, pipeline string
		status         int
		errorName      string
	}{
		{"did:plc:elsewhere", "3m5x7k2aaaaa2", http.StatusNotFound, "PipelineNotFound"},
		{testRepoDID, "3m5x7k2ddddd2", http.StatusNotFound, "PipelineNotFound"},
		{testRepoDID, "3m5x7k2zzzzz2", http.StatusNotFound, "PipelineNotFound"},
		{testRepoDID, "", http.StatusBadRequest, "InvalidRequest"},
		{"nope", "3m5x7k2aaaaa2", http.StatusBadRequest, "InvalidRequest"},
	} {
		response := get(test.repo, test.pipeline)
		if response.Code != test.status || decodeXRPCError(t, response) != test.errorName {
			t.Fatalf("get(%s, %s) = %d %s; want %d %s",
				test.repo, test.pipeline, response.Code, response.Body, test.status, test.errorName)
		}
	}
}

// TestOrgSubscribePipelineLogsValidation pins the errors subscribePipelineLogs returns before the
// WebSocket upgrade: a missing repo, a pipeline from another repository, a missing pipeline, and
// an unknown workflow. These are the only failures a client can read as XRPC errors. After the
// upgrade, a failure can only show up as a closed socket.
func TestOrgSubscribePipelineLogsValidation(t *testing.T) {
	f := newOrgFixture(t)
	seedMixedHistory(f)
	for _, test := range []struct {
		params    url.Values
		status    int
		errorName string
	}{
		{url.Values{"pipeline": {"3m5x7k2aaaaa2"}}, http.StatusBadRequest, "InvalidRequest"},
		{url.Values{"repo": {"did:plc:elsewhere"}, "pipeline": {"3m5x7k2aaaaa2"}}, http.StatusNotFound, "PipelineNotFound"},
		{url.Values{"repo": {testRepoDID}, "pipeline": {"3m5x7k2zzzzz2"}}, http.StatusNotFound, "PipelineNotFound"},
		{url.Values{"repo": {testRepoDID}, "pipeline": {"3m5x7k2aaaaa2"}, "workflows": {"nope.yml"}},
			http.StatusNotFound, "WorkflowNotFound"},
	} {
		response := f.get("/xrpc/" + orgtangled.NSIDSubscribePipelineLogs + "?" + test.params.Encode())
		if response.Code != test.status || decodeXRPCError(t, response) != test.errorName {
			t.Fatalf("%v = %d %s; want %d %s", test.params, response.Code, response.Body, test.status, test.errorName)
		}
	}
}

// TestOrgSubscribePipelineLogsBrowserOrigin pins the origin check on the log WebSocket. CORS does
// not apply to WebSocket handshakes, so corsPolicy.CheckOrigin is the only thing that keeps other
// sites out. A page on tangled.org (an allowed origin) and a client that sends no Origin header
// (a server-side client such as Tangled's cilogs SSH log viewer) both connect. An unlisted
// origin is refused with 403.
func TestOrgSubscribePipelineLogsBrowserOrigin(t *testing.T) {
	f := newOrgFixture(t)
	f.seedPipeline("3m5x7k2aaaaa2", pushRecord(testRepoDID, testNewSHA))
	// The workflow has already finished, so the stream ends on its own with a normal close and
	// each dial below completes without waiting on a live build.
	f.publishStatus("3m5x7k2aaaaa2", "ci.yml", "success", "2026-10-30T10:05:00Z")
	server := httptest.NewServer(f.handler)
	defer server.Close()

	endpoint := "ws" + strings.TrimPrefix(server.URL, "http") + "/xrpc/" +
		orgtangled.NSIDSubscribePipelineLogs + "?" +
		url.Values{"repo": {testRepoDID}, "pipeline": {"3m5x7k2aaaaa2"}, "workflows": {"ci.yml"}}.Encode()
	dial := func(origin string) (*websocket.Conn, *http.Response, error) {
		header := http.Header{}
		if origin != "" {
			header.Set("Origin", origin)
		}
		dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
		return dialer.Dial(endpoint, header)
	}

	for _, origin := range []string{"https://tangled.org", ""} {
		conn, _, err := dial(origin)
		if err != nil {
			t.Fatalf("origin %q: dial: %v", origin, err)
		}
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, _, err = conn.ReadMessage()
		if !websocket.IsCloseError(err, websocket.CloseNormalClosure) {
			t.Fatalf("origin %q: read = %v; want a normal close", origin, err)
		}
		conn.Close()
	}
	if _, response, err := dial("https://evil.example"); err == nil || response == nil ||
		response.StatusCode != http.StatusForbidden {
		t.Fatalf("unlisted origin: err=%v response=%v; want 403", err, response)
	}
}

// TestOrgRetryPipeline pins that retryPipeline creates a new pipeline that reruns the original
// compiled workflows under the original trigger, rather than compiling the branch again. It then
// pins the answer for each refusal, including a record key from another repository, which must
// be refused because the caller is only authorized for the repository it named.
func TestOrgRetryPipeline(t *testing.T) {
	f := newOrgFixture(t)
	f.seedPipeline("3m5x7k2aaaaa2", pushRecord(testRepoDID, testNewSHA))
	f.seedPipeline("3m5x7k2ddddd2", pushRecord("did:plc:elsewhere", testNewSHA))
	ownerKey := f.newSigner(testOwnerDID)
	// retry sends a correctly signed service auth token from issuer. Every issuer has a key in
	// the mock directory, so a refusal can only come from authorization, not from signature
	// checks.
	retry := func(issuer string, input orgtangled.RetryPipelineInput) *httptest.ResponseRecorder {
		key := ownerKey
		if issuer != testOwnerDID {
			key = f.newSigner(issuer)
		}
		return f.post(orgtangled.NSIDRetryPipeline,
			f.token(issuer, key, orgtangled.NSIDRetryPipeline, time.Minute), input)
	}

	response := retry(testOwnerDID, orgtangled.RetryPipelineInput{Repo: testRepoDID, Pipeline: "3m5x7k2aaaaa2"})
	if response.Code != http.StatusOK {
		t.Fatalf("retry = %d %s", response.Code, response.Body)
	}
	var output orgtangled.RetryPipelineOutput
	if err := json.Unmarshal(response.Body.Bytes(), &output); err != nil {
		t.Fatal(err)
	}
	if output.Pipeline == "" || output.Pipeline == "3m5x7k2aaaaa2" {
		t.Fatalf("retry output = %+v; want a new pipeline id", output)
	}
	if got := f.provider.names(); !equalStrings(got, []string{"ci.yml"}) {
		t.Fatalf("retry spawned %v", got)
	}
	rerun, _, err := f.st.GetRawPipeline(context.Background(), output.Pipeline)
	if err != nil {
		t.Fatal(err)
	}
	if rerun.TriggerMetadata.Kind != "push" || rerun.TriggerMetadata.Push.NewSha != testNewSHA ||
		len(rerun.Workflows) != 1 || rerun.Workflows[0].Name != "ci.yml" {
		t.Fatalf("rerun = %+v", rerun)
	}
	// After a retry the web app opens the same workflow id in the new pipeline, so getPipeline
	// must find it there.
	if get := f.get("/xrpc/" + orgtangled.NSIDGetPipeline + "?" + url.Values{
		"repo": {testRepoDID}, "pipeline": {output.Pipeline},
	}.Encode()); get.Code != http.StatusOK || !strings.Contains(get.Body.String(), `"id":"ci.yml"`) {
		t.Fatalf("rerun lookup = %d %s", get.Code, get.Body)
	}

	for _, test := range []struct {
		name      string
		issuer    string
		input     orgtangled.RetryPipelineInput
		status    int
		errorName string
	}{
		{"pipeline of another repository", testOwnerDID,
			orgtangled.RetryPipelineInput{Repo: testRepoDID, Pipeline: "3m5x7k2ddddd2"},
			http.StatusForbidden, "AccessControl"},
		{"missing pipeline", testOwnerDID,
			orgtangled.RetryPipelineInput{Repo: testRepoDID, Pipeline: "3m5x7k2zzzzz2"},
			http.StatusNotFound, "PipelineNotFound"},
		{"unknown workflow", testOwnerDID,
			orgtangled.RetryPipelineInput{Repo: testRepoDID, Pipeline: "3m5x7k2aaaaa2", Workflows: []string{"nope.yml"}},
			http.StatusBadRequest, "InvalidRequest"},
		{"unrelated user", "did:plc:mallory",
			orgtangled.RetryPipelineInput{Repo: testRepoDID, Pipeline: "3m5x7k2aaaaa2"},
			http.StatusForbidden, "AccessControl"},
		// did:plc:unknown does not resolve. On this authenticated path a resolution failure is an
		// operational error, not a refusal, so it is a 500 the client may retry.
		{"repo not served", testOwnerDID,
			orgtangled.RetryPipelineInput{Repo: "did:plc:unknown", Pipeline: "3m5x7k2aaaaa2"},
			http.StatusInternalServerError, "InternalError"},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := retry(test.issuer, test.input)
			if response.Code != test.status || decodeXRPCError(t, response) != test.errorName {
				t.Fatalf("response = %d %s; want %d %s", response.Code, response.Body, test.status, test.errorName)
			}
		})
	}
}

// TestOrgRetryPipelineAcceptsRepoToken pins that retryPipeline accepts a repo token: service auth
// issued in the repository DID's own name. The web app uses one whenever the knot can mint it,
// which knot2 can. The knot mints it only after its own permission check, so Tack skips the
// collaborator check for it.
func TestOrgRetryPipelineAcceptsRepoToken(t *testing.T) {
	f := newOrgFixture(t)
	f.seedPipeline("3m5x7k2aaaaa2", pushRecord(testRepoDID, testNewSHA))
	key := f.newSigner(testRepoDID)
	response := f.post(orgtangled.NSIDRetryPipeline,
		f.token(testRepoDID, key, orgtangled.NSIDRetryPipeline, time.Minute),
		orgtangled.RetryPipelineInput{Repo: testRepoDID, Pipeline: "3m5x7k2aaaaa2"})
	if response.Code != http.StatusOK {
		t.Fatalf("repo-token retry = %d %s", response.Code, response.Body)
	}
}

// TestOrgApproveAndCancel pins the two procedures Tack answers without acting on. Tack never
// holds a pipeline for approval, so an authorized approvePipeline gets 400 PipelineNotBlocked,
// and approvePipeline still requires service auth (403 without a token). cancelPipeline answers
// 501 MethodNotImplemented to every caller, so clients get a well-formed reason instead of a
// plain 404.
func TestOrgApproveAndCancel(t *testing.T) {
	f := newOrgFixture(t)
	f.seedPipeline("3m5x7k2aaaaa2", pushRecord(testRepoDID, testNewSHA))
	key := f.newSigner(testOwnerDID)
	approve := f.post(orgtangled.NSIDApprovePipeline,
		f.token(testOwnerDID, key, orgtangled.NSIDApprovePipeline, time.Minute),
		orgtangled.ApprovePipelineInput{Repo: testRepoDID, Pipeline: "3m5x7k2aaaaa2"})
	if approve.Code != http.StatusBadRequest || decodeXRPCError(t, approve) != "PipelineNotBlocked" {
		t.Fatalf("approve = %d %s; want 400 PipelineNotBlocked", approve.Code, approve.Body)
	}
	unauthenticated := f.post(orgtangled.NSIDApprovePipeline, "",
		orgtangled.ApprovePipelineInput{Repo: testRepoDID, Pipeline: "3m5x7k2aaaaa2"})
	if unauthenticated.Code != http.StatusForbidden {
		t.Fatalf("unauthenticated approve = %d", unauthenticated.Code)
	}
	cancel := f.post(orgtangled.NSIDCancelPipeline, "", nil)
	if cancel.Code != http.StatusNotImplemented || decodeXRPCError(t, cancel) != "MethodNotImplemented" {
		t.Fatalf("cancel = %d %s; want 501", cancel.Code, cancel.Body)
	}
}

// TestLegacyRoutesStillServed pins that the legacy sh.tangled.ci.queryPipelines and
// sh.tangled.owner routes stay registered beside the current ones. The legacy appview still calls
// them, so adding the org.tangled.* routes must not replace them.
func TestLegacyRoutesStillServed(t *testing.T) {
	f := newOrgFixture(t)
	f.seedPipeline("3m5x7k2aaaaa2", pushRecord(testRepoDID, testNewSHA))
	legacy := f.get("/xrpc/" + tangled.CiQueryPipelinesNSID + "?repo=" + url.QueryEscape(testRepoDID))
	if legacy.Code != http.StatusOK || !strings.Contains(legacy.Body.String(), "sh.tangled.ci.trigger#push") {
		t.Fatalf("legacy query = %d %s", legacy.Code, legacy.Body)
	}
	if owner := f.get("/xrpc/" + tangled.OwnerNSID); owner.Code != http.StatusOK {
		t.Fatalf("sh.tangled.owner = %d", owner.Code)
	}
}

// TestCORSForBrowserCalls pins the CORS headers the web app's browser requests depend on. CORS
// (Cross-Origin Resource Sharing) is the browser rule that hides a response from a page on another
// origin unless the server opts in. The test covers the preflight that an Authorization header
// triggers, Access-Control-Allow-Origin on error responses, no headers for an unlisted origin or
// for paths outside /xrpc, and a literal "*" when the policy allows every origin.
func TestCORSForBrowserCalls(t *testing.T) {
	f := newOrgFixture(t)
	request := func(method, target, origin string, headers map[string]string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, target, nil)
		r.Header.Set("Origin", origin)
		for key, value := range headers {
			r.Header.Set(key, value)
		}
		w := httptest.NewRecorder()
		f.handler.ServeHTTP(w, r)
		return w
	}

	preflight := request(http.MethodOptions, "/xrpc/"+orgtangled.NSIDRetryPipeline, "https://tangled.org",
		map[string]string{"Access-Control-Request-Method": "POST", "Access-Control-Request-Headers": "authorization"})
	if preflight.Code != http.StatusNoContent ||
		preflight.Header().Get("Access-Control-Allow-Origin") != "https://tangled.org" ||
		!strings.Contains(preflight.Header().Get("Access-Control-Allow-Headers"), "Authorization") ||
		!strings.Contains(preflight.Header().Get("Access-Control-Allow-Methods"), "POST") {
		t.Fatalf("preflight = %d %v", preflight.Code, preflight.Header())
	}

	// Error responses need the header too. Without it the browser hides the body, and the web
	// app cannot tell RepoNotFound from a network failure.
	notFound := request(http.MethodGet, "/xrpc/"+orgtangled.NSIDQueryPipelines+"?repo=did:plc:unknown",
		"https://tangled.org", nil)
	if notFound.Code != http.StatusNotFound || notFound.Header().Get("Access-Control-Allow-Origin") != "https://tangled.org" {
		t.Fatalf("error response = %d %v", notFound.Code, notFound.Header())
	}

	evil := request(http.MethodGet, "/xrpc/"+orgtangled.NSIDQueryPipelines+"?repo="+url.QueryEscape(testRepoDID),
		"https://evil.example", nil)
	if evil.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatalf("unlisted origin got CORS headers: %v", evil.Header())
	}
	if did := request(http.MethodGet, didDocumentPath, "https://tangled.org", nil); did.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatal("CORS applied outside /xrpc")
	}

	// The fixture allows only tangled.org, so the wildcard policy is checked on its own.
	wildcard := newCORSPolicy([]string{"*"})
	r := httptest.NewRequest(http.MethodGet, "/xrpc/x", nil)
	r.Header.Set("Origin", "https://anything.example")
	w := httptest.NewRecorder()
	wildcard.Middleware(http.NotFoundHandler()).ServeHTTP(w, r)
	if w.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Fatalf("wildcard allow-origin = %q", w.Header().Get("Access-Control-Allow-Origin"))
	}
}

// TestOrgQueryPipelinesBoundsPublicLookups pins the limits on repository lookups from the
// unauthenticated queryPipelines route. It never resolves a did:web repository, because that
// would make Tack fetch from a host the caller chose. It caches a failed lookup instead of
// repeating it on every request. When its lookup pool is full it answers 503 at once, and
// trusted trigger paths, which do not use that pool, keep working.
func TestOrgQueryPipelinesBoundsPublicLookups(t *testing.T) {
	f := newOrgFixture(t)
	// calls counts repository resolutions, the expensive network step in production.
	calls := 0
	verify := f.service.verify
	f.service.verify = func(ctx context.Context, repoDID repoident.RepoDid) (*RepoRef, error) {
		calls++
		return verify(ctx, repoDID)
	}
	query := func(repo string) int {
		return f.get("/xrpc/" + orgtangled.NSIDQueryPipelines + "?repo=" + url.QueryEscape(repo)).Code
	}

	if code := query("did:web:attacker.example"); code != http.StatusNotFound || calls != 0 {
		t.Fatalf("did:web query = %d after %d lookups; want 404 without a lookup", code, calls)
	}
	for range 3 {
		if code := query("did:plc:unresolvable"); code != http.StatusNotFound {
			t.Fatalf("unresolvable query = %d; want 404", code)
		}
	}
	// verifiedRepo retries a failed resolution once, so one lookup makes two calls. The second
	// and third queries are answered from the cached "not served" decision.
	if calls != 2 {
		t.Fatalf("unresolvable repo verified %d times; want 2 (one lookup)", calls)
	}

	// Take every slot in the public pool. The public route must answer 503 for an uncached
	// repository, while a knot's notifyEvent push, which looks up through servedRepo, still
	// creates a pipeline.
	for range cap(f.service.lookupSlots) {
		f.service.lookupSlots <- struct{}{}
	}
	if code := query("did:plc:another"); code != http.StatusServiceUnavailable {
		t.Fatalf("query with a full pool = %d; want 503", code)
	}
	response := f.post(orgtangled.NSIDNotifyEvent, f.knotToken(),
		pushEvent(testRepoDID, "refs/heads/main", testOldSHA, testNewSHA))
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "pipelines") {
		t.Fatalf("push with a full public pool = %d %s; want a pipeline", response.Code, response.Body)
	}
}

// TestMakeCacheRoom pins the eviction rule for the repository caches, whose keys public routes
// let any caller choose. At capacity, expired entries are swept to make room. A cache full of live
// entries reports no room instead of evicting one, so an attacker cannot flush useful entries by
// querying new DIDs.
func TestMakeCacheRoom(t *testing.T) {
	expires := func(at time.Time) time.Time { return at }
	cache := map[string]time.Time{
		"expired": time.Now().Add(-time.Minute),
		"live":    time.Now().Add(time.Minute),
	}
	if !makeCacheRoom(cache, 2, expires) || len(cache) != 1 || !cache["live"].After(time.Now()) {
		t.Fatalf("expired entry not swept: %v", cache)
	}
	cache["other"] = time.Now().Add(time.Minute)
	if makeCacheRoom(cache, 2, expires) {
		t.Fatal("full cache of live entries reported room")
	}
}
