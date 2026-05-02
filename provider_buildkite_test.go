package main

// Provider-level integration tests for the Buildkite implementation:
// Spawn → CreateBuild + persist + initial pending publish, and
// HandleWebhook → translate state + publish status. Buildkite itself
// is stubbed with httptest so the tests don't need network access.

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"tangled.org/core/api/tangled"

	"go.mitchellh.com/tack/internal/buildkite"
)

// newBuildkiteTestProvider wires a buildkiteProvider against an
// httptest server impersonating api.buildkite.com. Returns the
// store/broker so tests can inspect publishes + persistence.
func newBuildkiteTestProvider(
	t *testing.T,
	mode buildkite.WebhookMode,
	secret string,
	bkHandler http.HandlerFunc,
) (*buildkiteProvider, *store, *broker, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(bkHandler)
	t.Cleanup(srv.Close)

	prev := buildkite.APIBase
	buildkite.APIBase = srv.URL
	t.Cleanup(func() { buildkite.APIBase = prev })

	st := newTestStore(t)
	br := newBroker(st)
	logger := slog.Default()
	p := newBuildkiteProvider(
		br, st,
		buildkite.NewClient("tok"),
		"myorg",
		secret, mode,
		logger,
	)
	return p, st, br, srv
}

// TestBuildkiteSpawn covers the full create-build path: trigger →
// API call → DB row → "pending" status on the broker.
func TestBuildkiteSpawn(t *testing.T) {
	bk := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(buildkite.Build{
			ID:     "uuid-1",
			Number: 7,
		})
	})
	p, st, _, _ := newBuildkiteTestProvider(t, buildkite.WebhookModeToken, "secret", bk)

	trigger := &tangled.Pipeline_TriggerMetadata{
		Push: &tangled.Pipeline_PushTriggerData{
			NewSha: "abcdef0123",
			Ref:    "refs/heads/main",
		},
	}
	workflows := []*tangled.Pipeline_Workflow{
		{Name: "test.yml", Raw: "tack:\n  buildkite:\n    pipeline: mypipe\n"},
	}

	p.Spawn(context.Background(), "knot.example.com", "rkey-1", "did:plc:actor", trigger, workflows)

	// Spawn fans out into goroutines; wait briefly for the side
	// effects to land. The store row is the load-bearing artifact
	// — once it's present, the publish has already happened too.
	deadline := time.Now().Add(2 * time.Second)
	var ref *BuildkiteBuildRef
	for time.Now().Before(deadline) {
		var err error
		ref, err = st.LookupBuildkiteBuildByUUID(context.Background(), "uuid-1")
		if err != nil {
			t.Fatalf("lookup: %v", err)
		}
		if ref != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if ref == nil {
		t.Fatal("buildkite build row not persisted within deadline")
	}
	if ref.Workflow != "test.yml" || ref.Knot != "knot.example.com" || ref.PipelineRkey != "rkey-1" {
		t.Fatalf("ref mismatch: %+v", ref)
	}
	if ref.PipelineSlug != "mypipe" || ref.BuildNumber != 7 {
		t.Fatalf("buildkite ref mismatch: %+v", ref)
	}

	// One pending status should be on the events log.
	rows, err := st.EventsAfter(context.Background(), 0)
	if err != nil {
		t.Fatalf("EventsAfter: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("got %d events, want 1", len(rows))
	}
	var rec tangled.PipelineStatus
	if err := json.Unmarshal(rows[0].EventJSON, &rec); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	if rec.Status != "pending" || rec.Workflow != "test.yml" {
		t.Fatalf("unexpected status: %+v", rec)
	}
	if !strings.Contains(rec.Pipeline, "knot.example.com") ||
		!strings.Contains(rec.Pipeline, "rkey-1") {
		t.Fatalf("pipeline ATURI wrong: %s", rec.Pipeline)
	}
}

// TestBuildkiteSpawnNoCommit confirms we don't fire a build when the
// trigger has no commit to build — kicking one off would resolve to
// whatever main looks like at agent-fetch time, which is dangerously
// surprising.
func TestBuildkiteSpawnNoCommit(t *testing.T) {
	called := false
	bk := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	})
	p, st, _, _ := newBuildkiteTestProvider(t, buildkite.WebhookModeToken, "secret", bk)

	p.Spawn(context.Background(), "knot.example.com", "rkey-1", "did:plc:actor",
		&tangled.Pipeline_TriggerMetadata{Manual: &tangled.Pipeline_ManualTriggerData{}},
		[]*tangled.Pipeline_Workflow{{Name: "test.yml",
			Raw: "tack:\n  buildkite:\n    pipeline: mypipe\n"}},
	)

	// Give any rogue goroutine a moment.
	time.Sleep(50 * time.Millisecond)
	if called {
		t.Fatal("CreateBuild called despite missing commit")
	}
	rows, _ := st.EventsAfter(context.Background(), 0)
	if len(rows) != 0 {
		t.Fatalf("got %d events, want 0", len(rows))
	}
}

// TestBuildkiteSpawnWorkflowConfig pins the YAML → create-build
// translation: `tack.buildkite.{pipeline,org}` pick the request URL,
// `clean_checkout` flows through, and the trigger's PR target
// branch lands as `pull_request_base_branch` automatically.
func TestBuildkiteSpawnWorkflowConfig(t *testing.T) {
	type captured struct {
		path string
		body buildkite.CreateBuildRequest
	}
	gotCh := make(chan captured, 1)
	bk := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body buildkite.CreateBuildRequest
		_ = json.NewDecoder(r.Body).Decode(&body)
		gotCh <- captured{path: r.URL.Path, body: body}
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(buildkite.Build{ID: "uuid-9", Number: 9})
	})
	p, st, _, _ := newBuildkiteTestProvider(t, buildkite.WebhookModeToken, "s", bk)

	raw := strings.Join([]string{
		"tack:",
		"  buildkite:",
		"    pipeline: workflow-pipe",
		"    org: workflow-org",
		"    clean_checkout: true",
	}, "\n") + "\n"

	trigger := &tangled.Pipeline_TriggerMetadata{
		PullRequest: &tangled.Pipeline_PullRequestTriggerData{
			SourceSha:    "deadbeef",
			SourceBranch: "feature",
			TargetBranch: "main",
		},
	}

	p.Spawn(context.Background(), "knot.example.com", "rkey-x", "did:plc:actor", trigger,
		[]*tangled.Pipeline_Workflow{{Name: "ci.yml", Raw: raw}})

	select {
	case got := <-gotCh:
		// URL must reflect YAML org + pipeline.
		if !strings.Contains(got.path, "/organizations/workflow-org/pipelines/workflow-pipe/") {
			t.Fatalf("path = %q", got.path)
		}
		if got.body.Commit != "deadbeef" || got.body.Branch != "feature" {
			t.Fatalf("commit/branch = %q/%q", got.body.Commit, got.body.Branch)
		}
		// tack-managed env/meta still present.
		if got.body.Env["TACK_WORKFLOW"] != "ci.yml" {
			t.Fatalf("env[TACK_WORKFLOW] missing: %+v", got.body.Env)
		}
		if got.body.MetaData[bkMetaWorkflow] != "ci.yml" {
			t.Fatalf("meta_data missing identity tuple: %+v", got.body.MetaData)
		}
		if !got.body.CleanCheckout {
			t.Fatalf("clean_checkout not set")
		}
		// IgnorePipelineBranchFilters is hard-coded true; see
		// buildCreateRequest comment.
		if !got.body.IgnorePipelineBranchFilters {
			t.Fatalf("ignore_pipeline_branch_filters not on")
		}
		if got.body.PullRequestBaseBranch != "main" {
			t.Fatalf("pr base branch = %q; want main",
				got.body.PullRequestBaseBranch)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("CreateBuild not called")
	}

	// The workflow's `org` override has to round-trip through
	// the store: /logs (and any future post-creation API call)
	// looks the row up by tuple and re-issues calls against the
	// org we recorded here. If we drop it, log retrieval falls
	// back to defaultOrg and 404s for cross-org workflows.
	deadline := time.Now().Add(2 * time.Second)
	var ref *BuildkiteBuildRef
	for time.Now().Before(deadline) {
		var err error
		ref, err = st.LookupBuildkiteBuildByTuple(
			context.Background(),
			"knot.example.com", "rkey-x", "ci.yml",
		)
		if err != nil {
			t.Fatalf("lookup: %v", err)
		}
		if ref != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if ref == nil {
		t.Fatal("buildkite build row not persisted within deadline")
	}
	if ref.Org != "workflow-org" {
		t.Fatalf("ref.Org = %q; want %q", ref.Org, "workflow-org")
	}
}

// TestBuildkiteSpawnPersistsResolvedOrg pins that Spawn writes the
// *resolved* org to the buildkite_builds row when the workflow YAML
// omits `tack.buildkite.org`. Storing cfg.Org (empty) here would let
// historical lookups silently follow a later change to defaultOrg
// into the wrong organisation; storing the value we actually issued
// the CreateBuild against keeps each row self-describing.
func TestBuildkiteSpawnPersistsResolvedOrg(t *testing.T) {
	bk := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The default org in newBuildkiteTestProvider is "myorg";
		// the URL must reflect the resolved fallback so we know
		// what value the row is expected to capture.
		if !strings.Contains(r.URL.Path, "/organizations/myorg/") {
			t.Errorf("CreateBuild path = %q; want /organizations/myorg/", r.URL.Path)
		}
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(buildkite.Build{ID: "uuid-r", Number: 3})
	})
	p, st, _, _ := newBuildkiteTestProvider(t, buildkite.WebhookModeToken, "s", bk)

	p.Spawn(context.Background(), "knot.example.com", "rkey-r", "did:plc:actor",
		&tangled.Pipeline_TriggerMetadata{
			Push: &tangled.Pipeline_PushTriggerData{
				NewSha: "abc", Ref: "refs/heads/main",
			},
		},
		// No `org:` under tack.buildkite — must fall back to the
		// provider's defaultOrg AND persist that resolved value.
		[]*tangled.Pipeline_Workflow{
			{Name: "ci.yml", Raw: "tack:\n  buildkite:\n    pipeline: mypipe\n"},
		},
	)

	deadline := time.Now().Add(2 * time.Second)
	var ref *BuildkiteBuildRef
	for time.Now().Before(deadline) {
		var err error
		ref, err = st.LookupBuildkiteBuildByUUID(context.Background(), "uuid-r")
		if err != nil {
			t.Fatalf("lookup: %v", err)
		}
		if ref != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if ref == nil {
		t.Fatal("buildkite build row not persisted within deadline")
	}
	// The whole point of the change: ref.Org records the org the
	// build was created against, not "" (which would hand off to
	// whatever defaultOrg happens to be at read time).
	if ref.Org != "myorg" {
		t.Fatalf("ref.Org = %q; want %q (resolved defaultOrg)",
			ref.Org, "myorg")
	}

	// Belt-and-braces: simulate defaultOrg drifting after the row
	// was written. Logs() should still target the org persisted on
	// the row, not the new default. We swap defaultOrg out and
	// stand up a sibling httptest server that fails the test if
	// anything but /organizations/myorg/ is requested.
	gotPath := make(chan string, 1)
	logSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath <- r.URL.Path
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(buildkite.Build{
			ID: "uuid-r", Number: 3, Jobs: nil,
		})
	}))
	t.Cleanup(logSrv.Close)
	prev := buildkite.APIBase
	buildkite.APIBase = logSrv.URL
	t.Cleanup(func() { buildkite.APIBase = prev })

	p.defaultOrg = "newdefault"
	ch, err := p.Logs(context.Background(), "knot.example.com", "rkey-r", "ci.yml")
	if err != nil {
		t.Fatalf("Logs: %v", err)
	}
	// Drain so the goroutine completes; we only care about the
	// initial GetBuild path.
	for range ch {
	}
	select {
	case path := <-gotPath:
		if !strings.Contains(path, "/organizations/myorg/") {
			t.Fatalf("Logs hit %q; want path containing /organizations/myorg/", path)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Logs did not call GetBuild")
	}
}

// TestBuildkiteSpawnInvalidYAML proves a workflow without the
// required `tack.buildkite.pipeline` field is skipped — no API
// call, no DB row, no status. A misconfigured workflow shouldn't
// be silently swept onto some default pipeline.
func TestBuildkiteSpawnInvalidYAML(t *testing.T) {
	called := false
	bk := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	})
	p, st, _, _ := newBuildkiteTestProvider(t, buildkite.WebhookModeToken, "s", bk)

	p.Spawn(context.Background(), "knot.example.com", "rkey-z", "did:plc:actor",
		&tangled.Pipeline_TriggerMetadata{
			Push: &tangled.Pipeline_PushTriggerData{NewSha: "abc", Ref: "refs/heads/main"},
		},
		[]*tangled.Pipeline_Workflow{
			{Name: "broken.yml", Raw: "steps:\n  - run: true\n"},
		},
	)

	time.Sleep(50 * time.Millisecond)
	if called {
		t.Fatal("CreateBuild called for workflow missing tack.buildkite.pipeline")
	}
	rows, _ := st.EventsAfter(context.Background(), 0)
	if len(rows) != 0 {
		t.Fatalf("got %d events, want 0", len(rows))
	}
}

// TestBuildkiteHandleWebhook checks the translation pipeline:
// recorded build + matching webhook → success status published.
func TestBuildkiteHandleWebhook(t *testing.T) {
	p, st, _, _ := newBuildkiteTestProvider(t, buildkite.WebhookModeToken, "secret",
		func(w http.ResponseWriter, r *http.Request) { t.Fatal("buildkite shouldn't be called") })

	// Pre-seed a known build mapping.
	if err := st.InsertBuildkiteBuild(context.Background(), BuildkiteBuildRef{
		BuildUUID:    "uuid-1",
		BuildNumber:  7,
		PipelineSlug: "mypipe",
		Knot:         "knot.example.com",
		PipelineRkey: "rkey-1",
		Workflow:     "test.yml",
		PipelineURI:  "at://did:web:knot.example.com/sh.tangled.pipeline/rkey-1",
	}); err != nil {
		t.Fatalf("InsertBuildkiteBuild: %v", err)
	}

	err := p.HandleWebhook(context.Background(), buildkite.WebhookPayload{
		Event: "build.finished",
		Build: buildkite.Build{ID: "uuid-1", State: "passed"},
	})
	if err != nil {
		t.Fatalf("HandleWebhook: %v", err)
	}

	rows, err := st.EventsAfter(context.Background(), 0)
	if err != nil {
		t.Fatalf("EventsAfter: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("got %d events, want 1", len(rows))
	}
	var rec tangled.PipelineStatus
	if err := json.Unmarshal(rows[0].EventJSON, &rec); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if rec.Status != "success" || rec.Workflow != "test.yml" {
		t.Fatalf("bad status: %+v", rec)
	}
}

// TestBuildkiteHandleWebhookIgnored covers the "we don't care" paths:
// non-build events and unknown builds must be no-op (no publish, no
// error) so Buildkite doesn't retry them.
func TestBuildkiteHandleWebhookIgnored(t *testing.T) {
	p, st, _, _ := newBuildkiteTestProvider(t, buildkite.WebhookModeToken, "secret",
		func(w http.ResponseWriter, r *http.Request) {})

	// Non-build event: no lookup, no publish.
	if err := p.HandleWebhook(context.Background(), buildkite.WebhookPayload{
		Event: "job.started",
		Build: buildkite.Build{ID: "uuid-x"},
	}); err != nil {
		t.Fatalf("HandleWebhook (job.started): %v", err)
	}

	// Build event for unknown UUID: no publish.
	if err := p.HandleWebhook(context.Background(), buildkite.WebhookPayload{
		Event: "build.finished",
		Build: buildkite.Build{ID: "unknown-uuid", State: "passed"},
	}); err != nil {
		t.Fatalf("HandleWebhook (unknown): %v", err)
	}

	// Known build but unmapped state: no publish.
	if err := st.InsertBuildkiteBuild(context.Background(), BuildkiteBuildRef{
		BuildUUID: "uuid-blocked", PipelineSlug: "mypipe",
		Knot: "k", PipelineRkey: "r", Workflow: "w",
		PipelineURI: "at://x",
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := p.HandleWebhook(context.Background(), buildkite.WebhookPayload{
		Event: "build.finished",
		Build: buildkite.Build{ID: "uuid-blocked", State: "blocked"},
	}); err != nil {
		t.Fatalf("HandleWebhook (blocked): %v", err)
	}

	rows, _ := st.EventsAfter(context.Background(), 0)
	if len(rows) != 0 {
		t.Fatalf("got %d events, want 0", len(rows))
	}
}

// TestBuildkiteHandleWebhookMetaDataFallback covers the race where
// a webhook arrives before Spawn has persisted the UUID→tuple row.
// The handler must reconstruct the ref from the build's tack:*
// meta_data, publish a status event, and opportunistically persist
// the row so subsequent lookups hit the cache.
func TestBuildkiteHandleWebhookMetaDataFallback(t *testing.T) {
	p, st, _, _ := newBuildkiteTestProvider(t, buildkite.WebhookModeToken, "secret",
		func(w http.ResponseWriter, r *http.Request) { t.Fatal("buildkite shouldn't be called") })

	// No InsertBuildkiteBuild: simulate the race window where
	// CreateBuild has returned to Buildkite (so a webhook is in
	// flight) but the provider hasn't yet written the mapping row.
	err := p.HandleWebhook(context.Background(), buildkite.WebhookPayload{
		Event: "build.scheduled",
		Build: buildkite.Build{
			ID:     "uuid-race",
			Number: 42,
			State:  "scheduled",
			MetaData: map[string]string{
				bkMetaKnot:         "knot.example.com",
				bkMetaPipelineRkey: "rkey-race",
				bkMetaWorkflow:     "ci.yml",
			},
			Pipeline: map[string]any{"slug": "mypipe"},
		},
		Organization: buildkite.Organization{Slug: "myorg"},
	})
	if err != nil {
		t.Fatalf("HandleWebhook: %v", err)
	}

	// Status was published despite the missing cache row.
	rows, err := st.EventsAfter(context.Background(), 0)
	if err != nil {
		t.Fatalf("EventsAfter: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("got %d events, want 1", len(rows))
	}
	var rec tangled.PipelineStatus
	if err := json.Unmarshal(rows[0].EventJSON, &rec); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if rec.Status != "pending" || rec.Workflow != "ci.yml" {
		t.Fatalf("bad status: %+v", rec)
	}

	// Opportunistic insert means the row is now cached for any
	// subsequent webhook (or Logs call), including the Org and
	// PipelineSlug we recovered from the payload.
	ref, err := st.LookupBuildkiteBuildByUUID(context.Background(), "uuid-race")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if ref == nil {
		t.Fatal("expected opportunistic InsertBuildkiteBuild, got nil row")
	}
	if ref.Workflow != "ci.yml" || ref.Knot != "knot.example.com" ||
		ref.PipelineRkey != "rkey-race" || ref.BuildNumber != 42 ||
		ref.PipelineSlug != "mypipe" || ref.Org != "myorg" {
		t.Fatalf("ref mismatch: %+v", ref)
	}
}

// TestBuildkiteHandleWebhookForeignBuild covers the genuinely-foreign
// case: a webhook for a build that doesn't carry our tack:* meta_data
// must remain a silent no-op even though the new fallback path now
// inspects meta_data.
func TestBuildkiteHandleWebhookForeignBuild(t *testing.T) {
	p, st, _, _ := newBuildkiteTestProvider(t, buildkite.WebhookModeToken, "secret",
		func(w http.ResponseWriter, r *http.Request) {})

	if err := p.HandleWebhook(context.Background(), buildkite.WebhookPayload{
		Event: "build.finished",
		Build: buildkite.Build{
			ID:    "uuid-foreign",
			State: "passed",
			// No tack:* keys: build was triggered outside tack.
			MetaData: map[string]string{"someone-elses-key": "value"},
		},
	}); err != nil {
		t.Fatalf("HandleWebhook: %v", err)
	}

	rows, _ := st.EventsAfter(context.Background(), 0)
	if len(rows) != 0 {
		t.Fatalf("got %d events, want 0", len(rows))
	}
	ref, _ := st.LookupBuildkiteBuildByUUID(context.Background(), "uuid-foreign")
	if ref != nil {
		t.Fatalf("foreign build was opportunistically persisted: %+v", ref)
	}
}

// TestBuildkiteWebhookHandlerHTTP exercises the full HTTP path
// including auth: a request signed with the wrong secret must be
// rejected, and a correctly-signed one must reach the provider.
func TestBuildkiteWebhookHandlerHTTP(t *testing.T) {
	// Signature mode is the more interesting code path; we cover
	// token mode in the verifier-level tests above.
	const secret = "swordfish"
	p, st, _, _ := newBuildkiteTestProvider(t, buildkite.WebhookModeSignature, secret,
		func(w http.ResponseWriter, r *http.Request) { /* unused */ })

	// Pre-seed so the provider's HandleWebhook can resolve the build.
	if err := st.InsertBuildkiteBuild(context.Background(), BuildkiteBuildRef{
		BuildUUID:    "uuid-2",
		BuildNumber:  9,
		PipelineSlug: "mypipe",
		Knot:         "knot.example.com",
		PipelineRkey: "rkey-2",
		Workflow:     "test.yml",
		PipelineURI:  "at://did:web:knot.example.com/sh.tangled.pipeline/rkey-2",
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	body, _ := json.Marshal(map[string]any{
		"event": "build.finished",
		"build": map[string]any{
			"id":    "uuid-2",
			"state": "failed",
		},
	})

	logger := slog.Default()
	handler := buildkiteWebhookHandler(logger, p)

	// Unsigned request → 401.
	t.Run("rejects unsigned", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/webhooks/buildkite",
			strings.NewReader(string(body)))
		req.Header.Set("X-Buildkite-Event", "build.finished")
		w := httptest.NewRecorder()
		handler(w, req)
		if w.Code != http.StatusUnauthorized {
			t.Fatalf("status = %d; want 401", w.Code)
		}
	})

	// Wrong-secret request → 401.
	t.Run("rejects bad signature", func(t *testing.T) {
		ts := fmt.Sprintf("%d", time.Now().Unix())
		mac := hmac.New(sha256.New, []byte("wrong"))
		mac.Write([]byte(ts))
		mac.Write([]byte("."))
		mac.Write(body)
		sig := hex.EncodeToString(mac.Sum(nil))

		req := httptest.NewRequest(http.MethodPost, "/webhooks/buildkite",
			strings.NewReader(string(body)))
		req.Header.Set("X-Buildkite-Event", "build.finished")
		req.Header.Set("X-Buildkite-Signature", "timestamp="+ts+",signature="+sig)
		w := httptest.NewRecorder()
		handler(w, req)
		if w.Code != http.StatusUnauthorized {
			t.Fatalf("status = %d; want 401", w.Code)
		}
	})

	// Valid request → 200, status published.
	t.Run("accepts valid", func(t *testing.T) {
		ts := fmt.Sprintf("%d", time.Now().Unix())
		mac := hmac.New(sha256.New, []byte(secret))
		mac.Write([]byte(ts))
		mac.Write([]byte("."))
		mac.Write(body)
		sig := hex.EncodeToString(mac.Sum(nil))

		req := httptest.NewRequest(http.MethodPost, "/webhooks/buildkite",
			strings.NewReader(string(body)))
		req.Header.Set("X-Buildkite-Event", "build.finished")
		req.Header.Set("X-Buildkite-Signature", "timestamp="+ts+",signature="+sig)
		w := httptest.NewRecorder()
		handler(w, req)
		if w.Code != http.StatusOK {
			b, _ := io.ReadAll(w.Body)
			t.Fatalf("status = %d body=%s; want 200", w.Code, string(b))
		}
		rows, _ := st.EventsAfter(context.Background(), 0)
		if len(rows) != 1 {
			t.Fatalf("got %d events, want 1", len(rows))
		}
		var rec tangled.PipelineStatus
		if err := json.Unmarshal(rows[0].EventJSON, &rec); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if rec.Status != "failed" {
			t.Fatalf("status = %q; want failed", rec.Status)
		}
	})
}

// TestBuildkiteWebhookHandlerNoProvider confirms the 503 branch when
// tack is running with the fake provider — a misdirected webhook
// must get a clear "not configured here" instead of a misleading
// 200 OK that silently throws the event away.
func TestBuildkiteWebhookHandlerNoProvider(t *testing.T) {
	handler := buildkiteWebhookHandler(slog.Default(), nil)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/buildkite",
		strings.NewReader("{}"))
	w := httptest.NewRecorder()
	handler(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d; want 503", w.Code)
	}
}

// TestTriggerCommitAndBranch pins the trigger-shape mapping. Each
// case pairs an input trigger with the (commit, branch) tuple a
// real CI provider would feed into its build-creation API.
func TestTriggerCommitAndBranch(t *testing.T) {
	cases := []struct {
		name              string
		in                *tangled.Pipeline_TriggerMetadata
		wantCommit        string
		wantBranch        string
	}{
		{"nil", nil, "", ""},
		{"push refs/heads",
			&tangled.Pipeline_TriggerMetadata{
				Push: &tangled.Pipeline_PushTriggerData{NewSha: "abc", Ref: "refs/heads/main"},
			},
			"abc", "main",
		},
		{"push tag ref preserved",
			&tangled.Pipeline_TriggerMetadata{
				Push: &tangled.Pipeline_PushTriggerData{NewSha: "abc", Ref: "refs/tags/v1"},
			},
			"abc", "refs/tags/v1",
		},
		{"pull request",
			&tangled.Pipeline_TriggerMetadata{
				PullRequest: &tangled.Pipeline_PullRequestTriggerData{
					SourceSha: "def", SourceBranch: "feature",
				},
			},
			"def", "feature",
		},
		{"manual with default branch",
			&tangled.Pipeline_TriggerMetadata{
				Manual: &tangled.Pipeline_ManualTriggerData{},
				Repo:   &tangled.Pipeline_TriggerRepo{DefaultBranch: "main"},
			},
			"", "main",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			gotC, gotB := triggerCommitAndBranch(c.in)
			if gotC != c.wantCommit || gotB != c.wantBranch {
				t.Fatalf("got (%q,%q); want (%q,%q)",
					gotC, gotB, c.wantCommit, c.wantBranch)
			}
		})
	}
}

// TestStripTerminal exercises both shapes of Buildkite's inline
// timestamp metadata: the well-formed APC envelope ESC _ … BEL, and
// the bare "_bk;t=<digits>" residue we sometimes see when an upstream
// processor strips control bytes without understanding the envelope.
func TestStripTerminal(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"empty", "", ""},
		{"no metadata", "hello world\n", "hello world\n"},
		{
			"framed apc",
			"\x1b_bk;t=1777680702553\x07~~~ Running agent environment hook\n",
			"~~~ Running agent environment hook\n",
		},
		{
			"multiple framed apc per line",
			"\x1b_bk;t=1\x07a\x1b_bk;t=2\x07b\n",
			"ab\n",
		},
		{
			"bare residue",
			"_bk;t=1777680702553~~~ Running agent environment hook\n",
			"~~~ Running agent environment hook\n",
		},
		{
			"bare residue mid-line",
			"remote: Counting objects: 0% _bk;t=1777680705851\n",
			"remote: Counting objects: 0% \n",
		},
		{
			"unterminated apc dropped",
			"keep\x1b_bk;t=1no-bel-here",
			"keep",
		},
		{
			// Underscores unrelated to bk;t= must survive untouched.
			"underscore not metadata",
			"foo_bar baz_qux\n",
			"foo_bar baz_qux\n",
		},
		{
			// CSI colour codes are stripped down to plain text.
			"strips csi colour",
			"\x1b[90m# comment\x1b[0m\n",
			"# comment\n",
		},
		{
			// Clear-to-EOL embedded mid-line is removed too.
			"strips csi clear-to-eol",
			"remote: Counting objects: 1% (2/144) \x1b[K\n",
			"remote: Counting objects: 1% (2/144) \n",
		},
		{
			// OSC title-set sequences (terminated by BEL) drop.
			"strips osc bel terminated",
			"before\x1b]0;window title\x07after\n",
			"beforeafter\n",
		},
		{
			// OSC terminated by ST (ESC '\') also drops.
			"strips osc st terminated",
			"before\x1b]0;title\x1b\\after\n",
			"beforeafter\n",
		},
		{
			// Two-byte escapes like ESC '=' (DECKPAM) drop.
			"strips two byte escape",
			"a\x1b=b\n",
			"ab\n",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := stripTerminal(c.in)
			if got != c.want {
				t.Fatalf("got %q; want %q", got, c.want)
			}
		})
	}
}
