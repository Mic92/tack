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

	p.Spawn(context.Background(), "knot.example.com", "rkey-1", trigger, workflows)

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

	p.Spawn(context.Background(), "knot.example.com", "rkey-1",
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
	p, _, _, _ := newBuildkiteTestProvider(t, buildkite.WebhookModeToken, "s", bk)

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

	p.Spawn(context.Background(), "knot.example.com", "rkey-x", trigger,
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

	p.Spawn(context.Background(), "knot.example.com", "rkey-z",
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
