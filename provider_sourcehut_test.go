package main

// Provider-level integration tests for the sourcehut implementation:
// Spawn → SubmitJob + persist + initial pending publish, watch loop →
// status transitions, and Logs → snapshot of master + per-task logs.
// builds.sr.ht is stubbed with httptest so the tests don't need
// network access.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"tangled.org/core/api/tangled"
)

// newSourcehutTestProvider wires a sourcehutProvider against an
// httptest server impersonating builds.sr.ht.
func newSourcehutTestProvider(
	t *testing.T,
	handler http.HandlerFunc,
) (*sourcehutProvider, *store, *broker, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	st := newTestStore(t)
	br := newBroker(st)
	logger := slog.Default()
	p := newSourcehutProvider(br, st, "tok", srv.URL, logger)
	return p, st, br, srv
}

// gqlBody is a minimal GraphQL request body for routing test stubs
// without pulling in a real GraphQL parser.
type gqlBody struct {
	Query     string         `json:"query"`
	Variables map[string]any `json:"variables"`
}

// gqlOp returns the op keyword ("query" / "mutation" / "subscription")
// at the head of a GraphQL document. Used by test stubs to route a
// single /query endpoint to the right canned response without
// substring-matching on field names — which would silently misroute a
// query that happened to mention "submit" or vice-versa.
func gqlOp(query string) string {
	q := strings.TrimSpace(query)
	for _, kw := range []string{"mutation", "query", "subscription"} {
		if strings.HasPrefix(q, kw) {
			return kw
		}
	}
	return ""
}

// writeGQL is the test-side helper for shaping a successful GraphQL
// response. It wraps `data` under the canonical {"data": …} envelope
// the client decodes against.
func writeGQL(w http.ResponseWriter, data map[string]any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"data": data})
}

// TestSourcehutSpawn covers the full submit path: trigger → API call
// → DB row → "pending" status on the broker.
func TestSourcehutSpawn(t *testing.T) {
	manifestCh := make(chan string, 1)
	notesCh := make(chan string, 1)
	tagsCh := make(chan []string, 1)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/query" {
			http.NotFound(w, r)
			return
		}
		var body gqlBody
		_ = json.NewDecoder(r.Body).Decode(&body)
		switch gqlOp(body.Query) {
		case "mutation":
			if m, _ := body.Variables["manifest"].(string); m != "" {
				select {
				case manifestCh <- m:
				default:
				}
			}
			if n, _ := body.Variables["note"].(string); n != "" {
				select {
				case notesCh <- n:
				default:
				}
			}
			if raw, ok := body.Variables["tags"].([]any); ok {
				ts := make([]string, 0, len(raw))
				for _, v := range raw {
					if s, ok := v.(string); ok {
						ts = append(ts, s)
					}
				}
				select {
				case tagsCh <- ts:
				default:
				}
			}
			writeGQL(w, map[string]any{
				"submit": map[string]any{
					"id":     42,
					"status": "PENDING",
					"owner":  map[string]any{"canonicalName": "~tester"},
				},
			})
		case "query":
			// Watch loop; keep the job pending so the test exits
			// deterministically once the row and initial publish land.
			writeGQL(w, map[string]any{
				"job": map[string]any{
					"id":     42,
					"status": "PENDING",
					"owner":  map[string]any{"canonicalName": "~tester"},
					"tasks":  []any{},
				},
			})
		default:
			http.Error(w, "unknown op", http.StatusBadRequest)
		}
	})
	p, st, _, _ := newSourcehutTestProvider(t, handler)

	trigger := &tangled.Pipeline_TriggerMetadata{
		Push: &tangled.Pipeline_PushTriggerData{
			NewSha: "abcdef0123",
			Ref:    "refs/heads/main",
		},
	}
	raw := strings.Join([]string{
		"tack:",
		"  sourcehut:",
		"    manifest: |",
		"      image: alpine/edge",
		"      tasks:",
		"        - hello: |",
		"            echo hi",
		"    tags: [tack, ci]",
		"    note: integration",
	}, "\n") + "\n"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Spawn(ctx, "knot.example.com", "rkey-1", "did:plc:actor", trigger,
		[]*tangled.Pipeline_Workflow{{Name: "ci.yml", Raw: raw}},
	)

	select {
	case manifest := <-manifestCh:
		// Manifest should round-trip through injectSourcehutEnvironment;
		// we expect TACK_* env vars to land in a top-level environment
		// block that didn't originally exist.
		if !strings.Contains(manifest, "TACK_KNOT") {
			t.Fatalf("manifest missing TACK_KNOT: %q", manifest)
		}
		if !strings.Contains(manifest, "abcdef0123") {
			t.Fatalf("manifest missing TACK_COMMIT value: %q", manifest)
		}
		// User-supplied tasks must be preserved through the round trip.
		if !strings.Contains(manifest, "echo hi") {
			t.Fatalf("user manifest body lost: %q", manifest)
		}
		select {
		case note := <-notesCh:
			if note != "integration" {
				t.Fatalf("note=%q; want integration", note)
			}
		case <-time.After(time.Second):
			t.Fatal("note variable not sent")
		}
		select {
		case ts := <-tagsCh:
			if len(ts) != 2 || ts[0] != "tack" || ts[1] != "ci" {
				t.Fatalf("tags=%v", ts)
			}
		case <-time.After(time.Second):
			t.Fatal("tags variable not sent")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("SubmitJob not called")
	}

	// Wait for the row to land — that's the load-bearing artifact.
	deadline := time.Now().Add(2 * time.Second)
	var ref *SourcehutJobRef
	for time.Now().Before(deadline) {
		var err error
		ref, err = st.LookupSourcehutJobByTuple(
			context.Background(),
			"knot.example.com", "rkey-1", "ci.yml",
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
		t.Fatal("sourcehut job row not persisted within deadline")
	}
	if ref.JobID != 42 || ref.Owner != "~tester" {
		t.Fatalf("ref mismatch: %+v", ref)
	}

	rows, err := st.EventsAfter(context.Background(), 0)
	if err != nil {
		t.Fatalf("EventsAfter: %v", err)
	}
	if len(rows) == 0 {
		t.Fatal("no pending status published")
	}
	var rec tangled.PipelineStatus
	if err := json.Unmarshal(rows[0].EventJSON, &rec); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	if rec.Status != "pending" || rec.Workflow != "ci.yml" {
		t.Fatalf("unexpected status: %+v", rec)
	}
	if !strings.Contains(rec.Pipeline, "knot.example.com") ||
		!strings.Contains(rec.Pipeline, "rkey-1") {
		t.Fatalf("pipeline ATURI wrong: %s", rec.Pipeline)
	}
}

// TestSourcehutSpawnInvalidConfig confirms a workflow without a manifest
// is rejected loudly without firing any HTTP call.
func TestSourcehutSpawnInvalidConfig(t *testing.T) {
	var called atomic.Bool
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called.Store(true)
	})
	p, st, _, _ := newSourcehutTestProvider(t, handler)

	p.Spawn(context.Background(), "knot.example.com", "rkey-1", "did:plc:actor",
		&tangled.Pipeline_TriggerMetadata{Push: &tangled.Pipeline_PushTriggerData{
			NewSha: "abc", Ref: "refs/heads/main",
		}},
		[]*tangled.Pipeline_Workflow{
			// `tack.sourcehut` block but no manifest.
			{Name: "ci.yml", Raw: "tack:\n  sourcehut:\n    note: hi\n"},
		},
	)

	time.Sleep(50 * time.Millisecond)
	if called.Load() {
		t.Fatal("SubmitJob called despite missing manifest")
	}
	rows, _ := st.EventsAfter(context.Background(), 0)
	if len(rows) != 0 {
		t.Fatalf("got %d events, want 0", len(rows))
	}
}

// TestSourcehutWatchTransitions exercises the watch loop: a job that
// flips through PENDING → RUNNING → SUCCESS should produce three
// distinct status records in order, with no duplicates.
func TestSourcehutWatchTransitions(t *testing.T) {
	states := []string{"PENDING", "RUNNING", "RUNNING", "SUCCESS"}
	var idx atomic.Int32
	idx.Store(-1)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/query" {
			http.NotFound(w, r)
			return
		}
		var body gqlBody
		_ = json.NewDecoder(r.Body).Decode(&body)
		if gqlOp(body.Query) != "query" {
			http.Error(w, "unknown op", http.StatusBadRequest)
			return
		}
		n := int(idx.Add(1))
		if n >= len(states) {
			n = len(states) - 1
		}
		writeGQL(w, map[string]any{
			"job": map[string]any{
				"id":     7,
				"status": states[n],
				"owner":  map[string]any{"canonicalName": "~tester"},
				"tasks":  []any{},
			},
		})
	})
	p, st, _, _ := newSourcehutTestProvider(t, handler)

	pipelineURI := pipelineATURI("knot.example.com", "rkey-2")
	ref := SourcehutJobRef{
		Knot: "knot.example.com", PipelineRkey: "rkey-2", Workflow: "ci.yml",
		JobID: 7, Owner: "~tester",
		Instance: p.defaultInstance, PipelineURI: pipelineURI,
	}
	if err := st.InsertSourcehutJob(context.Background(), ref); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Lower the poll interval so the watch loop completes in
	// milliseconds rather than waiting for the production 5s tick.
	p.pollInterval = 20 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.watchJob(ctx, ref, p.defaultClient)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("watch loop did not exit on terminal status")
	}

	rows, err := st.EventsAfter(context.Background(), 0)
	if err != nil {
		t.Fatalf("EventsAfter: %v", err)
	}
	gotStatuses := []string{}
	for _, r := range rows {
		var rec tangled.PipelineStatus
		if err := json.Unmarshal(r.EventJSON, &rec); err != nil {
			t.Fatalf("decode: %v", err)
		}
		gotStatuses = append(gotStatuses, rec.Status)
	}
	want := []string{"pending", "running", "success"}
	if fmt.Sprint(gotStatuses) != fmt.Sprint(want) {
		t.Fatalf("statuses = %v; want %v", gotStatuses, want)
	}
}

// TestSourcehutLogs covers the snapshot Logs path: master log + each
// task in the job's tasks list, bracketed by control frames.
func TestSourcehutLogs(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/query":
			writeGQL(w, map[string]any{
				"job": map[string]any{
					"id":     99,
					"status": "SUCCESS",
					"owner":  map[string]any{"canonicalName": "~tester"},
					"tasks": []any{
						map[string]any{"name": "build", "status": "SUCCESS"},
						map[string]any{"name": "test", "status": "SUCCESS"},
					},
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/query/log/99/log":
			_, _ = io.WriteString(w, "setup-output\n")
		case r.Method == http.MethodGet && r.URL.Path == "/query/log/99/build/log":
			_, _ = io.WriteString(w, "build-line-1\nbuild-line-2\n")
		case r.Method == http.MethodGet && r.URL.Path == "/query/log/99/test/log":
			_, _ = io.WriteString(w, "test-line\n")
		default:
			http.NotFound(w, r)
		}
	})
	p, st, _, _ := newSourcehutTestProvider(t, handler)

	if err := st.InsertSourcehutJob(context.Background(), SourcehutJobRef{
		Knot: "knot.example.com", PipelineRkey: "rkey-3", Workflow: "ci.yml",
		JobID: 99, Owner: "~tester",
		Instance:    p.defaultInstance,
		PipelineURI: pipelineATURI("knot.example.com", "rkey-3"),
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	ch, err := p.Logs(context.Background(),
		"knot.example.com", "rkey-3", "ci.yml")
	if err != nil {
		t.Fatalf("Logs: %v", err)
	}
	var lines []LogLine
	for line := range ch {
		lines = append(lines, line)
	}

	wantContents := []string{
		"setup", "setup-output\n", "setup",
		"build", "build-line-1\n", "build-line-2\n", "build",
		"test", "test-line\n", "test",
	}
	got := make([]string, 0, len(lines))
	for _, l := range lines {
		got = append(got, l.Content)
	}
	if fmt.Sprint(got) != fmt.Sprint(wantContents) {
		t.Fatalf("log contents:\n got: %v\nwant: %v", got, wantContents)
	}
	if lines[0].Kind != LogKindControl || lines[0].StepStatus != StepStatusStart {
		t.Fatalf("first frame not start control: %+v", lines[0])
	}
	if lines[len(lines)-1].Kind != LogKindControl ||
		lines[len(lines)-1].StepStatus != StepStatusEnd {
		t.Fatalf("last frame not end control: %+v", lines[len(lines)-1])
	}
}

// TestSourcehutLogsNotFound asserts the Provider contract: looking up
// logs for a tuple no job was ever spawned for returns ErrLogsNotFound.
func TestSourcehutLogsNotFound(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("unexpected request to upstream: %s %s", r.Method, r.URL.Path)
	})
	p, _, _, _ := newSourcehutTestProvider(t, handler)

	_, err := p.Logs(context.Background(),
		"knot.example.com", "rkey-missing", "ci.yml")
	if err != ErrLogsNotFound {
		t.Fatalf("err = %v; want ErrLogsNotFound", err)
	}
}

// TestSourcehutMapStatus pins the mapping table independently of the
// rest of the provider, since it's the contract between us and the
// appview's status enum.
func TestSourcehutMapStatus(t *testing.T) {
	cases := []struct {
		in       string
		want     string
		terminal bool
		ok       bool
	}{
		// Sourcehut emits uppercase enum values over GraphQL; the
		// lowercased variants are kept here as a regression guard
		// against a future surface flip.
		{"PENDING", "pending", false, true},
		{"QUEUED", "pending", false, true},
		{"RUNNING", "running", false, true},
		{"SUCCESS", "success", true, true},
		{"FAILED", "failed", true, true},
		{"TIMEOUT", "failed", true, true},
		{"CANCELLED", "cancelled", true, true},
		{"pending", "pending", false, true},
		{"weird", "", false, false},
	}
	for _, c := range cases {
		got, terminal, ok := mapSourcehutStatus(c.in)
		if got != c.want || terminal != c.terminal || ok != c.ok {
			t.Errorf("mapSourcehutStatus(%q) = %q,%v,%v; want %q,%v,%v",
				c.in, got, terminal, ok, c.want, c.terminal, c.ok)
		}
	}
}

// TestSourcehutInjectEnvironmentPreservesUserOverride confirms a
// user-set env entry is not clobbered by our injected default —
// explicit user intent wins.
func TestSourcehutInjectEnvironmentPreservesUserOverride(t *testing.T) {
	manifest := strings.Join([]string{
		"image: alpine/edge",
		"environment:",
		"  TACK_KNOT: user-supplied",
		"tasks:",
		"  - hello: echo hi",
	}, "\n") + "\n"
	out, err := injectSourcehutEnvironment(manifest, map[string]string{
		"TACK_KNOT":   "from-tack",
		"TACK_COMMIT": "abc",
	})
	if err != nil {
		t.Fatalf("inject: %v", err)
	}
	if !strings.Contains(out, "TACK_KNOT: user-supplied") {
		t.Fatalf("user override clobbered: %q", out)
	}
	if !strings.Contains(out, "TACK_COMMIT: abc") {
		t.Fatalf("missing injected var: %q", out)
	}
}
