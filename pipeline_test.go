package main

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"tangled.org/core/api/tangled"
	"tangled.org/core/workflow"
)

// TestSyncWorkflowRepo exercises the Git 2.47-compatible init/fetch sequence
// against a real local repository and confirms sparse checkout materializes the
// workflow directory at the requested revision.
func TestSyncWorkflowRepo(t *testing.T) {
	ctx := context.Background()
	source := filepath.Join(t.TempDir(), "source")
	if err := runGit(ctx, "init", source); err != nil {
		t.Fatal(err)
	}
	if err := runGit(ctx, "-C", source, "config", "user.name", "Test"); err != nil {
		t.Fatal(err)
	}
	if err := runGit(ctx, "-C", source, "config", "user.email", "test@example.com"); err != nil {
		t.Fatal(err)
	}
	workflowDir := filepath.Join(source, ".tangled", "workflows")
	if err := os.MkdirAll(workflowDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workflowDir, "ci.yml"), []byte("engine: tack\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runGit(ctx, "-C", source, "add", "."); err != nil {
		t.Fatal(err)
	}
	if err := runGit(ctx, "-C", source, "commit", "-m", "workflow"); err != nil {
		t.Fatal(err)
	}
	revision, err := exec.CommandContext(ctx, "git", "-C", source, "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatal(err)
	}

	destination := filepath.Join(t.TempDir(), "checkout")
	if err := syncWorkflowRepo(ctx, source, destination, strings.TrimSpace(string(revision))); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(filepath.Join(destination, ".tangled", "workflows", "ci.yml"))
	if err != nil {
		t.Fatal(err)
	}
	if string(contents) != "engine: tack\n" {
		t.Fatalf("workflow contents = %q", contents)
	}
}

// TestPipelineHTTPClientRejectsPrivateAddresses pins the SSRF boundary while
// preserving loopback access required by local development mode.
func TestPipelineHTTPClientRejectsPrivateAddresses(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	if _, err := newPipelineHTTPClient(false).Get(server.URL); err == nil {
		t.Fatal("production client accepted a loopback PDS endpoint")
	}
	response, err := newPipelineHTTPClient(true).Get(server.URL)
	if err != nil {
		t.Fatalf("development client rejected loopback: %v", err)
	}
	response.Body.Close()
}

// commitFile writes contents to path inside repo, commits it, and returns the new commit's
// object ID. It creates parent directories so callers can commit nested workflow files.
func commitFile(t *testing.T, repo, path, contents string) string {
	t.Helper()
	ctx := context.Background()
	full := filepath.Join(repo, path)
	if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"add", "."}, {"commit", "-q", "-m", path}} {
		if err := runGit(ctx, append([]string{"-C", repo}, args...)...); err != nil {
			t.Fatal(err)
		}
	}
	sha, err := exec.CommandContext(ctx, "git", "-C", repo, "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(sha))
}

// TestPushChangedFiles pins how Tack recovers a push's changed-file list, which knot2 no longer
// sends with push events. It diffs in the same sparse workflow cache that syncWorkflowRepo fills,
// so the test also proves the shallow, treeless cache can fetch the base commit and then the trees
// the diff needs. It checks that a real diff is known, and that ref creation and a base commit the
// source repository does not have report the set as unknown instead of as empty.
func TestPushChangedFiles(t *testing.T) {
	ctx := context.Background()
	source := filepath.Join(t.TempDir(), "source")
	for _, args := range [][]string{
		{"init", "-q", source},
		{"-C", source, "config", "user.name", "Test"},
		{"-C", source, "config", "user.email", "test@example.com"},
	} {
		if err := runGit(ctx, args...); err != nil {
			t.Fatal(err)
		}
	}
	oldSHA := commitFile(t, source, ".tangled/workflows/ci.yml", "engine: tack\n")
	newSHA := commitFile(t, source, "src/main.go", "package main\n")

	cache := filepath.Join(t.TempDir(), "cache")
	if err := syncWorkflowRepo(ctx, source, cache, newSHA); err != nil {
		t.Fatal(err)
	}
	files, ok := pushChangedFiles(ctx, cache, oldSHA, newSHA)
	if !ok || !equalStrings(files, []string{"src/main.go"}) {
		t.Fatalf("changed files = %v, %v; want [src/main.go]", files, ok)
	}
	// Knots send an all-zero object ID as the old side of a ref creation. There is no base
	// commit to diff against, so the set is unknown.
	if _, ok := pushChangedFiles(ctx, cache, strings.Repeat("0", 40), newSHA); ok {
		t.Fatal("ref creation reported a known changed-file set")
	}
	// An object ID the source repository does not have makes the fetch fail. That must also
	// read as unknown, never as an empty set that would skip paths-gated workflows.
	if _, ok := pushChangedFiles(ctx, cache, strings.Repeat("9", 40), newSHA); ok {
		t.Fatal("missing base commit reported a known changed-file set")
	}
}

// TestCompileWorkflowsUnknownChangesRunPathsGatedWorkflows pins upstream Tangled's handling of
// `paths` filters on push triggers. A known changed-file set runs a paths-gated workflow only when
// a file matches. An unknown set runs it anyway. The Tangled core v1.16 compiler that Tack links
// against treats a nil list as "nothing changed" and would skip the workflow, so compileWorkflows
// clears the `paths` constraints when unknownChanges is true. Without this, a knot2 push whose
// changed files cannot be recovered would silently run no paths-gated CI.
func TestCompileWorkflowsUnknownChangesRunPathsGatedWorkflows(t *testing.T) {
	raw := workflow.RawPipeline{{
		Name: "docs.yml",
		Contents: []byte("when:\n  - event: push\n    branch: main\n    paths: [\"docs/**\"]\n" +
			"engine: tack\ntack:\n  fake: {}\n"),
	}}
	trigger := &tangled.Pipeline_TriggerMetadata{
		Kind: string(workflow.TriggerKindPush),
		Push: &tangled.Pipeline_PushTriggerData{Ref: "refs/heads/main", NewSha: strings.Repeat("2", 40)},
		Repo: &tangled.Pipeline_TriggerRepo{},
	}
	if got := compileWorkflows(slog.Default(), raw, trigger, []string{"src/main.go"}, false); len(got) != 0 {
		t.Fatalf("known non-matching changes compiled %d workflows; want 0", len(got))
	}
	if got := compileWorkflows(slog.Default(), raw, trigger, []string{"docs/index.md"}, false); len(got) != 1 {
		t.Fatalf("matching changes compiled %d workflows; want 1", len(got))
	}
	if got := compileWorkflows(slog.Default(), raw, trigger, nil, true); len(got) != 1 {
		t.Fatalf("unknown changes compiled %d workflows; want 1", len(got))
	}
}

// TestSyncWorkflowRepoSHA256 pins support for knot2 repositories that use SHA-256 object IDs. Git
// fixes a repository's object format when it is created, and a SHA-1 cache cannot fetch a SHA-256
// commit at all. syncWorkflowRepo must therefore create the cache in the revision's format and
// replace an existing cache whose format does not match, instead of failing every fetch. The test
// skips when the local Git is too old to create SHA-256 repositories.
func TestSyncWorkflowRepoSHA256(t *testing.T) {
	ctx := context.Background()
	source := filepath.Join(t.TempDir(), "source")
	for _, args := range [][]string{
		{"init", "-q", "--object-format=sha256", source},
		{"-C", source, "config", "user.name", "Test"},
		{"-C", source, "config", "user.email", "test@example.com"},
		{"-C", source, "config", "uploadpack.allowAnySHA1InWant", "true"},
	} {
		if err := runGit(ctx, args...); err != nil {
			t.Skipf("git cannot create SHA-256 repositories: %v", err)
		}
	}
	revision := commitFile(t, source, ".tangled/workflows/ci.yml", "engine: tack\n")
	if len(revision) != 64 {
		t.Fatalf("revision %q is not SHA-256", revision)
	}

	// Seed a SHA-1 cache at the cache path, like the ones Tack created before it detected object
	// formats. syncWorkflowRepo must discard it and start over.
	cache := filepath.Join(t.TempDir(), "cache")
	if err := runGit(ctx, "init", "-q", cache); err != nil {
		t.Fatal(err)
	}
	if err := syncWorkflowRepo(ctx, source, cache, revision); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if format := cacheObjectFormat(ctx, cache); format != "sha256" {
		t.Fatalf("cache object format = %q; want sha256", format)
	}
	if _, err := os.Stat(filepath.Join(cache, ".tangled", "workflows", "ci.yml")); err != nil {
		t.Fatalf("workflow not checked out: %v", err)
	}
}
