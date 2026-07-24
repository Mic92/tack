package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
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
