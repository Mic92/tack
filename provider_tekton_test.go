package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"tangled.org/core/api/tangled"

	"go.mitchellh.com/tack/internal/k8s"
)

func newTektonTestProvider(t *testing.T) (*tektonProvider, *store, *broker, *k8s.FakeClient) {
	t.Helper()
	st := newTestStore(t)
	br := newBroker(st)
	client := k8s.NewFakeClient()
	p := newTektonProvider(br, st, client, "ci", slog.Default())
	return p, st, br, client
}

func TestTektonWorkflowConfig(t *testing.T) {
	raw := "tack:\n  tekton:\n    pipeline: repo-ci\n    service_account: runner\n    params:\n      image: example/app\n"
	cfg, err := parseTektonWorkflowConfig(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cfg.Pipeline != "repo-ci" || cfg.ServiceAccount != "runner" {
		t.Fatalf("cfg mismatch: %+v", cfg)
	}
	if got := cfg.Params["image"]; got != "example/app" {
		t.Fatalf("params[image] = %q", got)
	}

	if _, err := parseTektonWorkflowConfig("tack:\n  tekton: {}\n"); err == nil {
		t.Fatal("missing pipeline should fail")
	}
}

func TestTektonWorkspaceValidation(t *testing.T) {
	tests := []struct {
		name    string
		yaml    string
		wantErr string
	}{
		{
			name:    "empty name",
			yaml:    "tack:\n  tekton:\n    pipeline: ci\n    workspaces:\n      - storage: 1Gi\n",
			wantErr: "name is required",
		},
		{
			name:    "no source",
			yaml:    "tack:\n  tekton:\n    pipeline: ci\n    workspaces:\n      - name: scratch\n",
			wantErr: "no volume source",
		},
		{
			name:    "multiple sources",
			yaml:    "tack:\n  tekton:\n    pipeline: ci\n    workspaces:\n      - name: data\n        storage: 5Gi\n        pvc: my-pvc\n",
			wantErr: "multiple volume sources",
		},
		{
			name: "valid single source",
			yaml: "tack:\n  tekton:\n    pipeline: ci\n    workspaces:\n      - name: scratch\n        storage: 1Gi\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseTektonWorkflowConfig(tt.yaml)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf(
					"error %q does not contain %q",
					err.Error(), tt.wantErr,
				)
			}
		})
	}
}

func TestTektonBuildPipelineRun(t *testing.T) {
	cfg := &tektonWorkflowConfig{
		Pipeline:       "repo-ci",
		ServiceAccount: "runner",
		Params: map[string]string{
			"image": "example/app",
		},
	}
	name := tektonPipelineRunName("knot.example.com", "rkey-1", "ci.yml", "abcdef", "main")
	if len(name) > 63 || name == "" {
		t.Fatalf("bad generated name: %q", name)
	}

	obj := buildTektonPipelineRun("ci", name, cfg,
		"knot.example.com", "rkey-1", "did:plc:actor", "abcdef", "main",
		&tangled.Pipeline_Workflow{Name: "ci.yml"},
	)
	if obj.GetAPIVersion() != tektonAPIVersion || obj.GetKind() != tektonRunKind {
		t.Fatalf("type meta mismatch: %s %s", obj.GetAPIVersion(), obj.GetKind())
	}
	pipeline, ok := obj.NestedString("spec", "pipelineRef", "name")
	if !ok || pipeline != "repo-ci" {
		t.Fatalf("pipelineRef.name = %q", pipeline)
	}
	sa, ok := obj.NestedString("spec", "taskRunTemplate", "serviceAccountName")
	if !ok || sa != "runner" {
		t.Fatalf("serviceAccountName = %q", sa)
	}
	// With cfg.Params={"image": "example/app"}, the merged params
	// should contain the 3 built-ins (actor, branch, commit) plus
	// the user-supplied "image" — 4 total, sorted alphabetically.
	params, ok := obj.NestedSlice("spec", "params")
	if !ok || len(params) != 4 {
		t.Fatalf("params count = %d, want 4: %+v", len(params), params)
	}
	if obj.GetAnnotations()[tektonAnnotationActor] != "did:plc:actor" ||
		obj.GetAnnotations()[tektonAnnotationCommit] != "abcdef" {
		t.Fatalf("annotations missing identity: %+v", obj.GetAnnotations())
	}
}

func TestTektonBuildPipelineRunParamsOverride(t *testing.T) {
	// When a user param collides with a built-in name, the user's
	// value should win so callers can customize what the upstream
	// Tekton Pipeline receives.
	cfg := &tektonWorkflowConfig{
		Pipeline: "repo-ci",
		Params: map[string]string{
			"commit": "user-override-sha",
			"extra":  "bonus",
		},
	}
	obj := buildTektonPipelineRun("ci", "run-1", cfg,
		"knot.example.com", "rkey-1", "did:plc:actor",
		"original-sha", "main",
		&tangled.Pipeline_Workflow{Name: "ci.yml"},
	)
	params, ok := obj.NestedSlice("spec", "params")
	if !ok {
		t.Fatal("params missing")
	}
	// Expect 4: actor, branch, commit (overridden), extra.
	if len(params) != 4 {
		t.Fatalf("params count = %d, want 4: %+v", len(params), params)
	}
	// Verify the user override took effect.
	for _, raw := range params {
		p, _ := raw.(map[string]any)
		if p["name"] == "commit" && p["value"] != "user-override-sha" {
			t.Fatalf(
				"commit param = %q, want user-override-sha",
				p["value"],
			)
		}
	}
}

func TestTektonBuildPipelineRunWorkspaces(t *testing.T) {
	storage := "5Gi"
	pvc := "shared-cache"
	secret := "git-credentials"
	configMap := "app-config"
	cfg := &tektonWorkflowConfig{
		Pipeline: "repo-ci",
		Workspaces: []tektonWorkspaceConfig{
			{Name: "scratch", AccessModes: []string{"ReadWriteOnce"}, Storage: &storage},
			{Name: "cache", PVC: &pvc},
			{Name: "git-auth", Secret: &secret},
			{Name: "config", ConfigMap: &configMap},
		},
	}

	obj := buildTektonPipelineRun("ci", "run-1", cfg,
		"knot.example.com", "rkey-1", "did:plc:actor", "abcdef", "main",
		&tangled.Pipeline_Workflow{Name: "ci.yml"},
	)

	podTemplate, ok := obj.NestedMap("spec", "podTemplate")
	if !ok {
		t.Fatal("podTemplate missing for workspace-backed PipelineRun")
	}
	fsGroup, ok := k8s.NestedMap(podTemplate, "securityContext")
	if !ok || fsGroup["fsGroup"] != 65532 {
		t.Fatalf("podTemplate.securityContext = %+v", podTemplate)
	}

	workspaces, ok := obj.NestedSlice("spec", "workspaces")
	if !ok || len(workspaces) != 4 {
		t.Fatalf("workspaces = %+v", workspaces)
	}

	scratch, ok := workspaces[0].(map[string]any)
	if !ok {
		t.Fatalf("scratch workspace = %#v", workspaces[0])
	}
	if scratch["name"] != "scratch" {
		t.Fatalf("scratch.name = %#v", scratch["name"])
	}
	storageSpec, ok := k8s.NestedMap(scratch, "volumeClaimTemplate", "spec", "resources", "requests")
	if !ok || storageSpec["storage"] != "5Gi" {
		t.Fatalf("scratch volumeClaimTemplate = %+v", scratch)
	}

	cache, ok := workspaces[1].(map[string]any)
	if !ok {
		t.Fatalf("cache workspace = %#v", workspaces[1])
	}
	claim, ok := k8s.NestedMap(cache, "persistentVolumeClaim")
	if !ok || claim["claimName"] != "shared-cache" {
		t.Fatalf("cache persistentVolumeClaim = %+v", cache)
	}

	gitAuth, ok := workspaces[2].(map[string]any)
	if !ok {
		t.Fatalf("git-auth workspace = %#v", workspaces[2])
	}
	secretRef, ok := k8s.NestedMap(gitAuth, "secret")
	if !ok || secretRef["secretName"] != "git-credentials" {
		t.Fatalf("git-auth secret = %+v", gitAuth)
	}

	cfgWs, ok := workspaces[3].(map[string]any)
	if !ok {
		t.Fatalf("config workspace = %#v", workspaces[3])
	}
	cmRef, ok := k8s.NestedMap(cfgWs, "configMap")
	if !ok || cmRef["name"] != "app-config" {
		t.Fatalf("config configMap = %+v", cfgWs)
	}
}

func TestTektonStatusMapping(t *testing.T) {
	tests := []struct {
		name     string
		cond     string
		reason   string
		status   string
		terminal bool
		ok       bool
	}{
		{name: "unknown", cond: "Unknown", status: "running", ok: true},
		{name: "success", cond: "True", status: "success", terminal: true, ok: true},
		{name: "failed", cond: "False", reason: "Failed", status: "failed", terminal: true, ok: true},
		{name: "cancelled", cond: "False", reason: "PipelineRunCancelled", status: "cancelled", terminal: true, ok: true},
		{name: "stopped", cond: "False", reason: "PipelineRunStopped", status: "cancelled", terminal: true, ok: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			obj := tektonStatusObject(tt.cond, tt.reason)
			status, terminal, ok := mapTektonPipelineRunStatus(obj)
			if status != tt.status || terminal != tt.terminal || ok != tt.ok {
				t.Fatalf("got %q/%v/%v; want %q/%v/%v",
					status, terminal, ok, tt.status, tt.terminal, tt.ok)
			}
		})
	}
}

func TestTektonSpawnCreatesPipelineRun(t *testing.T) {
	p, st, _, client := newTektonTestProvider(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	trigger := &tangled.Pipeline_TriggerMetadata{
		Push: &tangled.Pipeline_PushTriggerData{
			NewSha: "abcdef0123",
			Ref:    "refs/heads/main",
		},
	}
	p.Spawn(ctx, "knot.example.com", "rkey-1", "did:plc:actor", trigger,
		[]*tangled.Pipeline_Workflow{{Name: "ci.yml",
			Raw: "tack:\n  tekton:\n    pipeline: repo-ci\n"}},
	)

	ref := waitTektonRef(t, st, "knot.example.com", "rkey-1", "ci.yml")
	if ref.Namespace != "ci" || ref.PipelineName != "repo-ci" {
		t.Fatalf("ref mismatch: %+v", ref)
	}

	obj, err := client.GetObject(context.Background(), pipelineRunsGVR, "ci", ref.PipelineRunName)
	if err != nil {
		t.Fatalf("get PipelineRun: %v", err)
	}
	pipeline, ok := obj.NestedString("spec", "pipelineRef", "name")
	if !ok || pipeline != "repo-ci" {
		t.Fatalf("pipelineRef.name = %q", pipeline)
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
		t.Fatalf("decode status: %v", err)
	}
	if rec.Status != "pending" || rec.Workflow != "ci.yml" {
		t.Fatalf("bad pending status: %+v", rec)
	}
}

func TestTektonSpawnAlreadyExists(t *testing.T) {
	p, st, _, client := newTektonTestProvider(t)
	name := tektonPipelineRunName("knot.example.com", "rkey-1", "ci.yml", "abcdef0123", "main")
	existing := buildTektonPipelineRun("ci", name,
		&tektonWorkflowConfig{Pipeline: "repo-ci"},
		"knot.example.com", "rkey-1", "did:plc:actor", "abcdef0123", "main",
		&tangled.Pipeline_Workflow{Name: "ci.yml"},
	)
	existing.SetUID("uid-1")
	client.SeedObject(pipelineRunsGVR, "ci", existing)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Spawn(ctx, "knot.example.com", "rkey-1", "did:plc:actor",
		&tangled.Pipeline_TriggerMetadata{Push: &tangled.Pipeline_PushTriggerData{
			NewSha: "abcdef0123",
			Ref:    "refs/heads/main",
		}},
		[]*tangled.Pipeline_Workflow{{Name: "ci.yml",
			Raw: "tack:\n  tekton:\n    pipeline: repo-ci\n"}},
	)

	ref := waitTektonRef(t, st, "knot.example.com", "rkey-1", "ci.yml")
	if ref.PipelineRunName != name || ref.PipelineRunUID != "uid-1" {
		t.Fatalf("ref mismatch: %+v", ref)
	}
}

func TestTektonLogsLookup(t *testing.T) {
	p, st, _, client := newTektonTestProvider(t)
	ctx := context.Background()
	if _, err := p.Logs(ctx, "knot.example.com", "rkey-1", "ci.yml"); !errors.Is(err, ErrLogsNotFound) {
		t.Fatalf("logs before mapping err = %v; want ErrLogsNotFound", err)
	}
	ref := TektonRunRef{
		Knot:            "knot.example.com",
		PipelineRkey:    "rkey-1",
		Workflow:        "ci.yml",
		Namespace:       "ci",
		PipelineRunName: "run-1",
		PipelineRunUID:  "uid-1",
		PipelineName:    "repo-ci",
		PipelineURI:     pipelineATURI("knot.example.com", "rkey-1"),
	}
	if err := st.InsertTektonRun(ctx, ref); err != nil {
		t.Fatalf("insert ref: %v", err)
	}
	// With the mapping in place but no TaskRuns yet, Logs must NOT
	// return ErrLogsNotFound: the workflow has been spawned and is
	// just queueing inside Tekton. Surfacing 404 here mistranslates
	// "still scheduling" as "doesn't exist" at the HTTP layer (see
	// the CI log subscription handler). Verify we get an open channel that
	// stays open until ctx is cancelled.
	{
		waitCtx, cancel := context.WithCancel(ctx)
		ch, err := p.Logs(waitCtx, "knot.example.com", "rkey-1", "ci.yml")
		if err != nil {
			cancel()
			t.Fatalf("logs before TaskRuns err = %v; want nil", err)
		}
		if ch == nil {
			cancel()
			t.Fatalf("logs before TaskRuns: nil channel")
		}
		// Channel must not produce any frames or close before we
		// cancel. A premature close would mean the goroutine treated
		// "no TaskRuns yet" as "stream done", which is what we just
		// fixed.
		select {
		case line, ok := <-ch:
			cancel()
			if !ok {
				t.Fatalf("logs channel closed before TaskRuns appeared")
			}
			t.Fatalf("unexpected frame before TaskRuns: %+v", line)
		case <-time.After(50 * time.Millisecond):
		}
		cancel()
		// After cancellation the producer goroutine must close the
		// channel and not strand any frames.
		for line := range ch {
			t.Fatalf("unexpected frame after cancel: %+v", line)
		}
	}

	// Seed a terminal PipelineRun so Logs takes the snapshot path
	// (fetchCompletedTaskRunLogs, Follow=false) and closes the
	// channel after draining the seeded TaskRun. The non-terminal
	// path follows pod logs live and would only EOF on ctx cancel,
	// which is not what this assertion is exercising.
	client.SeedObject(pipelineRunsGVR, "ci", k8s.Object{
		"apiVersion": "tekton.dev/v1",
		"kind":       "PipelineRun",
		"metadata": map[string]any{
			"name":      "run-1",
			"namespace": "ci",
		},
		"status": map[string]any{
			"conditions": []any{map[string]any{
				"type":   "Succeeded",
				"status": "True",
			}},
		},
	})
	client.SeedObject(taskRunsGVR, "ci", k8s.Object{
		"apiVersion": "tekton.dev/v1",
		"kind":       "TaskRun",
		"metadata": map[string]any{
			"name":      "task-1",
			"namespace": "ci",
			"labels": map[string]any{
				"tekton.dev/pipelineRun": "run-1",
			},
		},
	})
	client.SeedPod("ci", k8s.Pod{
		Name:      "pod-1",
		Namespace: "ci",
		Labels: map[string]string{
			"tekton.dev/taskRun": "task-1",
		},
		Containers: []k8s.Container{{Name: "step-test"}},
	})
	client.SetPodLog("ci", "pod-1", "step-test", "hello\n")

	ch, err := p.Logs(ctx, "knot.example.com", "rkey-1", "ci.yml")
	if err != nil {
		t.Fatalf("Logs after pods: %v", err)
	}
	var got []LogLine
	for line := range ch {
		got = append(got, line)
	}
	if len(got) < 2 || got[0].StepStatus != StepStatusStart ||
		got[len(got)-1].StepStatus != StepStatusEnd {
		t.Fatalf("log frames = %+v", got)
	}
}

func tektonStatusObject(condStatus, reason string) k8s.Object {
	return k8s.Object{
		"status": map[string]any{
			"conditions": []any{map[string]any{
				"type":   "Succeeded",
				"status": condStatus,
				"reason": reason,
			}},
		},
	}
}

func waitTektonRef(t *testing.T, st *store, knot, rkey, workflow string) *TektonRunRef {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		ref, err := st.LookupTektonRunByTuple(context.Background(), knot, rkey, workflow)
		if err != nil {
			t.Fatalf("lookup: %v", err)
		}
		if ref != nil {
			return ref
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("tekton run row not persisted within deadline")
	return nil
}
