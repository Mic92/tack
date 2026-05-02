package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	runtimeschema "k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"tangled.org/core/api/tangled"
)

func newTektonTestProvider(t *testing.T, objs ...runtime.Object) (*tektonProvider, *store, *broker) {
	t.Helper()
	st := newTestStore(t)
	br := newBroker(st)
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(),
		map[runtimeschema.GroupVersionResource]string{
			pipelineRunsGVR: "PipelineRunList",
			taskRunsGVR:     "TaskRunList",
		},
		objs...,
	)
	kube := kubefake.NewSimpleClientset()
	p := newTektonProvider(br, st, dyn, kube, "ci", slog.Default())
	return p, st, br
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
	pipeline, _, _ := unstructured.NestedString(obj.Object, "spec", "pipelineRef", "name")
	if pipeline != "repo-ci" {
		t.Fatalf("pipelineRef.name = %q", pipeline)
	}
	sa, _, _ := unstructured.NestedString(obj.Object, "spec", "serviceAccountName")
	if sa != "runner" {
		t.Fatalf("serviceAccountName = %q", sa)
	}
	params, _, _ := unstructured.NestedSlice(obj.Object, "spec", "params")
	if len(params) != 1 {
		t.Fatalf("params = %+v", params)
	}
	if obj.GetAnnotations()[tektonAnnotationActor] != "did:plc:actor" ||
		obj.GetAnnotations()[tektonAnnotationCommit] != "abcdef" {
		t.Fatalf("annotations missing identity: %+v", obj.GetAnnotations())
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
	p, st, _ := newTektonTestProvider(t)
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

	obj, err := p.dyn.Resource(pipelineRunsGVR).Namespace("ci").
		Get(context.Background(), ref.PipelineRunName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get PipelineRun: %v", err)
	}
	pipeline, _, _ := unstructured.NestedString(obj.Object, "spec", "pipelineRef", "name")
	if pipeline != "repo-ci" {
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
	name := tektonPipelineRunName("knot.example.com", "rkey-1", "ci.yml", "abcdef0123", "main")
	existing := buildTektonPipelineRun("ci", name,
		&tektonWorkflowConfig{Pipeline: "repo-ci"},
		"knot.example.com", "rkey-1", "did:plc:actor", "abcdef0123", "main",
		&tangled.Pipeline_Workflow{Name: "ci.yml"},
	)
	existing.SetUID("uid-1")
	p, st, _ := newTektonTestProvider(t, existing)
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
	p, st, _ := newTektonTestProvider(t)
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
	if _, err := p.Logs(ctx, "knot.example.com", "rkey-1", "ci.yml"); !errors.Is(err, ErrLogsNotFound) {
		t.Fatalf("logs before TaskRuns err = %v; want ErrLogsNotFound", err)
	}

	taskRun := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "tekton.dev/v1",
		"kind":       "TaskRun",
		"metadata": map[string]interface{}{
			"name":      "task-1",
			"namespace": "ci",
			"labels": map[string]interface{}{
				"tekton.dev/pipelineRun": "run-1",
			},
		},
	}}
	_, err := p.dyn.Resource(taskRunsGVR).Namespace("ci").
		Create(ctx, taskRun, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create TaskRun: %v", err)
	}
	_, err = p.kube.CoreV1().Pods("ci").Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-1",
			Namespace: "ci",
			Labels: map[string]string{
				"tekton.dev/taskRun": "task-1",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "step-test", Image: "busybox"}},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create pod: %v", err)
	}

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

func tektonStatusObject(condStatus, reason string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"status": map[string]interface{}{
			"conditions": []interface{}{map[string]interface{}{
				"type":   "Succeeded",
				"status": condStatus,
				"reason": reason,
			}},
		},
	}}
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
