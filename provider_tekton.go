package main

// tektonProvider implements Provider by creating Tekton PipelineRuns
// directly inside the Kubernetes cluster. Tack already receives and
// authorizes Tangled pipeline triggers, so adding Tekton Triggers would
// duplicate the event-to-run translation layer instead of simplifying it.

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"
	"time"
	"unicode"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	runtimeschema "k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"tangled.org/core/api/tangled"

	"go.yaml.in/yaml/v2"
)

const (
	tektonAPIVersion = "tekton.dev/v1"
	tektonRunKind    = "PipelineRun"

	tektonLabelManagedBy    = "tack.mitchellh.com/managed-by"
	tektonLabelPipelineRkey = "tack.mitchellh.com/pipeline-rkey"
	tektonLabelWorkflow     = "tack.mitchellh.com/workflow"

	tektonAnnotationKnot         = "tack.mitchellh.com/knot"
	tektonAnnotationPipelineRkey = "tack.mitchellh.com/pipeline-rkey"
	tektonAnnotationWorkflow     = "tack.mitchellh.com/workflow"
	tektonAnnotationActor        = "tack.mitchellh.com/actor"
	tektonAnnotationCommit       = "tack.mitchellh.com/commit"
	tektonAnnotationBranch       = "tack.mitchellh.com/branch"
)

var (
	pipelineRunsGVR = runtimeschema.GroupVersionResource{
		Group: "tekton.dev", Version: "v1", Resource: "pipelineruns",
	}
	taskRunsGVR = runtimeschema.GroupVersionResource{
		Group: "tekton.dev", Version: "v1", Resource: "taskruns",
	}
)

// tektonWorkflowConfig is the Tekton-specific subset of workflow YAML.
// `pipeline` names an existing in-cluster Tekton Pipeline. Params are
// deliberately string-only in v1: tack is meant to select an existing
// runner and pass a small amount of routing data, not mirror Tekton's
// entire PipelineRun API.
type tektonWorkflowConfig struct {
	Pipeline       string            `yaml:"pipeline"`
	ServiceAccount string            `yaml:"service_account"`
	Params         map[string]string `yaml:"params"`
}

type tektonWorkflowDoc struct {
	Tack struct {
		Tekton tektonWorkflowConfig `yaml:"tekton"`
	} `yaml:"tack"`
}

// parseTektonWorkflowConfig decodes `tack.tekton` from a workflow body.
func parseTektonWorkflowConfig(raw string) (*tektonWorkflowConfig, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, errors.New("workflow body is empty")
	}
	var doc tektonWorkflowDoc
	if err := yaml.Unmarshal([]byte(raw), &doc); err != nil {
		return nil, fmt.Errorf("parse workflow yaml: %w", err)
	}
	cfg := doc.Tack.Tekton
	if cfg.Pipeline == "" {
		return nil, errors.New("workflow yaml: `tack.tekton.pipeline` is required")
	}
	return &cfg, nil
}

type tektonProvider struct {
	br        *broker
	st        *store
	log       *slog.Logger
	dyn       dynamic.Interface
	kube      kubernetes.Interface
	namespace string
}

var _ Provider = (*tektonProvider)(nil)

func newTektonProvider(
	br *broker,
	st *store,
	dyn dynamic.Interface,
	kube kubernetes.Interface,
	namespace string,
	log *slog.Logger,
) *tektonProvider {
	return &tektonProvider{
		br:        br,
		st:        st,
		log:       log.With("component", "provider", "kind", "tekton"),
		dyn:       dyn,
		kube:      kube,
		namespace: namespace,
	}
}

func newInClusterTektonProvider(
	br *broker,
	st *store,
	namespace string,
	log *slog.Logger,
) (*tektonProvider, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("load in-cluster kubernetes config: %w", err)
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("create dynamic kubernetes client: %w", err)
	}
	kube, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("create kubernetes client: %w", err)
	}
	return newTektonProvider(br, st, dyn, kube, namespace, log), nil
}

func (p *tektonProvider) Spawn(
	ctx context.Context,
	knot string,
	pipelineRkey string,
	actor string,
	trigger *tangled.Pipeline_TriggerMetadata,
	workflows []*tangled.Pipeline_Workflow,
) {
	if len(workflows) == 0 {
		p.log.Warn("pipeline has no workflows; nothing to spawn",
			"knot", knot, "rkey", pipelineRkey,
		)
		return
	}
	for _, wf := range workflows {
		if wf == nil || wf.Name == "" {
			continue
		}
		wf := wf
		go p.spawnWorkflow(ctx, knot, pipelineRkey, actor, trigger, wf)
	}
}

func (p *tektonProvider) spawnWorkflow(
	ctx context.Context,
	knot string,
	pipelineRkey string,
	actor string,
	trigger *tangled.Pipeline_TriggerMetadata,
	wf *tangled.Pipeline_Workflow,
) {
	logger := p.log.With(
		"knot", knot,
		"pipeline_rkey", pipelineRkey,
		"workflow", wf.Name,
		"actor", actor,
	)

	cfg, err := parseTektonWorkflowConfig(wf.Raw)
	if err != nil {
		logger.Error("invalid workflow config; refusing to spawn", "err", err)
		return
	}
	commit, branch := triggerCommitAndBranch(trigger)
	name := tektonPipelineRunName(knot, pipelineRkey, wf.Name, commit, branch)
	pr := buildTektonPipelineRun(
		p.namespace, name, cfg, knot, pipelineRkey, actor, commit, branch, wf,
	)

	runs := p.dyn.Resource(pipelineRunsGVR).Namespace(p.namespace)
	created, err := runs.Create(ctx, pr, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		created, err = runs.Get(ctx, name, metav1.GetOptions{})
	}
	if err != nil {
		logger.Error("create tekton PipelineRun", "err", err,
			"namespace", p.namespace, "pipeline_run", name,
			"pipeline", cfg.Pipeline,
		)
		return
	}

	ref := TektonRunRef{
		Knot:            knot,
		PipelineRkey:    pipelineRkey,
		Workflow:        wf.Name,
		Namespace:       p.namespace,
		PipelineRunName: name,
		PipelineRunUID:  string(created.GetUID()),
		PipelineName:    cfg.Pipeline,
		PipelineURI:     pipelineATURI(knot, pipelineRkey),
	}
	if err := p.st.InsertTektonRun(ctx, ref); err != nil {
		logger.Error("persist tekton run mapping", "err", err,
			"pipeline_run", name,
		)
		return
	}

	if err := p.publishStatus(ctx, ref.PipelineURI, wf.Name,
		"pending", name, nil, nil); err != nil {
		logger.Error("publish initial pending status", "err", err)
	}

	logger.Info("tekton PipelineRun created",
		"namespace", p.namespace,
		"pipeline", cfg.Pipeline,
		"pipeline_run", name,
		"uid", ref.PipelineRunUID,
	)
	go p.watchPipelineRun(ctx, ref)
}

func buildTektonPipelineRun(
	namespace, name string,
	cfg *tektonWorkflowConfig,
	knot, pipelineRkey, actor, commit, branch string,
	wf *tangled.Pipeline_Workflow,
) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": tektonAPIVersion,
			"kind":       tektonRunKind,
			"metadata": map[string]interface{}{
				"name":      name,
				"namespace": namespace,
				"labels": map[string]interface{}{
					tektonLabelManagedBy:    "tack",
					tektonLabelPipelineRkey: labelValue(pipelineRkey),
					tektonLabelWorkflow:     labelValue(wf.Name),
				},
				"annotations": map[string]interface{}{
					tektonAnnotationKnot:         knot,
					tektonAnnotationPipelineRkey: pipelineRkey,
					tektonAnnotationWorkflow:     wf.Name,
					tektonAnnotationActor:        actor,
					tektonAnnotationCommit:       commit,
					tektonAnnotationBranch:       branch,
				},
			},
			"spec": map[string]interface{}{
				"pipelineRef": map[string]interface{}{
					"name": cfg.Pipeline,
				},
			},
		},
	}
	spec := obj.Object["spec"].(map[string]interface{})
	if cfg.ServiceAccount != "" {
		spec["serviceAccountName"] = cfg.ServiceAccount
	}
	if len(cfg.Params) > 0 {
		keys := make([]string, 0, len(cfg.Params))
		for key := range cfg.Params {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		params := make([]interface{}, 0, len(keys))
		for _, key := range keys {
			params = append(params, map[string]interface{}{
				"name":  key,
				"value": cfg.Params[key],
			})
		}
		spec["params"] = params
	}
	return obj
}

func (p *tektonProvider) watchPipelineRun(ctx context.Context, ref TektonRunRef) {
	logger := p.log.With(
		"knot", ref.Knot,
		"pipeline_rkey", ref.PipelineRkey,
		"workflow", ref.Workflow,
		"namespace", ref.Namespace,
		"pipeline_run", ref.PipelineRunName,
	)

	logger.Info("watchPipelineRun: starting")

	last := ""
	if obj, err := p.dyn.Resource(pipelineRunsGVR).Namespace(ref.Namespace).
		Get(ctx, ref.PipelineRunName, metav1.GetOptions{}); err == nil {
		status, terminal, ok := mapTektonPipelineRunStatus(obj)
		logger.Info("watchPipelineRun: initial status read",
			"status", status, "terminal", terminal, "ok", ok,
		)
		if ok {
			last = status
			if err := p.publishStatus(ctx, ref.PipelineURI, ref.Workflow,
				status, ref.PipelineRunName, nil, nil); err != nil {
				logger.Error("publish tekton status", "err", err, "status", status)
			}
			if terminal {
				logger.Info("watchPipelineRun: already terminal on initial read; exiting", "status", status)
				return
			}
		}
	} else if apierrors.IsNotFound(err) {
		logger.Warn("PipelineRun disappeared while watching")
		return
	} else {
		logger.Debug("initial PipelineRun status read", "err", err)
	}

	w, err := p.dyn.Resource(pipelineRunsGVR).Namespace(ref.Namespace).
		Watch(ctx, metav1.ListOptions{
			FieldSelector: fields.OneTermEqualSelector(
				"metadata.name", ref.PipelineRunName,
			).String(),
		})
	if err != nil {
		logger.Info("watchPipelineRun: watch failed; falling back to polling", "err", err)
		p.pollPipelineRun(ctx, ref, logger, last)
		return
	}
	defer w.Stop()

	logger.Info("watchPipelineRun: watch established; entering event loop")
	for {
		select {
		case <-ctx.Done():
			logger.Info("watchPipelineRun: context cancelled")
			return
		case ev, ok := <-w.ResultChan():
			if !ok {
				logger.Info("watchPipelineRun: watch channel closed; falling back to polling")
				p.pollPipelineRun(ctx, ref, logger, last)
				return
			}
			obj, ok := ev.Object.(*unstructured.Unstructured)
			if !ok {
				logger.Debug("watchPipelineRun: non-Unstructured event object", "type", fmt.Sprintf("%T", ev.Object))
				continue
			}
			status, terminal, ok := mapTektonPipelineRunStatus(obj)
			logger.Info("watchPipelineRun: watch event",
				"event_type", ev.Type,
				"status", status, "terminal", terminal, "ok", ok, "last", last,
			)
			if !ok || status == last {
				if terminal {
					logger.Info("watchPipelineRun: terminal status unchanged; exiting", "status", status)
					return
				}
				continue
			}
			last = status
			if err := p.publishStatus(ctx, ref.PipelineURI, ref.Workflow,
				status, ref.PipelineRunName, nil, nil); err != nil {
				logger.Error("publish tekton status", "err", err, "status", status)
				continue
			}
			logger.Info("watchPipelineRun: published status", "status", status, "terminal", terminal)
			if terminal {
				logger.Info("watchPipelineRun: terminal status reached; exiting", "status", status)
				return
			}
		}
	}
}

func (p *tektonProvider) pollPipelineRun(
	ctx context.Context,
	ref TektonRunRef,
	logger *slog.Logger,
	last string,
) {
	logger.Info("pollPipelineRun: starting poll loop", "interval", "5s")
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			logger.Info("pollPipelineRun: context cancelled")
			return
		case <-ticker.C:
			obj, err := p.dyn.Resource(pipelineRunsGVR).Namespace(ref.Namespace).
				Get(ctx, ref.PipelineRunName, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				logger.Warn("PipelineRun disappeared while watching")
				return
			}
			if err != nil {
				logger.Debug("get PipelineRun status", "err", err)
				continue
			}
			status, terminal, ok := mapTektonPipelineRunStatus(obj)
			logger.Info("pollPipelineRun: poll tick",
				"status", status, "terminal", terminal, "ok", ok, "last", last,
			)
			if !ok || status == last {
				if terminal {
					logger.Info("pollPipelineRun: terminal status unchanged; exiting", "status", status)
					return
				}
				continue
			}
			last = status
			if err := p.publishStatus(ctx, ref.PipelineURI, ref.Workflow,
				status, ref.PipelineRunName, nil, nil); err != nil {
				logger.Error("publish tekton status", "err", err, "status", status)
				continue
			}
			logger.Info("pollPipelineRun: published status", "status", status, "terminal", terminal)
			if terminal {
				logger.Info("pollPipelineRun: terminal status reached; exiting", "status", status)
				return
			}
		}
	}
}

// mapTektonPipelineRunStatus translates Tekton's Succeeded condition
// into the Tangled status strings consumed by the appview.
func mapTektonPipelineRunStatus(obj *unstructured.Unstructured) (status string, terminal bool, ok bool) {
	conditions, ok, _ := unstructured.NestedSlice(obj.Object, "status", "conditions")
	if !ok || len(conditions) == 0 {
		slog.Debug("mapTektonPipelineRunStatus: no conditions found",
			"pipeline_run", obj.GetName(),
		)
		return "", false, false
	}
	for _, raw := range conditions {
		cond, _ := raw.(map[string]interface{})
		condType, _ := cond["type"].(string)
		condStatus, _ := cond["status"].(string)
		reason, _ := cond["reason"].(string)
		message, _ := cond["message"].(string)
		slog.Info("mapTektonPipelineRunStatus: condition",
			"pipeline_run", obj.GetName(),
			"type", condType,
			"status", condStatus,
			"reason", reason,
			"message", message,
		)
		if condType != "Succeeded" {
			continue
		}
		switch condStatus {
		case "True":
			return "success", true, true
		case "False":
			if tektonReasonCancelled(reason) {
				return "cancelled", true, true
			}
			return "failed", true, true
		case "Unknown":
			return "running", false, true
		default:
			return "", false, false
		}
	}
	return "", false, false
}

func tektonReasonCancelled(reason string) bool {
	r := strings.ToLower(reason)
	return strings.Contains(r, "cancel") || strings.Contains(r, "stop")
}

func (p *tektonProvider) Logs(
	ctx context.Context,
	knot string,
	pipelineRkey string,
	workflow string,
) (<-chan LogLine, error) {
	ref, err := p.st.LookupTektonRunByTuple(ctx, knot, pipelineRkey, workflow)
	if err != nil {
		return nil, fmt.Errorf("lookup tekton run mapping: %w", err)
	}
	if ref == nil {
		return nil, ErrLogsNotFound
	}

	taskRuns, err := p.taskRunsForPipelineRun(ctx, *ref)
	if err != nil {
		return nil, err
	}
	p.log.Info("Logs: found TaskRuns for PipelineRun",
		"pipeline_run", ref.PipelineRunName, "count", len(taskRuns),
	)
	if len(taskRuns) == 0 {
		return nil, ErrLogsNotFound
	}

	terminal := p.isPipelineRunTerminal(ctx, *ref)
	p.log.Info("Logs: pipeline run terminal state", "pipeline_run", ref.PipelineRunName, "terminal", terminal)

	out := make(chan LogLine, 32)
	go func() {
		defer close(out)
		stepID := 0
		for _, tr := range taskRuns {
			taskName := tr.GetName()
			if taskName == "" {
				taskName = fmt.Sprintf("task %d", stepID)
			}
			p.log.Info("Logs: streaming TaskRun", "task_run", taskName, "step_id", stepID, "terminal", terminal)
			if !sendLine(ctx, out, LogLine{
				Kind:       LogKindControl,
				Time:       time.Now(),
				Content:    taskName,
				StepId:     stepID,
				StepStatus: StepStatusStart,
			}) {
				return
			}

			if terminal {
				p.fetchCompletedTaskRunLogs(ctx, out, *ref, tr, stepID)
			} else {
				p.streamTaskRunLogs(ctx, out, *ref, tr, stepID)
			}

			if !sendLine(ctx, out, LogLine{
				Kind:       LogKindControl,
				Time:       time.Now(),
				Content:    taskName,
				StepId:     stepID,
				StepStatus: StepStatusEnd,
			}) {
				return
			}
			p.log.Info("Logs: finished TaskRun", "task_run", taskName, "step_id", stepID)
			stepID++
		}
		p.log.Info("Logs: all TaskRuns streamed", "pipeline_run", ref.PipelineRunName)
	}()
	return out, nil
}

// isPipelineRunTerminal returns true if the PipelineRun is in a terminal state right now.
func (p *tektonProvider) isPipelineRunTerminal(ctx context.Context, ref TektonRunRef) bool {
	obj, err := p.dyn.Resource(pipelineRunsGVR).Namespace(ref.Namespace).
		Get(ctx, ref.PipelineRunName, metav1.GetOptions{})
	if err != nil {
		p.log.Info("isPipelineRunTerminal: failed to get PipelineRun", "err", err, "pipeline_run", ref.PipelineRunName)
		return false
	}
	_, terminal, ok := mapTektonPipelineRunStatus(obj)
	p.log.Info("isPipelineRunTerminal: status check", "pipeline_run", ref.PipelineRunName, "terminal", terminal, "ok", ok)
	return ok && terminal
}

// fetchCompletedTaskRunLogs fetches all logs from a TaskRun that has already completed.
// It reads each step container's logs in one shot using all-containers traversal,
// and also inlines the Tekton step status from the TaskRun (exit code, reason) as
// control messages so the caller gets full context without needing to watch for events.
func (p *tektonProvider) fetchCompletedTaskRunLogs(
	ctx context.Context,
	out chan<- LogLine,
	ref TektonRunRef,
	tr unstructured.Unstructured,
	stepID int,
) {
	trName := tr.GetName()
	pods, err := p.podsForTaskRun(ctx, ref.Namespace, trName)
	if err != nil {
		p.log.Info("fetchCompletedTaskRunLogs: list pods failed", "err", err,
			"task_run", trName, "pipeline_run", ref.PipelineRunName,
		)
		return
	}
	p.log.Info("fetchCompletedTaskRunLogs: found pods",
		"task_run", trName, "pod_count", len(pods),
	)

	// Emit a summary line from the TaskRun status (steps[*].terminated) so we
	// get exit codes and reasons even if the pod logs are sparse.
	steps, _, _ := unstructured.NestedSlice(tr.Object, "status", "steps")
	for _, rawStep := range steps {
		step, _ := rawStep.(map[string]interface{})
		stepName, _ := step["name"].(string)
		term, _ := step["terminated"].(map[string]interface{})
		if term == nil {
			continue
		}
		exitCode, _ := term["exitCode"].(int64)
		reason, _ := term["reason"].(string)
		msg, _ := term["message"].(string)
		line := fmt.Sprintf("[step %s] exit=%d reason=%s", stepName, exitCode, reason)
		if msg != "" {
			line += " " + msg
		}
		p.log.Info("fetchCompletedTaskRunLogs: step terminated",
			"task_run", trName, "step", stepName,
			"exit_code", exitCode, "reason", reason,
		)
		if !sendLine(ctx, out, LogLine{
			Kind:    LogKindData,
			Time:    time.Now(),
			Content: line + "\n",
			StepId:  stepID,
			Stream:  "stdout",
		}) {
			return
		}
	}

	for _, pod := range pods {
		containers := append(pod.Spec.InitContainers, pod.Spec.Containers...)
		p.log.Info("fetchCompletedTaskRunLogs: reading pod containers",
			"pod", pod.Name, "container_count", len(containers),
		)
		for _, c := range containers {
			p.log.Info("fetchCompletedTaskRunLogs: reading container logs",
				"pod", pod.Name, "container", c.Name,
			)
			req := p.kube.CoreV1().Pods(ref.Namespace).GetLogs(pod.Name, &corev1.PodLogOptions{
				Container: c.Name,
			})
			rc, err := req.Stream(ctx)
			if err != nil {
				p.log.Info("fetchCompletedTaskRunLogs: stream failed", "err", err,
					"pod", pod.Name, "container", c.Name,
				)
				continue
			}
			p.sendReaderLines(ctx, out, rc, stepID)
			_ = rc.Close()
			p.log.Info("fetchCompletedTaskRunLogs: done reading container",
				"pod", pod.Name, "container", c.Name,
			)
		}
	}
}

func (p *tektonProvider) taskRunsForPipelineRun(ctx context.Context, ref TektonRunRef) ([]unstructured.Unstructured, error) {
	sel := labels.Set{"tekton.dev/pipelineRun": ref.PipelineRunName}.String()
	list, err := p.dyn.Resource(taskRunsGVR).Namespace(ref.Namespace).
		List(ctx, metav1.ListOptions{LabelSelector: sel})
	if err != nil {
		return nil, fmt.Errorf("list Tekton TaskRuns: %w", err)
	}
	items := append([]unstructured.Unstructured(nil), list.Items...)
	sort.Slice(items, func(i, j int) bool {
		ti := items[i].GetCreationTimestamp()
		tj := items[j].GetCreationTimestamp()
		return ti.Before(&tj)
	})
	return items, nil
}

func (p *tektonProvider) streamTaskRunLogs(
	ctx context.Context,
	out chan<- LogLine,
	ref TektonRunRef,
	tr unstructured.Unstructured,
	stepID int,
) {
	pods, err := p.podsForTaskRun(ctx, ref.Namespace, tr.GetName())
	if err != nil {
		p.log.Info("streamTaskRunLogs: list pods for TaskRun failed", "err", err,
			"task_run", tr.GetName(), "pipeline_run", ref.PipelineRunName,
		)
		return
	}
	p.log.Info("streamTaskRunLogs: found pods",
		"task_run", tr.GetName(), "pod_count", len(pods),
	)
	for _, pod := range pods {
		containers := append(pod.Spec.InitContainers, pod.Spec.Containers...)
		p.log.Info("streamTaskRunLogs: streaming pod containers",
			"pod", pod.Name, "container_count", len(containers),
		)
		for _, c := range containers {
			p.log.Info("streamTaskRunLogs: streaming container",
				"pod", pod.Name, "container", c.Name, "step_id", stepID,
			)
			req := p.kube.CoreV1().Pods(ref.Namespace).GetLogs(pod.Name, &corev1.PodLogOptions{
				Container: c.Name,
			})
			rc, err := req.Stream(ctx)
			if err != nil {
				p.log.Info("streamTaskRunLogs: stream pod logs failed", "err", err,
					"pod", pod.Name, "container", c.Name,
				)
				continue
			}
			p.sendReaderLines(ctx, out, rc, stepID)
			_ = rc.Close()
			p.log.Info("streamTaskRunLogs: finished container",
				"pod", pod.Name, "container", c.Name,
			)
		}
	}
}

func (p *tektonProvider) podsForTaskRun(ctx context.Context, namespace, taskRun string) ([]corev1.Pod, error) {
	sel := labels.Set{"tekton.dev/taskRun": taskRun}.String()
	list, err := p.kube.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: sel,
	})
	if err != nil {
		return nil, fmt.Errorf("list pods: %w", err)
	}
	pods := append([]corev1.Pod(nil), list.Items...)
	sort.Slice(pods, func(i, j int) bool {
		return pods[i].CreationTimestamp.Before(&pods[j].CreationTimestamp)
	})
	return pods, nil
}

func (p *tektonProvider) sendReaderLines(
	ctx context.Context,
	out chan<- LogLine,
	rc io.Reader,
	stepID int,
) {
	scanner := bufio.NewScanner(rc)
	for scanner.Scan() {
		if !sendLine(ctx, out, LogLine{
			Kind:    LogKindData,
			Time:    time.Now(),
			Content: scanner.Text() + "\n",
			StepId:  stepID,
			Stream:  "stdout",
		}) {
			return
		}
	}
	if err := scanner.Err(); err != nil {
		p.log.Debug("scan pod log", "err", err)
	}
}

func (p *tektonProvider) publishStatus(
	ctx context.Context,
	pipelineURI, workflow, status, runName string,
	errMsg *string,
	exitCode *int64,
) error {
	rec := tangled.PipelineStatus{
		LexiconTypeID: tangled.PipelineStatusNSID,
		Pipeline:      pipelineURI,
		Workflow:      workflow,
		Status:        status,
		CreatedAt:     time.Now().UTC().Format(time.RFC3339),
		Error:         errMsg,
		ExitCode:      exitCode,
	}
	body, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("marshal pipeline.status: %w", err)
	}
	rkey := fmt.Sprintf("tk-%s-%s-%d", runName, status, time.Now().UnixNano())
	if _, err := p.br.Publish(ctx, rkey, tangled.PipelineStatusNSID, body); err != nil {
		return fmt.Errorf("publish pipeline.status: %w", err)
	}
	return nil
}

func tektonPipelineRunName(knot, pipelineRkey, workflow, commit, branch string) string {
	h := sha256.Sum256([]byte(strings.Join(
		[]string{knot, pipelineRkey, workflow, commit, branch}, "\x00",
	)))
	suffix := hex.EncodeToString(h[:])[:12]
	base := dnsLabel("tack-" + workflow)
	maxBase := 63 - len(suffix) - 1
	if len(base) > maxBase {
		base = strings.TrimRight(base[:maxBase], "-")
	}
	if base == "" {
		base = "tack"
	}
	return base + "-" + suffix
}

func dnsLabel(s string) string {
	var b strings.Builder
	lastDash := false
	for _, r := range strings.ToLower(s) {
		ok := unicode.IsLetter(r) || unicode.IsDigit(r)
		if ok {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}
	return strings.Trim(b.String(), "-")
}

func labelValue(s string) string {
	v := dnsLabel(s)
	if len(v) > 63 {
		v = strings.TrimRight(v[:63], "-")
	}
	if v == "" {
		return "unknown"
	}
	return v
}
