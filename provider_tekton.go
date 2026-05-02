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

	last := ""
	if obj, err := p.dyn.Resource(pipelineRunsGVR).Namespace(ref.Namespace).
		Get(ctx, ref.PipelineRunName, metav1.GetOptions{}); err == nil {
		status, terminal, ok := mapTektonPipelineRunStatus(obj)
		if ok {
			last = status
			if err := p.publishStatus(ctx, ref.PipelineURI, ref.Workflow,
				status, ref.PipelineRunName, nil, nil); err != nil {
				logger.Error("publish tekton status", "err", err, "status", status)
			}
			if terminal {
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
		logger.Debug("watch PipelineRun status; falling back to polling", "err", err)
		p.pollPipelineRun(ctx, ref, logger, last)
		return
	}
	defer w.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-w.ResultChan():
			if !ok {
				p.pollPipelineRun(ctx, ref, logger, last)
				return
			}
			obj, ok := ev.Object.(*unstructured.Unstructured)
			if !ok {
				continue
			}
			status, terminal, ok := mapTektonPipelineRunStatus(obj)
			if !ok || status == last {
				if terminal {
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
			if terminal {
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
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
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
			if !ok || status == last {
				if terminal {
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
			if terminal {
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
		return "", false, false
	}
	for _, raw := range conditions {
		cond, _ := raw.(map[string]interface{})
		if cond["type"] != "Succeeded" {
			continue
		}
		condStatus, _ := cond["status"].(string)
		reason, _ := cond["reason"].(string)
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
	if len(taskRuns) == 0 {
		return nil, ErrLogsNotFound
	}

	out := make(chan LogLine, 32)
	go func() {
		defer close(out)
		stepID := 0
		for _, tr := range taskRuns {
			taskName := tr.GetName()
			if taskName == "" {
				taskName = fmt.Sprintf("task %d", stepID)
			}
			if !sendLine(ctx, out, LogLine{
				Kind:       LogKindControl,
				Time:       time.Now(),
				Content:    taskName,
				StepId:     stepID,
				StepStatus: StepStatusStart,
			}) {
				return
			}

			p.streamTaskRunLogs(ctx, out, *ref, tr, stepID)

			if !sendLine(ctx, out, LogLine{
				Kind:       LogKindControl,
				Time:       time.Now(),
				Content:    taskName,
				StepId:     stepID,
				StepStatus: StepStatusEnd,
			}) {
				return
			}
			stepID++
		}
	}()
	return out, nil
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
		p.log.Debug("list pods for TaskRun", "err", err,
			"task_run", tr.GetName(), "pipeline_run", ref.PipelineRunName,
		)
		return
	}
	for _, pod := range pods {
		for _, c := range append(pod.Spec.InitContainers, pod.Spec.Containers...) {
			req := p.kube.CoreV1().Pods(ref.Namespace).GetLogs(pod.Name, &corev1.PodLogOptions{
				Container: c.Name,
			})
			rc, err := req.Stream(ctx)
			if err != nil {
				p.log.Debug("stream pod logs", "err", err,
					"pod", pod.Name, "container", c.Name,
				)
				continue
			}
			p.sendReaderLines(ctx, out, rc, stepID)
			_ = rc.Close()
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
