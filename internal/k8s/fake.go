package k8s

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

// FakeClient is a tiny in-memory implementation used by Tekton tests. It keeps
// the test surface aligned with the real client package without reintroducing
// external Kubernetes fakes.
type FakeClient struct {
	mu          sync.Mutex
	nextUID     int64
	nextWatchID int
	objects     map[objectKey]Object
	watchers    map[int]*watchRegistration
	pods        map[string]map[string]Pod
	podLogs     map[podLogKey]string
}

var _ Client = (*FakeClient)(nil)

type objectKey struct {
	gvr       GVR
	namespace string
	name      string
}

type podLogKey struct {
	namespace string
	podName   string
	container string
}

type watchRegistration struct {
	gvr       GVR
	namespace string
	opts      ListOptions
	watch     *fakeWatch
}

// NewFakeClient returns an empty fake client.
func NewFakeClient() *FakeClient {
	return &FakeClient{
		objects:  map[objectKey]Object{},
		watchers: map[int]*watchRegistration{},
		pods:     map[string]map[string]Pod{},
		podLogs:  map[podLogKey]string{},
	}
}

// SeedObject preloads an object without emitting watch events.
func (c *FakeClient) SeedObject(gvr GVR, namespace string, obj Object) {
	c.mu.Lock()
	defer c.mu.Unlock()
	stored := c.prepareObjectLocked(namespace, obj.DeepCopy())
	c.objects[objectKey{gvr: gvr, namespace: namespace, name: stored.GetName()}] = stored
}

// UpdateObject overwrites an existing object and emits a MODIFIED watch event.
func (c *FakeClient) UpdateObject(gvr GVR, namespace string, obj Object) {
	stored, watchers := c.storeObject(gvr, namespace, obj.DeepCopy())
	c.broadcast(watchers, WatchEvent{Type: "MODIFIED", Object: stored})
}

// SeedPod preloads a pod for later ListPods or StreamPodLogs calls.
func (c *FakeClient) SeedPod(namespace string, pod Pod) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if namespace == "" {
		namespace = pod.Namespace
	}
	if namespace == "" {
		namespace = "default"
	}
	pod.Namespace = namespace
	if pod.CreationTimestamp.IsZero() {
		pod.CreationTimestamp = time.Now().UTC()
	}
	if pod.Labels != nil {
		pod.Labels = cloneLabels(pod.Labels)
	}
	if _, ok := c.pods[namespace]; !ok {
		c.pods[namespace] = map[string]Pod{}
	}
	c.pods[namespace][pod.Name] = clonePod(pod)
}

// SetPodLog stores the exact bytes StreamPodLogs should later return.
func (c *FakeClient) SetPodLog(namespace, podName, container, content string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.podLogs[podLogKey{
		namespace: namespace,
		podName:   podName,
		container: container,
	}] = content
}

func (c *FakeClient) CreateObject(
	ctx context.Context,
	gvr GVR,
	namespace string,
	obj Object,
) (Object, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	stored, watchers, err := c.createObject(gvr, namespace, obj.DeepCopy())
	if err != nil {
		return nil, err
	}
	c.broadcast(watchers, WatchEvent{Type: "ADDED", Object: stored})
	return stored.DeepCopy(), nil
}

func (c *FakeClient) GetObject(
	ctx context.Context,
	gvr GVR,
	namespace string,
	name string,
) (Object, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	obj, ok := c.objects[objectKey{gvr: gvr, namespace: namespace, name: name}]
	if !ok {
		return nil, fmt.Errorf("%w: %s/%s", ErrNotFound, namespace, name)
	}
	return obj.DeepCopy(), nil
}

func (c *FakeClient) ListObjects(
	ctx context.Context,
	gvr GVR,
	namespace string,
	opts ListOptions,
) ([]Object, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]Object, 0)
	for key, obj := range c.objects {
		if key.gvr != gvr || key.namespace != namespace {
			continue
		}
		if !matchesListOptions(obj, opts) {
			continue
		}
		out = append(out, obj.DeepCopy())
	}
	return out, nil
}

func (c *FakeClient) WatchObjects(
	ctx context.Context,
	gvr GVR,
	namespace string,
	opts ListOptions,
) (WatchInterface, error) {
	c.mu.Lock()
	id := c.nextWatchID
	c.nextWatchID++
	w := &fakeWatch{ch: make(chan WatchEvent, 64)}
	w.stop = func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		delete(c.watchers, id)
	}
	c.watchers[id] = &watchRegistration{
		gvr:       gvr,
		namespace: namespace,
		opts:      opts,
		watch:     w,
	}
	c.mu.Unlock()

	go func() {
		<-ctx.Done()
		w.Stop()
	}()
	return w, nil
}

func (c *FakeClient) ListPods(
	ctx context.Context,
	namespace string,
	labelSelector string,
) ([]Pod, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	entries := c.pods[namespace]
	out := make([]Pod, 0, len(entries))
	for _, pod := range entries {
		if !matchesSelector(pod.Labels, labelSelector) {
			continue
		}
		out = append(out, clonePod(pod))
	}
	return out, nil
}

func (c *FakeClient) StreamPodLogs(
	ctx context.Context,
	namespace string,
	podName string,
	container string,
) (io.ReadCloser, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	raw, ok := c.podLogs[podLogKey{
		namespace: namespace,
		podName:   podName,
		container: container,
	}]
	if !ok {
		return nil, fmt.Errorf(
			"%w: pod log %s/%s[%s]",
			ErrNotFound, namespace, podName, container,
		)
	}
	return io.NopCloser(strings.NewReader(raw)), nil
}

func (c *FakeClient) createObject(
	gvr GVR,
	namespace string,
	obj Object,
) (Object, []*fakeWatch, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	stored := c.prepareObjectLocked(namespace, obj)
	key := objectKey{gvr: gvr, namespace: namespace, name: stored.GetName()}
	if _, ok := c.objects[key]; ok {
		return nil, nil, fmt.Errorf("%w: %s/%s", ErrAlreadyExists, namespace, key.name)
	}
	c.objects[key] = stored
	return stored.DeepCopy(), c.matchingWatchersLocked(gvr, namespace, stored), nil
}

func (c *FakeClient) storeObject(
	gvr GVR,
	namespace string,
	obj Object,
) (Object, []*fakeWatch) {
	c.mu.Lock()
	defer c.mu.Unlock()
	stored := c.prepareObjectLocked(namespace, obj)
	key := objectKey{gvr: gvr, namespace: namespace, name: stored.GetName()}
	c.objects[key] = stored
	return stored.DeepCopy(), c.matchingWatchersLocked(gvr, namespace, stored)
}

func (c *FakeClient) prepareObjectLocked(namespace string, obj Object) Object {
	if obj.GetName() == "" {
		panic("k8s.FakeClient: object missing metadata.name")
	}
	meta := ensureNestedMap(obj, "metadata")
	if namespace != "" && meta["namespace"] == nil {
		meta["namespace"] = namespace
	}
	if obj.GetUID() == "" {
		c.nextUID++
		obj.SetUID(fmt.Sprintf("fake-%d", c.nextUID))
	}
	if obj.GetCreationTimestamp().IsZero() {
		obj.SetCreationTimestamp(time.Now().UTC())
	}
	return obj
}

func (c *FakeClient) matchingWatchersLocked(
	gvr GVR,
	namespace string,
	obj Object,
) []*fakeWatch {
	watchers := make([]*fakeWatch, 0)
	for _, registration := range c.watchers {
		if registration.gvr != gvr || registration.namespace != namespace {
			continue
		}
		if !matchesListOptions(obj, registration.opts) {
			continue
		}
		watchers = append(watchers, registration.watch)
	}
	return watchers
}

func (c *FakeClient) broadcast(watchers []*fakeWatch, event WatchEvent) {
	for _, watcher := range watchers {
		watcher.send(event)
	}
}

type fakeWatch struct {
	ch   chan WatchEvent
	stop func()
	once sync.Once
}

var _ WatchInterface = (*fakeWatch)(nil)

func (w *fakeWatch) ResultChan() <-chan WatchEvent {
	return w.ch
}

func (w *fakeWatch) Stop() {
	w.once.Do(func() {
		if w.stop != nil {
			w.stop()
		}
		close(w.ch)
	})
}

func (w *fakeWatch) send(event WatchEvent) {
	defer func() {
		_ = recover()
	}()
	w.ch <- WatchEvent{Type: event.Type, Object: event.Object.DeepCopy()}
}

func matchesListOptions(obj Object, opts ListOptions) bool {
	if !matchesSelector(obj.GetLabels(), opts.LabelSelector) {
		return false
	}
	if !matchesFieldSelector(obj, opts.FieldSelector) {
		return false
	}
	return true
}

func matchesFieldSelector(obj Object, selector string) bool {
	selector = strings.TrimSpace(selector)
	if selector == "" {
		return true
	}
	parts := strings.Split(selector, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		key, value, ok := strings.Cut(part, "=")
		if !ok {
			return false
		}
		if strings.TrimSpace(key) != "metadata.name" {
			return false
		}
		if obj.GetName() != strings.TrimSpace(value) {
			return false
		}
	}
	return true
}

func matchesSelector(labels map[string]string, selector string) bool {
	selector = strings.TrimSpace(selector)
	if selector == "" {
		return true
	}
	parts := strings.Split(selector, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		key, value, ok := strings.Cut(part, "=")
		if !ok {
			return false
		}
		if labels[strings.TrimSpace(key)] != strings.TrimSpace(value) {
			return false
		}
	}
	return true
}

func clonePod(pod Pod) Pod {
	out := pod
	out.Labels = cloneLabels(pod.Labels)
	out.InitContainers = append([]Container(nil), pod.InitContainers...)
	out.Containers = append([]Container(nil), pod.Containers...)
	return out
}
