package k8s

// Package k8s exposes the tiny slice of the Kubernetes API tack's Tekton
// provider actually needs. The goal is to keep Tekton support in-process
// without dragging the full client-go stack into the module graph.

import (
	"context"
	"errors"
	"io"
	"time"
)

// ErrNotFound mirrors the API server's 404 in a way callers can branch on with
// errors.Is instead of inspecting status codes or response bodies.
var ErrNotFound = errors.New("k8s: not found")

// ErrAlreadyExists mirrors the API server's 409 for create calls whose name is
// already present.
var ErrAlreadyExists = errors.New("k8s: already exists")

// Client is the minimal Kubernetes surface tack needs today: CRUD-ish access to
// arbitrary JSON objects, plus pod listing and log streaming.
type Client interface {
	CreateObject(
		ctx context.Context,
		gvr GVR,
		namespace string,
		obj Object,
	) (Object, error)

	GetObject(
		ctx context.Context,
		gvr GVR,
		namespace string,
		name string,
	) (Object, error)

	ListObjects(
		ctx context.Context,
		gvr GVR,
		namespace string,
		opts ListOptions,
	) ([]Object, error)

	WatchObjects(
		ctx context.Context,
		gvr GVR,
		namespace string,
		opts ListOptions,
	) (WatchInterface, error)

	ListPods(
		ctx context.Context,
		namespace string,
		labelSelector string,
	) ([]Pod, error)

	StreamPodLogs(
		ctx context.Context,
		namespace string,
		podName string,
		container string,
		opts LogOptions,
	) (io.ReadCloser, error)
}

// LogOptions controls how StreamPodLogs reads container logs.
type LogOptions struct {
	// Follow tells the API server to keep the connection open and stream
	// new log bytes as the container writes them, only EOFing when the
	// container terminates (or ctx is cancelled). Without Follow, the
	// returned reader is a snapshot: it yields the bytes that exist at
	// request time and then EOFs immediately, even if the container is
	// still running. Live-streaming callers MUST set Follow=true; using
	// the snapshot mode for a still-running container makes the caller
	// look at a frozen view and treat the container as if it had finished.
	Follow bool
}

// GVR identifies a Kubernetes resource by the path segments the API server
// routes on.
type GVR struct {
	Group    string
	Version  string
	Resource string
}

// ListOptions is the subset of query options tack currently uses.
type ListOptions struct {
	LabelSelector string
	FieldSelector string
}

// WatchInterface matches the shape provider code already expects from a watch:
// a receive-only event channel and an explicit stop hook.
type WatchInterface interface {
	ResultChan() <-chan WatchEvent
	Stop()
}

// WatchEvent mirrors the API server's streaming watch envelope.
type WatchEvent struct {
	Type   string
	Object Object
}

// Object is an arbitrary Kubernetes resource decoded from JSON.
type Object map[string]any

// DeepCopy returns a recursive copy so callers can safely mutate the returned
// object without aliasing shared test fixtures or cached state.
func (o Object) DeepCopy() Object {
	if o == nil {
		return nil
	}
	return Object(deepCopyMap(map[string]any(o)))
}

// GetName returns metadata.name, or the empty string when absent.
func (o Object) GetName() string {
	v, _ := NestedString(map[string]any(o), "metadata", "name")
	return v
}

// GetNamespace returns metadata.namespace, or the empty string when absent.
func (o Object) GetNamespace() string {
	v, _ := NestedString(map[string]any(o), "metadata", "namespace")
	return v
}

// GetAPIVersion returns apiVersion, or the empty string when absent.
func (o Object) GetAPIVersion() string {
	v, _ := NestedString(map[string]any(o), "apiVersion")
	return v
}

// GetKind returns kind, or the empty string when absent.
func (o Object) GetKind() string {
	v, _ := NestedString(map[string]any(o), "kind")
	return v
}

// GetUID returns metadata.uid, or the empty string when absent.
func (o Object) GetUID() string {
	v, _ := NestedString(map[string]any(o), "metadata", "uid")
	return v
}

// SetUID writes metadata.uid, creating metadata as needed.
func (o Object) SetUID(uid string) {
	meta := ensureNestedMap(o, "metadata")
	meta["uid"] = uid
}

// SetCreationTimestamp writes metadata.creationTimestamp in UTC RFC3339 form.
func (o Object) SetCreationTimestamp(ts time.Time) {
	meta := ensureNestedMap(o, "metadata")
	meta["creationTimestamp"] = ts.UTC().Format(time.RFC3339Nano)
}

// GetCreationTimestamp returns metadata.creationTimestamp, or the zero time when
// the field is missing or malformed.
func (o Object) GetCreationTimestamp() time.Time {
	raw, ok := NestedString(map[string]any(o), "metadata", "creationTimestamp")
	if !ok || raw == "" {
		return time.Time{}
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if ts, err := time.Parse(layout, raw); err == nil {
			return ts
		}
	}
	return time.Time{}
}

// GetLabels returns metadata.labels as a string map. Non-string label values are
// ignored because Kubernetes labels are strings on the wire.
func (o Object) GetLabels() map[string]string {
	raw, ok := NestedMap(map[string]any(o), "metadata", "labels")
	if !ok {
		return nil
	}
	out := make(map[string]string, len(raw))
	for key, value := range raw {
		if s, ok := value.(string); ok {
			out[key] = s
		}
	}
	return out
}

// GetAnnotations returns metadata.annotations as a string map.
func (o Object) GetAnnotations() map[string]string {
	raw, ok := NestedMap(map[string]any(o), "metadata", "annotations")
	if !ok {
		return nil
	}
	out := make(map[string]string, len(raw))
	for key, value := range raw {
		if s, ok := value.(string); ok {
			out[key] = s
		}
	}
	return out
}

// NestedString returns a nested string field from the object.
func (o Object) NestedString(fields ...string) (string, bool) {
	return NestedString(map[string]any(o), fields...)
}

// NestedSlice returns a nested slice field from the object.
func (o Object) NestedSlice(fields ...string) ([]any, bool) {
	return NestedSlice(map[string]any(o), fields...)
}

// NestedMap returns a nested map field from the object.
func (o Object) NestedMap(fields ...string) (map[string]any, bool) {
	return NestedMap(map[string]any(o), fields...)
}

// NestedString returns a nested string field from obj.
func NestedString(obj map[string]any, fields ...string) (string, bool) {
	value, ok := nestedValue(obj, fields...)
	if !ok {
		return "", false
	}
	s, ok := value.(string)
	return s, ok
}

// NestedSlice returns a nested slice field from obj.
func NestedSlice(obj map[string]any, fields ...string) ([]any, bool) {
	value, ok := nestedValue(obj, fields...)
	if !ok {
		return nil, false
	}
	slice, ok := value.([]any)
	return slice, ok
}

// NestedMap returns a nested map field from obj.
func NestedMap(obj map[string]any, fields ...string) (map[string]any, bool) {
	value, ok := nestedValue(obj, fields...)
	if !ok {
		return nil, false
	}
	m, ok := value.(map[string]any)
	return m, ok
}

// Container is the small slice of a pod container spec tack cares about for
// log streaming.
type Container struct {
	Name string
}

// Pod is the reduced pod shape tack uses to order containers and request logs.
type Pod struct {
	Name              string
	Namespace         string
	UID               string
	Labels            map[string]string
	CreationTimestamp time.Time
	InitContainers    []Container
	Containers        []Container
}

func nestedValue(obj map[string]any, fields ...string) (any, bool) {
	current := any(obj)
	for _, field := range fields {
		m, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = m[field]
		if !ok {
			return nil, false
		}
	}
	return current, true
}

func ensureNestedMap(obj map[string]any, fields ...string) map[string]any {
	current := obj
	for _, field := range fields {
		next, ok := current[field].(map[string]any)
		if !ok {
			next = map[string]any{}
			current[field] = next
		}
		current = next
	}
	return current
}

func deepCopyMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = deepCopyValue(value)
	}
	return out
}

func deepCopySlice(in []any) []any {
	out := make([]any, len(in))
	for i, value := range in {
		out[i] = deepCopyValue(value)
	}
	return out
}

func deepCopyValue(value any) any {
	switch v := value.(type) {
	case map[string]any:
		return deepCopyMap(v)
	case []any:
		return deepCopySlice(v)
	default:
		return v
	}
}
