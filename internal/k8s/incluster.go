package k8s

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	serviceAccountDir  = "/var/run/secrets/kubernetes.io/serviceaccount"
	serviceTokenPath   = serviceAccountDir + "/token"
	serviceAccountCA   = serviceAccountDir + "/ca.crt"
	podsResource       = "pods"
	podLogsSubresource = "log"
)

// InClusterClient talks to the Kubernetes API server from inside the cluster
// using the pod's mounted service-account credentials.
type InClusterClient struct {
	baseURL string
	token   string
	http    *http.Client
}

var _ Client = (*InClusterClient)(nil)

// NewInClusterClient loads the conventional in-cluster service-account files and
// builds an HTTPS client pinned to the cluster CA.
func NewInClusterClient() (*InClusterClient, error) {
	host := os.Getenv("KUBERNETES_SERVICE_HOST")
	port := os.Getenv("KUBERNETES_SERVICE_PORT_HTTPS")
	if port == "" {
		port = os.Getenv("KUBERNETES_SERVICE_PORT")
	}
	if host == "" || port == "" {
		return nil, fmt.Errorf(
			"kubernetes service env is incomplete: host=%q port=%q",
			host, port,
		)
	}

	token, err := os.ReadFile(serviceTokenPath)
	if err != nil {
		return nil, fmt.Errorf("read service-account token: %w", err)
	}
	caPEM, err := os.ReadFile(serviceAccountCA)
	if err != nil {
		return nil, fmt.Errorf("read service-account CA: %w", err)
	}

	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("parse service-account CA: no certificates found")
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{RootCAs: roots}

	return newClient(
		fmt.Sprintf("https://%s:%s", host, port),
		strings.TrimSpace(string(token)),
		&http.Client{Transport: transport},
	), nil
}

func newClient(baseURL, token string, httpClient *http.Client) *InClusterClient {
	return &InClusterClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		token:   token,
		http:    httpClient,
	}
}

func (c *InClusterClient) CreateObject(
	ctx context.Context,
	gvr GVR,
	namespace string,
	obj Object,
) (Object, error) {
	body, err := json.Marshal(obj)
	if err != nil {
		return nil, fmt.Errorf("marshal object: %w", err)
	}
	resp, err := c.do(ctx, http.MethodPost, resourcePath(gvr, namespace, ""),
		nil, bytes.NewReader(body), "application/json")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode created object: %w", err)
	}
	return Object(out), nil
}

func (c *InClusterClient) GetObject(
	ctx context.Context,
	gvr GVR,
	namespace string,
	name string,
) (Object, error) {
	resp, err := c.do(ctx, http.MethodGet, resourcePath(gvr, namespace, name),
		nil, nil, "")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode object: %w", err)
	}
	return Object(out), nil
}

func (c *InClusterClient) ListObjects(
	ctx context.Context,
	gvr GVR,
	namespace string,
	opts ListOptions,
) ([]Object, error) {
	resp, err := c.do(ctx, http.MethodGet, resourcePath(gvr, namespace, ""),
		listQuery(opts), nil, "")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var out struct {
		Items []map[string]any `json:"items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode object list: %w", err)
	}
	items := make([]Object, 0, len(out.Items))
	for _, item := range out.Items {
		items = append(items, Object(item))
	}
	return items, nil
}

func (c *InClusterClient) WatchObjects(
	ctx context.Context,
	gvr GVR,
	namespace string,
	opts ListOptions,
) (WatchInterface, error) {
	watchCtx, cancel := context.WithCancel(ctx)
	query := listQuery(opts)
	query.Set("watch", "1")

	resp, err := c.do(watchCtx, http.MethodGet,
		resourcePath(gvr, namespace, ""), query, nil, "")
	if err != nil {
		cancel()
		return nil, err
	}

	w := &httpWatch{
		ch:     make(chan WatchEvent),
		cancel: cancel,
	}
	go w.run(watchCtx, resp)
	return w, nil
}

func (c *InClusterClient) ListPods(
	ctx context.Context,
	namespace string,
	labelSelector string,
) ([]Pod, error) {
	query := url.Values{}
	if labelSelector != "" {
		query.Set("labelSelector", labelSelector)
	}
	resp, err := c.do(ctx, http.MethodGet, coreResourcePath(namespace, podsResource, "", ""),
		query, nil, "")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var out struct {
		Items []struct {
			Metadata struct {
				Name              string            `json:"name"`
				Namespace         string            `json:"namespace"`
				UID               string            `json:"uid"`
				Labels            map[string]string `json:"labels"`
				CreationTimestamp time.Time         `json:"creationTimestamp"`
			} `json:"metadata"`
			Spec struct {
				InitContainers []struct {
					Name string `json:"name"`
				} `json:"initContainers"`
				Containers []struct {
					Name string `json:"name"`
				} `json:"containers"`
			} `json:"spec"`
		} `json:"items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode pod list: %w", err)
	}

	pods := make([]Pod, 0, len(out.Items))
	for _, item := range out.Items {
		pod := Pod{
			Name:              item.Metadata.Name,
			Namespace:         item.Metadata.Namespace,
			UID:               item.Metadata.UID,
			Labels:            cloneLabels(item.Metadata.Labels),
			CreationTimestamp: item.Metadata.CreationTimestamp,
			InitContainers:    make([]Container, 0, len(item.Spec.InitContainers)),
			Containers:        make([]Container, 0, len(item.Spec.Containers)),
		}
		for _, container := range item.Spec.InitContainers {
			pod.InitContainers = append(pod.InitContainers, Container{Name: container.Name})
		}
		for _, container := range item.Spec.Containers {
			pod.Containers = append(pod.Containers, Container{Name: container.Name})
		}
		pods = append(pods, pod)
	}
	return pods, nil
}

func (c *InClusterClient) StreamPodLogs(
	ctx context.Context,
	namespace string,
	podName string,
	container string,
) (io.ReadCloser, error) {
	query := url.Values{}
	if container != "" {
		query.Set("container", container)
	}
	resp, err := c.do(ctx, http.MethodGet,
		coreResourcePath(namespace, podsResource, podName, podLogsSubresource),
		query, nil, "")
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (c *InClusterClient) do(
	ctx context.Context,
	method string,
	path string,
	query url.Values,
	body io.Reader,
	contentType string,
) (*http.Response, error) {
	uri := c.baseURL + path
	if encoded := query.Encode(); encoded != "" {
		uri += "?" + encoded
	}

	req, err := http.NewRequestWithContext(ctx, method, uri, body)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request %s %s: %w", method, path, err)
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return resp, nil
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	bodyText := strings.TrimSpace(string(raw))
	switch resp.StatusCode {
	case http.StatusNotFound:
		return nil, fmt.Errorf("%w: %s %s: %s", ErrNotFound, method, path, bodyText)
	case http.StatusConflict:
		return nil, fmt.Errorf("%w: %s %s: %s", ErrAlreadyExists, method, path, bodyText)
	default:
		return nil, fmt.Errorf("k8s api %s %s: status %d: %s",
			method, path, resp.StatusCode, bodyText,
		)
	}
}

func listQuery(opts ListOptions) url.Values {
	query := url.Values{}
	if opts.LabelSelector != "" {
		query.Set("labelSelector", opts.LabelSelector)
	}
	if opts.FieldSelector != "" {
		query.Set("fieldSelector", opts.FieldSelector)
	}
	return query
}

func resourcePath(gvr GVR, namespace, name string) string {
	prefix := "/apis/" + url.PathEscape(gvr.Group) + "/" + url.PathEscape(gvr.Version)
	if gvr.Group == "" {
		prefix = "/api/" + url.PathEscape(gvr.Version)
	}
	path := prefix
	if namespace != "" {
		path += "/namespaces/" + url.PathEscape(namespace)
	}
	path += "/" + url.PathEscape(gvr.Resource)
	if name != "" {
		path += "/" + url.PathEscape(name)
	}
	return path
}

func coreResourcePath(namespace, resource, name, subresource string) string {
	path := "/api/v1"
	if namespace != "" {
		path += "/namespaces/" + url.PathEscape(namespace)
	}
	path += "/" + url.PathEscape(resource)
	if name != "" {
		path += "/" + url.PathEscape(name)
	}
	if subresource != "" {
		path += "/" + url.PathEscape(subresource)
	}
	return path
}

func cloneLabels(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

type httpWatch struct {
	ch     chan WatchEvent
	cancel context.CancelFunc
	once   sync.Once
}

var _ WatchInterface = (*httpWatch)(nil)

func (w *httpWatch) ResultChan() <-chan WatchEvent {
	return w.ch
}

func (w *httpWatch) Stop() {
	w.close()
}

func (w *httpWatch) run(ctx context.Context, resp *http.Response) {
	defer resp.Body.Close()
	defer w.close()

	dec := json.NewDecoder(resp.Body)
	for {
		var event struct {
			Type   string         `json:"type"`
			Object map[string]any `json:"object"`
		}
		if err := dec.Decode(&event); err != nil {
			if err == io.EOF || ctx.Err() != nil {
				return
			}
			return
		}

		select {
		case w.ch <- WatchEvent{Type: event.Type, Object: Object(event.Object)}:
		case <-ctx.Done():
			return
		}
	}
}

func (w *httpWatch) close() {
	w.once.Do(func() {
		w.cancel()
		close(w.ch)
	})
}
