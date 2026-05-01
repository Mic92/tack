package main

// Tests for providerRouter. We use a tiny in-test stub Provider —
// stubProvider — that records every Spawn call and serves canned
// Logs responses, so the tests stay focused on routing behaviour
// (which provider got called for which workflow YAML, and how
// ErrLogsNotFound is fanned out) without dragging in either the
// fake or Buildkite providers' end-to-end machinery.

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"

	"tangled.org/core/api/tangled"
)

// stubProvider is a minimal Provider for routing tests. spawnCalls
// captures the workflow names handed to Spawn (single-element slices
// per the router's contract) so a test can assert which provider got
// which workflow. logsErr / logsCh govern what Logs returns; the
// default (zero value) is ErrLogsNotFound + nil channel, which makes
// fan-out tests easy to express by overriding only the provider that
// should "claim" the request.
type stubProvider struct {
	mu         sync.Mutex
	spawnCalls []string

	logsErr error
	logsCh  chan LogLine
}

var _ Provider = (*stubProvider)(nil)

func (s *stubProvider) Spawn(
	_ context.Context,
	_ string,
	_ string,
	_ *tangled.Pipeline_TriggerMetadata,
	workflows []*tangled.Pipeline_Workflow,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, wf := range workflows {
		if wf == nil {
			continue
		}
		s.spawnCalls = append(s.spawnCalls, wf.Name)
	}
}

func (s *stubProvider) Logs(
	_ context.Context,
	_ string,
	_ string,
	_ string,
) (<-chan LogLine, error) {
	if s.logsErr != nil {
		return nil, s.logsErr
	}
	if s.logsCh != nil {
		return s.logsCh, nil
	}
	return nil, ErrLogsNotFound
}

// names returns a defensive copy of spawnCalls so the test can read
// it without racing the router's per-workflow loop. The router calls
// Spawn synchronously in the test process, but a copy is the safer
// pattern if that ever changes.
func (s *stubProvider) names() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.spawnCalls))
	copy(out, s.spawnCalls)
	return out
}

// newRouterTest wires a router with a fixed pair of stubs ("a", "b")
// so tests can focus on the YAML → provider mapping. The stubs are
// returned alongside so each case can inspect what it received.
func newRouterTest() (*providerRouter, *stubProvider, *stubProvider) {
	a := &stubProvider{}
	b := &stubProvider{}
	r := newProviderRouter(slog.Default(), map[string]Provider{
		"a": a,
		"b": b,
	})
	return r, a, b
}

// TestProviderRouterSpawnRoutesByYAMLKey exercises the basic happy
// path: each workflow's `tack:` block names exactly one provider key,
// and the router hands that workflow to the matching provider only.
func TestProviderRouterSpawnRoutesByYAMLKey(t *testing.T) {
	r, a, b := newRouterTest()

	r.Spawn(context.Background(), "knot", "rkey", nil,
		[]*tangled.Pipeline_Workflow{
			{Name: "wf-a.yml", Raw: "tack:\n  a: {}\n"},
			{Name: "wf-b.yml", Raw: "tack:\n  b: {}\n"},
		},
	)

	if got, want := a.names(), []string{"wf-a.yml"}; !equalStrings(got, want) {
		t.Fatalf("provider a got %v; want %v", got, want)
	}
	if got, want := b.names(), []string{"wf-b.yml"}; !equalStrings(got, want) {
		t.Fatalf("provider b got %v; want %v", got, want)
	}
}

// TestProviderRouterSpawnFirstYAMLKeyWins pins tie-breaking when a
// workflow lists multiple provider keys: the YAML's document-order
// child of `tack:` wins, regardless of the Go map's iteration order.
func TestProviderRouterSpawnFirstYAMLKeyWins(t *testing.T) {
	r, a, b := newRouterTest()

	// `b` is listed first under `tack:` so it should claim the
	// workflow even though both keys are registered.
	r.Spawn(context.Background(), "knot", "rkey", nil,
		[]*tangled.Pipeline_Workflow{
			{Name: "both.yml", Raw: "tack:\n  b: {}\n  a: {}\n"},
		},
	)

	if got := a.names(); len(got) != 0 {
		t.Fatalf("provider a should not have been called; got %v", got)
	}
	if got, want := b.names(), []string{"both.yml"}; !equalStrings(got, want) {
		t.Fatalf("provider b got %v; want %v", got, want)
	}
}

// TestProviderRouterSpawnSkipsUnroutable confirms that workflows
// whose YAML has no matching provider key are skipped (logged but
// not dispatched) and don't poison the rest of the batch.
func TestProviderRouterSpawnSkipsUnroutable(t *testing.T) {
	r, a, b := newRouterTest()

	r.Spawn(context.Background(), "knot", "rkey", nil,
		[]*tangled.Pipeline_Workflow{
			// No `tack:` key at all.
			{Name: "bare.yml", Raw: "steps: []\n"},
			// `tack:` present but with an unknown sub-key.
			{Name: "unknown.yml", Raw: "tack:\n  nope: {}\n"},
			// Empty body — also unroutable.
			{Name: "empty.yml", Raw: ""},
			// And one good one to prove the loop kept going.
			{Name: "good.yml", Raw: "tack:\n  a: {}\n"},
		},
	)

	if got, want := a.names(), []string{"good.yml"}; !equalStrings(got, want) {
		t.Fatalf("provider a got %v; want %v", got, want)
	}
	if got := b.names(); len(got) != 0 {
		t.Fatalf("provider b should not have been called; got %v", got)
	}
}

// TestProviderRouterLogsFanOut verifies that Logs walks the
// providers and returns the channel from the first one that doesn't
// say ErrLogsNotFound. We seed exactly one provider with a real
// channel; map iteration order is unspecified but only one provider
// can possibly answer, so the test is deterministic.
func TestProviderRouterLogsFanOut(t *testing.T) {
	r, _, b := newRouterTest()

	want := make(chan LogLine)
	b.logsCh = want

	got, err := r.Logs(context.Background(), "k", "p", "w")
	if err != nil {
		t.Fatalf("Logs: %v", err)
	}
	if got != (<-chan LogLine)(want) {
		t.Fatalf("got channel %v; want %v", got, want)
	}
}

// TestProviderRouterLogsAllNotFound makes sure that when no provider
// claims the tuple, the router surfaces ErrLogsNotFound itself —
// this is what the HTTP handler maps to a 404.
func TestProviderRouterLogsAllNotFound(t *testing.T) {
	r, _, _ := newRouterTest()

	ch, err := r.Logs(context.Background(), "k", "p", "w")
	if !errors.Is(err, ErrLogsNotFound) {
		t.Fatalf("err = %v; want ErrLogsNotFound", err)
	}
	if ch != nil {
		t.Fatalf("channel should be nil on not-found")
	}
}

// TestProviderRouterLogsBackendError confirms that a non-NotFound
// error from a provider is returned verbatim instead of being
// masked by the fan-out — backend failures must reach the HTTP
// caller as 5xx, not be silently retried elsewhere.
func TestProviderRouterLogsBackendError(t *testing.T) {
	a := &stubProvider{logsErr: errors.New("boom")}
	r := newProviderRouter(slog.Default(), map[string]Provider{"a": a})

	ch, err := r.Logs(context.Background(), "k", "p", "w")
	if err == nil || err.Error() != "boom" {
		t.Fatalf("err = %v; want boom", err)
	}
	if ch != nil {
		t.Fatalf("channel should be nil on backend error")
	}
}

// equalStrings is a small helper to compare ordered string slices —
// the router preserves workflow order within Spawn, so the tests
// assert against ordered slices rather than sets.
func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
