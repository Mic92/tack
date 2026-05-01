package main

// Test fake for KnotConsumer.
//
// Lives in a non-_test.go file so it can be referenced from tests across
// any future test files (and, if we ever split tack into subpackages, can
// be promoted to an exported helper without moving code around).
//
// It does no I/O: AddKnot and RemoveKnot just record the knot they were
// handed so tests can assert on the side effect.

import (
	"context"
	"sync"
)

// fakeKnotConsumer is an in-memory KnotConsumer suitable for tests. The
// zero value is ready to use; concurrent calls are safe.
type fakeKnotConsumer struct {
	mu      sync.Mutex
	added   []string
	removed []string
}

// Compile-time interface conformance check — keeps the fake honest if
// the KnotConsumer surface ever grows a new method.
var _ KnotConsumer = (*fakeKnotConsumer)(nil)

// AddKnot records the knot for later inspection via Added().
func (f *fakeKnotConsumer) AddKnot(_ context.Context, knot string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.added = append(f.added, knot)
}

// RemoveKnot records the knot for later inspection via Removed().
func (f *fakeKnotConsumer) RemoveKnot(_ context.Context, knot string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.removed = append(f.removed, knot)
}

// Added returns a copy of the knots passed to AddKnot, in call order.
// A copy is returned so callers can't accidentally mutate the fake's
// internal slice while comparing.
func (f *fakeKnotConsumer) Added() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.added))
	copy(out, f.added)
	return out
}

// Removed returns a copy of the knots passed to RemoveKnot, in call
// order. See Added() for the rationale behind copying.
func (f *fakeKnotConsumer) Removed() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.removed))
	copy(out, f.removed)
	return out
}
