package main

// Test fake for KnotConsumer.
//
// Lives in a non-_test.go file so it can be referenced from tests across
// any future test files (and, if we ever split tack into subpackages, can
// be promoted to an exported helper without moving code around).
//
// It does no I/O: AddKnot just records the knot it was handed so tests
// can assert on the side effect.

import (
	"context"
	"sync"
)

// fakeKnotConsumer is an in-memory KnotConsumer suitable for tests. The
// zero value is ready to use; concurrent calls to AddKnot are safe.
type fakeKnotConsumer struct {
	mu    sync.Mutex
	added []string
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
