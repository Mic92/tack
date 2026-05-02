package jetstream

import (
	"context"
	"sync"
)

// CursorStore persists the jetstream cursor. Implementations commonly back this
// with a database row so process restarts can resume from the last applied
// event.
type CursorStore interface {
	LoadCursor(context.Context) (*int64, error)
	SaveCursor(context.Context, int64) error
}

// MemoryCursorStore is an in-memory CursorStore implementation. It is useful
// for tests and for consumers that intentionally do not need cursor persistence
// across process restarts.
//
// The zero value is ready to use and starts with no cursor, which makes the
// consumer start from "now" until SaveCursor is called.
type MemoryCursorStore struct {
	mu     sync.Mutex
	cursor *int64
}

var _ CursorStore = (*MemoryCursorStore)(nil)

// NewMemoryCursorStore returns a MemoryCursorStore initialized with cursor. A
// nil cursor means no cursor has been saved yet.
func NewMemoryCursorStore(cursor *int64) *MemoryCursorStore {
	s := &MemoryCursorStore{}
	if cursor != nil {
		v := *cursor
		s.cursor = &v
	}
	return s
}

// LoadCursor returns the last cursor saved in memory, or nil if none has been
// saved. The returned pointer is a copy so callers cannot mutate store state
// without going through SaveCursor.
func (s *MemoryCursorStore) LoadCursor(context.Context) (*int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cursor == nil {
		return nil, nil
	}
	cursor := *s.cursor
	return &cursor, nil
}

// SaveCursor stores cursor in memory.
func (s *MemoryCursorStore) SaveCursor(_ context.Context, cursor int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.cursor = &cursor
	return nil
}
