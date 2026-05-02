package jetstream

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	jsmodels "github.com/bluesky-social/jetstream/pkg/models"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func requireMemoryCursor(t *testing.T, store CursorStore, want *int64) {
	t.Helper()

	got, err := store.LoadCursor(context.Background())
	if err != nil {
		t.Fatalf("load cursor: %v", err)
	}
	if want == nil {
		if got != nil {
			t.Fatalf("cursor = %v, want nil", *got)
		}
		return
	}
	if got == nil || *got != *want {
		t.Fatalf("cursor = %v, want %d", got, *want)
	}
}

func testCommit(timeUS int64, collection string) *jsmodels.Event {
	return &jsmodels.Event{
		Did:    "did:plc:test",
		TimeUS: timeUS,
		Kind:   jsmodels.EventKindCommit,
		Commit: &jsmodels.Commit{
			Operation:  "create",
			Collection: collection,
			RKey:       "rk",
		},
	}
}

func TestProcessorCommitAdvancesCursor(t *testing.T) {
	store := NewMemoryCursorStore(nil)
	called := false

	processor := &Processor{
		Collections: []string{"example.collection"},
		CursorStore: store,
		Handler: HandlerFunc(func(ctx context.Context, event *jsmodels.Event) error {
			called = true
			return nil
		}),
		Logger: testLogger(),
	}

	if err := processor.HandleEvent(context.Background(), testCommit(123, "example.collection")); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if !called {
		t.Fatalf("handler was not called")
	}
	want := int64(123)
	requireMemoryCursor(t, store, &want)
}

func TestProcessorIgnoresNonCommit(t *testing.T) {
	store := NewMemoryCursorStore(nil)
	processor := &Processor{
		CursorStore: store,
		Handler: HandlerFunc(func(ctx context.Context, event *jsmodels.Event) error {
			t.Fatalf("handler should not be called")
			return nil
		}),
		Logger: testLogger(),
	}

	event := &jsmodels.Event{
		Did:    "did:plc:test",
		TimeUS: 123,
		Kind:   jsmodels.EventKindAccount,
	}
	if err := processor.HandleEvent(context.Background(), event); err != nil {
		t.Fatalf("handle: %v", err)
	}
	requireMemoryCursor(t, store, nil)
}

func TestProcessorUnexpectedCollectionAdvancesCursor(t *testing.T) {
	store := NewMemoryCursorStore(nil)
	processor := &Processor{
		Collections: []string{"wanted.collection"},
		CursorStore: store,
		Handler: HandlerFunc(func(ctx context.Context, event *jsmodels.Event) error {
			t.Fatalf("handler should not be called")
			return nil
		}),
		Logger: testLogger(),
	}

	if err := processor.HandleEvent(context.Background(), testCommit(456, "other.collection")); err != nil {
		t.Fatalf("handle: %v", err)
	}
	want := int64(456)
	requireMemoryCursor(t, store, &want)
}

func TestProcessorBadRecordAdvancesCursor(t *testing.T) {
	store := NewMemoryCursorStore(nil)
	processor := &Processor{
		Collections: []string{"example.collection"},
		CursorStore: store,
		Handler: HandlerFunc(func(ctx context.Context, event *jsmodels.Event) error {
			return BadRecord(errors.New("decode failed"))
		}),
		Logger: testLogger(),
	}

	if err := processor.HandleEvent(context.Background(), testCommit(789, "example.collection")); err != nil {
		t.Fatalf("handle: %v", err)
	}
	want := int64(789)
	requireMemoryCursor(t, store, &want)
}

func TestProcessorTransientErrorDoesNotAdvanceCursor(t *testing.T) {
	cursor := int64(100)
	store := NewMemoryCursorStore(&cursor)
	transientErr := errors.New("database busy")
	processor := &Processor{
		Collections: []string{"example.collection"},
		CursorStore: store,
		Handler: HandlerFunc(func(ctx context.Context, event *jsmodels.Event) error {
			return transientErr
		}),
		Logger: testLogger(),
	}

	err := processor.HandleEvent(context.Background(), testCommit(200, "example.collection"))
	if !errors.Is(err, transientErr) {
		t.Fatalf("handle error = %v, want %v", err, transientErr)
	}
	requireMemoryCursor(t, store, &cursor)
}
