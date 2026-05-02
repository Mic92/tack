package jetstream

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	jsmodels "github.com/bluesky-social/jetstream/pkg/models"
)

// Handler applies one commit event. Returning BadRecord(err) tells the
// Processor that the event is permanently unusable and that the cursor should
// still advance; any other error is treated as transient and leaves the cursor
// unchanged for a later retry.
type Handler interface {
	HandleJetstreamEvent(context.Context, *jsmodels.Event) error
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc func(context.Context, *jsmodels.Event) error

// HandleJetstreamEvent calls f(ctx, event).
func (f HandlerFunc) HandleJetstreamEvent(ctx context.Context, event *jsmodels.Event) error {
	return f(ctx, event)
}

// Processor handles per-event policy that is independent of websocket IO.
type Processor struct {
	// Collections is the set of record collection NSIDs to process locally. An
	// empty slice means "all collections".
	Collections []string

	// CursorStore persists progress after events are handled.
	CursorStore CursorStore

	// Handler applies commit events that pass the collection filter.
	Handler Handler

	// Logger receives apply errors and ignored-collection diagnostics. A nil
	// Logger uses slog.Default().
	Logger *slog.Logger
}

// HandleEvent applies a single jetstream event and advances the cursor when it
// is safe to do so.
func (p *Processor) HandleEvent(ctx context.Context, event *jsmodels.Event) error {
	if event == nil || event.Kind != jsmodels.EventKindCommit || event.Commit == nil {
		return nil
	}

	if err := p.validate(); err != nil {
		return err
	}

	logger := loggerOrDefault(p.Logger)
	wanted, err := p.wantsCollection(event.Commit.Collection)
	if err != nil {
		return err
	}
	if !wanted {
		logger.Debug("ignoring unexpected collection",
			"collection", event.Commit.Collection)
		return p.saveCursor(ctx, event.TimeUS)
	}

	applyErr := p.Handler.HandleJetstreamEvent(ctx, event)
	if applyErr != nil {
		logger.Error("apply commit",
			"err", applyErr,
			"did", event.Did,
			"collection", event.Commit.Collection,
			"op", event.Commit.Operation,
			"rkey", event.Commit.RKey,
			"transient", !IsBadRecord(applyErr),
		)

		if !IsBadRecord(applyErr) {
			return applyErr
		}
	}

	return p.saveCursor(ctx, event.TimeUS)
}

func (p *Processor) saveCursor(ctx context.Context, cursor int64) error {
	if err := p.CursorStore.SaveCursor(ctx, cursor); err != nil {
		return fmt.Errorf("save cursor: %w", err)
	}
	return nil
}

func (p *Processor) validate() error {
	if p.CursorStore == nil {
		return errors.New("cursor store is required")
	}
	if p.Handler == nil {
		return errors.New("handler is required")
	}
	for _, collection := range p.Collections {
		if collection == "" {
			return errors.New("collection must not be empty")
		}
	}
	return nil
}

func (p *Processor) wantsCollection(collection string) (bool, error) {
	if len(p.Collections) == 0 {
		return true, nil
	}
	for _, wanted := range p.Collections {
		if wanted == "" {
			return false, errors.New("collection must not be empty")
		}
		if wanted == collection {
			return true, nil
		}
	}
	return false, nil
}

// badRecordError marks a handler failure as caused by the record itself being
// permanently unusable, e.g. malformed JSON or an unrecoverable schema
// violation. Processors advance the cursor past these errors so one bad event
// cannot stall every later event on restart.
type badRecordError struct{ err error }

func (e *badRecordError) Error() string { return e.err.Error() }
func (e *badRecordError) Unwrap() error { return e.err }

// BadRecord wraps err so Processor recognizes it as a permanent,
// cursor-advancing failure. Do not use this for storage, network, or other
// transient infrastructure failures.
func BadRecord(err error) error {
	if err == nil {
		return nil
	}
	return &badRecordError{err: err}
}

// IsBadRecord reports whether err, or anything it wraps, came from BadRecord.
func IsBadRecord(err error) bool {
	var b *badRecordError
	return errors.As(err, &b)
}
