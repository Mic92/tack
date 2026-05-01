package main

// In-process event broker for the /events websocket fan-out.
//
// Lifecycle of an outbound event:
//
//	publisher (e.g. Buildkite webhook handler)
//	     │
//	     ▼
//	broker.Publish ─── store.InsertEvent ──▶ events table (rowid = cursor)
//	     │
//	     └── notify() ──▶ each subscriber's signal channel
//	                            │
//	                            ▼
//	                      /events handler wakes,
//	                      calls store.EventsAfter(cursor),
//	                      writes envelope JSON to its websocket.
//
// Subscribers don't receive events through the channel directly — only
// a "wake up" signal. They re-read from the store using the cursor they
// last delivered. This means:
//
//   - slow clients can't make us drop events (they just lag behind in
//     rowid space and catch up on their own pace),
//   - reconnecting clients can resume by passing ?cursor=N, hitting the
//     same EventsAfter path used for live deliveries,
//   - we never have to bound a per-subscriber buffer.
//
// This mirrors the upstream Tangled spindle's notifier+stream design
// (see tangled.org/core/spindle/stream.go), which is the source of
// truth for the wire format on /events.

import (
	"context"
	"encoding/json"
	"sync"
)

// eventsEnvelope is the wire shape we emit on /events frames. It must
// match the upstream Tangled spindle byte-for-byte so the appview's
// eventconsumer treats us as a drop-in source.
//
// Upstream defines this shape in two places that don't quite agree:
//
//   - The producer (tangled.org/core/spindle/stream.go, streamPipelines)
//     marshals an inline map[string]any with lowercase keys.
//   - The consumer (tangled.org/core/eventconsumer.Message) is exported
//     but its Rkey/Nsid fields are missing JSON tags, so reusing it for
//     marshalling here would emit "Rkey"/"Nsid" — accepted on read only
//     because Go's json package matches field names case-insensitively.
//
// Defining our own struct keeps the wire output identical to the
// upstream producer and lets every site that emits an event (the
// /events handler today, future re-publishers tomorrow) share one
// canonical type.
//
// Event is held as RawMessage so callers can splice a stored record
// body straight in without an unmarshal/remarshal round-trip.
type eventsEnvelope struct {
	Rkey    string          `json:"rkey"`
	Nsid    string          `json:"nsid"`
	Event   json.RawMessage `json:"event"`
	Created int64           `json:"created"`
}

// broker fans out event-table writes to connected /events subscribers.
// Construct with newBroker; safe for concurrent use.
type broker struct {
	st *store

	mu   sync.Mutex
	subs map[chan struct{}]struct{}
}

// newBroker returns a broker bound to st. The store is used both for
// durable writes in Publish and for cursor-based reads in EventsAfter
// from the /events handler.
func newBroker(st *store) *broker {
	return &broker{
		st:   st,
		subs: make(map[chan struct{}]struct{}),
	}
}

// Subscribe registers a new subscriber and returns its signal channel.
// The channel is buffered with a capacity of 1: notify() does a
// non-blocking send, so a pending notification simply coalesces with
// the next one rather than blocking the publisher. Subscribers must
// call Unsubscribe when done to free the slot.
func (b *broker) Subscribe() chan struct{} {
	ch := make(chan struct{}, 1)
	b.mu.Lock()
	b.subs[ch] = struct{}{}
	b.mu.Unlock()
	return ch
}

// Unsubscribe removes ch from the broker. Safe to call with a channel
// that was never subscribed (no-op) or to call more than once.
func (b *broker) Unsubscribe(ch chan struct{}) {
	b.mu.Lock()
	delete(b.subs, ch)
	b.mu.Unlock()
}

// Publish persists an event and wakes every subscriber. The returned
// int64 is the assigned cursor (rowid) for the new row, useful in tests
// and for any caller that wants to log "published as cursor=N".
//
// eventJSON is the record body — typically a marshalled
// tangled.PipelineStatus. The caller is responsible for choosing rkey
// (atproto record key) and nsid (collection NSID).
func (b *broker) Publish(ctx context.Context, rkey, nsid string, eventJSON []byte) (int64, error) {
	created, err := b.st.InsertEvent(ctx, rkey, nsid, eventJSON)
	if err != nil {
		return 0, err
	}
	b.notify()
	return created, nil
}

// notify sends a non-blocking signal to every subscriber. Held lock
// covers iteration only — the send itself is O(1) and never blocks
// because of the buffered channel + default case.
func (b *broker) notify() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for ch := range b.subs {
		select {
		case ch <- struct{}{}:
		default:
			// A previous signal hasn't been drained yet; coalesce —
			// the subscriber will catch up on the next read since it
			// queries by cursor, not by message count.
		}
	}
}
