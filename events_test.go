package main

// Tests for the /events fan-out: the store's event log methods, the
// in-process broker, and the eventsHandler's wire output. The handler
// test boots a real httptest server and a real gorilla websocket client
// so we exercise the upgrade + envelope codec end to end.

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// TestEventsLogRoundtrip covers InsertEvent / EventsAfter together
// because they're a tightly-coupled pair: the cursor returned by Insert
// is the same value EventsAfter must accept to skip past that row.
func TestEventsLogRoundtrip(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// Empty log → empty slice (not nil), so callers can range freely.
	got, err := s.EventsAfter(ctx, 0)
	if err != nil {
		t.Fatalf("EventsAfter empty: %v", err)
	}
	if got == nil || len(got) != 0 {
		t.Fatalf("empty log: got %v, want empty non-nil slice", got)
	}

	c1, err := s.InsertEvent(ctx, "rk1", "sh.tangled.pipeline.status", []byte(`{"a":1}`))
	if err != nil {
		t.Fatalf("insert 1: %v", err)
	}
	c2, err := s.InsertEvent(ctx, "rk2", "sh.tangled.pipeline.status", []byte(`{"a":2}`))
	if err != nil {
		t.Fatalf("insert 2: %v", err)
	}
	if c2 <= c1 {
		t.Fatalf("cursors must be monotonically increasing: c1=%d c2=%d", c1, c2)
	}

	// cursor=0 returns everything; cursor=c1 skips the first row.
	got, err = s.EventsAfter(ctx, 0)
	if err != nil {
		t.Fatalf("EventsAfter 0: %v", err)
	}
	if len(got) != 2 || got[0].Created != c1 || got[1].Created != c2 {
		t.Fatalf("EventsAfter(0) = %+v, want both rows in order", got)
	}
	if got[0].Rkey != "rk1" || got[1].Rkey != "rk2" {
		t.Fatalf("rkey order wrong: %q %q", got[0].Rkey, got[1].Rkey)
	}
	// json.RawMessage round-trip — matters because the /events handler
	// splices these straight into the envelope.
	if string(got[0].EventJSON) != `{"a":1}` {
		t.Fatalf("event_json round-trip = %q", got[0].EventJSON)
	}

	got, err = s.EventsAfter(ctx, c1)
	if err != nil {
		t.Fatalf("EventsAfter c1: %v", err)
	}
	if len(got) != 1 || got[0].Created != c2 {
		t.Fatalf("EventsAfter(c1) = %+v, want only row c2", got)
	}
}

// TestBrokerPublishWakesSubscribers asserts the core invariant: a
// Publish causes Subscribe()'d channels to fire (at least once) and the
// row is durably visible via EventsAfter so the subscriber can drain
// it. Two subscribers cover the multi-fanout case.
func TestBrokerPublishWakesSubscribers(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	br := newBroker(s)

	a := br.Subscribe()
	b := br.Subscribe()
	defer br.Unsubscribe(a)
	defer br.Unsubscribe(b)

	cursor, err := br.Publish(ctx, "rk", "sh.tangled.pipeline.status", []byte(`{}`))
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if cursor <= 0 {
		t.Fatalf("publish cursor = %d, want > 0", cursor)
	}

	// Both subscribers must receive the wake-up promptly. Use a
	// generous timeout so flaky CI doesn't false-alarm.
	for name, ch := range map[string]chan struct{}{"a": a, "b": b} {
		select {
		case <-ch:
		case <-time.After(time.Second):
			t.Fatalf("subscriber %s did not receive signal", name)
		}
	}

	rows, err := s.EventsAfter(ctx, 0)
	if err != nil {
		t.Fatalf("EventsAfter: %v", err)
	}
	if len(rows) != 1 || rows[0].Created != cursor {
		t.Fatalf("after publish, EventsAfter(0) = %+v, want one row with cursor=%d", rows, cursor)
	}
}

// TestBrokerCoalescesPendingSignal ensures Publish never blocks on a
// subscriber that hasn't drained its channel: a second Publish while
// the first signal is still pending must coalesce, not deadlock. This
// is the property that lets slow clients lag without backpressuring
// the rest of the system.
func TestBrokerCoalescesPendingSignal(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	br := newBroker(s)

	ch := br.Subscribe()
	defer br.Unsubscribe(ch)

	// First publish lands a pending signal in ch (cap=1).
	if _, err := br.Publish(ctx, "rk1", "n", []byte(`{}`)); err != nil {
		t.Fatalf("publish 1: %v", err)
	}
	// Second publish must succeed immediately — without a default
	// branch in notify(), this would block forever waiting on the
	// unread ch.
	done := make(chan error, 1)
	go func() {
		_, err := br.Publish(ctx, "rk2", "n", []byte(`{}`))
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("publish 2: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Publish blocked on un-drained subscriber")
	}

	// One drain is enough — cursor-based catch-up will pick up *both*
	// rows in a single EventsAfter call.
	<-ch
	rows, err := s.EventsAfter(ctx, 0)
	if err != nil {
		t.Fatalf("EventsAfter: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows after 2 publishes, got %d", len(rows))
	}
}

// TestBrokerUnsubscribeStopsDelivery confirms an unsubscribed channel
// no longer receives wake-ups. Without this we'd leak signals to dead
// websockets and (worse) hold their channels in the broker map.
func TestBrokerUnsubscribeStopsDelivery(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	br := newBroker(s)

	ch := br.Subscribe()
	br.Unsubscribe(ch)

	if _, err := br.Publish(ctx, "rk", "n", []byte(`{}`)); err != nil {
		t.Fatalf("publish: %v", err)
	}
	select {
	case <-ch:
		t.Fatal("unsubscribed channel still received signal")
	case <-time.After(50 * time.Millisecond):
	}
}

// TestEventsHandlerStreamsLiveAndBackfill exercises the full HTTP
// surface: open a websocket, observe a backfill of pre-existing rows,
// publish a new row, observe it arrive live, then reconnect with a
// cursor and observe only events strictly after that cursor.
func TestEventsHandlerStreamsLiveAndBackfill(t *testing.T) {
	s := newTestStore(t)
	br := newBroker(s)
	ctx := context.Background()

	// Seed two rows so the first connection has something to backfill.
	pre1, err := br.Publish(ctx, "rk-pre1", "sh.tangled.pipeline.status", []byte(`{"i":1}`))
	if err != nil {
		t.Fatalf("seed 1: %v", err)
	}
	pre2, err := br.Publish(ctx, "rk-pre2", "sh.tangled.pipeline.status", []byte(`{"i":2}`))
	if err != nil {
		t.Fatalf("seed 2: %v", err)
	}

	// Boot the handler behind an httptest server. Using a discarding
	// logger keeps test output quiet.
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := httptest.NewServer(eventsHandler(logger, br))
	t.Cleanup(srv.Close)

	// First connection: no cursor, expect both seeded rows plus a
	// freshly published live row.
	c1 := dialEvents(t, srv.URL, 0)
	defer c1.Close()

	got1 := readEnvelope(t, c1)
	if got1.Created != pre1 || got1.Rkey != "rk-pre1" {
		t.Fatalf("first frame = %+v, want pre1 (cursor=%d)", got1, pre1)
	}
	got2 := readEnvelope(t, c1)
	if got2.Created != pre2 || got2.Rkey != "rk-pre2" {
		t.Fatalf("second frame = %+v, want pre2 (cursor=%d)", got2, pre2)
	}
	// Verify the wire envelope's `event` field round-trips as the raw
	// record body, not a re-encoded blob.
	if strings.TrimSpace(string(got2.Event)) != `{"i":2}` {
		t.Fatalf("event body = %q, want %q", got2.Event, `{"i":2}`)
	}

	// Live publish — handler should wake on broker signal and emit it.
	live, err := br.Publish(ctx, "rk-live", "sh.tangled.pipeline.status", []byte(`{"i":3}`))
	if err != nil {
		t.Fatalf("live publish: %v", err)
	}
	got3 := readEnvelope(t, c1)
	if got3.Created != live || got3.Rkey != "rk-live" {
		t.Fatalf("live frame = %+v, want rk-live (cursor=%d)", got3, live)
	}

	// Second connection with cursor=pre2: must skip pre1 and pre2,
	// receive only the live row. No timeout is set; if the handler
	// over-delivers we'll fail in readEnvelope's assert below.
	c2 := dialEvents(t, srv.URL, pre2)
	defer c2.Close()
	got := readEnvelope(t, c2)
	if got.Created != live || got.Rkey != "rk-live" {
		t.Fatalf("cursor-resume frame = %+v, want rk-live (cursor=%d)", got, live)
	}
}

// TestEventsHandlerBadCursorStartsFromZero confirms a malformed cursor
// query parameter doesn't 4xx the upgrade — the handler logs and falls
// back to the full backfill, matching the upstream spindle's behaviour.
func TestEventsHandlerBadCursorStartsFromZero(t *testing.T) {
	s := newTestStore(t)
	br := newBroker(s)
	ctx := context.Background()

	if _, err := br.Publish(ctx, "rk", "sh.tangled.pipeline.status", []byte(`{}`)); err != nil {
		t.Fatalf("publish: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := httptest.NewServer(eventsHandler(logger, br))
	t.Cleanup(srv.Close)

	// Build the URL by hand so we can inject a non-numeric cursor.
	u, _ := url.Parse(srv.URL)
	u.Scheme = "ws"
	q := u.Query()
	q.Set("cursor", "not-a-number")
	u.RawQuery = q.Encode()

	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, msg, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var env eventsEnvelope
	if err := json.Unmarshal(msg, &env); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if env.Rkey != "rk" {
		t.Fatalf("expected backfill of seeded row, got envelope %+v", env)
	}
}

// dialEvents opens a websocket against an httptest server (which
// returns http://) by rewriting the scheme to ws://. cursor=0 omits the
// query parameter entirely so we exercise the "no cursor" code path.
func dialEvents(t *testing.T, base string, cursor int64) *websocket.Conn {
	t.Helper()
	u, err := url.Parse(base)
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	u.Scheme = "ws"
	if cursor != 0 {
		q := u.Query()
		q.Set("cursor", strconv.FormatInt(cursor, 10))
		u.RawQuery = q.Encode()
	}
	conn, _, err := websocket.DefaultDialer.Dial(u.String(), http.Header{})
	if err != nil {
		t.Fatalf("dial %s: %v", u, err)
	}
	return conn
}

// readEnvelope reads one TextMessage frame, decodes it as the spindle
// wire envelope, and returns it. It enforces a read deadline so a
// handler bug that fails to flush events doesn't hang the test forever.
func readEnvelope(t *testing.T, conn *websocket.Conn) eventsEnvelope {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	mt, msg, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if mt != websocket.TextMessage {
		t.Fatalf("frame type = %d, want TextMessage", mt)
	}
	var env eventsEnvelope
	if err := json.Unmarshal(msg, &env); err != nil {
		t.Fatalf("decode envelope: %v (raw: %s)", err, msg)
	}
	return env
}

