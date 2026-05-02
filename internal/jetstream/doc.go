// Package jetstream provides a reusable Bluesky Jetstream consumer.
//
// The package owns the mechanics that are common to firehose consumers:
// configuring the upstream websocket client, filtering by record collection,
// reconnecting after dropped reads, rewinding time-based cursors on reconnect,
// persisting cursor progress, and distinguishing permanent bad-record failures
// from transient handler failures.
//
// Callers provide a CursorStore and Handler, either through Config for a full
// Consumer or directly on Processor when they already have events from another
// source. The handler receives commit events for the configured collections and
// should apply domain-specific mutations. If a record is permanently unusable,
// the handler should return BadRecord(err) so the processor can advance the
// cursor and avoid replaying the same broken event forever. Any other error
// leaves the cursor unchanged so a later delivery can retry the event.
package jetstream
