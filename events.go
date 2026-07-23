package main

import "context"

// broker is the persistence boundary shared by pipeline creators and provider
// status publishers. Current appviews query the retained rows through the
// sh.tangled.ci XRPC API rather than consuming an event WebSocket.
type broker struct {
	st *store
}

func newBroker(st *store) *broker {
	return &broker{st: st}
}

// Publish appends a spindle-owned pipeline or status record and returns its
// monotonic creation cursor.
func (b *broker) Publish(
	ctx context.Context,
	rkey, nsid string,
	eventJSON []byte,
) (int64, error) {
	return b.st.InsertEvent(ctx, rkey, nsid, eventJSON)
}
