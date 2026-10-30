package orgtangled

import (
	"bytes"
	"testing"

	"tangled.org/core/api/tangled"
)

// TestLogEventDecodesAsLegacyFrame pins that [LogEvent.Serialize] writes frames the generated legacy
// sh.tangled.ci.subscribePipelineLogs decoder still reads, for both the #control and #data branches.
// Serialize hand-writes the CBOR instead of using generated code, so this test guards the header,
// key names, and value types against drift from what existing clients already decode. It does not
// check key order: the generated cbor-gen decoder matches map keys by name and accepts any order.
// It also checks that the current-only outcome entry is present in control frames, because that
// entry is the reason Serialize exists.
func TestLogEventDecodesAsLegacyFrame(t *testing.T) {
	status, kind, outcome := "end", "user", "failed"
	for _, event := range []*LogEvent{
		{Control: &LogControl{
			Time: "2026-10-30T10:00:00Z", Workflow: "ci.yml", Step: 3, Content: "test",
			Status: &status, Kind: &kind, Outcome: &outcome,
		}},
		{Data: &LogData{
			Time: "2026-10-30T10:00:00Z", Workflow: "ci.yml", Step: 3, Content: "ok\n", Stream: "stderr",
		}},
	} {
		var encoded bytes.Buffer
		if err := event.Serialize(&encoded); err != nil {
			t.Fatal(err)
		}
		raw := encoded.Bytes()
		var decoded tangled.CiSubscribePipelineLogs_Event
		if err := decoded.Deserialize(bytes.NewReader(raw)); err != nil {
			t.Fatalf("legacy decoder rejected frame: %v", err)
		}
		if event.Control != nil {
			c := decoded.Control
			if c == nil || c.Workflow != "ci.yml" || c.Step != 3 || c.Content != "test" ||
				c.Status == nil || *c.Status != "end" || c.Kind == nil || *c.Kind != "user" {
				t.Fatalf("control = %+v", c)
			}
			// The legacy type has no outcome field, so the decoder skips it and the decoded value
			// cannot show it. Look for the raw entry instead: a CBOR text string header for
			// length 7 (0x67) and "outcome", then a text string header for length 6 (0x66) and
			// "failed".
			want := append(append([]byte{0x67}, "outcome"...), append([]byte{0x66}, "failed"...)...)
			if !bytes.Contains(raw, want) {
				t.Fatal("control frame lacks the outcome entry")
			}
		} else {
			d := decoded.Data
			if d == nil || d.Workflow != "ci.yml" || d.Step != 3 || d.Content != "ok\n" || d.Stream != "stderr" {
				t.Fatalf("data = %+v", d)
			}
		}
	}
}
