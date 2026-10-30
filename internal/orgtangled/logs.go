package orgtangled

import (
	"fmt"
	"io"

	"github.com/bluesky-social/indigo/events"
	cbg "github.com/whyrusleeping/cbor-gen"
)

// This file encodes the frames of the org.tangled.ci.subscribePipelineLogs subscription by
// hand. The generated sh.tangled encoder in the pinned Tangled v1.16 module has no outcome
// field, and the v1.20 module that has one cannot be imported (see the package comment).

// LogControl is a #control message on org.tangled.ci.subscribePipelineLogs. It marks a step
// starting or ending. Nil pointer fields are optional in the lexicon and are left out of the
// encoded frame.
type LogControl struct {
	// Time is when the event happened, as an RFC 3339 datetime.
	Time string

	// Workflow is the name of the workflow the step belongs to.
	Workflow string

	// Step is the step ID. Clients group log lines into steps by this value.
	Step int64

	// Content is the step name.
	Content string

	// Command is the command the step runs, if known.
	Command *string

	// Status is "start" or "end".
	Status *string

	// Kind is "system" for steps the spindle adds itself and "user" for steps from the
	// workflow file.
	Kind *string

	// Outcome is "success" or "failed". It is set only on end frames for steps that can
	// fail. The sh.tangled control type in the pinned Tangled v1.16 module has no such field,
	// and the Tangled web app uses it to mark which step failed.
	Outcome *string
}

// LogData is a #data message: one line of step output.
type LogData struct {
	// Time is when the line was produced, as an RFC 3339 datetime.
	Time string

	// Workflow is the name of the workflow the line belongs to.
	Workflow string

	// Step is the ID of the step that produced the line.
	Step int64

	// Content is the line text, including its trailing newline if it had one.
	Content string

	// Stream is "stdout" or "stderr".
	Stream string
}

// LogEvent is one message of the log subscription. The lexicon defines the message as a
// union of #control and #data, so callers set exactly one field. If both are set, Serialize
// encodes Control.
type LogEvent struct {
	Control *LogControl
	Data    *LogData
}

// Serialize writes the event as one atproto event-stream frame: a DAG-CBOR header naming
// the message type, followed by the DAG-CBOR message body. It returns an error if neither
// field is set or the writer fails.
//
// The frames use the same framing and field names as the sh.tangled.ci.subscribePipelineLogs
// frames Tack builds from the pinned Tangled v1.16 types, so a decoder for those still reads
// them. The only difference is the outcome entry on control frames. Without it the Tangled web
// app cannot tell which step of a failed workflow failed, and it marks the last step as the
// failure.
func (e *LogEvent) Serialize(w io.Writer) error {
	header := events.EventHeader{Op: events.EvtKindMessage}
	var fields []cborField
	switch {
	case e.Control != nil:
		header.MsgType = "#control"
		c := e.Control
		fields = []cborField{
			{key: "kind", text: c.Kind},
			{key: "step", integer: &c.Step},
			{key: "time", text: &c.Time},
			{key: "status", text: c.Status},
			{key: "command", text: c.Command},
			{key: "content", text: &c.Content},
			{key: "outcome", text: c.Outcome},
			{key: "workflow", text: &c.Workflow},
		}
	case e.Data != nil:
		header.MsgType = "#data"
		d := e.Data
		fields = []cborField{
			{key: "step", integer: &d.Step},
			{key: "time", text: &d.Time},
			{key: "stream", text: &d.Stream},
			{key: "content", text: &d.Content},
			{key: "workflow", text: &d.Workflow},
		}
	default:
		return fmt.Errorf("log event has no message set")
	}

	cw := cbg.NewCborWriter(w)
	if err := header.MarshalCBOR(cw); err != nil {
		return fmt.Errorf("write log frame header: %w", err)
	}
	return writeCBORMap(cw, fields)
}

// cborField is one entry of a CBOR map with a text key. At most one of text and integer is
// set. An entry with neither set is left out of the map, which is how optional lexicon
// fields are omitted.
type cborField struct {
	key     string
	text    *string
	integer *int64
}

// writeCBORMap encodes fields as a DAG-CBOR map, skipping entries that have no value. It
// does not sort the keys. Callers must list them in the order DAG-CBOR requires (shorter
// keys first, then bytewise), because decoders that check canonical form reject any other
// order.
func writeCBORMap(cw *cbg.CborWriter, fields []cborField) error {
	// The map header carries the entry count, so count the present entries first.
	count := 0
	for _, field := range fields {
		if field.text != nil || field.integer != nil {
			count++
		}
	}
	if err := cw.WriteMajorTypeHeader(cbg.MajMap, uint64(count)); err != nil {
		return err
	}
	for _, field := range fields {
		switch {
		case field.text != nil:
			if err := writeCBORText(cw, field.key); err != nil {
				return err
			}
			if err := writeCBORText(cw, *field.text); err != nil {
				return err
			}
		case field.integer != nil:
			if err := writeCBORText(cw, field.key); err != nil {
				return err
			}
			// CBOR stores a negative integer n as major type 1 with the value -1-n.
			value := *field.integer
			var err error
			if value >= 0 {
				err = cw.WriteMajorTypeHeader(cbg.MajUnsignedInt, uint64(value))
			} else {
				err = cw.WriteMajorTypeHeader(cbg.MajNegativeInt, uint64(-value-1))
			}
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// writeCBORText writes value as a CBOR text string: a length header followed by the UTF-8 bytes.
func writeCBORText(cw *cbg.CborWriter, value string) error {
	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len(value))); err != nil {
		return err
	}
	_, err := cw.WriteString(value)
	return err
}
