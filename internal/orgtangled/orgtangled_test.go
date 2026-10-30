package orgtangled

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestPipelineTriggerMarshalWritesBareNSID pins the Tangled v1.20 encoding of a pipeline's unions.
// The trigger's $type must be the bare NSID (org.tangled.event.push), not a legacy fragment name
// such as sh.tangled.ci.trigger#push, and the workflow source must carry
// org.tangled.ci.defs#fileSource. Callers never set LexiconTypeID, so this also proves the
// MarshalJSON methods fill it in. Decoding the same bytes then confirms both unions round-trip.
func TestPipelineTriggerMarshalWritesBareNSID(t *testing.T) {
	pipeline := Pipeline{
		ID:     "3m5x7k2abcde2",
		Repo:   "did:plc:repo",
		Commit: "2222222222222222222222222222222222222222",
		Trigger: &Trigger{Push: &PushEvent{
			Repo: "did:plc:repo", Ref: "refs/heads/main",
			OldSHA: "1111111111111111111111111111111111111111",
			NewSHA: "2222222222222222222222222222222222222222",
		}},
		Workflows: []*WorkflowRun{{
			ID: "ci.yml", Status: "pending",
			Definition: &Workflow{
				ID: "ci.yml",
				Source: &WorkflowSource{File: &FileSource{
					Repo: "did:plc:repo", Commit: "2222222222222222222222222222222222222222",
					Path: ".tangled/workflows/ci.yml",
				}},
				Triggers: []string{NSIDPushEvent},
			},
		}},
	}
	encoded, err := json.Marshal(pipeline)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`"$type":"org.tangled.event.push"`,
		`"$type":"org.tangled.ci.defs#fileSource"`,
	} {
		if !strings.Contains(string(encoded), want) {
			t.Fatalf("encoded pipeline %s lacks %s", encoded, want)
		}
	}

	var decoded Pipeline
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Trigger == nil || decoded.Trigger.Push == nil ||
		decoded.Trigger.Push.Ref != "refs/heads/main" {
		t.Fatalf("decoded trigger = %+v", decoded.Trigger)
	}
	source := decoded.Workflows[0].Definition.Source
	if source == nil || source.File == nil || source.File.Path != ".tangled/workflows/ci.yml" {
		t.Fatalf("decoded source = %+v", source)
	}
}

// TestNotifyEventUnknownTypeDecodesEmpty pins that an event with an unrecognized or missing $type
// decodes without error and leaves every member of [Event] nil. This matches upstream's generated
// union. It matters because the notifyEvent handler relies on it: a decode error would surface as a
// generic "invalid request body", while an empty union reaches the handler's switch and gets a
// specific InvalidRequest message. The test uses a pull request event because [Event] leaves that
// member out on purpose: v1.20 starts pull request pipelines from records, not knot notifications.
func TestNotifyEventUnknownTypeDecodesEmpty(t *testing.T) {
	for _, body := range []string{
		`{"repo":"did:plc:repo","event":{"$type":"org.tangled.event.pullRequest"}}`,
		`{"repo":"did:plc:repo","event":{"ref":"refs/heads/main"}}`,
	} {
		var input NotifyEventInput
		if err := json.Unmarshal([]byte(body), &input); err != nil {
			t.Fatalf("%s: %v", body, err)
		}
		if input.Event == nil || input.Event.Push != nil || input.Event.Manual != nil {
			t.Fatalf("%s decoded to %+v", body, input.Event)
		}
	}
}

// TestNotifyEventDecodesKnotPush decodes a push in the exact body shape knot2 builds, including the
// optional actor and pushOptions fields. Tack rejects a push with no actor and reads push options
// for CI skip requests, so silently dropping either field would change how pushes are handled.
func TestNotifyEventDecodesKnotPush(t *testing.T) {
	body := `{"repo":"did:plc:repo","event":{"$type":"org.tangled.event.push","repo":"did:plc:repo",` +
		`"ref":"refs/heads/main","oldSha":"0000000000000000000000000000000000000000",` +
		`"newSha":"2222222222222222222222222222222222222222","actor":"did:plc:alice",` +
		`"pushOptions":["skip-ci"]}}`
	var input NotifyEventInput
	if err := json.Unmarshal([]byte(body), &input); err != nil {
		t.Fatal(err)
	}
	push := input.Event.Push
	if push == nil || push.Actor == nil || *push.Actor != "did:plc:alice" ||
		len(push.PushOptions) != 1 || push.PushOptions[0] != "skip-ci" {
		t.Fatalf("push = %+v", push)
	}
}

// TestNotifyEventOutputOmitsEmptyCI pins the response body Tack sends when an event starts nothing:
// a bare {} with no ci key, which is what the lexicon and knot2 expect for "nothing scheduled".
// The lexicon marks ci optional but not nullable, so "ci":null would be an invalid response even
// though knot2's decoder happens to accept it.
func TestNotifyEventOutputOmitsEmptyCI(t *testing.T) {
	encoded, err := json.Marshal(NotifyEventOutput{})
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != "{}" {
		t.Fatalf("empty output = %s; want {}", encoded)
	}
}
