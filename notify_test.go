package main

// This file tests org.tangled.event.notifyEvent, the XRPC procedure that carries both pushes
// reported by knot2 and manual runs requested through the Tangled web app (the current
// replacement for the legacy sh.tangled.ci.triggerPipeline). The push cases mirror the upstream
// spindle's spindle/xrpc/org_tangled_event_notify_event_test.go. Status codes are asserted
// exactly because knot2 acts on the status class: 2xx means delivered, a 4xx other than 408 or
// 429 drops the event for good, and anything else is retried while every later event from that
// knot to Tack waits behind it.

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/golang-jwt/jwt/v5"
	"tangled.org/core/api/tangled"

	"go.mitchellh.com/tack/internal/orgtangled"
)

// decodeNotifyOutput parses the body of a 200 notifyEvent response and fails the test if it is
// not valid JSON.
func decodeNotifyOutput(t *testing.T, body []byte) orgtangled.NotifyEventOutput {
	t.Helper()
	var output orgtangled.NotifyEventOutput
	if err := json.Unmarshal(body, &output); err != nil {
		t.Fatalf("decode notify output %q: %v", body, err)
	}
	return output
}

// TestNotifyEventPushCreatesPipeline covers the success path end to end. A push signed by the
// hosting knot must create exactly one pipeline, hand its workflow to the provider, and return
// the pipeline record key, which knot2 reads as a scheduled run. It also checks that the
// stored trigger metadata carries the push and repository details that workflows and the
// Tangled web app read back later.
func TestNotifyEventPushCreatesPipeline(t *testing.T) {
	f := newOrgFixture(t)
	response := f.post(orgtangled.NSIDNotifyEvent, f.knotToken(),
		pushEvent(testRepoDID, "refs/heads/main", testOldSHA, testNewSHA))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d; body %s", response.Code, response.Body)
	}
	output := decodeNotifyOutput(t, response.Body.Bytes())
	if output.CI == nil || len(output.CI.Pipelines) != 1 {
		t.Fatalf("output = %s; want one pipeline", response.Body)
	}
	if got, want := f.provider.names(), []string{"ci.yml"}; !equalStrings(got, want) {
		t.Fatalf("Spawn workflows = %v; want %v", got, want)
	}

	raw, _, err := f.st.GetRawPipeline(context.Background(), output.CI.Pipelines[0])
	if err != nil {
		t.Fatal(err)
	}
	trigger := raw.TriggerMetadata
	if trigger.Kind != "push" || trigger.Push.Ref != "refs/heads/main" ||
		trigger.Push.NewSha != testNewSHA || trigger.Repo.DefaultBranch != "main" ||
		pointerValue(trigger.Repo.RepoDid) != testRepoDID || trigger.Repo.Knot != testKnotHost {
		t.Fatalf("trigger = %+v push=%+v repo=%+v", trigger, trigger.Push, trigger.Repo)
	}
}

// TestNotifyEventRedeliveryIsIdempotent models knot2 redelivering a push after it lost the
// first answer. The redelivery carries a new token (with a fresh jti, the JWT's unique token
// ID), so the replay guard admits it, and Tack must recognize the push by its content. It must
// answer with the original pipeline instead of starting a second provider build, which would
// cost real CI time.
func TestNotifyEventRedeliveryIsIdempotent(t *testing.T) {
	f := newOrgFixture(t)
	body := pushEvent(testRepoDID, "refs/heads/main", testOldSHA, testNewSHA)
	first := f.post(orgtangled.NSIDNotifyEvent, f.knotToken(), body)
	second := f.post(orgtangled.NSIDNotifyEvent, f.knotToken(), body)
	if first.Code != http.StatusOK || second.Code != http.StatusOK {
		t.Fatalf("statuses = %d, %d", first.Code, second.Code)
	}
	a, b := decodeNotifyOutput(t, first.Body.Bytes()), decodeNotifyOutput(t, second.Body.Bytes())
	if a.CI == nil || b.CI == nil || a.CI.Pipelines[0] != b.CI.Pipelines[0] {
		t.Fatalf("outputs = %s, %s; want the same pipeline", first.Body, second.Body)
	}
	if got := f.provider.names(); len(got) != 1 {
		t.Fatalf("redelivery spawned %v; want one workflow total", got)
	}

	// Deduplication keys on the old and new SHA together, so moving the ref back to the old
	// commit is a new push and must build again.
	reverse := f.post(orgtangled.NSIDNotifyEvent, f.knotToken(),
		pushEvent(testRepoDID, "refs/heads/main", testNewSHA, testOldSHA))
	if out := decodeNotifyOutput(t, reverse.Body.Bytes()); out.CI == nil ||
		out.CI.Pipelines[0] == a.CI.Pipelines[0] {
		t.Fatalf("reverse push output = %s; want a new pipeline", reverse.Body)
	}
}

// TestNotifyEventAcknowledgesPushesThatRunNothing pins the cases that answer 200 {} and start
// nothing: repositories Tack does not serve, and pushes that should not build (ref deletions,
// refs that are neither branches nor tags, and CI skip push options). A 5xx would make knot2
// retry for days and hold up its queue behind the event. A 4xx would report a valid request as
// invalid. Both "not ours" and "nothing to do" are therefore plain successes, which is also how
// the upstream spindle answers.
func TestNotifyEventAcknowledgesPushesThatRunNothing(t *testing.T) {
	for _, test := range []struct {
		name  string
		setup func(*orgFixture)
		body  orgtangled.NotifyEventInput
	}{
		{
			name: "manifest names another spindle",
			setup: func(f *orgFixture) {
				f.repos[testRepoDID].Spindle = "did:web:other-spindle.example"
			},
			body: pushEvent(testRepoDID, "refs/heads/main", testOldSHA, testNewSHA),
		},
		{
			name: "owner is not a spindle member",
			setup: func(f *orgFixture) {
				f.repos[testRepoDID].OwnerDID = "did:plc:stranger"
			},
			body: pushEvent(testRepoDID, "refs/heads/main", testOldSHA, testNewSHA),
		},
		{
			name:  "owner PDS does not declare the repo",
			setup: func(f *orgFixture) { f.ownerDeclares = false },
			body:  pushEvent(testRepoDID, "refs/heads/main", testOldSHA, testNewSHA),
		},
		{
			name: "ref deletion",
			body: pushEvent(testRepoDID, "refs/heads/main", testOldSHA, testZeroSHA),
		},
		{
			name: "non-branch, non-tag ref",
			body: pushEvent(testRepoDID, "refs/cobs/issue", testOldSHA, testNewSHA),
		},
		{
			name: "skip-ci push option",
			body: pushEvent(testRepoDID, "refs/heads/main", testOldSHA, testNewSHA, "skip-ci"),
		},
		{
			name: "ci-skip push option",
			body: pushEvent(testRepoDID, "refs/heads/main", testOldSHA, testNewSHA, "ci-skip"),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newOrgFixture(t)
			if test.setup != nil {
				test.setup(f)
			}
			response := f.post(orgtangled.NSIDNotifyEvent, f.knotToken(), test.body)
			if response.Code != http.StatusOK || strings.TrimSpace(response.Body.String()) != "{}" {
				t.Fatalf("response = %d %s; want 200 {}", response.Code, response.Body)
			}
			if got := f.provider.names(); len(got) != 0 {
				t.Fatalf("spawned %v; want nothing", got)
			}
		})
	}
}

// TestNotifyEventNoMatchingWorkflowsIsEmpty pins that a served push whose workflows all filter
// it out answers 200 {}. The push was delivered and handled, so knot2 must not retry it.
func TestNotifyEventNoMatchingWorkflowsIsEmpty(t *testing.T) {
	f := newOrgFixture(t)
	f.service.load = func(
		context.Context, *RepoRef, string, *tangled.Pipeline_TriggerMetadata, []string,
	) ([]*tangled.Pipeline_Workflow, error) {
		return nil, nil
	}
	response := f.post(orgtangled.NSIDNotifyEvent, f.knotToken(),
		pushEvent(testRepoDID, "refs/heads/main", testOldSHA, testNewSHA))
	if response.Code != http.StatusOK || strings.TrimSpace(response.Body.String()) != "{}" {
		t.Fatalf("response = %d %s; want 200 {}", response.Code, response.Body)
	}
}

// TestNotifyEventRejectsMalformedPushes covers request bodies that no retry can fix. Each must
// answer 4xx so knot2 drops the event instead of retrying it at the head of its queue. Envelope
// and actor problems are 400. A SHA that is not a full object ID passes those checks and is
// refused later with 422 PushRefused, the status Tack reserves for pushes that can never build.
func TestNotifyEventRejectsMalformedPushes(t *testing.T) {
	noActor := pushEvent(testRepoDID, "refs/heads/main", testOldSHA, testNewSHA)
	noActor.Event.Push.Actor = nil
	badActor := pushEvent(testRepoDID, "refs/heads/main", testOldSHA, testNewSHA)
	notDID := "alice"
	badActor.Event.Push.Actor = &notDID
	otherRepo := pushEvent(testRepoDID, "refs/heads/main", testOldSHA, testNewSHA)
	otherRepo.Event.Push.Repo = "did:plc:other"

	for _, test := range []struct {
		name      string
		body      any
		status    int
		errorName string
	}{
		{"malformed JSON", []byte(`{"repo":`), http.StatusBadRequest, "InvalidRequest"},
		{"repo is not a DID", []byte(`{"repo":"nope","event":{}}`), http.StatusBadRequest, "InvalidRequest"},
		{"pull request event", []byte(`{"repo":"did:plc:repo","event":{"$type":"org.tangled.event.pullRequest"}}`),
			http.StatusBadRequest, "InvalidRequest"},
		{"untyped event", []byte(`{"repo":"did:plc:repo","event":{"ref":"refs/heads/main"}}`),
			http.StatusBadRequest, "InvalidRequest"},
		{"event for another repo", otherRepo, http.StatusBadRequest, "InvalidRequest"},
		{"missing actor", noActor, http.StatusBadRequest, "InvalidRequest"},
		{"actor is not a DID", badActor, http.StatusBadRequest, "InvalidRequest"},
		{"SHA is not an object ID",
			pushEvent(testRepoDID, "refs/heads/main", testOldSHA, "main"),
			http.StatusUnprocessableEntity, "PushRefused"},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newOrgFixture(t)
			response := f.post(orgtangled.NSIDNotifyEvent, f.knotToken(), test.body)
			if response.Code != test.status || decodeXRPCError(t, response) != test.errorName {
				t.Fatalf("response = %d %s; want %d %s", response.Code, response.Body, test.status, test.errorName)
			}
			if got := f.provider.names(); len(got) != 0 {
				t.Fatalf("spawned %v; want nothing", got)
			}
		})
	}
}

// TestNotifyEventRejectsBadServiceAuth walks every way the service auth on a push can fail.
// Service auth is the signed JWT that proves which atproto service is calling. Bad or
// misdirected credentials, callers other than the repository's hosting knot, and tokens the
// replay guard cannot track all answer 403, which makes knot2 drop the event. Failures to
// resolve the token issuer or the repository's knot answer 503 instead, so a resolver outage
// delays the push rather than losing it.
func TestNotifyEventRejectsBadServiceAuth(t *testing.T) {
	body := pushEvent(testRepoDID, "refs/heads/main", testOldSHA, testNewSHA)
	for _, test := range []struct {
		name      string
		token     func(*orgFixture) string
		setup     func(*orgFixture)
		status    int
		errorName string
	}{
		{
			name:   "missing token",
			token:  func(*orgFixture) string { return "" },
			status: http.StatusForbidden, errorName: "AuthRequired",
		},
		{
			name:   "garbage token",
			token:  func(*orgFixture) string { return "not.a.jwt" },
			status: http.StatusForbidden, errorName: "Auth",
		},
		{
			name: "wrong audience",
			token: func(f *orgFixture) string {
				return signClaims(t, f.knotKey, jwt.MapClaims{
					"iss": testKnotDID, "aud": "did:web:other-spindle.example",
					"lxm": orgtangled.NSIDNotifyEvent, "jti": "aud",
					"iat": time.Now().Unix(), "exp": time.Now().Add(time.Minute).Unix(),
				})
			},
			status: http.StatusForbidden, errorName: "Auth",
		},
		{
			name: "token for another method",
			token: func(f *orgFixture) string {
				return f.token(testKnotDID, f.knotKey, orgtangled.NSIDRetryPipeline, time.Minute)
			},
			status: http.StatusForbidden, errorName: "Auth",
		},
		{
			name: "expired token",
			token: func(f *orgFixture) string {
				return f.token(testKnotDID, f.knotKey, orgtangled.NSIDNotifyEvent, -time.Minute)
			},
			status: http.StatusForbidden, errorName: "Auth",
		},
		{
			name: "signed with the wrong key",
			token: func(f *orgFixture) string {
				key, err := atcrypto.GeneratePrivateKeyK256()
				if err != nil {
					t.Fatal(err)
				}
				return f.token(testKnotDID, key, orgtangled.NSIDNotifyEvent, time.Minute)
			},
			status: http.StatusForbidden, errorName: "Auth",
		},
		{
			name: "issuer does not resolve",
			token: func(f *orgFixture) string {
				key, err := atcrypto.GeneratePrivateKeyK256()
				if err != nil {
					t.Fatal(err)
				}
				return f.token("did:web:unresolvable.example", key, orgtangled.NSIDNotifyEvent, time.Minute)
			},
			status: http.StatusServiceUnavailable, errorName: "IssuerUnavailable",
		},
		{
			name: "issuer is not a knot",
			token: func(f *orgFixture) string {
				key := f.newSigner("did:plc:alice")
				return f.token("did:plc:alice", key, orgtangled.NSIDNotifyEvent, time.Minute)
			},
			status: http.StatusForbidden, errorName: "Auth",
		},
		{
			name: "issuer is a different knot",
			token: func(f *orgFixture) string {
				key := f.newSigner("did:web:other-knot.example")
				return f.token("did:web:other-knot.example", key, orgtangled.NSIDNotifyEvent, time.Minute)
			},
			status: http.StatusForbidden, errorName: "Auth",
		},
		{
			name:   "repository knot does not resolve",
			token:  func(f *orgFixture) string { return f.knotToken() },
			setup:  func(f *orgFixture) { delete(f.repos, testRepoDID) },
			status: http.StatusServiceUnavailable, errorName: "RepoKnotUnavailable",
		},
		{
			name: "token without jti",
			token: func(f *orgFixture) string {
				return signClaims(t, f.knotKey, jwt.MapClaims{
					"iss": testKnotDID, "aud": testSpindleDID, "lxm": orgtangled.NSIDNotifyEvent,
					"iat": time.Now().Unix(), "exp": time.Now().Add(time.Minute).Unix(),
				})
			},
			status: http.StatusForbidden, errorName: "Auth",
		},
		{
			name: "token lives too long to track",
			token: func(f *orgFixture) string {
				return f.token(testKnotDID, f.knotKey, orgtangled.NSIDNotifyEvent, time.Hour)
			},
			status: http.StatusForbidden, errorName: "Auth",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newOrgFixture(t)
			if test.setup != nil {
				test.setup(f)
			}
			response := f.post(orgtangled.NSIDNotifyEvent, test.token(f), body)
			if response.Code != test.status || decodeXRPCError(t, response) != test.errorName {
				t.Fatalf("response = %d %s; want %d %s", response.Code, response.Body, test.status, test.errorName)
			}
			if got := f.provider.names(); len(got) != 0 {
				t.Fatalf("spawned %v; want nothing", got)
			}
		})
	}
}

// TestNotifyEventRejectsReplayedToken pins that a push token is accepted only once. Someone
// who captures a knot's token must not be able to send it again to start more builds. Honest
// knot2 retries are unaffected because each delivery is signed with a fresh token, as
// TestNotifyEventRedeliveryIsIdempotent shows.
func TestNotifyEventRejectsReplayedToken(t *testing.T) {
	f := newOrgFixture(t)
	token := f.knotToken()
	body := pushEvent(testRepoDID, "refs/heads/main", testOldSHA, testNewSHA)
	if first := f.post(orgtangled.NSIDNotifyEvent, token, body); first.Code != http.StatusOK {
		t.Fatalf("first delivery = %d %s", first.Code, first.Body)
	}
	replay := f.post(orgtangled.NSIDNotifyEvent, token, body)
	if replay.Code != http.StatusForbidden || !strings.Contains(replay.Body.String(), "already used") {
		t.Fatalf("replay = %d %s; want 403 already used", replay.Code, replay.Body)
	}
}

// signClaims signs arbitrary JWT claims with key using the ES256K method that indigo registers
// with golang-jwt. Tests use it for tokens that auth.SignServiceAuth will not mint, such as one
// with no jti or one addressed to another audience.
func signClaims(t *testing.T, key atcrypto.PrivateKey, claims jwt.MapClaims) string {
	t.Helper()
	token, err := jwt.NewWithClaims(jwt.GetSigningMethod("ES256K"), claims).SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	return token
}

// manualEvent returns a notifyEvent body requesting a manual run of commit on testRepoDID,
// limited to the named workflow files, with one input pair (k=v) so tests can check that
// inputs reach the stored trigger.
func manualEvent(commit string, workflows ...string) orgtangled.NotifyEventInput {
	return orgtangled.NotifyEventInput{
		Repo: testRepoDID,
		Event: &orgtangled.Event{Manual: &orgtangled.ManualTrigger{
			Repo: testRepoDID, Commit: commit, Workflows: workflows,
			Inputs: []*orgtangled.ManualInput{{Key: "k", Value: "v"}},
		}},
	}
}

// TestNotifyEventManualTrigger covers manual runs under each kind of caller the Tangled web app
// can send: a repo token (service auth the hosting knot issues in the repository's own DID), the
// repository owner, and a collaborator listed by the knot. Each must start a pipeline whose
// stored trigger keeps the commit and inputs. A user with no relation to the repository must
// get 403 AccessControl and start nothing, because manual runs skip the knot check that guards
// pushes and rely on this authorization alone.
func TestNotifyEventManualTrigger(t *testing.T) {
	for _, test := range []struct {
		name    string
		issuer  string
		setup   func(*orgFixture)
		status  int
		started bool
	}{
		{name: "repo token from the knot", issuer: testRepoDID, status: http.StatusOK, started: true},
		{name: "repository owner", issuer: testOwnerDID, status: http.StatusOK, started: true},
		{
			name:   "knot collaborator",
			issuer: "did:plc:alice",
			setup:  func(f *orgFixture) { f.collaborators = []string{"did:plc:alice"} },
			status: http.StatusOK, started: true,
		},
		{name: "unrelated user", issuer: "did:plc:mallory", status: http.StatusForbidden},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newOrgFixture(t)
			if test.setup != nil {
				test.setup(f)
			}
			key := f.newSigner(test.issuer)
			token := f.token(test.issuer, key, orgtangled.NSIDNotifyEvent, time.Minute)
			response := f.post(orgtangled.NSIDNotifyEvent, token, manualEvent(testNewSHA, "ci.yml"))
			if response.Code != test.status {
				t.Fatalf("response = %d %s; want %d", response.Code, response.Body, test.status)
			}
			if !test.started {
				if decodeXRPCError(t, response) != "AccessControl" || len(f.provider.names()) != 0 {
					t.Fatalf("denied manual run = %s spawned %v", response.Body, f.provider.names())
				}
				return
			}
			output := decodeNotifyOutput(t, response.Body.Bytes())
			if output.CI == nil || len(output.CI.Pipelines) != 1 {
				t.Fatalf("output = %s", response.Body)
			}
			raw, _, err := f.st.GetRawPipeline(context.Background(), output.CI.Pipelines[0])
			if err != nil {
				t.Fatal(err)
			}
			manual := raw.TriggerMetadata.Manual
			if raw.TriggerMetadata.Kind != "manual" || manual == nil || manual.Sha != testNewSHA ||
				len(manual.Inputs) != 1 || manual.Inputs[0].Key != "k" {
				t.Fatalf("manual trigger = %+v", raw.TriggerMetadata)
			}
		})
	}
}

// TestNotifyEventRejectsMalformedManualTriggers pins the error statuses for bad manual run
// requests. An abbreviated commit and a trigger that names a different repository than the
// envelope are client errors (400). A repository whose identity cannot be resolved is a 500,
// not a 4xx, because the failure may be temporary.
func TestNotifyEventRejectsMalformedManualTriggers(t *testing.T) {
	f := newOrgFixture(t)
	token := func() string {
		return f.token(testRepoDID, f.newSigner(testRepoDID), orgtangled.NSIDNotifyEvent, time.Minute)
	}
	if response := f.post(orgtangled.NSIDNotifyEvent, token(), manualEvent("main")); response.Code != http.StatusBadRequest {
		t.Fatalf("short commit = %d %s; want 400", response.Code, response.Body)
	}
	other := manualEvent(testNewSHA)
	other.Event.Manual.Repo = "did:plc:other"
	if response := f.post(orgtangled.NSIDNotifyEvent, token(), other); response.Code != http.StatusBadRequest {
		t.Fatalf("mismatched repo = %d %s; want 400", response.Code, response.Body)
	}
	unknown := manualEvent(testNewSHA)
	unknown.Repo, unknown.Event.Manual.Repo = "did:plc:unknown", "did:plc:unknown"
	response := f.post(orgtangled.NSIDNotifyEvent, token(), unknown)
	// A failed lookup says nothing about whether Tack serves the repository, so it must not be
	// reported as 404 RepoNotFound.
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("unknown repo = %d %s; want 500", response.Code, response.Body)
	}
}
