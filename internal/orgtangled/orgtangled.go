// Package orgtangled holds the parts of Tangled's org.tangled.* lexicons that Tack serves or
// reads as of Tangled v1.20.0. A lexicon is an atproto schema. It defines an XRPC method (an
// atproto RPC call), a record, or an object type, each named by an NSID such as
// org.tangled.ci.getPipeline.
//
// Upstream generates these types in tangled.org/core/api/org_tangled, but Tack cannot import
// the v1.20.0 module. Its tree contains test fixtures whose paths include ':' (did:plc:...),
// and Go refuses to build a module zip that contains such paths, so the Go module proxy cannot
// serve it. Until upstream fixes that, the shapes Tack needs are ported here by hand. The JSON
// field names match the generated code. The Go names are shorter (Pipeline rather than
// CiDefs_Pipeline, ManualTrigger rather than CiTriggerManual), so switching to the upstream
// import later means renaming identifiers at each call site.
//
// The types in this file encode as JSON. The CBOR frames of the log subscription live in
// logs.go.
package orgtangled

import (
	"encoding/json"
	"fmt"
)

// These constants are the NSIDs of every XRPC method, record, and object type Tack handles.
// The first group are XRPC methods Tack serves. The second group are the $type values of
// pipeline trigger union members. The third group are workflow source union members. The last
// group are records Tack reads from knots (servers that host git repositories) and PDSes
// (users' atproto Personal Data Servers).
const (
	NSIDQueryPipelines        = "org.tangled.ci.queryPipelines"
	NSIDGetPipeline           = "org.tangled.ci.getPipeline"
	NSIDSubscribePipelineLogs = "org.tangled.ci.subscribePipelineLogs"
	NSIDRetryPipeline         = "org.tangled.ci.retryPipeline"
	NSIDApprovePipeline       = "org.tangled.ci.approvePipeline"
	NSIDCancelPipeline        = "org.tangled.ci.cancelPipeline"
	NSIDNotifyEvent           = "org.tangled.event.notifyEvent"
	NSIDDescribeSpindle       = "org.tangled.spindle.describeSpindle"

	NSIDPushEvent        = "org.tangled.event.push"
	NSIDPullRequestEvent = "org.tangled.event.pullRequest"
	NSIDManualTrigger    = "org.tangled.ci.trigger.manual"
	NSIDScheduleTrigger  = "org.tangled.ci.trigger.schedule"

	NSIDFileSource     = "org.tangled.ci.defs#fileSource"
	NSIDExternalSource = "org.tangled.ci.defs#externalSource"

	NSIDRepoManifest    = "org.tangled.repo.manifest"
	NSIDRepoDeclaration = "org.tangled.repo.declaration"
	NSIDCollaborator    = "org.tangled.graph.collaborator"
	NSIDGitRefs         = "org.tangled.git.refs"
)

// Pipeline is org.tangled.ci.defs#pipeline, the pipeline view that queryPipelines and
// getPipeline return. It is shaped like a record, but the spindle (the Tangled CI service,
// here Tack) owns and stores it rather than any user's PDS.
type Pipeline struct {
	// ID is the spindle-local pipeline ID, a record key.
	ID string `json:"id"`
	// Repo is the DID of the repository the pipeline belongs to.
	Repo    string   `json:"repo"`
	Trigger *Trigger `json:"trigger"`
	// Commit is the commit the pipeline runs on.
	Commit string `json:"commit"`
	// SourceRepo is the DID of the repository the commit was checked out from, when that
	// differs from Repo. A pull request from a fork is the usual case.
	SourceRepo *string        `json:"sourceRepo,omitempty"`
	CreatedAt  *string        `json:"createdAt,omitempty"`
	Workflows  []*WorkflowRun `json:"workflows"`
}

// Trigger is the event that started a pipeline, encoded as an open lexicon union. An open
// union may gain member types in later lexicon versions, so readers must tolerate a $type
// they do not recognize. Exactly one field should be set. On the wire the $type is the
// member's bare NSID (org.tangled.event.push, with no #fragment).
//
// Unlike upstream's generated type, Trigger keeps no copy of an unrecognized member.
// Decoding one leaves every field nil, and encoding that value fails.
type Trigger struct {
	Push        *PushEvent
	PullRequest *PullRequestEvent
	Manual      *ManualTrigger
	Schedule    *ScheduleTrigger
}

// MarshalJSON encodes the set member with its $type. It writes LexiconTypeID into that member
// itself, so callers never have to fill it in. It returns an error when no member is set.
func (t *Trigger) MarshalJSON() ([]byte, error) {
	switch {
	case t.Push != nil:
		t.Push.LexiconTypeID = NSIDPushEvent
		return json.Marshal(t.Push)
	case t.PullRequest != nil:
		t.PullRequest.LexiconTypeID = NSIDPullRequestEvent
		return json.Marshal(t.PullRequest)
	case t.Manual != nil:
		t.Manual.LexiconTypeID = NSIDManualTrigger
		return json.Marshal(t.Manual)
	case t.Schedule != nil:
		t.Schedule.LexiconTypeID = NSIDScheduleTrigger
		return json.Marshal(t.Schedule)
	default:
		return nil, fmt.Errorf("pipeline trigger union has no member set")
	}
}

// UnmarshalJSON decodes the member named by $type. A $type it does not know, or a missing
// one, is not an error because the union is open. It leaves every member nil instead.
func (t *Trigger) UnmarshalJSON(data []byte) error {
	typ, err := lexiconType(data)
	if err != nil {
		return err
	}
	switch typ {
	case NSIDPushEvent:
		t.Push = new(PushEvent)
		return json.Unmarshal(data, t.Push)
	case NSIDPullRequestEvent:
		t.PullRequest = new(PullRequestEvent)
		return json.Unmarshal(data, t.PullRequest)
	case NSIDManualTrigger:
		t.Manual = new(ManualTrigger)
		return json.Unmarshal(data, t.Manual)
	case NSIDScheduleTrigger:
		t.Schedule = new(ScheduleTrigger)
		return json.Unmarshal(data, t.Schedule)
	}
	return nil
}

// WorkflowRun is org.tangled.ci.defs#workflowRun, one workflow's run inside a pipeline.
type WorkflowRun struct {
	// ID is the pipeline-local workflow ID. It is usually the same as the workflow name.
	ID   string  `json:"id"`
	Name *string `json:"name,omitempty"`
	// Status is one of the closed enum values pending, blocked, running, failed, timeout,
	// cancelled, or success. A closed enum admits no other values.
	Status     string  `json:"status"`
	StartedAt  *string `json:"startedAt,omitempty"`
	FinishedAt *string `json:"finishedAt,omitempty"`
	Error      *string `json:"error,omitempty"`
	// Definition is the workflow this run came from.
	Definition *Workflow `json:"definition"`
}

// Workflow is org.tangled.ci.defs#workflow, the definition a workflow run came from.
type Workflow struct {
	ID   string  `json:"id"`
	Name *string `json:"name,omitempty"`
	// Hash is a fingerprint of the definition as the spindle resolved it.
	Hash   *string         `json:"hash,omitempty"`
	Source *WorkflowSource `json:"source"`
	// Triggers lists the trigger NSIDs the workflow declares, such as NSIDPushEvent.
	Triggers []string `json:"triggers"`
}

// WorkflowSource says where a workflow definition lives, encoded as a lexicon union. Exactly
// one field should be set. Like [Trigger], it keeps no copy of a member it does not recognize.
type WorkflowSource struct {
	File     *FileSource
	External *ExternalSource
}

// MarshalJSON encodes the set member with its $type, writing LexiconTypeID into that member
// itself. It returns an error when no member is set.
func (s *WorkflowSource) MarshalJSON() ([]byte, error) {
	switch {
	case s.File != nil:
		s.File.LexiconTypeID = NSIDFileSource
		return json.Marshal(s.File)
	case s.External != nil:
		s.External.LexiconTypeID = NSIDExternalSource
		return json.Marshal(s.External)
	default:
		return nil, fmt.Errorf("workflow source union has no member set")
	}
}

// UnmarshalJSON decodes the member named by $type. An unknown or missing $type leaves both
// members nil and is not an error.
func (s *WorkflowSource) UnmarshalJSON(data []byte) error {
	typ, err := lexiconType(data)
	if err != nil {
		return err
	}
	switch typ {
	case NSIDFileSource:
		s.File = new(FileSource)
		return json.Unmarshal(data, s.File)
	case NSIDExternalSource:
		s.External = new(ExternalSource)
		return json.Unmarshal(data, s.External)
	}
	return nil
}

// FileSource is org.tangled.ci.defs#fileSource. It locates a workflow file in a repository at
// a commit. The lexicon's optional lines range is not ported because Tack never sets it, so
// decoding and re-encoding a source that has one drops it. LexiconTypeID is filled in by
// [WorkflowSource.MarshalJSON].
type FileSource struct {
	LexiconTypeID string `json:"$type"`
	// Repo is the DID of the repository that holds the file.
	Repo   string `json:"repo"`
	Commit string `json:"commit"`
	// Path is the file's path within the repository, such as .tangled/workflows/ci.yml.
	Path string `json:"path"`
}

// ExternalSource is org.tangled.ci.defs#externalSource. It points at a workflow defined
// outside the repository. LexiconTypeID is filled in by [WorkflowSource.MarshalJSON].
type ExternalSource struct {
	LexiconTypeID string  `json:"$type"`
	Link          string  `json:"link"`
	Name          *string `json:"name,omitempty"`
}

// PushEvent is org.tangled.event.push, an update to one ref in a repository. Knots send it to
// the spindle through notifyEvent, and a pipeline started by a push echoes it as its trigger.
// LexiconTypeID is filled in by the union's MarshalJSON method.
type PushEvent struct {
	LexiconTypeID string `json:"$type"`
	// Repo is the repository DID.
	Repo string `json:"repo"`
	// Ref is the full ref name, such as refs/heads/main.
	Ref string `json:"ref"`
	// OldSHA and NewSHA are the ref's commit before and after the push. An all-zero OldSHA
	// means the push created the ref, and an all-zero NewSHA means it deleted the ref.
	OldSHA string `json:"oldSha"`
	NewSHA string `json:"newSha"`
	// Actor is the DID of the account that pushed, when the knot reports it.
	Actor *string `json:"actor,omitempty"`
	// PushOptions are the options sent with git push -o. The knot omits them when there were
	// none.
	PushOptions []string `json:"pushOptions,omitempty"`
}

// PullRequestEvent is org.tangled.event.pullRequest, the trigger of a pipeline started by a
// pull request. Every field is optional in the lexicon. LexiconTypeID is filled in by
// [Trigger.MarshalJSON].
type PullRequestEvent struct {
	LexiconTypeID string `json:"$type"`
	// Action is the pull request lifecycle action, such as opened or synchronize.
	Action *string `json:"action,omitempty"`
	// Pull is the AT-URI of the pull request record. An AT-URI is an at:// link to an atproto
	// record.
	Pull *string `json:"pull,omitempty"`
	// SourceRepo is the DID of the repository to check out code and workflows from, when it
	// differs from the target repository.
	SourceRepo   *string `json:"sourceRepo,omitempty"`
	SourceBranch *string `json:"sourceBranch,omitempty"`
	// SourceSHA is the commit at the head of the pull request.
	SourceSHA    *string `json:"sourceSha,omitempty"`
	TargetBranch *string `json:"targetBranch,omitempty"`
}

// ManualTrigger is org.tangled.ci.trigger.manual, a run someone requested by hand. It arrives
// through notifyEvent and becomes the pipeline's trigger. LexiconTypeID is filled in by the
// union's MarshalJSON method.
type ManualTrigger struct {
	LexiconTypeID string `json:"$type"`
	// Repo is the repository DID.
	Repo string `json:"repo"`
	// Commit is the full commit ID the pipeline runs on.
	Commit string `json:"commit"`
	// Ref is the ref the commit was resolved from, if any.
	Ref *string `json:"ref,omitempty"`
	// Workflows limits the run to the named workflows. When it is empty, every workflow that
	// accepts manual runs is run.
	Workflows []string       `json:"workflows,omitempty"`
	Inputs    []*ManualInput `json:"inputs,omitempty"`
}

// ManualInput is one key and value pair supplied with a manual run.
type ManualInput struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// ScheduleTrigger is org.tangled.ci.trigger.schedule, the trigger of a scheduled run. Tack
// never starts scheduled runs. The type exists so the trigger union covers every member the
// lexicon lists and a schedule trigger decodes and encodes again without loss.
type ScheduleTrigger struct {
	LexiconTypeID string `json:"$type"`
	Repo          string `json:"repo"`
	Commit        string `json:"commit"`
	Ref           string `json:"ref"`
	ScheduledAt   string `json:"scheduledAt"`
}

// QueryPipelinesOutput is the response body of org.tangled.ci.queryPipelines.
type QueryPipelinesOutput struct {
	// Cursor is the value to pass back for the next page. It is absent on the last page.
	Cursor *string `json:"cursor,omitempty"`
	// Total counts every pipeline that matches the query, not only those on this page.
	Total     int64       `json:"total"`
	Pipelines []*Pipeline `json:"pipelines"`
}

// RetryPipelineInput is the request body of org.tangled.ci.retryPipeline, which starts a new
// pipeline as a copy of an existing one.
type RetryPipelineInput struct {
	// Repo is the repository DID.
	Repo string `json:"repo"`
	// Pipeline is the ID of the pipeline to copy.
	Pipeline string `json:"pipeline"`
	// Workflows limits the copy to the named workflows. When it is empty, the whole pipeline
	// is copied.
	Workflows []string `json:"workflows,omitempty"`
}

// RetryPipelineOutput is the response body of org.tangled.ci.retryPipeline. Pipeline is the
// ID of the new pipeline.
type RetryPipelineOutput struct {
	Pipeline string `json:"pipeline"`
}

// ApprovePipelineInput is the request body of org.tangled.ci.approvePipeline. Pipeline is the
// ID of the blocked pipeline to approve, and Repo is its repository DID.
type ApprovePipelineInput struct {
	Repo     string `json:"repo"`
	Pipeline string `json:"pipeline"`
}

// NotifyEventInput is the request body of org.tangled.event.notifyEvent. Knots call this
// method to report pushes, and the Tangled web app calls it to request manual runs. Repo is the
// repository DID.
type NotifyEventInput struct {
	Repo  string `json:"repo"`
	Event *Event `json:"event"`
}

// Event is the union that notifyEvent carries. It has no pull request member because Tangled
// v1.20 starts pull request pipelines from records, never from knots.
//
// Like upstream's generated type, Event keeps nothing for an unrecognized member. An unknown
// or missing $type decodes without error and leaves both fields nil, and the notifyEvent
// handler then rejects the request with a specific InvalidRequest message.
type Event struct {
	Push   *PushEvent
	Manual *ManualTrigger
}

// MarshalJSON encodes the set member with its $type, writing LexiconTypeID into that member
// itself. It returns an error when no member is set.
func (e *Event) MarshalJSON() ([]byte, error) {
	switch {
	case e.Push != nil:
		e.Push.LexiconTypeID = NSIDPushEvent
		return json.Marshal(e.Push)
	case e.Manual != nil:
		e.Manual.LexiconTypeID = NSIDManualTrigger
		return json.Marshal(e.Manual)
	default:
		return nil, fmt.Errorf("notify event union has no member set")
	}
}

// UnmarshalJSON decodes the member named by $type. An unknown or missing $type leaves both
// members nil and is not an error.
func (e *Event) UnmarshalJSON(data []byte) error {
	typ, err := lexiconType(data)
	if err != nil {
		return err
	}
	switch typ {
	case NSIDPushEvent:
		e.Push = new(PushEvent)
		return json.Unmarshal(data, e.Push)
	case NSIDManualTrigger:
		e.Manual = new(ManualTrigger)
		return json.Unmarshal(data, e.Manual)
	}
	return nil
}

// NotifyEventOutput is the response body of org.tangled.event.notifyEvent. An empty object
// means the spindle accepted the event but started nothing.
type NotifyEventOutput struct {
	CI *NotifyEventCIOutput `json:"ci,omitempty"`
}

// NotifyEventCIOutput reports what an event started.
type NotifyEventCIOutput struct {
	// Pipelines holds the IDs of the pipelines the event started.
	Pipelines []string `json:"pipelines,omitempty"`
	// Message is text for the person who pushed, such as where to follow the run. knot2 (the
	// Rust knot shipped with Tangled v1.20) waits about two seconds for the reply and prints
	// Message in the git push output only if the reply arrives within that time.
	Message *string `json:"message,omitempty"`
}

// DescribeSpindleOutput is the response body of org.tangled.spindle.describeSpindle, which
// tells clients who the spindle is and which protocol it speaks.
type DescribeSpindleOutput struct {
	// DID is the spindle's own DID.
	DID     string `json:"did"`
	Version string `json:"version"`
	// Capabilities lists the protocol tokens the spindle implements. The token "tangled1.0"
	// marks a spindle that speaks the current org.tangled.* protocol. Tangled treats a spindle
	// that omits the field as legacy, meaning it speaks only the v1.16 sh.tangled.* protocol.
	Capabilities []string        `json:"capabilities,omitempty"`
	Contact      *SpindleContact `json:"contact,omitempty"`
}

// SpindleContact is how to reach the spindle's operator.
type SpindleContact struct {
	Email *string `json:"email,omitempty"`
}

// RepoManifest is the repo manifest, the org.tangled.repo.manifest/self record a knot
// publishes in each repository DID's atproto repo. Tack reads it to learn which spindle a
// repository uses and who claims to own it. The knot alone asserts these values, so Tack
// confirms the owner from the owner's side (see pipelineService.ownerDeclaresRepo) before it
// grants the repository anything.
type RepoManifest struct {
	// Spindle is the DID of the spindle the repository uses for CI. It is absent when the
	// repository has no spindle.
	Spindle     *string                  `json:"spindle,omitempty"`
	Declaration *RepoManifestDeclaration `json:"declaration"`
	CreatedAt   string                   `json:"createdAt"`
}

// RepoManifestDeclaration names the owner whose PDS should declare the repository. Owner is
// the owner's DID and Name is the repository name. The owner's org.tangled.repo.declaration
// record for this repository uses strings.ToLower(Name) as its record key.
type RepoManifestDeclaration struct {
	Owner string `json:"owner"`
	Name  string `json:"name"`
}

// RepoDeclaration is the org.tangled.repo.declaration record a repository owner publishes on
// their own PDS. Its record key is the repository slug, and Repo is the repository DID that
// slug points to. It is the owner's side of the claim a [RepoManifest] makes, so a knot cannot
// attach a repository to an owner who never declared it.
type RepoDeclaration struct {
	Repo      string `json:"repo"`
	CreatedAt string `json:"createdAt"`
}

// GitRefs is the part of the knot-hosted org.tangled.git.refs/self record that Tack reads.
// knot2 publishes the default branch here. Unlike the Go knot, it does not serve the legacy
// sh.tangled.repo.getDefaultBranch method, so on knot2 this record is the only way to learn the
// branch.
type GitRefs struct {
	// DefaultBranch is the default branch name. The lexicon says it has no refs/heads/
	// prefix, but callers strip one anyway in case a knot includes it.
	DefaultBranch *string `json:"defaultBranch,omitempty"`
}

// lexiconType reads the $type of a JSON object without decoding the rest, so a union can
// choose which member type to decode into. A missing $type yields "", which every union in
// this package treats as an unknown member. Malformed JSON is an error.
func lexiconType(data []byte) (string, error) {
	var typed struct {
		Type string `json:"$type"`
	}
	if err := json.Unmarshal(data, &typed); err != nil {
		return "", err
	}
	return typed.Type, nil
}
