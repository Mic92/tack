# tack - Connect Tangled to your CI

Tack is a custom [Tangled](https://tangled.org) spindle that runs
CI on alternate providers and reports their results back
to Tangled using standard ATProto records so they show up natively
in Tangled's UI.

## What it does

Tack is a drop-in alternative to the stock `spindle` runner. You run
`tack` and [register it using the standard UI](https://tangled.org/settings/spindles).

Instead of executing workflows in local containers, tack translates each
Tangled pipeline trigger into a 3rd party CI build, and reports build state
back to Tangled using the existing `sh.tangled.pipeline.status` wire format.

This makes even 3rd party CIs integrate first class into Tangled so their
status, counts, etc. can show up inline in things like pull requests.

```
                 sh.tangled.pipeline
  Jetstream  ───────────────────────▶  tack
                                        │
                                        │  Create Build
                                        ▼
                                    Buildkite
                                        │
                                        │  webhooks
                                        ▼
                                       tack  ──── /events (WebSocket) ────▶  Tangled appview
                                                  sh.tangled.pipeline.status
```

```sh
go run . -addr :8080
```

## Configuration

Core configuration controls how tack talks to Tangled. Provider-specific
configuration (e.g. Buildkite) lives in its own section below.

### Required

| Env var          | Description                                                 |
| ---------------- | ----------------------------------------------------------- |
| `TACK_HOSTNAME`  | This spindle's hostname (matches `sh.tangled.repo.spindle`) |
| `TACK_OWNER_DID` | DID of the spindle operator                                 |

### Optional

| Env var              | Description                                              |
| -------------------- | -------------------------------------------------------- |
| `TACK_LISTEN_ADDR`   | HTTP listen address (default `:8080`)                    |
| `TACK_DB_PATH`       | Local SQLite path (default `tack.db`)                    |
| `TACK_JETSTREAM_URL` | Tangled Jetstream WebSocket URL                          |
| `TACK_DEV`           | Use `ws://` for knot event-streams (any non-empty value) |

When no provider is configured, tack runs an in-process fake provider
that's useful for exercising the jetstream → knot → `/events` flow
locally without a real CI account.

## Buildkite

[Buildkite](https://buildkite.com) is the primary provider tack
supports today. In Buildkite mode, every Tangled pipeline trigger
fans out into one Buildkite build per workflow on the pipeline that
workflow names; build state flows back to Tangled via Buildkite's
notification webhooks.

### How it fits together

```
  sh.tangled.pipeline           Buildkite
  trigger record   ──▶ tack ──▶ Create Build  ─┐
                                                │
  /webhooks/buildkite ◀──── notification ◀─────┘
         │
         ▼
  sh.tangled.pipeline.status (broadcast on /events)
```

* **Spawn:** for each workflow on a pipeline trigger, tack POSTs to
  `/v2/organizations/<org>/pipelines/<slug>/builds`. Both `<org>` and
  `<slug>` come from the workflow's YAML body (see
  [Configuring your workflows](#configuring-your-workflows)).
* **Track:** tack persists the resulting `(build_uuid → knot, rkey,
  workflow)` mapping in its local SQLite store so it can later
  resolve incoming webhooks back to the originating Tangled
  pipeline.
* **Report:** Buildkite delivers `build.*` events to
  `POST /webhooks/buildkite`. tack authenticates each request,
  translates the Buildkite state into a Tangled status, and
  broadcasts a `sh.tangled.pipeline.status` record on `/events`.

### Setting up Buildkite

These steps happen once on the Buildkite side, before tack can talk
to it.

#### 1. Create one or more pipelines

Each Tangled workflow targets exactly one Buildkite pipeline by
slug. There's no requirement that pipelines map 1:1 to workflows —
many users point every workflow at a single pipeline whose
`pipeline.yml` does `pipeline upload some-file-${TACK_WORKFLOW}.yml`,
keeping all the per-workflow logic in the repo rather than in
Buildkite's UI.

In your Buildkite org, **Pipelines → New pipeline**:

* Repository: any URL (the agent only needs to be able to clone it).
* Steps: a minimal `pipeline upload` is usually enough — tack passes
  the workflow name through `$TACK_WORKFLOW` so you can branch on
  it.

Note the pipeline slug from the URL
(`https://buildkite.com/<org>/<pipeline-slug>`); your workflow YAML
will reference it.

#### 2. Create an API access token

Tack uses a single API token to create builds, list jobs, and fetch
logs. Generate one at
<https://buildkite.com/user/api-access-tokens> with these scopes:

| Scope               | Used for                                          |
| ------------------- | ------------------------------------------------- |
| `read_organizations`| Sanity-checking the configured org slug           |
| `write_builds`      | `POST .../builds` when a Tangled trigger arrives  |
| `read_builds`       | Resolving build → jobs for the `/logs` endpoint   |
| `read_build_logs`   | Streaming job logs back to the Tangled appview    |

Restrict the token to the specific organization(s) tack will spawn
into.

#### 3. Configure a notification webhook

Builds report their state back to tack through Buildkite's
notification service.

In your Buildkite org, **Settings → Notification Services → Add →
Webhook**:

* **Webhook URL:** `https://<your-tack-host>/webhooks/buildkite`
* **Token / Secret:** any high-entropy string. You'll set the same
  value in `TACK_BUILDKITE_WEBHOOK_SECRET`.
* **Events:** `build.scheduled`, `build.running`, `build.finished`
  (job-level events are ignored).
* **Pipelines:** the pipelines tack will fire builds on.

Buildkite supports two header schemes for authenticating webhooks;
tack supports both:

| Header scheme           | `TACK_BUILDKITE_WEBHOOK_MODE` | Notes                                        |
| ----------------------- | ----------------------------- | -------------------------------------------- |
| `X-Buildkite-Token`     | `token` (default)             | Secret is sent verbatim in the header        |
| `X-Buildkite-Signature` | `signature`                   | HMAC-SHA256 of `<timestamp>.<body>`; safer   |

Pick `signature` if the notification setting offers it — it doesn't
expose the secret on the wire.

### Configuring tack

Setting `TACK_BUILDKITE_TOKEN` is the master switch that puts tack
into Buildkite mode. The other variables in this section are then
required.

| Env var                         | Description                                                                    |
| ------------------------------- | ------------------------------------------------------------------------------ |
| `TACK_BUILDKITE_TOKEN`          | Buildkite API token (enables Buildkite mode)                                   |
| `TACK_BUILDKITE_ORG`            | Default Buildkite organization slug (workflows may override via YAML)          |
| `TACK_BUILDKITE_WEBHOOK_SECRET` | Shared secret for `/webhooks/buildkite` auth                                   |
| `TACK_BUILDKITE_WEBHOOK_MODE`   | `token` (default) or `signature` — must match the notification service        |

The pipeline a workflow runs against is **not** an environment
variable. It lives inside the workflow YAML so each repo can target
its own pipeline without an operator round-trip.

### Configuring your workflows

A Tangled workflow's `raw` body is parsed by tack as YAML. Only
`pipeline` is required — every other field is an optional override
or extension of what the trigger metadata already provides:

```yaml
# Required: which Buildkite pipeline this workflow fires.
pipeline: my-pipeline-slug

# Optional: org override. Defaults to TACK_BUILDKITE_ORG. The API
# token must have access to whichever org you target.
org: another-org

# Optional: human-readable build message (default: "tangled: <name>").
message: "Custom build message"

# Optional: pin the commit/branch tack would otherwise derive from
# the trigger. Useful for manual triggers (which carry no commit).
commit: abcdef0123
branch: main

# Optional: extra env + meta_data merged on top of tack's defaults
# (see "What tack injects into every build" below).
env:
  CUSTOM_VAR: value
meta_data:
  custom-key: value

# Optional: forwarded verbatim to the Buildkite create-build API.
clean_checkout: true
ignore_pipeline_branch_filters: true   # default: true
author:
  name: "Author Name"
  email: "author@example.com"
```

When the trigger is a pull request, tack auto-populates Buildkite's
`pull_request_base_branch` from the PR target so step-level branch
filters work without extra config.

#### What tack injects into every build

Regardless of what the workflow YAML adds on top, tack always
provides the following so your Buildkite pipeline can recover the
Tangled identity of the build:

| Channel     | Key                  | Value                                    |
| ----------- | -------------------- | ---------------------------------------- |
| `env`       | `TACK_KNOT`          | knot hostname the pipeline came from     |
| `env`       | `TACK_PIPELINE_RKEY` | rkey of the originating pipeline record  |
| `env`       | `TACK_WORKFLOW`      | workflow name (typically a YAML filename) |
| `env`       | `TACK_WORKFLOW_RAW`  | the workflow's raw YAML body             |
| `meta_data` | `tack:knot`          | same as `TACK_KNOT`                      |
| `meta_data` | `tack:pipeline_rkey` | same as `TACK_PIPELINE_RKEY`             |
| `meta_data` | `tack:workflow`      | same as `TACK_WORKFLOW`                  |

A common pattern is for the Buildkite pipeline's root step to do a
`pipeline upload` against a workflow-specific YAML file based on
`$TACK_WORKFLOW`, e.g.:

```yaml
# Buildkite pipeline.yml
steps:
  - label: ":pipeline: dispatch ${TACK_WORKFLOW}"
    command: "buildkite-agent pipeline upload .buildkite/${TACK_WORKFLOW}"
```
