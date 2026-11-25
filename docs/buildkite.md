# Buildkite

For [Buildkite](https://buildkite.com), every Tangled pipeline trigger
fans out into one Buildkite build per workflow on the pipeline that
workflow configures. Buildkite must be configured with a webhook
back to Tack to communicate status updates.

## Setting up Buildkite

This must happen before configuring tack within Buildkite.

### 1. Create one or more pipelines

In your Buildkite org, **Pipelines → New pipeline**:

* Repository: any URL (the agent only needs to be able to clone it).
* Steps: whatever you want.

Note the pipeline slug from the URL
(`https://buildkite.com/<org>/<pipeline-slug>`); your workflow YAML
will reference it.

### 2. Create an API access token

Tack uses a single API token to create builds, list jobs, and fetch
logs. Generate one at
<https://buildkite.com/user/api-access-tokens> with these scopes:

| Scope               | Used for                                          |
| ------------------- | ------------------------------------------------- |
| `read_organizations`| Sanity-checking the configured org slug           |
| `write_builds`      | `POST .../builds` when a Tangled trigger arrives  |
| `read_builds`       | Resolving builds and jobs for log subscriptions  |
| `read_build_logs`   | Streaming job logs back to the Tangled appview    |

Restrict the token to the specific organization(s) tack will spawn
into.

### 3. Configure a notification webhook

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

Buildkite supports two header schemes for authenticating webhooks and
Tack supports both:

| Header scheme           | `TACK_BUILDKITE_WEBHOOK_MODE` | Notes                                        |
| ----------------------- | ----------------------------- | -------------------------------------------- |
| `X-Buildkite-Token`     | `token` (default)             | Secret is sent verbatim in the header        |
| `X-Buildkite-Signature` | `signature`                   | HMAC-SHA256 of `<timestamp>.<body>`; safer   |

## Configure Tack

| Env var                         | Description                                                                    |
| ------------------------------- | ------------------------------------------------------------------------------ |
| `TACK_BUILDKITE_TOKEN`          | Buildkite API token (enables Buildkite mode)                                   |
| `TACK_BUILDKITE_ORG`            | Default Buildkite organization slug (workflows may override via YAML)          |
| `TACK_BUILDKITE_WEBHOOK_SECRET` | Shared secret for `/webhooks/buildkite` auth                                   |
| `TACK_BUILDKITE_WEBHOOK_MODE`   | `token` (default) or `signature` — must match the notification service        |
| `TACK_BUILDKITE_API_URL`        | Buildkite REST API root (default `https://api.buildkite.com`)                  |

The pipeline a workflow runs against is **not** an environment
variable. It lives inside the workflow YAML so each repo can target
its own pipeline without an operator round-trip.

## Configuring your Tangled workflows

Tack's configuration lives under a `tack:` namespace so the workflow body
can grow other top-level keys without colliding.

Only `pipeline` is required:

```yaml
tack:
  buildkite:
    # Required: which Buildkite pipeline this workflow fires.
    pipeline: my-pipeline-slug

    # Optional: org override. Defaults to TACK_BUILDKITE_ORG. The
    # API token must have access to whichever org you target.
    org: another-org

    # Optional: forwarded verbatim to the Buildkite create-build
    # API. Omit to use Buildkite's default (false).
    clean_checkout: true
```

When the trigger is a pull request, tack auto-populates Buildkite's
`pull_request_base_branch` from the PR target so step-level branch
filters work without extra config.

### What tack injects into every build

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
