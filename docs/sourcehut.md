# sourcehut

The sourcehut provider submits jobs to a
[builds.sr.ht](https://man.sr.ht/builds.sr.ht/) instance. Each Tangled
workflow becomes one job: tack submits the job via GraphQL, polls it
until terminal, and publishes `sh.tangled.pipeline.status` records on
each transition.

## Configure tack

| Env var                 | Description                                            |
| ----------------------- | ------------------------------------------------------ |
| `TACK_SOURCEHUT_TOKEN`  | Personal access token for builds.sr.ht (enables provider) |
| `TACK_SOURCEHUT_INSTANCE` | Base URL override (default `https://builds.sr.ht`)   |

Generate a token at `https://meta.sr.ht/oauth2/personal-token` with both
`builds.sr.ht/JOBS:RW` and `builds.sr.ht/LOGS:RO` access, set the
`TACK_SOURCEHUT_TOKEN=$token` env var, then start tack.

`JOBS:RW` is required to submit jobs and poll their status. `LOGS:RO`
is required because tack fetches per-task logs over an authenticated
HTTP endpoint when serving the appview's log stream — without it the
build will run to completion but every log request will fail.

## Workflow YAML

Your sourcehut build manifest is defined via tangled workflow inline - see `tack.sourcehut.manifest`.
It is submitted to builds.sr.ht verbatim, with a few `TACK_*` environment
variables merged into its top-level `environment:` map.

```yaml
when:
  - event: ["push"]
    branch: ["main"]

engine: tack

tack:
  sourcehut:
    manifest: |
      image: alpine/edge
      sources:
        - https://tangled.org/j3s.sh/testy
      tasks:
        - test: |
            ls -l testy
```

Optional fields:

```yaml
tack:
  sourcehut:
    instance: https://selfhosted.sr.ht.example.org
    tags: ["tack", "ci"]
    note: "manual note for the job list"
    secrets: true
```

* `instance`: full URL (with scheme) of an alternate builds.sr.ht
  deployment. Defaults to the provider's configured instance.
* `tags`: passed through to the submit API. Defaults to `["tack"]` so
  jobs are filterable in the builds.sr.ht UI.
* `note`: passed through to the submit API. Defaults to
  `tangled: <workflow> @ <short-commit>`.
* `secrets`: opt in to sourcehut secret injection. Default `false`.

## Injected environment

Tack merges the following into the manifest's `environment:` map
before submitting. These can be overridden via user definition.

| Variable             | Value                                  |
| -------------------- | -------------------------------------- |
| `TACK_KNOT`          | Knot host the trigger came from        |
| `TACK_PIPELINE_RKEY` | Pipeline record rkey                   |
| `TACK_WORKFLOW`      | Workflow name                          |
| `TACK_WORKFLOW_RAW`  | Raw workflow YAML body                 |
| `TACK_ACTOR`         | Triggering DID                         |
| `TACK_COMMIT`        | Commit SHA                             |
| `TACK_BRANCH`        | Branch ref                             |

## Status mapping

| builds.sr.ht status   | tack status |
| --------------------- | ----------- |
| `pending`, `queued`   | `pending`   |
| `running`             | `running`   |
| `success`             | `success`   |
| `failed`, `timeout`   | `failed`    |
| `cancelled`           | `cancelled` |

Unknown upstream statuses are logged.
