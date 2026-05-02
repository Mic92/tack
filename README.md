# tack - Connect Tangled to your CI

Tack is a custom [Tangled](https://tangled.org) spindle that runs
CI on alternate providers and reports their results back
to Tangled using standard ATProto records so they show up natively
in Tangled's UI.

> [!WARNING]
>
> **Project status:** Tack is early and experimental software.
> This repo itself uses Tack and I (mitchellh) use Tack for my projects,
> but it is still new and relatively unproven.

## Example

A Tangled workflow that fires a Buildkite pipeline on every push to
`main` and every pull request targeting `main`:

```yaml
# .tangled/workflows/ci.yml
when:
  - event: ["push"]
    branch: ["main"]
  - event: ["pull_request"]
    branch: ["main"]

# this does nothing for Tack but is required by Tangled
engine: nixery

tack:
  buildkite:
    pipeline: my-app-ci
```

The `when:` block is the standard Tangled trigger schema; the
`tack:` block tells tack which Buildkite pipeline to fire and how.
See [docs/buildkite.md](docs/buildkite.md) for the full set of
options and end-to-end Buildkite setup. Tack can support multiple
providers.

## How it Works

Tack is a drop-in alternative to the stock `spindle` runner. You run
`tack` and [register it using the standard UI](https://tangled.org/settings/spindles).

Instead of executing workflows in local containers, tack translates each
Tangled pipeline trigger into a 3rd party CI build, and reports build state
back to Tangled using the existing `sh.tangled.pipeline.status` wire format.

This makes even 3rd party CIs integrate first class into Tangled so their
status, counts, etc. can show up inline in things like pull requests.

```
   Jetstream                Knot /events (WebSocket)
   sh.tangled.repo          sh.tangled.pipeline triggers
   sh.tangled.spindle.member
   sh.tangled.repo.collaborator
        │                            │
        │ (discover knots            │
        │  + membership)             │
        ▼                            ▼
       ╭────────────────────────────────╮
       │             tack               │
       │  ╭──────────────────────────╮  │
       │  │     provider router      │  │   selected per-workflow
       │  │   (fake │ buildkite)     │  │   via `tack: { … }` YAML
       │  ╰──────────────────────────╯  │
       ╰────────────────────────────────╯
                       │
                       │  Create Build (REST)
                       ▼
                   Buildkite
                       │
                       │  POST /webhooks/buildkite
                       ▼
                      tack  ──── /events (WebSocket) ────▶  Tangled appview
                                 sh.tangled.pipeline.status
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

All configured providers are active simultaneously. Each workflow
chooses its provider via the first key under its top-level `tack:`
block. e.g. `tack: { buildkite: { ... } }` runs on Buildkite,
`tack: { fake: {} }` runs on the in-process fake provider.

## Providers

Provider-specific setup (Buildkite-side configuration, the
provider's tack env vars, and the workflow YAML schema) lives in
its own doc per provider:

* [Buildkite](docs/buildkite.md)
* [Tekton](docs/tekton.md)
