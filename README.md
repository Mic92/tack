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

engine: tack

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
Native installations require `git` in `PATH`; the Docker and Nix packages
include it automatically.

Instead of executing workflows in local containers, tack receives push triggers from knots
(the Tangled servers that host git repositories), loads `.tangled/workflows` at the pushed
commit, and translates matching workflows into 3rd party CI builds. Tack owns the resulting
pipeline records and serves them through Tangled's standard CI XRPC API.

This makes even 3rd party CIs integrate first class into Tangled so their
status, counts, etc. can show up inline in things like pull requests.

Tack speaks both generations of the Tangled spindle protocol:

* **Current (Tangled v1.20, knot2):** a repo selects tack by setting the `spindle` field of
  its repo manifest (the knot-hosted `org.tangled.repo.manifest` record) to tack's DID,
  `did:web:<TACK_HOSTNAME>`. The repo's pipeline settings on tangled.org set this field.
  For every push, knot2 resolves that DID by fetching tack's `/.well-known/did.json` and then
  POSTs the event to `org.tangled.event.notifyEvent` with service auth (a signed atproto JWT
  that identifies the calling knot). The Tangled web app calls tack's `org.tangled.ci.*`
  methods from the browser.
* **Legacy (Go knots, Tangled v1.16 and earlier):** tack discovers repos from `sh.tangled.repo`
  records on Jetstream (the public stream of atproto record changes), consumes each knot's
  `/events` stream for `sh.tangled.git.refUpdate`, and serves `sh.tangled.ci.*` to the legacy
  appview.

```
     knot2                 Go knot /events        Jetstream
     notifyEvent (POST)    sh.tangled.git.        sh.tangled.repo
     after resolving       refUpdate              sh.tangled.spindle.member
     did:web:<host>              │                sh.tangled.repo.collaborator
        │                        │                        │
        ▼                        ▼                        ▼
       ╭──────────────────────────────────────────────────╮
       │                       tack                       │
       │  ╭────────────────────────────────────────────╮  │
       │  │   provider router (fake │ buildkite │ …)   │  │   selected per-workflow
       │  ╰────────────────────────────────────────────╯  │   via `tack: { … }` YAML
       ╰──────────────────────────────────────────────────╯
                               │
                               │  Create Build (REST)
                               ▼
                           Buildkite
                               │
                               │  POST /webhooks/buildkite
                               ▼
                              tack  ◀── org.tangled.ci.* ──  Tangled web app
                                    ◀── sh.tangled.ci.* ───  legacy appview
                                        query/get/trigger/retry/approve + CBOR log stream
```

### Who may use a tack instance

Tack runs CI only for repos owned by `TACK_OWNER_DID` or by DIDs the owner grants with a
`sh.tangled.spindle.member` record naming `TACK_HOSTNAME`. For knot2 repos, the owner named in
the repo manifest must also confirm the repo from their own PDS (Personal Data Server), with
either an `org.tangled.repo.declaration` record or a legacy `sh.tangled.repo` record. The knot
writes the manifest, so without this check any knot could claim that you own a repo and run CI
on your provider accounts.

### Deployment requirements

Tack must be reachable at `https://<TACK_HOSTNAME>` with no redirects. knot2 fetches
`/.well-known/did.json` from that host before every push delivery, and the Tangled web app
follows no redirects on spindle requests. When the DID document is missing, knot2 logs
`DID document fetch returned HTTP 404`.

## Configuration

Core configuration controls how tack talks to Tangled. Provider-specific
configuration (e.g. Buildkite) lives in its own section below.

### Required

| Env var          | Description                                                                                                     |
| ---------------- | --------------------------------------------------------------------------------------------------------------- |
| `TACK_HOSTNAME`  | This spindle's hostname. Legacy repos name it in `sh.tangled.repo.spindle`. Tack's DID is `did:web:<hostname>`. |
| `TACK_OWNER_DID` | DID of the spindle operator                                                                                     |

### Optional

| Env var              | Description                                                                                                                                                   |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `TACK_LISTEN_ADDR`   | HTTP listen address (default `:8080`)                                                                                                                         |
| `TACK_DB_PATH`       | Local SQLite path (default `tack.db`)                                                                                                                         |
| `TACK_REPO_DIR`      | Sparse workflow checkout cache (default `tack.db.repos`)                                                                                                      |
| `TACK_JETSTREAM_URL` | Tangled Jetstream WebSocket URL                                                                                                                               |
| `TACK_DEV`           | Local development mode (any non-empty value): plain `ws://` and `http://` to knots, `http://` PDS URLs allowed, and no block on loopback or private addresses |
| `TACK_CORS_ORIGINS`  | Browser origins allowed to call `/xrpc`, comma-separated (default `https://tangled.org`, empty disables)                                                      |
| `TACK_KEY_PATH`      | P-256 PEM key whose public half goes in the DID document, like the stock spindle's. Tangled v1.20 does not need it.                                           |
| `TACK_CONTACT_EMAIL` | Operator contact email reported by `org.tangled.spindle.describeSpindle`                                                                                      |

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
* [sourcehut](docs/sourcehut.md)
