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

### Buildkite

Setting `TACK_BUILDKITE_TOKEN` enables Buildkite mode; when unset, tack
runs the in-process fake provider for local development. When
Buildkite mode is enabled, every other variable in this section is
required.

| Env var                         | Description                                                                    |
| ------------------------------- | ------------------------------------------------------------------------------ |
| `TACK_BUILDKITE_TOKEN`          | Buildkite API token (enables Buildkite mode)                                   |
| `TACK_BUILDKITE_ORG`            | Buildkite organization slug                                                    |
| `TACK_BUILDKITE_PIPELINE`       | Buildkite pipeline slug to fire builds on                                      |
| `TACK_BUILDKITE_WEBHOOK_SECRET` | Shared secret for `/webhooks/buildkite` auth                                   |
| `TACK_BUILDKITE_WEBHOOK_MODE`   | `token` (default) or `signature` — must match Buildkite's notification setting |
