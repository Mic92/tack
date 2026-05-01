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

## Endpoints (planned)

- `GET  /events`              — WebSocket stream of pipeline status events,
                                consumed by the Tangled appview.
- `POST /webhooks/buildkite`  — Buildkite webhook receiver.
- `POST /xrpc/sh.tangled.pipeline.cancelPipeline` — cancel a running build.

## Configuration (planned)

| Env var                | Description                          |
| ---------------------- | ------------------------------------ |
| `TACK_BUILDKITE_TOKEN` | Buildkite API token                  |
| `TACK_BUILDKITE_ORG`   | Buildkite organization slug          |
| `TACK_JETSTREAM_URL`   | Tangled Jetstream WebSocket URL      |
| `TACK_DB_PATH`         | Local SQLite path for the event log  |
| `TACK_OWNER_DID`       | DID of the spindle operator          |
