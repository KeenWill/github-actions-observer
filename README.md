# GitHub Actions Observer

A Rust webhook receiver that stores GitHub deliveries and Actions job history in PostgreSQL. Grafana can query exact completion timestamps through SQL; Prometheus scrapes bounded operational metrics. Dashboard queries never call GitHub.

This is an initial implementation. PostgreSQL is required; any compatible PostgreSQL deployment works. Kubernetes, CNPG, ingress, credentials, backups and dashboard provisioning belong in your deployment configuration, outside this application.

## Run

Requires Rust 1.94.1 and PostgreSQL 17 or newer. Set `DATABASE_URL` using your secret manager or a private environment file (do not commit credentials). Run migrations with a schema-owning role before starting a separately permissioned runtime role:

```sh
cargo run --locked -- migrate
cargo run --locked -- serve
```

`serve` requires `GITHUB_WEBHOOK_SECRET`. Configure GitHub to send JSON to `https://your-public-host/webhook` with that same secret. HMAC SHA-256 is verified over the original request bytes with a constant-time comparison. The internal listener binds loopback by default. Container deployments can explicitly set `INTERNAL_BIND=0.0.0.0:9090` and restrict that port with private networking. Never expose the internal listener publicly.

| Variable | Default | Purpose |
| --- | --- | --- |
| `DATABASE_URL` | required | Standard PostgreSQL connection URL; configure TLS for remote connections |
| `GITHUB_WEBHOOK_SECRET` | required for serve | Shared webhook signing secret |
| `WEBHOOK_BIND` | `0.0.0.0:8080` | Public listener, only `POST /webhook` |
| `INTERNAL_BIND` | `127.0.0.1:9090` | Internal `/healthz`, `/readyz`, `/metrics` |
| `REPOSITORIES_INCLUDE` | empty | Comma-separated repository names or `*` |
| `REPOSITORIES_EXCLUDE` | empty | Repository exclusions; exclusions win |
| `EVENTS_INCLUDE` | empty | Comma-separated event types or `event.action` or `*` |
| `EVENTS_EXCLUDE` | empty | Event exclusions; exclusions win |
| `RUST_LOG` | `info` | Application log filter |

Empty include lists or `*` accept any repository/event, including newly created repositories and unknown future event types. Repository names are case-insensitive exact `owner/repository` matches; partial globs are not supported. Events may match either the event type (`workflow_job`) or action (`workflow_job.completed`). A non-repository event such as `ping` is accepted only when repository inclusion is unrestricted. For example, `REPOSITORIES_INCLUDE=*` with `REPOSITORIES_EXCLUDE=example-org/ignored-repo` accepts future repositories automatically. Filters affect both webhook ingestion and imports and take effect on restart.

Unrestricted filtering does **not** disable signature authentication. GitHub must separately be configured to send events: accepting all repositories cannot make GitHub subscribe automatically.

## Delivery guarantees and data

An authenticated, included JSON delivery receives HTTP 202 only after its raw JSON and supported projections commit in one transaction. Duplicate `(source, delivery_id)` receipts return 200 without repeating updates. Reusing an ID with different content fails. Filtered deliveries return 200 `filtered` and are deliberately not retained. Invalid signatures return 401; missing headers or invalid JSON return 400. Payloads are limited to 25 MiB. Database failures return 503 without acknowledging persistence. Pool acquisition is bounded to two seconds; statements to five seconds, lock waits to three seconds, and entire ingestion transactions to five seconds (PostgreSQL 17+). These limits leave room within GitHub’s webhook acknowledgement deadline.

Unknown event types and structurally unsupported Actions payloads are retained, with `projection_status` set to `unhandled` or `invalid`. Their data is available for future projectors. Payloads and secrets are never intentionally logged. Raw deliveries may contain sensitive repository metadata: protect database access and backups accordingly.

GitHub does not automatically redeliver failed webhook deliveries. Monitor receiver/database errors and recover missed jobs through backfill or GitHub's redelivery tools. This application cannot guarantee capture when GitHub did not successfully deliver. There is no automatic GitHub polling in `serve`.

- `gha_deliveries`: JSON payload, event/action/repository, receipt timestamp, projection status and provenance (`webhook`, `backfill`, `replay`). Original JSON bytes and headers are not retained.
- `gha_workflow_runs`: one row per repository ID, run ID and attempt, preserving reruns. Run `updated_at` is not advertised as an exact completion timestamp.
- `gha_jobs`: one row per repository ID and job ID, with original creation/start/completion timestamps and runner metadata. No parent foreign key blocks jobs arriving before run events.
- `gha_steps`: the latest known step states included in job payloads; these are not independent live step events.
- `gha_job_history`: joined job/run details with duration and queue seconds for direct Grafana SQL.

Late queued/in-progress deliveries cannot regress terminal job/run states. Runs from different attempts remain separate. Upserts use lifecycle progression and source timestamps where available; GitHub jobs lack a general update timestamp, so equal-state conflicting updates cannot be perfectly ordered. Fields absent from the source remain null; no timestamps are fabricated.

Create a Grafana role with SELECT access to the intended views only. Example exact half-open completion-window query:

```sql
SELECT count(*) AS completed,
       count(*) FILTER (WHERE conclusion = 'failure') AS failed
FROM gha_job_history
WHERE completed_at >= $__timeFrom()
  AND completed_at < $__timeTo();
```

No retention deletion is automatic yet. Plan and monitor database capacity; apply your retention policy to raw deliveries and completed history separately. Prometheus exposes process-local request counters, database query health, an observed active-job gauge, and durable completed-job count gauges with a fixed set of conclusion labels, without repository/job/delivery labels. Completion counts derive from retained job records: backfills increase them at import time, while deleting history or correcting conclusions can decrease them. They are gauges, not counters; do not use `rate()` or `increase()` on them. Query completion timestamps through SQL for throughput and historical counts. Detailed history lives in PostgreSQL, avoiding unbounded Prometheus series.

## Backfill

Use a GitHub token with repository Actions read access in `GITHUB_TOKEN`. The token is only required by `backfill`, never by `serve`.

```sh
cargo run --locked -- backfill \
  --repository example-org/example-repo \
  --created-since 2026-01-01T00:00:00Z \
  --created-until 2026-01-02T00:00:00Z \
  --cache-dir /private/backfill-snapshots \
  --max-requests 1000 \
  --rate-limit-reserve 100
```

Repeat `--repository` for multiple repositories. Bounds are inclusive, whole-second **run creation times**, not job completion times. A run created before the window may have jobs completed inside it and will not be discovered. Use a broad enough creation window for your recovery task. Adjacent overlapping windows are safe to reimport.

The listed run object supplies its latest attempt, avoiding a redundant metadata GET per run. Older attempt metadata is fetched separately, and job lists are paginated per attempt. Creation windows with more than 1,000 matching runs split recursively into inclusive whole-second windows `[start, midpoint]` and `[midpoint + 1 second, end]`. Exactly 1,000 results remain a valid paginated window. More than 1,000 runs created within a single second fails explicitly because that search cannot be partitioned further. Changed page counts, duplicate IDs and short pages fail rather than silently losing records. GitHub/API errors stop the import with a nonzero exit; earlier commits survive and reruns are idempotent. Backfill IDs hash the normalized payload and are separate from webhook IDs. The import preserves historical job timestamps without backdating Prometheus samples. Current repo/event filters also apply; backfill action is `backfill`, so an action-only include filter must explicitly allow that action.

### Request budgets and resumable snapshots

`--max-requests` defaults to 1,000 outbound GETs per invocation. `--rate-limit-reserve` defaults to 100 remaining primary requests. Before the first uncached data request, the importer calls `/rate_limit` once to establish available quota. That preflight counts toward the invocation's outbound GET budget; GitHub does not charge it to the primary allotment, though secondary limits still apply. Response headers update remaining/reset information after each request. The importer stops before the next request would consume the reserve or exceed its GET budget. Shared credentials can have concurrent consumers, so the reserve is based on the latest observed quota rather than an exclusive allocation.

Requests are sequential. Automatic retries and redirects are disabled. Throttling produces a nonzero exit with `outbound_requests`, `cache_hits`, `remaining`, `reserve`, `reset_unix` and any `Retry-After` value. A wrapper may resume after the reported reset or delay; a secondary-limit response without `Retry-After` requires waiting at least one minute. Do not repeatedly restart a throttled import. There is no built-in scheduler or sleeping retry loop.

Optional `--cache-dir` stores successful JSON response bodies plus their fetch timestamp, keyed by a hash of the request URL. It never stores the token, authorization headers, or rate-limit preflight. Cache hits consume no API requests; an entirely cached import works with `--max-requests 0` and does not perform a quota preflight. Use the same fixed creation window and cache directory to resume after a budget stop. Database imports remain idempotent, so completed portions can be processed again without creating duplicate history.

These are **immutable per-request snapshots**, not a live refresh cache or a globally consistent GitHub snapshot. Cached active runs/jobs will not become completed merely by rerunning against the same directory. Start with a new cache directory when fresh data is needed, after changing the authenticated account or access scope, or if cached/live pagination becomes inconsistent. Missing pages are fetched on resume, and consistency checks reject detected count/ID changes. Use one importer per cache directory.

Cache directories require Unix filesystem permissions: the importer creates the final directory with mode `0700` (its parent must exist) and writes files atomically with mode `0600`. Existing permissive paths and symlinks are rejected. Keep this directory outside any source repository: payloads can contain private repository/job metadata even though credentials are excluded. No automatic cache eviction is performed; delete snapshots according to your local retention policy.

GitHub Enterprise deployments may override `--api-base` with an HTTPS API base ending in `/`. Redirects are disabled to avoid forwarding credentials unexpectedly.

## Replay and validation

`replay --input synthetic-events.jsonl` reads one envelope per line:

```json
{"delivery_id":"synthetic-1","event":"ping","payload":{"zen":"synthetic example"}}
```

Replay bypasses HTTP authentication and uses the same filters, schema and transaction/projectors. Only run it against a database you intend to modify. Stable IDs make reruns idempotent; distinct IDs allow synthetic load generation. Use synthetic inputs for benchmarks; do not publish captured private webhook payloads.

```sh
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
```

Set `TEST_DATABASE_URL` to run the PostgreSQL integration test. It creates and drops its own unique schema and requires schema-creation permission. Without the variable, that test reports a skip; CI always provides PostgreSQL. Tests cover signatures/filtering, delivery deduplication, ordering, reruns, transaction rollback, exact SQL time boundaries and listener separation.

Build the portable container with `docker build -t github-actions-observer .`. It runs as an unprivileged user. Run `migrate` as a separate deployment job before `serve`. Configure platform probes and private metrics discovery against port 9090.

Version tags such as `v0.1.0` trigger the generic GHCR publication workflow, publishing `ghcr.io/<repository-owner>/<repository-name>:0.1.0`. The repository's `GITHUB_TOKEN` needs package-write permission; registry/package visibility is managed by the repository owner. Tags must use `vMAJOR.MINOR.PATCH`.
