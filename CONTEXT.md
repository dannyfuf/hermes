# CONTEXT.md — Hermes

> Onboarding and design-alignment guide for anyone (human or agent) working on
> this library. Read this before proposing or implementing changes. When a
> decision isn't covered here, derive it from the **Design philosophy** section
> — that's what this document exists to protect.
>
> This document describes the _durable_ intent and conventions of the project.
> For the current snapshot of known gaps, inconsistencies, and pending work, see
> [KNOWN_GAPS.md](./KNOWN_GAPS.md). For current version, tasks, and
> dependencies, `deno.json` is always the source of truth.

## What Hermes is

Hermes is a small, backend-agnostic **background job framework for TypeScript**,
Deno-first, published to JSR as `@dafu/hermes` (MIT). Applications define jobs
as classes, enqueue work with `job.performLater(body)`, and run a worker process
that loads job classes from a manifest module and dispatches queued payloads by
`jobName`. Two backends ship today:

- **Deno KV** (`DenoKvBackend`) — zero-infrastructure, works on Deno Deploy,
  uses `kv.enqueue()` / `kv.listenQueue()` and `Deno.cron()` for recurrence.
- **BullMQ / Redis** (`BullMQBackend`) — production-grade queues, imported from
  the separate subpath `@dafu/hermes/backends/bullmq`.

The one-sentence pitch: **Sidekiq for the TypeScript/Deno world** — convention
over configuration, a tiny API surface, and the heavy lifting delegated to a
pluggable queue backend.

## Design philosophy

These are the principles behind every existing choice. New code should honor
them; deviating from one is an explicit design decision that deserves
discussion, not a side effect of a feature.

### 1. Convention over configuration (the Sidekiq model)

An early implementation plan (since removed from the repo) stated it verbatim:
_"Like Sidekiq: convention over configuration, simple API, powerful internals."_
The entire user workflow is declarative:

```ts
class SendEmailJob extends Job {
  jobName = "send_email"; // identity — how queued payloads find this class
  queueName = "mailers"; // routing
  every = "5m"; // optional recurrence (or `cron = "0 9 * * 1-5"`)
  timeout = "30s"; // optional execution deadline
  priority = 5; // optional backend priority

  async perform(jobBody: unknown): Promise<void> {/* ... */}
}
```

No decorators, no per-job registration calls, no config objects. A job declares
_what it is_ via instance properties; the framework handles payload
construction, routing, and dispatch. Preserve this: any new job-level capability
should be a declared property or a `performLater` option, not a new registration
API.

### 2. The manifest is the registry

Hermes never scans directories. The worker receives **one module path** whose
default export (or named `jobs` export) is an array of job class constructors:

```ts
// jobs/main.ts
export default [SendEmailJob, HealthCheckJob];
```

The manifest is the single source of truth for the worker: it defines which jobs
exist _and_ which queues the worker listens to (queue names are derived from the
loaded classes). Don't add auto-discovery or alternative registration
mechanisms; extend the manifest convention instead.

**History lesson:** early in the project the manifest path was briefly hardcoded
(ignoring the caller-supplied path) and that was later explicitly reverted as a
bug. The path must remain caller-configurable, resolved relative to `Deno.cwd()`
(absolute paths and `http(s)://`/`file://` URLs pass through).

### 3. `BackendAdapter` is the only extension point

The core (Job, Worker, Hermes, loaders) depends exclusively on the
`BackendAdapter` interface — never on Deno KV, BullMQ, or any concrete queue.
This was the project's defining architectural refactor: `performLater()`
originally called `Deno.openKv()` directly, and that coupling was deliberately
removed in a breaking release.

The contract lives in `src/lib/backend.ts` (authoritative source; shape at the
time of writing):

```ts
export interface BackendAdapter {
  enqueue(payload: JobPayload, options?: EnqueueOptions): Promise<void>;
  listen(
    handler: (payload: JobPayload) => Promise<void>,
    options?: { queueNames?: string[]; concurrency?: number },
  ): Promise<void>;
  close(options?: { force?: boolean }): Promise<void>;
  registerRecurringJob?(config: RecurringJobConfig): Promise<void>; // optional capability
  removeRecurringJob?(jobName: string): Promise<void>; // optional capability
  getQueueStats?(queueName: string): Promise<QueueStats>; // optional capability
}
```

Rules that follow from this:

- New queue features (retries, priorities, retention, rate limits…) enter as
  **backend capabilities** — optional methods or backend factory options — never
  as core logic. The core stays a thin dispatcher.
- Optional methods model capability differences between backends (Deno KV can't
  do everything BullMQ can). Feature-detect with `if (backend.method)`, as
  `hermes.ts` does for recurrence.
- Keep the interface payload-centric: backends receive the raw `JobPayload` and
  know nothing about the `Job` class hierarchy.

### 4. The root entry point stays npm-free

The root export (`src/main.ts`) must never import `bullmq` or any npm package.
BullMQ lives behind the export-map subpath `@dafu/hermes/backends/bullmq`
precisely so Deno KV users never pull the npm dependency tree (bullmq brings
ioredis, msgpackr, etc.). Any backend with third-party deps gets the same
treatment: its own `./backends/<name>` subpath, invisible from the root.

### 5. Small API, factories over classes, explicit types

- The public surface is deliberately tiny. Internals (`Worker`, `JobLoader`,
  `ManifestLoader`, `Logger`, the backend registry) are not exported and must
  stay private. `src/main.ts` is the authoritative export list; treat every
  addition to it as a permanent API commitment.
- Public constructors are **factory functions returning interface types**
  (`Hermes()`, `DenoKvBackend()`, `BullMQBackend()`), hiding the concrete
  classes. `Job` is the one exception because users must subclass it.
- All public functions carry explicit return types — this is a JSR "slow types"
  requirement, not a style preference. Never rely on inference in exported
  signatures.

### 6. Reliability is backend-owned; the core validates early and stays honest at runtime

- **Fail fast at startup:** invalid manifests, duplicate `jobName`s, and a job
  with both `every` and `cron` throw during `start()`.
- **Tolerate at runtime:** a queued payload whose `jobName` has no registered
  class is logged and _skipped_ (acknowledged, not retried — it's likely an
  obsolete deploy artifact). Errors thrown by `perform()` are logged with
  duration and **rethrown to the backend**, so the backend's own retry/failure
  machinery decides what happens next.
- The core intentionally has **no retry counts, backoff, or dead-letter logic**.
  Resist adding them; expose the backend's equivalents through backend options
  instead.
- The core may enforce a configured execution deadline so a stuck `perform()`
  does not permanently consume a worker slot. It aborts the per-execution
  `JobContext.signal`, logs and rethrows; storage, attempts, and recovery remain
  backend-owned.

### 7. One global backend, tiny enqueue API

`backend_registry.ts` holds a single module-global `BackendAdapter`, set by
`Hermes()` construction or by `configure({ backend })` for enqueue-only
processes (web servers). This is a conscious Sidekiq-like tradeoff:
`performLater()` needs no handle, at the cost of one backend per process. Don't
"fix" this into multi-backend DI without an explicit design decision — the
simple enqueue call is a core feature.

### 8. Structured JSON logging, machine-first

Every log line is `console.log(JSON.stringify(event))` with an ISO timestamp and
an event name (`job_started`, `job_completed`, `job_failed` with `duration_ms`,
…). The `Logger` is static and internal — not injectable, not configurable.
Tests assert on parsed log output; treat log event shapes as a semi-public
contract.

### 9. Naming: camelCase identifiers, snake_case filenames

Public identifiers follow TypeScript norms (`performLater`, `jobName`); files
keep Ruby-style snake_case (`job_loader.ts`, `deno_kv.ts`). Both are deliberate
— the codebase was intentionally migrated to camelCase identifiers while
filenames were explicitly kept snake_case. Match this in new files.

## Architecture

```text
ENQUEUE PATH (any process)
  job.performLater(body, { delay, priority }?)
    → builds JobPayload { jobName, queueName, jobBody, metadata? }
    → global backend registry → BackendAdapter.enqueue()

WORKER PATH (worker process)
  Hermes({ manifest, backend })      // sets global backend at construction
  .start()
    → ManifestLoader: dynamic-import manifest, validate exported array
    → JobLoader: new each class once → Map<jobName, constructor> (dupes throw)
    → validate worker configuration and resolve every job timeout
    → derive unique queueNames from classes
    → if backend.registerRecurringJob: register every/cron jobs
    → Worker.start() → backend.listen(handler, { queueNames, concurrency })
        handler: look up class by payload.jobName
          → unknown: log + skip
          → known:  new JobClass().perform(jobBody, { signal })
                    // timeout/other errors logged + rethrown
  .stats() → optional backend.getQueueStats() for every manifest queue
  .stop() → graceful backend.close(), then close({ force: true }) on deadline
```

### Module map (`src/lib/`)

| File                  | Responsibility                                                             |
| --------------------- | -------------------------------------------------------------------------- |
| `backend.ts`          | The adapter contract + enqueue, recurrence, and queue-stats types          |
| `backend_registry.ts` | Module-global backend: `setBackend`/`getBackend`/`clearBackend` (internal) |
| `types.ts`            | `JobPayload`, `HermesParams`, `WorkerConfig`, `PerformLaterOptions`, …     |
| `job.ts`              | Abstract `Job` class: `performLater()`, recurrence helpers                 |
| `manifest_loader.ts`  | Resolves + dynamically imports the manifest, validates the export          |
| `job_loader.ts`       | Builds `Map<jobName, constructor>`, rejects duplicates                     |
| `worker.ts`           | Dispatch loop: payload → class lookup → `perform()`, lifecycle logging     |
| `hermes.ts`           | Facade: `Hermes()` factory, `configure()`, orchestrates startup/shutdown   |
| `schedule.ts`         | `every`-string parsing (`"5m"`, `"1h"`…), cron conversion/validation       |
| `logger.ts`           | Static JSON logger                                                         |
| `backends/deno_kv.ts` | Deno KV adapter (lazy `Deno.openKv`, `Deno.cron` recurrence)               |
| `backends/bullmq.ts`  | BullMQ adapter (Queue/Worker per queue name, job schedulers)               |

## Public API

The authoritative export list is `src/main.ts` (root) and the `exports` map in
`deno.json` (subpaths). The stable user-facing touchpoints are:

- `Job` — abstract class users subclass; provides
  `performLater(jobBody?, opts?)`.
- `Hermes({ manifest, backend, ... }) → HermesInstance` — worker facade with
  `start()` / `stop()` / optional-backend `stats()`.
- `configure({ backend })` — backend registration for enqueue-only processes.
- `DenoKvBackend(options?)` — bundled zero-dependency backend.
- `BullMQBackend(options)` — from `@dafu/hermes/backends/bullmq` only.
- Schedule utilities (`parseEveryInterval`, `intervalToMs`,
  `validateCronExpression`) and the supporting types (`BackendAdapter`,
  `JobContext`, `JobPayload`, `QueueStats`, `HermesParams`, …).

**Deliberately not exported:** the internals listed in principle 5 (worker,
loaders, logger, backend registry) and all concrete implementation classes. When
something feels missing from the API, first check whether exposing it would
violate principles 3–5.

**Typing stance:** payloads are `unknown`, not generic.
`perform(jobBody: unknown, context?: JobContext)` and README examples tell users
to cast/validate. `JobContext` currently supplies an `AbortSignal` for
cooperative timeout cancellation. The framework types the transport envelope and
lifecycle, not the coupling between a specific job and its payload. A generic
`Job<TPayload>` would be a real design change — plausible someday, but do it
deliberately, keeping JSR slow-type constraints in mind.

## Job-model rules (constraints agents must not break)

- Job classes **must be constructible with zero arguments** and constructors
  should be cheap and side-effect-free — startup may instantiate each class
  several times (name loading, queue derivation, recurrence detection). There is
  no constructor injection; each queued payload also gets a **fresh instance**.
- `jobName` must be unique per manifest. BullMQ uses `hermes:${jobName}` as its
  recurring-scheduler identity; Deno KV uses a readable, stable-hash-suffixed
  cron name capped at 64 characters and validates all derived names before any
  registration. Renaming a job orphans its old schedule.
- `every` strings accept any positive integer with units `s|m|h|d`, as long as
  conversion to milliseconds is a safe integer. BullMQ supports all such
  intervals. Deno KV only supports cron intervals that preserve true elapsed
  cadence: minute values dividing 60, hour values dividing 24, and exactly one
  day. It converts exact multiples upward (`120s` → `2m`) and rejects values
  such as `7m`, `5h`, `2d`, `90s`, `90m`, and `25h`. A job may declare `every`
  **or** `cron`, never both.
- Delays are **milliseconds**, passed through unchanged to the backend.
- Priorities are passed through unchanged; BullMQ uses values `1..2^21` with
  lower values first, while Deno KV ignores them.
- A per-job `timeout` overrides `worker.defaultJobTimeout`; absent both, jobs
  retain unlimited execution time. Timeout errors are detected by
  `error.name === "JobTimeoutError"` because the class stays internal. Resolved
  timeouts and `worker.gracefulShutdownTimeout` cannot exceed `2_147_483_647`ms
  (about 24.8 days), the JavaScript timer ceiling.
- BullMQ queue stats report `waiting` as the ready backlog (ordinary waiting
  plus prioritized jobs) and expose the raw prioritized subset separately.
- The global backend is set at `Hermes()` **construction** (not `start()`), and
  `stop()` does **not** clear it.

## Testing

- Run with `deno task test` (see `deno.json` for the exact flags). The suite
  needs `-A` (it creates temp dirs and probes Redis over TCP) and
  `--unstable-kv` plus `--unstable-cron` (Deno KV and local recurrence are
  behind flags).
- Unit suites (`src/lib/tests/*_test.ts`) use `Deno.test` + `t.step()`, with
  `helpers/mock_backend.ts` (an in-memory `BackendAdapter` that records enqueues
  and recurring registrations, and exposes `process()` to hand-feed the
  handler), `helpers/test_jobs.ts`, and manifest fixtures covering both valid
  export forms and the invalid ones.
- Integration tests hit real backends: Deno KV against a temp-dir KV store
  (cleaned up after); BullMQ against local Redis, **self-skipping** when Redis
  is unreachable and using randomized queue names, with test sanitizers disabled
  for the long-lived connections. Keep new backend tests in this style: real
  backend, self-skip when infra is absent, no cross-run collisions.
- New features should protect their invariants at the same level the current
  suite does: loaders/validation at unit level, backend behavior at integration
  level, observable log shapes asserted where relevant.

## Tooling, CI, release

- `deno.json` defines all tasks (test/lint/fmt/check plus example runners) and
  is the source of truth for version and dependencies.
- CI lives in `.github/workflows/`. Run `deno task check` and `deno task test`
  before considering a change done; run `deno fmt` on files you touch.
- Publishing is JSR trusted publishing via `deno publish`, triggered by a
  **published GitHub release** — bumping the version in `deno.json` alone
  releases nothing. While pre-1.0, breaking changes bump the minor version.

## Gotchas (quick list)

- Local Deno KV needs `--unstable-kv`, and Deno KV recurrence additionally needs
  `--unstable-cron`; the BullMQ examples/tests need Redis on `localhost:6379`
  (tests skip silently without it).
- Import BullMQ **only** from `@dafu/hermes/backends/bullmq` — it's absent from
  the root export on purpose.
- Relative manifest paths resolve against `Deno.cwd()`, not the caller's module
  — launching the same script from another directory breaks it. Manifest import
  is a real dynamic import: top-level code in the manifest executes, and module
  caching applies.
- If a manifest has both a default and a named `jobs` export, the default wins.
- Queue routing comes from each job class's `queueName`, on both sides: a
  producer whose class declares a different `queueName` than the consumer's
  class will enqueue to a queue nobody listens to.
- Deno KV is a single global queue — it cannot filter by queue name; per-queue
  workers and concurrency settings are BullMQ capabilities.
- Unknown-job payloads are acknowledged and dropped (after a log); failing known
  jobs are rethrown to the backend. Those are different reliability outcomes —
  keep the distinction.
- The manifest-not-found friendly error matches on Deno's `"Module not found"`
  message text; Deno wording changes can silently bypass it.

## How to make aligned decisions (checklist for agents)

Before designing a change, ask:

1. **Does the happy path stay declarative?** New capabilities should read as job
   properties, factory options, or `performLater` options — not new APIs to
   call.
2. **Is this core or backend?** Anything about _how_ jobs are stored, retried,
   scheduled, or cleaned up belongs behind `BackendAdapter` (optional method or
   factory option). Core only validates, routes, and logs.
3. **Does the root entry stay npm-free and small?** New heavyweight backends get
   their own export subpath. New exports are commitments — default to not
   exporting.
4. **Fail fast or tolerate?** Configuration/registration mistakes → throw at
   `start()`. Runtime queue anomalies → log, and either skip (unknown work) or
   rethrow to the backend (failing work).
5. **Explicit public types?** Every exported symbol needs explicit type
   annotations (JSR slow types), interface returns from factories, snake_case
   filename, camelCase identifiers.
6. **Tested at the right level?** Unit with `MockBackend`, integration against
   the real backend with self-skip, log shapes asserted if you touch lifecycle
   events.
7. **Does it close a known gap consistently?** Check
   [KNOWN_GAPS.md](./KNOWN_GAPS.md) — prefer finishing a half-built seam over
   adding a parallel one, and update README, examples, and KNOWN_GAPS.md when
   behavior changes.
