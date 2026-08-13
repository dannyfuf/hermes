---
name: hermes
description: Guide for building on @dafu/hermes, the Deno-first background-job library (Sidekiq-style). Use when creating or modifying job classes, the job manifest, enqueueing (performLater, delays, priorities, metadata), configuring backends (Deno KV or BullMQ/Redis), recurring schedules (every/cron), timeouts and cancellation, retries, graceful shutdown, queue stats, observability hooks (enqueueMetadata/aroundPerform), or the logger sink — or when debugging why a job does not run, retries unexpectedly, or logs oddly.
---

# Working with @dafu/hermes

Applies to `@dafu/hermes` 0.3.0. Install: `deno add jsr:@dafu/hermes`.

## Mental model

Sidekiq for TypeScript/Deno: convention over configuration. Jobs are classes
declaring what they are via instance properties; a single manifest module is the
registry; all queue mechanics (storage, retries, scheduling) live behind a
pluggable backend. Two processes exist in a typical app:

- **Worker process**: `Hermes({ manifest, backend, worker?, hooks?, logger? })`
  → `start()`. Loads the manifest, derives queues, registers recurring jobs,
  listens, and dispatches.
- **Enqueue-only process** (web server):
  `configure({ backend, hooks?, logger? })` once at boot, then
  `new SomeJob().performLater(body)` anywhere. No worker.

One backend per process (module-global registry — deliberate design; do not try
to run two backends in one process).

## Defining a job

```ts
import { Job } from "@dafu/hermes";
import type { JobContext } from "@dafu/hermes";

export class SendEmailJob extends Job {
  jobName = "send_email"; // identity: how payloads find this class. UNIQUE per manifest.
  queueName = "mailers"; // routing. Producer and consumer classes MUST agree.
  timeout = "30s"; // optional execution deadline ("30s" or ms number)
  priority = 5; // optional; BullMQ only (1..2^21, LOWER runs first)
  // every = "5m";        // optional recurrence — never together with cron
  // cron = "0 9 * * 1-5";

  async perform(jobBody: unknown, ctx?: JobContext): Promise<void> {
    const { to, subject } = jobBody as { to: string; subject: string };
    // honor ctx?.signal for cancellation; read ctx?.metadata for tracing
  }
}
```

Rules that break at runtime if violated:

- Job classes must be constructible with **zero arguments**; constructors must
  be cheap and side-effect-free (startup instantiates each class several times,
  and every delivery gets a fresh instance). No constructor injection — pass
  data through `jobBody`.
- `jobBody` must be JSON/structured-clone serializable. It is typed `unknown` on
  purpose: cast and validate inside `perform`.
- Duplicate `jobName`s throw at `start()`. Renaming a recurring job orphans its
  old schedule (no cleanup API yet).
- Declaring both `every` and `cron` throws at `start()`.

## The manifest

One module exporting an array of job classes — `default` export or named `jobs`
(if both exist, `default` wins):

```ts
// jobs/main.ts
export default [SendEmailJob, DailyReportJob];
```

- The manifest defines which jobs exist AND which queues the worker listens to
  (derived from the classes' `queueName`s).
- Relative manifest paths resolve against `Deno.cwd()`, not the calling module —
  launching the worker from another directory breaks it. Absolute paths and
  `file://`/`https://` URLs pass through.
- The import is a real dynamic import: top-level code in the manifest runs.
- No directory scanning, no decorators, no per-job registration calls exist. To
  add a job: create the class, add it to the manifest array.

## Enqueueing

```ts
await new SendEmailJob().performLater(
  { to: "a@b.c", subject: "hi" }, // jobBody (optional)
  {
    delay: 5 * 60 * 1000, //          milliseconds, not seconds
    priority: 1, //                   overrides the class default (BullMQ only)
    metadata: { traceId, requestedBy }, // opaque; delivered to ctx.metadata
  },
);
```

- Requires a configured backend first (`Hermes()` construction or
  `configure()`); otherwise it throws "No backend configured".
- Enqueueing from inside another job's `perform()` works with no extra wiring
  (job → job chains) and is stamped by `enqueueMetadata` if configured.
- If a registered `enqueueMetadata` hook throws, `performLater` rejects and
  **nothing is enqueued** — handle it like any other caller-side error.

## Backends

### Deno KV — zero infra, Deno Deploy compatible

```ts
import { DenoKvBackend } from "@dafu/hermes";
const backend = DenoKvBackend(); // or { path: "./data.sqlite" } locally
```

- Local runs need `--unstable-kv`; recurring jobs additionally need
  `--unstable-cron`. Neither is needed on Deno Deploy.
- **One global queue**, filtered by `queueName` after delivery. Routing to a
  specific listener is best-effort; a payload for a queue nobody listens to is
  rejected for native retry and eventually dropped.
- **Priorities are ignored** (accepted, no-op). **No queue stats** —
  `hermes.stats()` rejects.
- Retries: Deno KV's native queue retry redelivers when `perform` throws (fixed
  backoff schedule, then the message is dropped — no dead-letter).
- Enqueued messages are durable in the store: they survive process restarts.

### BullMQ / Redis — production queues

```ts
// ONLY from the subpath — bullmq is deliberately absent from the root export:
import { BullMQBackend } from "@dafu/hermes/backends/bullmq";

const backend = BullMQBackend({
  connection: { host: "localhost", port: 6379 }, // or { url }
  concurrency: 5, //            per-queue worker concurrency (default 1)
  defaultQueueName: "default",
  defaultJobOptions: {
    attempts: 3, //             total attempts (default 1 = no retries!)
    backoff: { type: "exponential", delay: 1000 },
    removeOnComplete: { count: 1000 }, // defaults are bounded on purpose
    removeOnFail: { count: 5000 },
  },
});
```

- `worker.concurrency` on `Hermes()` overrides the backend's `concurrency`.
- Retention: completed/failed jobs are trimmed to 1000/5000 by default to
  prevent unbounded Redis growth. Explicit `false`/`0` override; properties set
  to `undefined` are ignored (they cannot erase the bounded defaults).
- Independent worker per queue; real priorities; `hermes.stats()` supported.

## Recurring jobs (`every` / `cron`)

`every` format: `[positive integer][unit]` with units `s | m | h | d` (`"30s"`,
`"5m"`, `"1h"`, `"7d"`). `cron`: standard 5- or 6-field expression.

**Backend support differs — this is the #1 recurring-job pitfall:**

| Interval             | BullMQ | Deno KV                                |
| -------------------- | ------ | -------------------------------------- |
| any positive `every` | ✅     | only cadence-preserving values (below) |
| `cron` expressions   | ✅     | ✅ (Deno.cron syntax)                  |

Deno KV converts `every` to a cron schedule, so it only accepts values whose
true elapsed cadence survives: **minute values that divide 60** (1, 2, 3, 4, 5,
6, 10, 12, 15, 20, 30), **hour values that divide 24** (1, 2, 3, 4, 6, 8, 12),
and **exactly `1d`**. Exact multiples convert upward (`120s` → `2m`, `24h` →
`1d`). Everything else — `7m`, `90s`, `90m`, `5h`, `2d`, `25h` — **rejects at
`start()`**. Minimum effective granularity on Deno KV is 1 minute (`30s` is
invalid there); BullMQ handles seconds fine.

Facts to rely on:

- Registration happens during `start()` when the backend supports it.
- Scheduled ticks always run with `jobBody: undefined` and
  `ctx.metadata === undefined` — there is no way to attach a body or metadata to
  a schedule. If a recurring job needs parameters, read them from config or have
  the tick enqueue parameterized child jobs via `performLater`.
- A recurring tick that fails or times out does not wedge the schedule; the next
  tick still fires.

## Timeouts and cancellation (resilience config #1)

**Always set a timeout in production** — without one, a permanently pending
`perform()` occupies a worker slot forever.

```ts
const hermes = Hermes({
  manifest: "./jobs/main.ts",
  backend,
  worker: {
    concurrency: 5,
    defaultJobTimeout: "2m", // per-job `timeout` property overrides this
    gracefulShutdownTimeout: 10_000, // ms; default 30_000
  },
});
```

- On timeout: the job fails with `JobTimeoutError` (detect via
  `error.name === "JobTimeoutError"` — the class is not exported), the context
  signal aborts, the error is logged and rethrown to the backend (BullMQ retries
  it if `attempts` allow).
- Cancellation is **cooperative**: timed-out JS keeps running unless `perform`
  honors `ctx.signal` (pass it to `fetch`, check `signal.aborted` in loops, or
  listen for `"abort"`). Ignoring it risks overlapping a retry.
- Timeouts and `gracefulShutdownTimeout` are capped at `2_147_483_647` ms (~24.8
  days). Delay time does NOT consume the execution timeout budget.

## Reliability model (what the core does and does not do)

- **Fail fast at startup**: invalid manifest, duplicate `jobName`, both
  `every`+`cron`, invalid timeouts/concurrency, KV-unsupported intervals — all
  throw from `start()`.
- **Tolerate at runtime**: a payload whose `jobName` has no class is logged
  (`unknown_job`) and **skipped/acknowledged** — not retried (it's likely an old
  deploy's artifact). A throwing `perform` is logged (`job_failed`) and
  **rethrown to the backend**, which owns retries.
- The core has **no retry counts, backoff, or dead-letter logic** — configure
  retries on the backend (BullMQ `attempts`/`backoff`; KV native schedule). Do
  not build retry loops inside `perform`; throw and let the backend drive.
- Design `perform` to be **idempotent**: at-least-once delivery means retries
  and (after ignored aborts) overlaps can happen on every backend.
- A failed `start()` consumes the instance — create a new `Hermes()` to retry. A
  closed backend is terminal — construct a new backend, never reuse.
- `stop()` drains in-flight jobs up to `gracefulShutdownTimeout`, then force
  closes (waiting at most ~5s more). Wire it to signals:

```ts
const shutdown = async () => {
  await hermes.stop();
  Deno.exit(0);
};
Deno.addSignalListener("SIGINT", shutdown);
Deno.addSignalListener("SIGTERM", shutdown);
```

## Observability

### Metadata (correlation IDs, tracing context)

- Per-call: `performLater(body, { metadata: {...} })` → delivered verbatim as
  `ctx.metadata`. `undefined` means "no ambient context" (scheduled tick or a
  payload from an older producer) — treat it as start-fresh, never an error.
- Keep metadata small: it rides the payload envelope AND is echoed on the
  `job_received`/`job_started`/`job_succeeded`/`job_failed` log events.

### Hooks (`Hermes({ hooks })`; enqueue side also via `configure({ hooks })`)

```ts
hooks: {
  // Sync. Contribute ambient metadata to every enqueue (e.g. read ALS).
  // Per-call metadata keys win on collision. Throw = enqueue fails, nothing sent.
  enqueueMetadata: () => {
    const ctx = getTraceContext();
    return ctx && { traceId: ctx.traceId, parentSpanId: ctx.spanId };
  },
  // Wraps every execution (worker only). MUST call next() exactly once and
  // SHOULD await it. The job's outcome is next()'s outcome — ALWAYS.
  aroundPerform: async (payload, next) => {
    const span = startSpan(payload.jobName, linksFrom(payload.metadata));
    try {
      await next();
    } catch (err) {
      span.recordException(err);
      throw err; // rethrow is optional — the outcome is protected either way
    } finally {
      span.end();
    }
  },
},
```

`aroundPerform` is **outcome-inert by mechanism** — rely on it:

- Swallowing `next()`'s rejection cannot fake success; the backend still sees
  the failure and retries. (A `hook_error` event flags the swallow.)
- Throwing after `next()` resolved cannot fail the job — no phantom retry.
- Calling `next()` twice returns the same promise; the job runs once.
- Not calling `next()` fails the job with
  `aroundPerform completed without invoking next()`.
- Wrapper time counts against the job's timeout; keep wrappers thin. No
  middleware framework exists — compose around-functions with plain function
  composition.
- Registration replaces: every `Hermes()`/`configure()` call overwrites hooks
  and logger; omitting them **clears** previous ones. Last call wins per
  process. `configure()` accepts only `enqueueMetadata` (by type) — there is
  nothing to wrap in an enqueue-only process.

### Logger sink (`logger` param on both `Hermes()` and `configure()`)

Default: every event is `console.log(JSON.stringify(event))`. To redirect:

```ts
logger: (event: LogEvent) => posthog.capture("hermes_log", event),
```

- Called **synchronously**; return value ignored. An async transport must do its
  own buffering/flushing — never make Hermes await I/O.
- Guarded: if the sink throws, the event falls back to console plus one
  `logger_error` breadcrumb; dispatch is never affected.
- Errors in events are **strings**. Need the real `Error` (stack, cause)?
  Observe it in `aroundPerform` via `next()`'s rejection.
- Key events to alert on: `job_failed`, `worker_job_failed` (BullMQ attempt
  failure with `attemptsMade`), `job_stalled`, `hook_error`, `logger_error`,
  `unknown_job`, `worker_force_closed`. Lifecycle: `worker_started`,
  `job_received`, `job_started`, `job_succeeded` (`durationMs`),
  `worker_stopping`, `worker_stopped`, `recurring_job_registered`, `job_skipped`
  (KV queue filtering).

## Queue health (BullMQ only)

```ts
const stats = await hermes.stats(); // after start(); one entry per queue
// { queueName, counts: { waiting, active, delayed, failed, completed,
//   prioritized? }, oldestActiveJobAgeMs? }
```

`waiting` includes prioritized jobs; `oldestActiveJobAgeMs` spiking usually
means stuck jobs — check timeouts. On Deno KV `stats()` rejects: guard calls.

## Pitfall checklist (verify before shipping changes)

1. Producer and consumer share the same job class (or identical
   `jobName`/`queueName`) — a mismatched `queueName` enqueues into the void.
2. Delays and numeric timeouts are **milliseconds**.
3. BullMQ default is `attempts: 1` — no retries unless you configure them.
4. Deno KV: `every` values must divide 60 (minutes) / 24 (hours) / be `1d`.
5. Worker launched from the directory the manifest path assumes (cwd-based).
6. Every new job class is in the manifest array — nothing is auto-discovered.
7. Long-running `perform` honors `ctx.signal`.
8. `perform` is idempotent under retry.
9. Import BullMQ only from `@dafu/hermes/backends/bullmq`.
10. One `Hermes()`/`configure()` call per process defines hooks/logger — later
    calls replace (and omitted fields clear) earlier registrations.

## Testing jobs you write

- Unit-test `perform` directly:
  `await new MyJob().perform(body, { signal: new AbortController().signal })` —
  jobs are plain classes; no framework needed.
- Integration: use `DenoKvBackend({ path })` pointed at a temp dir for a real,
  zero-infra queue round-trip; or BullMQ against local Redis with randomized
  queue names, self-skipping when Redis is absent.
- Assert on log events by injecting a `logger` sink that pushes into an array —
  cleaner than intercepting `console.log`.
