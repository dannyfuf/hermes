# KNOWN_GAPS.md — Hermes

> Living snapshot of known gaps, inconsistencies, and pending work. Unlike
> [CONTEXT.md](./CONTEXT.md) (durable philosophy and conventions), this file is
> **expected to go stale and be updated**: when you close a gap, remove it from
> here; when you discover one, add it. Don't rediscover items on this list, and
> when working nearby, prefer closing one over working around it.
>
> Last reviewed: 2026-08-05, `main` @ `7496257` (0.3.0 working tree).

## Documented or typed but not implemented

Wire these up or descope them — don't build parallel mechanisms next to them.

- `JobPayload.metadata` exists but is never populated or read.
- `removeRecurringJob?()` is in the `BackendAdapter` contract and implemented by
  the test mock, but no concrete backend implements it and Hermes never calls it
  — recurring-schedule cleanup is an open problem (renamed jobs orphan their
  schedules). Startup pre-validation now prevents Deno cron-name collisions and
  invalid worker concurrency from causing partial registration, but it cannot
  remove schedules left by renames or other later startup failures.
- `validateCronExpression()` is public but never invoked by the framework, and
  only checks field count and permitted characters, not ranges or cron
  semantics.
- Recurring jobs registered from class declarations always get
  `jobBody: undefined` — there is no way to attach a body to a schedule.
- Several `Logger` events (`recurring_job_skipped`, `info`/`warn`) and the
  corresponding README log-table entries have no emission path.

## Tooling issues

- `deno task dev` targets a repository-root `main.ts` that doesn't exist (the
  entry point is `src/main.ts`; runnable apps live in `example/`).
- CI does not run `deno lint` or `deno fmt --check`, and there is formatting
  drift in several checked-in files (`deno fmt --check` fails on ~8 files).
  Format files you touch; formatting the whole repo will produce unrelated
  diffs.
- BullMQ integration tests use unique queue names but never delete queues or
  recurring schedulers, so test Redis instances accumulate state.
- The missing-manifest test only asserts that _some_ `Error` is thrown, not the
  friendly "Job manifest not found" message; the error translator matches Deno's
  `"Module not found"` wording, so upstream message changes could silently
  bypass it without failing tests.

## Pending / apparent next steps

- There is no failed-job inspection or retry API (list failed jobs, retry one,
  drain failures). Consumers currently read BullMQ Redis keys or use BullMQ
  directly. Optional `getFailedJobs?` / `retryJob?` backend capabilities are the
  natural seam.
- Recurring-schedule cleanup remains open: design how Hermes should use
  `removeRecurringJob?()` without deleting schedules owned by another
  deployment.

## Reliability constraints (intentional, not gaps)

- Local Deno KV recurrence requires `--unstable-cron` alongside `--unstable-kv`;
  Hermes derives a stable-hashed, 64-character-capped registration name from
  `jobName` and rejects a manifest-wide derived-name collision before
  registering any cron.
- A failed `start()` permanently consumes that instance's startup attempt;
  callers must construct a new instance because durable schedule registration
  may already have partially succeeded.
- Timed-out JavaScript can keep running if it ignores `JobContext.signal` and
  can overlap a backend retry. Graceful shutdown tracks those bodies until its
  deadline; force shutdown does not wait for them.
- After the configurable graceful shutdown deadline, Hermes waits at most five
  additional seconds for force-close before returning.
- Job execution and graceful-shutdown timeouts are capped at `2_147_483_647`ms
  (about 24.8 days), matching the reliable JavaScript timer ceiling.
- BullMQ queue stats treat prioritized jobs as part of the `waiting` backlog and
  expose their raw count as optional `counts.prioritized`.
- Deno KV queue filtering rejects mismatched deliveries from its global queue.
  Native retry may redeliver the job to a matching listener, but routing is
  best-effort. If no listener accepts it, Deno KV only persists the exhausted
  message when it was enqueued with `keysIfUndelivered`; Hermes does not expose
  that native enqueue option yet.
