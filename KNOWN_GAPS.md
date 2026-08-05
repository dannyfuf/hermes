# KNOWN_GAPS.md — Hermes

> Living snapshot of known gaps, inconsistencies, and pending work. Unlike
> [CONTEXT.md](./CONTEXT.md) (durable philosophy and conventions), this file is
> **expected to go stale and be updated**: when you close a gap, remove it from
> here; when you discover one, add it. Don't rediscover items on this list, and
> when working nearby, prefer closing one over working around it.
>
> Last reviewed: 2026-08-05, `main` @ `5ab955a` (v0.2.1).

## Documented or typed but not implemented

Wire these up or descope them — don't build parallel mechanisms next to them.

- `HermesParams.worker` (`WorkerConfig`) is accepted but never read —
  `concurrency` and `gracefulShutdownTimeout` are inert. BullMQ concurrency only
  works via `BullMQBackend({ concurrency })`.
- Graceful shutdown with timeout is described in the README, but `stop()` just
  awaits `backend.close()` with no timeout handling.
- `JobPayload.metadata` and `EnqueueOptions.queueName` exist but are never
  populated or read (routing always comes from `payload.queueName`).
- `removeRecurringJob?()` is in the `BackendAdapter` contract and implemented by
  the test mock, but no concrete backend implements it and Hermes never calls it —
  recurring-schedule cleanup is an open problem (renamed jobs orphan their
  `hermes:${jobName}` schedulers).
- `validateCronExpression()` is public but never invoked by the framework, and only
  checks field count and permitted characters, not ranges or cron semantics.
- Recurring jobs registered from class declarations always get
  `jobBody: undefined` — there is no way to attach a body to a schedule.
- Several `Logger` events (`worker_stopping`, `worker_stopped`, `job_skipped`,
  `recurring_job_skipped`, `info`/`warn`/`error`) and the corresponding README
  log-table entries have no emission path.

## Known inconsistencies (docs / examples / code)

- Module JSDoc in `src/main.ts` and `src/lib/backends/bullmq.ts` shows
  `manifest: "./jobs/manifest.json"` — wrong; the manifest is a TS/JS module
  exporting constructors, as the README and `example/` correctly show.
- README says BullMQ "supports retries / all BullMQ features," but the adapter only
  exposes connection, queue name, concurrency, and delay — no attempts/backoff
  options.
- Deno KV ignores `listen({ queueNames })` — it has a single global KV queue. The
  README's per-queue-worker claims apply to BullMQ only; broader "process queues
  independently" language overstates Deno KV behavior.
- The repo's worker examples (`example/worker.ts`, `example/worker_redis.ts`) don't
  install signal handlers or call `hermes.stop()`; the README's worker example does.
- `example/worker.ts` passes `worker: { gracefulShutdownTimeout: 1000 }`, which has
  no runtime effect (see inert `WorkerConfig` above).

## Tooling issues

- `deno task dev` targets a repository-root `main.ts` that doesn't exist (the entry
  point is `src/main.ts`; runnable apps live in `example/`).
- CI does not run `deno lint` or `deno fmt --check`, and there is formatting drift
  in several checked-in files (`deno fmt --check` fails on ~8 files). Format files
  you touch; formatting the whole repo will produce unrelated diffs.
- BullMQ integration tests use unique queue names but never delete queues or
  recurring schedulers, so test Redis instances accumulate state.
- The missing-manifest test only asserts that *some* `Error` is thrown, not the
  friendly "Job manifest not found" message; the error translator matches Deno's
  `"Module not found"` wording, so upstream message changes could silently bypass
  it without failing tests.

## Pending / apparent next steps

- Unmerged branch `fix/bullmq-job-retention` adds BullMQ `defaultJobOptions` with
  bounded retention (`removeOnComplete: { count: 1000 }`,
  `removeOnFail: { count: 5000 }`) for both ordinary jobs and scheduler templates,
  plus an integration test. This is the clearest pending change — unbounded Redis
  job accumulation is a real, observed concern on `main`.
- Natural follow-ups implied by the gaps above: wire up `WorkerConfig`, implement
  graceful shutdown, expose BullMQ retry options (attempts/backoff), and design
  recurring-schedule cleanup (`removeRecurringJob`).
