# Hermes hooks & observability surface — Plan
> Tracker: ./hooks-observability-2026-08-12-tracker.md
> KEEP THE TRACKER UPDATED. The plan is reference; the tracker is truth. Update it before you commit.
>
> **Normative spec:** `/private/tmp/claude-501/-Users-danny-personal-hermes/5fee11ff-dcd5-4976-86d9-00a2cacf3646/scratchpad/hermes-hooks-design.md` — read it end to end before starting. This plan sequences that design; it does not redesign it. Where this plan and the design doc conflict on *semantics*, the design doc wins; where they conflict on *task ordering or file layout*, this plan wins. If the scratchpad file is gone by the time you pick this up, stop and ask for the design doc — do not reconstruct it from this plan alone.

**Locked decisions (design doc §9 open questions — resolved, do not reopen):**

1. **Names:** the hooks are `enqueueMetadata` and `aroundPerform` (not `onEnqueue` / `wrapPerform`).
2. **Matrix row 3 (swallow):** YES — emit `hook_error` when a wrapper resolves after `next()` rejected.
3. **`configure()` accepts `logger`:** YES — both `Hermes()` and `configure()` take the sink.
4. **Abort signal in `aroundPerform`:** NO for v1 — the wrapper receives `(payload, next)` only, no `JobContext`.

## Summary

Hermes (`@dafu/hermes`, Deno-first background-job library on JSR) currently has one extension point, the `BackendAdapter`. This work adds a minimal observability surface — five additive changes: (1) `PerformLaterOptions.metadata`, (2) `JobContext.metadata`, (3) an enqueue-side `hooks.enqueueMetadata` hook, (4) an execution-side `hooks.aroundPerform` wrapper that is **outcome-inert by mechanism** (a buggy hook cannot change whether a job succeeds, fails, or retries — enforced in code via the design doc §5.2 matrix), and (5) an injectable logger sink with guarded fallback and metadata echo. A consumer configuring none of these sees byte-identical behavior, including payloads on the wire. Backends require **zero changes** — if any task here seems to need an edit under `src/lib/backends/`, the implementation is wrong, not the backends.

## Sizing call

**Standard, with five hard internal phase gates.** This is roughly one focused engineer-week in a single repo, touching one cluster of core files (`types.ts`, `job.ts`, `worker.ts`, `logger.ts`, `hermes.ts`, a new registry module, `main.ts`) plus docs — not multi-stretch work, so no roadmap split. I considered phased mode and rejected it: separate per-phase documents would be ceremony for work this size. But the ordering constraint is real — each phase below must leave `deno task check` and `deno task test` green, because the highest-risk piece (the §5.2 outcome-protection matrix in `worker.ts`) should land on top of already-proven plumbing. Task IDs are phase-namespaced (`P1-T01`…) so the gates are explicit in the tracker.

## Repository context

- Deno-first TypeScript library, JSR `@dafu/hermes`, version 0.3.0 **unpublished** — this surface ships inside 0.3.0, so no compat shims for a published API are needed. `deno.json` is the source of truth for version/tasks.
- Repo at commit `17aef78` (verified via `git rev-parse HEAD`) — the exact commit the design doc's §2 facts were verified against. KNOWN_GAPS.md's header says "main @ `3cdb67d`"; that is merely a stale review stamp, not a conflict.
- Commands: `deno task test` (full suite, needs `-A`, `--unstable-kv`, `--unstable-cron`), `deno task check` (type-checks the public entry point), `deno task lint`, `deno fmt <files>` — **format only files you touch**; the repo has known pre-existing formatting drift (~8 files fail `deno fmt --check`).
- Tests live in `src/lib/tests/` (`*_test.ts`, `Deno.test` + `t.step`), with `helpers/mock_backend.ts` (in-memory `BackendAdapter` recording enqueues/recurring registrations, exposing `process()` to hand-feed the listen handler), `helpers/test_jobs.ts`, and `integration/` for real-backend tests.
- **Local Redis is available in this environment (verified: `redis-cli ping` → `PONG`).** The BullMQ integration tests self-skip without Redis, but here they must actually execute — a "pass" via silent skip does not satisfy Phase 5.
- Source anchors re-verified against the tree (design doc §2 line numbers hold within ±1): `PerformLaterOptions` `types.ts:4-7`, `JobContext` `types.ts:10-12`, `JobPayload` `types.ts:15-20` (`metadata?` already typed, never read/written), `Job.performLater` `job.ts:28-43`, worker choke point `worker.ts:77-131`, `Logger.log` hard-wired `console.log(JSON.stringify(event))` `logger.ts:221-223`, module-global backend `backend_registry.ts` (`setBackend`/`getBackend`/`clearBackend`), `configure()` `hermes.ts:286-290`, exports `src/main.ts`.
- README has a `## Logging` section (~line 599) with a log-event table (~line 616) — the `hook_error`/`logger_error` rows go there.
- Required reading per AGENTS.md: `CONTEXT.md` (design philosophy; principles 3 and 8 get amended by this work) and `KNOWN_GAPS.md` (the `JobPayload.metadata` item gets closed by this work).

## Assumptions

Deltas between the design doc and the actual tree, plus decisions the doc leaves unstated. These are resolved here so the executor has one answer; each is also flagged to the plan's requester, so treat them as overridable if the design doc is revised.

- **Type-alias style:** the design doc's §3.1 snippets use `interface`; the actual `types.ts` uses `type` aliases. Keep the existing `type` alias style — the shapes are what's normative, not the keyword. `HermesHooks` may be either form; match file convention and keep JSR slow types happy.
- **`LogEvent` location:** the doc's §2 says `LogEvent` is "not exported"; in the actual tree it is already `export interface LogEvent` at `logger.ts:1` — it is just not re-exported from `main.ts`. So the work is: leave the interface in `logger.ts` unchanged, add a *type-only* re-export from `main.ts` (per doc §7.1 "LogEvent re-exported"). The `Logger` class itself stays private. `LoggerSink` lives in `types.ts` with a type-only import of `LogEvent` from `logger.ts` (type-only cycles are erased; no runtime import cycle).
- **Registration replace semantics:** the doc does not say what happens to previously registered hooks/logger when `Hermes()` or `configure()` is called again without them. Decision: every call **fully replaces** the hooks and logger registration (omitted ⇒ cleared), mirroring the way `setBackend` always overwrites. This is deterministic and testable; document it in the README sentence for `configure()`.
- **`Readonly<JobPayload>` is type-level only** — no runtime `Object.freeze`. The doc says "typed `Readonly<JobPayload>`"; take it literally.
- **CONTEXT.md principle 8 has pre-existing drift** unrelated to this work (it names `job_completed` and `duration_ms`; the code emits `job_succeeded` and `durationMs`). The P4 amendment should change only the injectability sentence per design §7.3 — do not silently "fix" the event names; instead add a follow-up entry in the tracker so it gets addressed deliberately.
- Empty-object `opts.metadata` with no hook produces an empty merge ⇒ no `metadata` key on the payload (empty-result rule applies to per-call metadata too, per §4.1).

## Out of scope

Non-goals, restated from design §1/§8/§9 so nobody "helpfully" adds them:

- Middleware arrays / `hermes.use()` — composition is plain function composition (doc §6.3).
- Control-flow hooks (dedup, rate limiting, skipping) — queue mechanics belong behind `BackendAdapter`.
- Any change under `src/lib/backends/` — zero backend changes is a hard invariant, verified in P5.
- Changes to recurring/scheduled payload construction — the 3-field payload freeze is load-bearing (§4.4/§5.3).
- `JobContext`/abort signal passed to `aroundPerform` (locked decision 4), metadata on `RecurringJobConfig` (deferred per §8).
- Publishing/releasing 0.3.0, and the unrelated KNOWN_GAPS items (`removeRecurringJob`, failed-job APIs, `deno task dev`, CI fmt checks).

## Affected areas

- `src/lib/types.ts` — `metadata` on `PerformLaterOptions` and `JobContext`; new `HermesHooks`, `LoggerSink`; `HermesParams` + `hooks?`/`logger?`.
- `src/lib/hooks_registry.ts` — **new file** (snake_case): module-global hooks + logger sink, set/get/clear.
- `src/lib/hermes.ts` — constructor registers hooks/logger; `configure()` extended.
- `src/lib/job.ts` — `performLater()` runs `enqueueMetadata`, merges, conditionally sets `payload.metadata`.
- `src/lib/worker.ts` — `JobContext.metadata`; `aroundPerform` wrapper with outcome protection (the core of the change, ~30 lines).
- `src/lib/logger.ts` — `log()` routes through the sink with guard; metadata echo on four job events; new `hookError`/`loggerError` emitters.
- `src/main.ts` — three new type exports: `HermesHooks`, `LoggerSink`, `LogEvent`. Nothing else.
- `src/lib/tests/` — new/extended unit suites; `integration/` additions for BullMQ + Deno KV.
- Docs: `README.md`, `CONTEXT.md` (principles 3 and 8), `KNOWN_GAPS.md`.
- **Explicitly untouched:** `src/lib/backends/deno_kv.ts`, `src/lib/backends/bullmq.ts`, `src/lib/backend.ts`, `src/lib/backend_registry.ts` (the new registry is a sibling, not an edit — keeping the backend global's file history clean).

## Tasks

Tasks are grouped into five phases. **Each phase is a gate: `deno task check` and `deno task test` must pass at the end of every phase, and each phase's docs land in the same commits as its code** (per design §7.3 — constitution/doc changes are not a trailing cleanup phase). The §7.5 numbered test items (1–10) are each assigned to exactly one task below.

---

**Phase 1 — Types, registry, and config plumbing (inert).** Introduce every type, the global registry, and the `Hermes()`/`configure()` plumbing, with nothing yet consulting the hooks. Gate: check + test green; zero behavior change (existing suites untouched and passing); new exports type-check under JSR slow-types rules.

### P1-T01 — Add hook and sink types to types.ts
- **Intent:** Land all new/changed public type shapes from design §3.1 in one place.
- **Touches:** `src/lib/types.ts`
- **Steps:**
  - Add `metadata?: Record<string, unknown>` to `PerformLaterOptions` and `JobContext`, with the doc-comment semantics from §3.1 (explicit wins on merge; `undefined` for scheduled runs and pre-hooks payloads).
  - Add `HermesHooks` with `enqueueMetadata?` (sync, takes `Readonly<JobPayload>`, returns `Record<string, unknown> | undefined`) and `aroundPerform?` (takes `Readonly<JobPayload>` and `next: () => Promise<unknown>`, returns `Promise<void>`), carrying the §3.1 doc comments ("MUST invoke next() exactly once", outcome-inert).
  - Add `LoggerSink` as a function type over `LogEvent` (type-only import from `./logger.ts`).
  - Extend `HermesParams` with `hooks?: HermesHooks` and `logger?: LoggerSink`.
  - Keep the file's existing `type`-alias style; every export explicitly typed.
- **Verification:** `deno task check`; `deno task test` (no regressions); `deno fmt src/lib/types.ts`.
- **Done when:** All §3.1 shapes exist, compile, and nothing consumes them yet.

### P1-T02 — Create hooks_registry.ts module-global store
- **Intent:** One module-global home for hooks and the logger sink, mirroring `backend_registry.ts`.
- **Touches:** `src/lib/hooks_registry.ts` (new)
- **Steps:**
  - Store `HermesHooks | null` and `LoggerSink | null` module-globals.
  - Export `setHooks`/`getHooks`/`clearHooks` and `setLoggerSink`/`getLoggerSink`/`clearLoggerSink` with explicit return types; getters return `null`/`undefined` rather than throwing (unlike `getBackend()` — absence of hooks is the normal case).
  - The clear functions exist for test isolation, mirroring `clearBackend()`; the module is internal — not exported from `main.ts`.
- **Verification:** `deno task check`; `deno fmt src/lib/hooks_registry.ts`.
- **Done when:** The registry compiles and is importable by `hermes.ts`, `job.ts`, `worker.ts`, `logger.ts`.

### P1-T03 — Plumb hooks and logger through Hermes() and configure()
- **Intent:** Registration wiring per design §3.2: worker processes get both hooks; enqueue-only processes get `enqueueMetadata` by type.
- **Touches:** `src/lib/hermes.ts`
- **Steps:**
  - `THermes` constructor: alongside `setBackend`, register `params.hooks` and `params.logger` (registration at **construction**, matching the backend's documented timing; `stop()` does not clear, also matching).
  - Extend `configure()` to accept `{ backend, hooks?: Pick<HermesHooks, "enqueueMetadata">, logger? }` — `aroundPerform` excluded by type, per §3.2. Keep the explicit `: void` return type.
  - Both paths use replace semantics: omitted `hooks`/`logger` clears any previous registration (see Assumptions).
- **Verification:** `deno task check`; `deno task test`; `deno fmt src/lib/hermes.ts`.
- **Done when:** `Hermes({ hooks, logger })` and `configure({ backend, hooks, logger })` compile and populate the registry; passing `aroundPerform` to `configure()` is a type error.

### P1-T04 — Export the three new types from main.ts
- **Intent:** Public API delta per design §3.3 — exactly three type exports, nothing else.
- **Touches:** `src/main.ts`
- **Steps:**
  - `export type { HermesHooks, LoggerSink }` from `./lib/types.ts`; `export type { LogEvent }` from `./lib/logger.ts` (type-only — the `Logger` class stays private).
  - Confirm no value exports were added and the root stays npm-free.
- **Verification:** `deno task check` (this is the entry point it checks); `deno fmt src/main.ts`.
- **Done when:** The three types are importable from `@dafu/hermes`'s root and `deno task check` passes slow-types validation.

### P1-T05 — Unit-test registration plumbing and replace semantics
- **Intent:** Pin the wiring before anything consumes it.
- **Touches:** `src/lib/tests/hooks_registry_test.ts` (new), possibly `src/lib/tests/hermes_test.ts`
- **Steps:**
  - Test set/get/clear for hooks and sink; test that `Hermes()` construction and `configure()` both register; test replace-on-omit semantics; ensure every test clears registry state (backend and hooks) on the way out, following the existing `clearBackend()` pattern.
- **Verification:** `deno task test`; `deno fmt` on new test file.
- **Done when:** Phase 1 gate holds: check + full test suite green with the surface present but inert.

---

**Phase 2 — Enqueue path.** `performLater` runs the hook, merges, and conditionally stamps the payload. Gate: §7.5 tests **1, 2, 3** pass; a no-hooks enqueue produces a payload deep-equal to today's (no `metadata` key); backends untouched.

### P2-T01 — Run enqueueMetadata and merge in performLater
- **Intent:** Implement design §4.1 exactly: hook in core, before the adapter boundary, identical for both backends.
- **Touches:** `src/lib/job.ts`
- **Steps:**
  - In `performLater()`, after building the 3-field payload: fetch hooks from the registry; if `enqueueMetadata` is set, call it **synchronously** with the payload (typed `Readonly<JobPayload>`, built without metadata at that point). A throw propagates to the caller — do not catch (§5.4: fail loud, nothing enqueued).
  - Merge hook result under per-call `opts.metadata` (spread hook result first, then opts — explicit wins on collision). Treat `undefined` from either side as an empty contribution.
  - Set `payload.metadata` **only if** the merged object has at least one key (empty-result rule: no-hooks payloads stay byte-identical to v0.2.1).
  - Enqueue options (`delay`, `priority ?? this.priority`) are untouched — the hook cannot reach routing or options.
- **Verification:** `deno task check`; `deno task test`; `deno fmt src/lib/job.ts`.
- **Done when:** Hook-contributed and per-call metadata ride the envelope; absent both, the enqueued payload is key-for-key identical to pre-change.

### P2-T02 — Unit tests for the enqueue path (§7.5 #1, #2, #3)
- **Intent:** Pin wire-compat, merge rules, and the throw-propagation contract.
- **Touches:** `src/lib/tests/job_test.ts` (extend or add sibling)
- **Steps:**
  - **#1** No hooks ⇒ MockBackend-recorded payload deep-equals today's shape, and `"metadata" in payload` is false.
  - **#2** Merge: hook keys present; per-call keys win on collision; empty merge (hook returns `undefined`/`{}`, no opts metadata) ⇒ no `metadata` key; empty `opts.metadata` alone ⇒ no key.
  - **#3** Throwing `enqueueMetadata` rejects the `performLater` call with the hook's error and MockBackend records **zero** enqueues.
  - Also assert the hook received a payload with no `metadata` key (pre-merge view, §4.1).
- **Verification:** `deno task test`; `deno fmt` on touched test files.
- **Done when:** All three numbered items pass and are clearly labeled in test names.

### P2-T03 — README: performLater metadata + enqueueMetadata (same commits as P2 code)
- **Intent:** Document the enqueue half as it lands, not later.
- **Touches:** `README.md`
- **Steps:**
  - Document `performLater(body, { metadata })` (opaque, JSON/structured-clone-serializable — same constraint as `jobBody`) with the §6.4 per-call example shape.
  - Start the hooks section: `enqueueMetadata` semantics — sync, contribute-only, explicit-wins merge, throws fail the enqueue with nothing enqueued (§5.4), registered via `Hermes({ hooks })` or `configure({ hooks })` (enqueue-only processes; note replace-on-omit).
- **Verification:** Proofread rendered Markdown; `deno fmt README.md` if the formatter covers it (skip if it introduces unrelated drift).
- **Done when:** A reader can use per-call metadata and the enqueue hook from the README alone.

---

**Phase 3 — Execution path and outcome protection (highest-risk phase).** `worker.ts` delivers `JobContext.metadata` and wraps execution in `aroundPerform` with the §5.2 matrix enforced by mechanism. Gate: **one unit test per matrix row 1–9** passing; §7.5 tests **4, 5, 6, 8** pass; `git diff --stat src/lib/backends/` is empty; `Logger.jobFailed` + rethrow-to-backend untouched.

### P3-T01 — Deliver JobContext.metadata in the worker
- **Intent:** Change #2 of the design: `payload.metadata`, verbatim, into the context.
- **Touches:** `src/lib/worker.ts`
- **Steps:**
  - When building the context (`worker.ts:93`), include `metadata: payload.metadata` alongside `signal`. `undefined` (scheduled runs, old payloads) passes through as `undefined` — that is the documented fresh-context signal (§4.4).
- **Verification:** `deno task check`; `deno task test`; `deno fmt src/lib/worker.ts`.
- **Done when:** `perform(jobBody, context)` sees `context.metadata` exactly equal to the envelope's, or `undefined`.

### P3-T02 — Implement the aroundPerform wrapper with outcome protection
- **Intent:** The core of the whole change (~30 lines): the job's outcome is exactly `next()`'s outcome; wrapper faults are logged, never outcome-affecting. This is the §5.2 matrix as mechanism.
- **Touches:** `src/lib/worker.ts`, `src/lib/logger.ts` (the `hookError` emitter only)
- **Steps:**
  - Add `Logger.hookError(hook, jobName, queueName, error)` emitting event `hook_error` with fields `hook` (the string `"aroundPerform"`), `jobName`, `queueName`, `error` (stringified via the existing `errorMessage` helper) — §5.5.
  - In the listen handler, fetch hooks from the registry. **No hook configured ⇒ the existing code path runs byte-for-byte unchanged** (same `performPromise` construction, tracking, race).
  - Hook configured — the mechanism, concretely:
    - **Memoized `next`:** Hermes owns `next`. First call flips an `invoked` flag and creates the real perform promise (`Promise.resolve().then(() => job.perform(jobBody, context))`), memoized; any subsequent call returns the **same promise** (matrix row 8 — single execution, ever).
    - **Wrapper invocation:** call `aroundPerform(payload, next)` inside `Promise.resolve().then(...)` so synchronous throws become rejections; capture its settlement separately from next's.
    - **Outcome resolution** — build one composite "execution promise" that: awaits the wrapper's settlement (recording its rejection, if any, without throwing yet); then, if `next` was **never invoked**: emit `hook_error` and fail the job — with the wrapper's own error if it rejected (row 5), or with a new error whose message is exactly `aroundPerform completed without invoking next()` if it resolved (row 6). If `next` **was invoked**: await the memoized next-promise (this is what makes fire-and-forget row 7 correct — Hermes awaits next even after the wrapper resolved); if next rejected, rethrow **next's error** — and if the wrapper had *resolved* (swallow), also emit `hook_error` noting the wrapper resolved after next rejected (row 3, locked decision 2); if next resolved, the job **succeeded** — and if the wrapper had rejected, emit `hook_error` with the wrapper's error but do not fail the job (row 4: no phantom retry/duplicate execution). Rows 1–2 fall out: outcome = next's settlement, wrapper agreeing changes nothing, and on row 2 the error surfaced is always next's, never a substitute the wrapper threw.
    - **In-flight tracking and timeout (row 9):** the composite execution promise takes the place `performPromise` holds today — added to `inFlightPerforms`, removed on settlement, raced against the existing timeout. On timeout: `JobTimeoutError` rejects the race, the controller aborts the signal, and the composite chain (wrapper + perform) keeps running and stays tracked until it settles — unchanged 0.3.0 semantics; wrapper time counts against the job's timeout budget by construction. Note the composite settles only after **both** wrapper and next settle, so graceful shutdown's `awaitInFlight` covers hung wrappers too.
    - `Logger.jobSucceeded`/`jobFailed` and the retry-critical **rethrow to the backend** stay exactly where they are — outside all of this, in the existing try/catch. `job_received`/`job_started` fire before the wrapper (outside any ALS scope it establishes); unknown-job skip stays before the wrapper (§4.2).
  - Keep everything internal; no new exports.
- **Verification:** `deno task check`; `deno task test`; `deno fmt src/lib/worker.ts src/lib/logger.ts`.
- **Done when:** All nine matrix behaviors are mechanically true (proven by P3-T03) and a diff of the no-hook path shows no behavioral change.

### P3-T03 — Unit tests: outcome matrix, one per row (§7.5 #4)
- **Intent:** One test per §5.2 row, 1 through 9 — the invariant that keeps future evolution honest.
- **Touches:** `src/lib/tests/worker_test.ts` (or a dedicated `around_perform_test.ts`)
- **Steps:** Using MockBackend's `process()` to feed payloads, one named test per row:
  - Row 1: well-behaved wrapper, next resolves ⇒ job succeeds, `job_succeeded` logged, no `hook_error`.
  - Row 2: wrapper awaits and rethrows, next rejects ⇒ handler rejects with **next's** error (backend sees the rethrow ⇒ retries intact), no `hook_error`.
  - Row 3: wrapper catches next's rejection and resolves ⇒ handler still rejects with next's error **and** one `hook_error` is emitted (wrapper resolved after next rejected).
  - Row 4: wrapper throws its own error after next resolved ⇒ job **succeeds** (handler resolves; no rethrow to backend), one `hook_error` with the wrapper's error.
  - Row 5: wrapper throws before calling next ⇒ job fails with the wrapper's error, `hook_error` emitted, `perform` never ran.
  - Row 6: wrapper resolves without calling next ⇒ job fails with message `aroundPerform completed without invoking next()`, `hook_error` emitted, `perform` never ran.
  - Row 7: wrapper resolves while next still pending ⇒ Hermes awaits next; outcome equals next's eventual settlement.
  - Row 8: wrapper calls next twice ⇒ same promise instance both times; `perform` executed exactly once.
  - Row 9: extend the existing timeout tests with a wrapper present ⇒ `JobTimeoutError` failure, signal aborted, wrapped chain keeps running and remains tracked in `inFlightPerforms` until settled (assert via `awaitInFlight` behavior), i.e. current 0.3.0 timeout semantics survive the wrapper.
  - Assert `hook_error` presence/absence per row and its fields (`hook: "aroundPerform"`, `jobName`, `queueName`, `error`).
- **Verification:** `deno task test`; `deno fmt` on touched test files.
- **Done when:** Nine row-named tests pass and each failure mode fails the suite if the mechanism regresses.

### P3-T04 — Unit tests: context delivery, scheduled-run pin, job→job chain (§7.5 #5, #6, #8)
- **Intent:** Pin the metadata delivery contract and the two design invariants around it.
- **Touches:** `src/lib/tests/worker_test.ts`, `src/lib/tests/job_test.ts` or sibling
- **Steps:**
  - **#5** Payload with metadata ⇒ `context.metadata` delivered verbatim; payload without ⇒ `context.metadata === undefined`.
  - **#6** Scheduled-run pin (§5.3, a must-NOT-change invariant): register a recurring job via MockBackend, simulate a tick by feeding the 3-field payload the backends build at registration time ⇒ `aroundPerform` and `perform` both see `metadata === undefined`, and `enqueueMetadata` never ran for registration.
  - **#8** Job→job chain (§4.3): a job whose `perform` calls another job's `performLater()` inside a worker constructed with `enqueueMetadata` ⇒ the child enqueue recorded by MockBackend is stamped — proving the constructor registered the hook in the same global registry `getBackend()` uses.
- **Verification:** `deno task test`; `deno fmt` on touched files.
- **Done when:** All three numbered items pass; #6's test name says it pins §5.3.

### P3-T05 — Same-commit docs: CONTEXT principle 3, KNOWN_GAPS metadata item, README aroundPerform
- **Intent:** Constitution and gap-list reflect reality in the same commits as the code (design §7.3).
- **Touches:** `CONTEXT.md`, `KNOWN_GAPS.md`, `README.md`
- **Steps:**
  - CONTEXT.md principle 3 ("`BackendAdapter` is the only extension point") → amend per §7.3 verbatim intent: it is the only extension point **for queue mechanics**; the hooks surface (`HermesHooks`, logger sink) is the observability seam, deliberately outcome-inert, and any hook that could alter whether/how a job runs is a design violation, not a feature request.
  - KNOWN_GAPS.md: **remove** the "`JobPayload.metadata` exists but is never populated or read" item — it is now both populated and read.
  - README: `aroundPerform` docs — the §5.2 outcome-inertness contract in user terms ("MUST call next() exactly once and SHOULD await it; the job's outcome is next()'s outcome, always"), the §5.4 asymmetry (enqueue hook throws fail the caller; execution hook faults never fail the job), `JobContext.metadata`, the `undefined`-means-fresh rule for scheduled/old payloads, the §6.3 three-line composition idiom, and a `hook_error` row in the log-event table (~line 616).
- **Verification:** Proofread; confirm KNOWN_GAPS "Last reviewed" stamp updated per its own convention.
- **Done when:** Phase 3 gate holds and no doc claims the old world.

---

**Phase 4 — Logger sink and metadata echo.** `logger.ts` routes through the sink, guarded; the four job lifecycle events echo metadata. Gate: §7.5 test **7** passes; a throwing sink demonstrably cannot break dispatch; default (no sink) output byte-identical to today.

### P4-T01 — Route Logger.log through the guarded sink
- **Intent:** Change #5: injectable transport with §5.5 semantics; default behavior unchanged.
- **Touches:** `src/lib/logger.ts`
- **Steps:**
  - In the private `log()` (`logger.ts:221-223`): fetch the sink from the registry. No sink ⇒ exactly today's `console.log(JSON.stringify(event))`.
  - Sink present ⇒ call it **synchronously**, return value ignored, wrapped in try/catch. On throw: fall back to `console.log(JSON.stringify(...))` for the original event, then emit one `logger_error` event (with the sink's error, stringified) **through the fallback directly** — never through the sink, never recursing into `log()`'s sink path.
  - Add `Logger.loggerError(...)` or inline the fallback emission — internal either way.
- **Verification:** `deno task check`; `deno task test`; `deno fmt src/lib/logger.ts`.
- **Done when:** Every event in the process (worker lifecycle, backend-emitted events like `worker_job_failed` — with zero backend edits, since backends already call `Logger`) routes through the sink when configured, and a throwing sink degrades to console with a `logger_error` breadcrumb.

### P4-T02 — Echo metadata on the four job lifecycle events
- **Intent:** §5.5 metadata echo — the only way Hermes's own lines are correlatable, since they fire outside any ALS scope a wrapper establishes.
- **Touches:** `src/lib/logger.ts`, `src/lib/worker.ts`
- **Steps:**
  - `jobReceived`, `jobStarted`, `jobSucceeded`, `jobFailed` gain an optional metadata parameter; set the `metadata` field on the event **only when the payload carries one** (absent otherwise — keeps no-metadata event shapes byte-identical for tests and consumers).
  - Worker passes `payload.metadata` at all four call sites. No other events change shape.
- **Verification:** `deno task check`; `deno task test`; `deno fmt` on touched files.
- **Done when:** The four events carry `metadata` iff the payload did; all other emitters unchanged.

### P4-T03 — Unit tests for the sink (§7.5 #7)
- **Intent:** Pin routing, the guard, and the echo.
- **Touches:** `src/lib/tests/` (new `logger_test.ts` or extend worker tests)
- **Steps:**
  - Configured sink receives structured events (assert on a captured array — no console interception needed).
  - Throwing sink: original event lands on console (intercept `console.log`), exactly one `logger_error` follows via console, dispatch continues (subsequent jobs still process), and no recursion/second sink call for the failed event.
  - Metadata echo on all four job events when the payload has metadata; field absent when it doesn't.
  - Sink registered via `configure()` works from an enqueue-only setup (locked decision 3).
- **Verification:** `deno task test`; `deno fmt` on touched files.
- **Done when:** §7.5 #7 passes in full.

### P4-T04 — Same-commit docs: CONTEXT principle 8, README sink section + logger_error + size warning
- **Intent:** Amend the "not injectable, not configurable" constitution line in the same commits that make it false.
- **Touches:** `CONTEXT.md`, `README.md`, `KNOWN_GAPS.md`
- **Steps:**
  - CONTEXT.md principle 8 → per §7.3: the **sink is injectable**; event shapes and emission points remain the semi-public contract; the `Logger` class stays internal. Touch only that sentence — the principle's pre-existing `job_completed`/`duration_ms` naming drift is a flagged follow-up, not this task (see Assumptions).
  - README: logger-sink section — signature, sync/non-throwing contract, guarded fallback, async shipping is the sink's buffering problem (§5.5), accepted by both `Hermes()` and `configure()`; a `logger_error` row in the log-event table; the **metadata size warning** (metadata rides both the envelope and the log stream; consumers own size/content); the §5.5 note that errors are strings in `LogEvent` and integrations wanting the raw `Error` observe it in `aroundPerform` via `next()`'s rejection.
  - KNOWN_GAPS.md: verify the existing "`info`/`warn` events have no emission path" item still stands (the sink adds transport, not emission paths) and leave it listed, per design §7.1.
- **Verification:** Proofread; Phase 4 gate (`deno task check` + `deno task test`).
- **Done when:** Constitution, README, and gap list match shipped behavior.

---

**Phase 5 — Integration tests and final sweep.** End-to-end proof on real backends, then the definition-of-done sweep. Gate: §7.5 tests **9, 10** actually execute (Redis is up — a skip is a failure of this gate, not a pass); full check/test/lint green; zero diffs under `src/lib/backends/`.

### P5-T01 — BullMQ integration: metadata round-trip + row-3 retry invariant (§7.5 #9)
- **Intent:** Observe the two load-bearing behaviors end-to-end on Redis.
- **Touches:** `src/lib/tests/integration/` (extend the existing BullMQ suite style: self-skipping, randomized queue names, sanitizers disabled for long-lived connections)
- **Steps:**
  - Metadata set at `performLater` round-trips `queue.add → job.data → JobContext.metadata` with zero backend changes.
  - Row 3 end-to-end: a wrapper that swallows next's rejection ⇒ BullMQ still sees the failure and **retries** the job (assert attempts > 1 or failed-state transition) — outcome inertness observed at the backend boundary.
  - Confirm the test **ran** in this environment (Redis answered `PONG` at planning time); do not accept a silent self-skip as success.
- **Verification:** `deno task test` with local Redis; inspect output to confirm the BullMQ steps executed rather than skipped.
- **Done when:** Both assertions pass against real Redis.

### P5-T02 — Deno KV integration: structured-clone round-trip + metadata-free tick (§7.5 #10)
- **Intent:** Same invariants on the zero-infra backend.
- **Touches:** `src/lib/tests/integration/` (temp-dir KV store, cleaned up after, existing style)
- **Steps:**
  - Metadata survives Deno KV's structured clone through `kv.enqueue → listenQueue → JobContext.metadata`.
  - A scheduled/recurring tick arrives with `metadata === undefined` (the §4.4 freeze, observed on the real backend; if driving a real `Deno.cron` tick is impractical in-test, exercising the registration-built payload through the real KV queue is acceptable — note whichever was done in the tracker).
- **Verification:** `deno task test` (task supplies `--unstable-kv`/`--unstable-cron`); `deno fmt` on touched files.
- **Done when:** Both assertions pass against a real temp-dir KV store.

### P5-T03 — Final sweep: zero-backend-diff check, formatting, docs consistency
- **Intent:** Definition-of-done pass before calling the feature complete.
- **Touches:** verification only (plus any stragglers it surfaces)
- **Steps:**
  - `git diff --stat 17aef78 -- src/lib/backends/ src/lib/backend.ts` must be **empty** — the design's zero-backend-changes invariant, checked mechanically.
  - Run the full Verification block below; `deno fmt` on every touched file (and only touched files); confirm every new export in `main.ts` has explicit types and the root pulls no npm dependency.
  - Re-read README's hooks/sink/logging sections top to bottom for coherence with the shipped code; confirm CONTEXT.md and KNOWN_GAPS.md amendments landed; confirm the tracker matches reality and follow-ups are captured.
- **Verification:** `deno task check`; `deno task test`; `deno task lint`; `deno fmt --check <each touched file>`.
- **Done when:** Everything in Definition of done is checked off.

## Verification

Run from the repo root (`/Users/danny/personal/hermes`) at every phase gate and at the end:

- `deno task check` — type-checks the public entry point (JSR slow-types surface).
- `deno task test` — full suite; supplies `-A`, `--unstable-kv`, `--unstable-cron`. **Local Redis is available here, so the BullMQ integration steps must show as executed, not skipped** — eyeball the output.
- `deno task lint`
- `deno fmt <file...>` — **touched files only**; whole-repo formatting produces unrelated diffs (~8 files of pre-existing drift).
- `git diff --stat 17aef78 -- src/lib/backends/ src/lib/backend.ts` — must print nothing (zero-backend-changes invariant).

## Definition of done

- [ ] All tasks checked off in the tracker, and the tracker reflects reality.
- [ ] All ten §7.5 test items implemented and passing (unit #1–8; integration #9–10 actually executed against Redis and temp-dir KV, not skipped).
- [ ] One unit test per §5.2 matrix row (1–9), each passing.
- [ ] `deno task check`, `deno task test`, `deno task lint` all green; every touched file formatted.
- [ ] Zero diffs under `src/lib/backends/` and `src/lib/backend.ts`.
- [ ] Exactly three new root exports (`HermesHooks`, `LoggerSink`, `LogEvent`), all type-only, explicit types throughout; root export npm-free; camelCase identifiers, snake_case filenames.
- [ ] No-config behavior byte-identical: payload shapes (test #1) and log output (no-sink path) unchanged.
- [ ] CONTEXT.md principles 3 and 8 amended, KNOWN_GAPS.md metadata item closed, README hooks/sink/log-table/size-warning additions landed — each in the same commits as its phase's code.
- [ ] Follow-ups (below and any discovered mid-flight) captured in the tracker.

## Risks and rollback

- **The §5.2 mechanism is subtle** (memoized next, composite settlement, row 3/4 asymmetry, row 9 interplay). Mitigation: the composite promise must settle only after *both* wrapper and next settle; the nine row tests are written to fail on any shortcut. If the mechanism fights back, do not weaken the invariant — the outcome-is-next's-settlement rule is the whole point (design §5.2/§8).
- **Timeout-budget interaction:** a wrapper that hangs after next resolves holds the in-flight slot (and trips the timeout if one is set). This is by design (wrapper time counts); README documents it. Watch for graceful-shutdown test flakiness around hung-wrapper cases.
- **Global registry leakage across tests** — same story as the backend global. Every suite that registers hooks/sink must clear them; a leaked `aroundPerform` will corrupt unrelated worker tests confusingly.
- **Type-only import cycle** (`types.ts` → `logger.ts` for `LogEvent` while `logger.ts` → `hooks_registry.ts` → `types.ts`): erased at runtime, but if `deno check` objects, move `LogEvent` into `types.ts` and re-export from `logger.ts` instead — shape and public surface identical either way; record the choice in the tracker.
- **Sink recursion:** a `logger_error` emission that re-enters the sink loops forever. The fallback path must write to console directly, and the throwing-sink test must assert single emission.
- **Permanent API commitment:** three exports and two hook shapes are forever once 0.3.0 publishes. They copy the design doc §3.1 verbatim; any deviation discovered mid-implementation goes back to the design doc's owner, not into improvisation.
- **Rollback:** all changes are additive and unpublished. Reverting is `git revert` of the feature commits back to `17aef78` behavior; no wire compat concerns because no-hooks payloads never changed and 0.3.0 was never published with this surface.
