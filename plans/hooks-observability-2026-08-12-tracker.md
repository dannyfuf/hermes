# Hermes hooks & observability surface — Tracker
> Plan: ./hooks-observability-2026-08-12-plan.md
> READ ME FIRST. Update this file as you work. The plan is reference; this tracker is the source of truth for state. If reality diverges from the plan, update both.

## Working agreement
- Check the kickoff box below before starting.
- Move tasks through: [ ] todo → [~] in progress → [x] done. One task in progress at a time.
- After each task: tick its box, paste the verification command output (or a one-line "verified: <how>"), and commit.
- Phases are gates: do not start a phase until the previous phase's gate line is ticked (`deno task check` + `deno task test` green).
- Docs tasks (P3-T05, P4-T04, P2-T03) land in the same commits as their phase's code — they are not deferrable to the end.
- If you discover work the plan missed, add a new task with the next ID in its phase. Never silently expand an existing task.
- Definition of done is not met until every box is ticked and this tracker matches reality.

## Kickoff
- [x] I have read the plan end to end.
- [x] I have read the normative design doc (path in the plan header) end to end, including the §5.2 matrix and the four locked decisions.
- [x] I have read CONTEXT.md and KNOWN_GAPS.md per AGENTS.md.
- [x] I have run `deno task check` and `deno task test` once on a clean tree to confirm a green baseline, and confirmed the BullMQ integration steps executed (Redis up) rather than skipped. (80 passed, 0 failed, 1 ignored — the ignored one is the pre-existing slow-gated integration test behind `slowTestsEnabled`; BullMQ suites visibly executed against Redis.)
- [x] I am ready to start.

## Tasks

### Phase 1 — Types, registry, config plumbing (inert)
- [x] P1-T01 — Add hook and sink types to types.ts
- [x] P1-T02 — Create hooks_registry.ts module-global store
- [x] P1-T03 — Plumb hooks and logger through Hermes() and configure()
- [x] P1-T04 — Export the three new types from main.ts
- [x] P1-T05 — Unit-test registration plumbing and replace semantics
- [x] **Phase 1 gate:** `deno task check` + `deno task test` green; zero behavior change. (verified: `deno task check` Check src/main.ts OK; `deno task test` → 81 passed (93 steps), 0 failed, 1 ignored — same pre-existing slow-gated skip as baseline)

### Phase 2 — Enqueue path
- [x] P2-T01 — Run enqueueMetadata and merge in performLater
- [x] P2-T02 — Unit tests for the enqueue path (§7.5 #1, #2, #3)
- [x] P2-T03 — README: performLater metadata + enqueueMetadata (same commits as P2 code)
- [x] **Phase 2 gate:** check + test green; no-hooks payload byte-identical (test #1). (verified: check OK; test → 82 passed (101 steps), 0 failed, 1 ignored)

### Phase 3 — Execution path and outcome protection (highest risk)
- [x] P3-T01 — Deliver JobContext.metadata in the worker
- [x] P3-T02 — Implement the aroundPerform wrapper with outcome protection
- [x] P3-T03 — Unit tests: outcome matrix, one per row 1–9 (§7.5 #4)
- [x] P3-T04 — Unit tests: context delivery, scheduled-run pin, job→job chain (§7.5 #5, #6, #8)
- [x] P3-T05 — Same-commit docs: CONTEXT principle 3, KNOWN_GAPS metadata item, README aroundPerform + hook_error
- [x] **Phase 3 gate:** check + test green; nine row tests passing; `git diff --stat 17aef78 -- src/lib/backends/ src/lib/backend.ts` empty. (verified: check OK; test → 84 passed (117 steps), 0 failed, 1 ignored; lint clean; backend diff empty. Beyond the 9 rows: extra tests for wrapper-substitute-error, hung-wrapper-after-next-resolved, no-hook path, and unknown-job-skips-before-wrapper.)

### Phase 4 — Logger sink and metadata echo
- [x] P4-T01 — Route Logger.log through the guarded sink
- [x] P4-T02 — Echo metadata on the four job lifecycle events
- [x] P4-T03 — Unit tests for the sink (§7.5 #7)
- [x] P4-T04 — Same-commit docs: CONTEXT principle 8, README sink section + logger_error + size warning
- [x] **Phase 4 gate:** check + test green; throwing sink cannot break dispatch; no-sink output byte-identical. (verified: check OK; test → 85 passed (122 steps), 0 failed, 1 ignored; KNOWN_GAPS `info`/`warn` no-emission-path item confirmed still listed and still true — the sink adds transport, not emission paths)

### Phase 5 — Integration tests and final sweep
- [x] P5-T01 — BullMQ integration: metadata round-trip + row-3 retry invariant (§7.5 #9) — EXECUTED against local Redis (H1, H1b, H2 in `hooks.integration.test.ts` all show `ok` in the run output, not skipped)
- [x] P5-T02 — Deno KV integration: structured-clone round-trip + metadata-free tick (§7.5 #10) — real temp-dir KV store; the tick test exercises the registration-built 3-field payload through the real KV queue (driving a real `Deno.cron` tick is impractical in-test — minimum 1-minute granularity; see note below)
- [x] P5-T03 — Final sweep: zero-backend-diff check, formatting, docs consistency (verified: `git diff --stat 17aef78 -- src/lib/backends/ src/lib/backend.ts` empty; check OK; lint clean; `deno fmt --check` clean on all 20 touched files; no `npm:` imports reachable from root; README hooks/sink/logging sections proofread)
- [x] **Phase 5 gate:** full Definition of done in the plan checked off. (final: `deno task test` → 90 passed (122 steps), 0 failed, 1 ignored — the pre-existing slow-gated test behind `HERMES_SLOW_TESTS`)

## Notes / decisions log
(Append-only. Date-stamp entries. Capture anything that surprised you or that future-you will want.)

- 2026-08-12 — Plan authored against repo @ `17aef78` (matches the design doc's verified facts). Local Redis verified up (`redis-cli ping` → `PONG`) at planning time — re-verify at kickoff.
- 2026-08-12 — Pre-resolved in the plan's Assumptions (record here if you deviate): type-alias style kept; `LogEvent` stays in logger.ts with a type-only re-export from main.ts; registration is replace-on-call (omitted hooks/logger clears); `Readonly<JobPayload>` is type-level only.
- 2026-08-12 — Implementation complete. No deviations from the Assumptions. The type-only `types.ts → logger.ts` import for `LogEvent` caused no `deno check` complaints (no runtime cycle: `logger.ts → hooks_registry.ts → types.ts` is type-only at the last hop), so `LogEvent` stayed in logger.ts as planned.
- 2026-08-12 — P5-T02 tick test: exercised the registration-built 3-field payload through the real KV queue via `backend.enqueue(...)` — this is byte-for-byte the payload the `Deno.cron` closure enqueues (verified in `deno_kv.ts` `registerRecurringJob`); a real cron tick was not driven (1-minute minimum granularity).
- 2026-08-12 — Beyond the plan's required tests, added: row-2 variant (wrapper substitute error never replaces next's error), hung-wrapper-after-next-resolved (timeout budget + in-flight slot), no-wrapper-configured control, unknown-job-skips-before-wrapper, H1b (metadata-less payload → undefined context metadata on real Redis), and no-sink console-unchanged.
- 2026-08-12 — `IntegrationScope.hermes()` gained an optional 4th param `{ hooks?, logger? }` (test helper only; passes through to `Hermes()`).

## Follow-ups
(Things discovered mid-flight that are out of scope for this plan. Each gets a one-line description.)

- CONTEXT.md principle 8 names `job_completed` / `duration_ms` but the code emits `job_succeeded` / `durationMs` — pre-existing drift; fix deliberately in a separate change, not inside the P4 amendment.
- KNOWN_GAPS "Last reviewed" stamp references `3cdb67d` while HEAD is `17aef78` — refresh the stamp when touching the file in P3-T05.
