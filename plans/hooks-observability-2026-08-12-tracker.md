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
- [ ] P3-T01 — Deliver JobContext.metadata in the worker
- [ ] P3-T02 — Implement the aroundPerform wrapper with outcome protection
- [ ] P3-T03 — Unit tests: outcome matrix, one per row 1–9 (§7.5 #4)
- [ ] P3-T04 — Unit tests: context delivery, scheduled-run pin, job→job chain (§7.5 #5, #6, #8)
- [ ] P3-T05 — Same-commit docs: CONTEXT principle 3, KNOWN_GAPS metadata item, README aroundPerform + hook_error
- [ ] **Phase 3 gate:** check + test green; nine row tests passing; `git diff --stat 17aef78 -- src/lib/backends/ src/lib/backend.ts` empty.

### Phase 4 — Logger sink and metadata echo
- [ ] P4-T01 — Route Logger.log through the guarded sink
- [ ] P4-T02 — Echo metadata on the four job lifecycle events
- [ ] P4-T03 — Unit tests for the sink (§7.5 #7)
- [ ] P4-T04 — Same-commit docs: CONTEXT principle 8, README sink section + logger_error + size warning
- [ ] **Phase 4 gate:** check + test green; throwing sink cannot break dispatch; no-sink output byte-identical.

### Phase 5 — Integration tests and final sweep
- [ ] P5-T01 — BullMQ integration: metadata round-trip + row-3 retry invariant (§7.5 #9) — must EXECUTE, not skip (Redis is available here)
- [ ] P5-T02 — Deno KV integration: structured-clone round-trip + metadata-free tick (§7.5 #10)
- [ ] P5-T03 — Final sweep: zero-backend-diff check, formatting, docs consistency
- [ ] **Phase 5 gate:** full Definition of done in the plan checked off.

## Notes / decisions log
(Append-only. Date-stamp entries. Capture anything that surprised you or that future-you will want.)

- 2026-08-12 — Plan authored against repo @ `17aef78` (matches the design doc's verified facts). Local Redis verified up (`redis-cli ping` → `PONG`) at planning time — re-verify at kickoff.
- 2026-08-12 — Pre-resolved in the plan's Assumptions (record here if you deviate): type-alias style kept; `LogEvent` stays in logger.ts with a type-only re-export from main.ts; registration is replace-on-call (omitted hooks/logger clears); `Readonly<JobPayload>` is type-level only.

## Follow-ups
(Things discovered mid-flight that are out of scope for this plan. Each gets a one-line description.)

- CONTEXT.md principle 8 names `job_completed` / `duration_ms` but the code emits `job_succeeded` / `durationMs` — pre-existing drift; fix deliberately in a separate change, not inside the P4 amendment.
- KNOWN_GAPS "Last reviewed" stamp references `3cdb67d` while HEAD is `17aef78` — refresh the stamp when touching the file in P3-T05.
