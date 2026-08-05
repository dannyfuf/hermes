# AGENTS.md — Hermes

Instructions for AI agents working in this repository.

## Required reading

1. **[CONTEXT.md](./CONTEXT.md)** — the project's intent, design philosophy,
   architecture, and conventions. **Read it before designing or implementing any
   change.** When a decision isn't covered there, derive it from its "Design
   philosophy" section and run your plan through its "How to make aligned
   decisions" checklist.
2. **[KNOWN_GAPS.md](./KNOWN_GAPS.md)** — living list of known gaps and
   inconsistencies. Check it before investigating an oddity (it's probably
   already listed) and before adding a feature (a half-built seam for it may
   already exist). Update it when you close or discover a gap.

## Project snapshot

Deno-first TypeScript background-job library published to JSR as `@dafu/hermes`.
Root entry point is `src/main.ts`; `deno.json` is the source of truth for
version, tasks, and dependencies.

## Commands

- `deno task test` — full suite (BullMQ tests self-skip without local Redis).
- `deno task check` — type-check the public entry point.
- `deno task lint` / `deno task fmt` — lint and format. Format **only the files
  you touch**; the repo has pre-existing formatting drift.

## Non-negotiables (details and rationale in CONTEXT.md)

- The root export stays npm-free; npm-dependent backends live behind
  `./backends/<name>` subpaths.
- Queue mechanics (retries, retention, scheduling, cleanup) belong behind the
  `BackendAdapter` interface, never in core.
- Keep the public API minimal: factory functions returning interface types,
  explicit return types on every export (JSR slow types), internals unexported.
- camelCase identifiers, snake_case filenames.
- The job workflow stays declarative: class properties + `performLater()`, one
  manifest module as the registry, caller-configurable manifest path.

## Definition of done

`deno task check` and `deno task test` pass; touched files are formatted; new
invariants are tested (unit via `MockBackend`, backend behavior via
self-skipping integration tests); README, examples, and KNOWN_GAPS.md are
updated if behavior changed.
