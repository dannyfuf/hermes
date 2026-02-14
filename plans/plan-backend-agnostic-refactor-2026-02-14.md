# Implementation Plan: Backend-Agnostic Queue Refactor

Generated: 2026-02-14

## Summary

Refactor Hermes from a Deno KV-only job queue library into a **backend-agnostic
async workload framework** (think: Sidekiq for Deno/TypeScript). The library
should expose a simple `Job` abstract class that hides queue complexity, while
supporting pluggable backends starting with **Deno KV** (existing) and **Redis
via BullMQ** (new). The refactored library must be publishable to JSR and
installable in any Deno project.

### Why

The current implementation is tightly coupled to `Deno.openKv()` — the `Job`
class calls it directly in `performLater()`, and the `Worker` class calls
`kv.listenQueue()` directly. This makes the library unusable outside Deno Deploy
or without the `--unstable-kv` flag, and impossible to swap backends. Decoupling
the backend enables Redis-based production deployments (via BullMQ) while
preserving the simple developer experience.

As part of this major version bump, all identifiers will also be migrated from
snake_case to **camelCase** to follow standard TypeScript conventions (e.g.,
`job_name` → `jobName`, `perform_later` → `performLater`).

### Design Philosophy

Like Sidekiq: **convention over configuration, simple API, powerful internals**.
A developer should only need to:

1. Extend `Job` and implement `perform()`
2. Register jobs in a manifest
3. Call `performLater()` to enqueue
4. Start a worker process

Everything else (connection management, serialization, dispatch, retries) is
handled by the framework.

---

## Prerequisites

### Environment

- **Deno** >= 2.x (for JSR publishing, `deno.json` workspace support)
- **Redis** running locally for BullMQ development/testing (e.g.,
  `docker run -d -p 6379:6379 redis:7`)
- **Node.js compatibility**: BullMQ is an npm package; Deno supports npm
  specifiers natively (`npm:bullmq`)

### Dependencies to Add

| Package        | Purpose                        | Specifier          |
| -------------- | ------------------------------ | ------------------ |
| `bullmq`       | Redis-backed queue/worker      | `npm:bullmq@^5`    |
| `@std/assert`  | Test assertions                | `jsr:@std/assert`  |
| `@std/testing` | Test utilities (BDD, mocking)  | `jsr:@std/testing` |
| `ioredis`      | Redis client (BullMQ peer dep) | `npm:ioredis@^5`   |

### Coding Conventions (Target — Post-Refactor)

The current codebase uses **snake_case** (`job_name`, `queue_name`,
`perform_later`), which is non-standard for TypeScript. As part of this
refactor, all identifiers will be migrated to **camelCase** to follow JS/TS
ecosystem conventions.

- **camelCase** for properties, variables, and methods (`jobName`, `queueName`,
  `jobBody`, `jobsMap`, `performLater`)
- **PascalCase** for classes, interfaces, and type aliases (`Job`, `JobPayload`,
  `WorkerConfig`)
- **Factory functions for all public API classes** — no `new` keyword in
  consumer code. Every class exposed to the user is wrapped in a factory
  function with the same PascalCase name. The internal class is private (not
  exported). Examples: `Hermes()`, `DenoKvBackend()`, `BullMQBackend()`. The
  only exception is `Job`, which is an abstract class users extend — `new` is
  unavoidable there.
- **Static utility classes** for stateless operations (`Logger`,
  `ManifestLoader`, `Worker`) — these are internal, not part of the public API
- **Barrel exports** through `src/main.ts`
- **Type-only imports** where possible (`import type { ... }`)
- All source in `src/lib/`, examples in `example/`, entry point at `src/main.ts`
- **File names**: Keep existing underscore-separated names to minimize rename
  noise in this PR

### Background Reading

- [BullMQ Docs](https://docs.bullmq.io/) — Queue, Worker, Job classes
- [Deno KV Queue](https://docs.deno.com/deploy/kv/manual/queue_overview/) —
  `kv.enqueue()`, `kv.listenQueue()`
- [JSR Publishing](https://jsr.io/docs/publishing-packages) — `deno.json`
  exports, slow types policy

---

### Task 0: Migrate All Identifiers from snake_case to camelCase

**Complexity**: Medium **Dependencies**: None (do this FIRST, before any
structural changes) **Files to modify**: Every file in `src/lib/` and `example/`

Rename all snake_case identifiers to camelCase across the entire codebase. This
is a mechanical refactor — no logic changes.

**Rename map** (complete list):

| Old (snake_case)           | New (camelCase)          |
| -------------------------- | ------------------------ |
| `job_name`                 | `jobName`                |
| `queue_name`               | `queueName`              |
| `job_body`                 | `jobBody`                |
| `jobs_hash`                | `jobsMap`                |
| `perform_later`            | `performLater`           |
| `job_manifest`             | `jobManifest`            |
| `job_class`                | `jobClass`               |
| `manifest_module`          | `manifestModule`         |
| `job_loader`               | `jobLoader`              |
| `handle_error_not_found`   | `handleErrorNotFound`    |
| `validate_manifest_module` | `validateManifestModule` |
| `validate_manifest_type`   | `validateManifestType`   |
| `validate_job_uniqueness`  | `validateJobUniqueness`  |
| `error_message`            | `errorMessage`           |
| `close_promises`           | `closePromises`          |
| `default_queue_name`       | `defaultQueueName`       |
| `queue_names`              | `queueNames`             |
| `redis_available`          | `redisAvailable`         |

**Type/interface field renames**:

| Type             | Old Field    | New Field   |
| ---------------- | ------------ | ----------- |
| `JobPayload`     | `job_name`   | `jobName`   |
| `JobPayload`     | `queue_name` | `queueName` |
| `JobPayload`     | `job_body`   | `jobBody`   |
| `WorkerParams`   | `jobs_hash`  | `jobsMap`   |
| `EnqueueOptions` | `queue_name` | `queueName` |

**Process**:

1. Start with `src/lib/types.ts` (the shared vocabulary)
2. Update `src/lib/job.ts` (abstract class properties and methods)
3. Update `src/lib/job_loader.ts`
4. Update `src/lib/worker.ts`
5. Update `src/lib/hermes.ts`
6. Update `src/lib/manifest_loader.ts`
7. Update `src/lib/logger.ts` (method parameters)
8. Update `example/` files

**Acceptance criteria**:

- [ ] Zero snake_case identifiers remain in source code (except `Deno.openKv`
      and external API calls)
- [ ] All existing functionality preserved (no logic changes)
- [ ] `deno check src/main.ts` passes
- [ ] `deno lint` passes

---

## Task Breakdown

### Task 1: Define the Backend Adapter Interface (Strategy Pattern)

**Complexity**: Medium **Dependencies**: Task 0 **Files to create**:
`src/lib/backend.ts`

Define TypeScript interfaces that abstract the two operations any backend must
support: **enqueue** and **listen/process**.

```typescript
// src/lib/backend.ts

export interface EnqueueOptions {
  delay?: number;
  queueName?: string;
}

export interface BackendAdapter {
  /**
   * Enqueue a job payload for async processing.
   */
  enqueue(payload: JobPayload, options?: EnqueueOptions): Promise<void>;

  /**
   * Start listening for jobs and dispatch them to the handler.
   * The handler receives the raw JobPayload.
   * Returns a cleanup/stop function or void.
   */
  listen(handler: (payload: JobPayload) => Promise<void>): Promise<void>;

  /**
   * Gracefully shut down the backend connection.
   */
  close(): Promise<void>;
}
```

**Acceptance criteria**:

- [ ] `BackendAdapter` interface defined with `enqueue`, `listen`, and `close`
      methods
- [ ] `EnqueueOptions` type defined
- [ ] Interface is generic enough to support both Deno KV and BullMQ semantics
- [ ] Exported from `src/lib/backend.ts`

---

### Task 2: Implement Deno KV Backend Adapter

**Complexity**: Medium **Dependencies**: Task 1 **Files to create**:
`src/lib/backends/deno_kv.ts`

Extract the existing `Deno.openKv()` logic from `Job.performLater()` and
`Worker.start()` into a class implementing `BackendAdapter`.

```typescript
// src/lib/backends/deno_kv.ts
import type { BackendAdapter, EnqueueOptions } from "../backend.ts";
import type { JobPayload } from "../types.ts";

class TDenoKvBackend implements BackendAdapter {
  private kv: Deno.Kv | null = null;

  private async getKv(): Promise<Deno.Kv> {
    if (!this.kv) {
      this.kv = await Deno.openKv();
    }
    return this.kv;
  }

  async enqueue(payload: JobPayload, options?: EnqueueOptions): Promise<void> {
    const kv = await this.getKv();
    await kv.enqueue(payload, { delay: options?.delay });
  }

  async listen(handler: (payload: JobPayload) => Promise<void>): Promise<void> {
    const kv = await this.getKv();
    kv.listenQueue(async (message: unknown) => {
      await handler(message as JobPayload);
    });
  }

  async close(): Promise<void> {
    if (this.kv) {
      this.kv.close();
      this.kv = null;
    }
  }
}

export const DenoKvBackend = (): BackendAdapter => {
  return new TDenoKvBackend();
};
```

**Key decisions**:

- Singleton `Deno.Kv` instance (lazy-initialized) instead of opening a new
  connection per `enqueue()` call (fixes current inefficiency in
  `Job.performLater()`)
- `close()` enables graceful shutdown (currently not possible)
- Factory function `DenoKvBackend()` — no `new` keyword for consumers; internal
  `TDenoKvBackend` class is private

**Acceptance criteria**:

- [ ] `TDenoKvBackend` implements `BackendAdapter`
- [ ] Exported as `DenoKvBackend()` factory function (no `new`)
- [ ] Single KV connection reused across calls
- [ ] Existing Deno KV behavior preserved (at-least-once delivery, delay
      support)
- [ ] `close()` properly disposes the KV handle

---

### Task 3: Implement BullMQ (Redis) Backend Adapter

**Complexity**: High **Dependencies**: Task 1 **Files to create**:
`src/lib/backends/bullmq.ts`

Implement the `BackendAdapter` using BullMQ's `Queue` and `Worker` classes.

```typescript
// src/lib/backends/bullmq.ts
import { Queue, Worker } from "npm:bullmq@^5";
import type { BackendAdapter, EnqueueOptions } from "../backend.ts";
import type { JobPayload } from "../types.ts";

export interface BullMQBackendOptions {
  connection: {
    host?: string;
    port?: number;
    password?: string;
    url?: string;
  };
  defaultQueueName?: string;
  concurrency?: number;
}

class TBullMQBackend implements BackendAdapter {
  private queues: Map<string, Queue> = new Map();
  private workers: Map<string, Worker> = new Map();
  private options: BullMQBackendOptions;

  constructor(options: BullMQBackendOptions) {
    this.options = options;
  }

  private getOrCreateQueue(queueName: string): Queue {
    if (!this.queues.has(queueName)) {
      this.queues.set(
        queueName,
        new Queue(queueName, {
          connection: this.options.connection,
        }),
      );
    }
    return this.queues.get(queueName)!;
  }

  async enqueue(payload: JobPayload, options?: EnqueueOptions): Promise<void> {
    const queueName = payload.queueName || this.options.defaultQueueName ||
      "default";
    const queue = this.getOrCreateQueue(queueName);
    await queue.add(payload.jobName, payload, {
      delay: options?.delay,
    });
  }

  async listen(handler: (payload: JobPayload) => Promise<void>): Promise<void> {
    throw new Error("Use listenOnQueues() for BullMQ backend");
  }

  async listenOnQueues(
    queueNames: string[],
    handler: (payload: JobPayload) => Promise<void>,
  ): Promise<void> {
    for (const queueName of queueNames) {
      const worker = new Worker(
        queueName,
        async (job) => {
          await handler(job.data as JobPayload);
        },
        {
          connection: this.options.connection,
          concurrency: this.options.concurrency ?? 1,
        },
      );
      this.workers.set(queueName, worker);
    }
  }

  async close(): Promise<void> {
    const closePromises: Promise<void>[] = [];
    for (const [_, queue] of this.queues) {
      closePromises.push(queue.close());
    }
    for (const [_, worker] of this.workers) {
      closePromises.push(worker.close());
    }
    await Promise.all(closePromises);
    this.queues.clear();
    this.workers.clear();
  }
}

export const BullMQBackend = (
  options: BullMQBackendOptions,
): BackendAdapter => {
  return new TBullMQBackend(options);
};
```

**Key design decisions**:

- **One BullMQ `Queue` per `queueName`**: BullMQ queues are named, mapping
  naturally to Hermes `queueName`. Queue instances are lazily created and
  cached.
- **One BullMQ `Worker` per `queueName`**: BullMQ workers listen on a specific
  queue. We create one per registered queue name.
- **`listenOnQueues()`**: Extended interface method. The `BackendAdapter` base
  `listen()` is for simple backends (Deno KV has a single global queue). For
  BullMQ, the orchestrator extracts unique queue names from the job registry and
  passes them in. Consider making `listen()` accept an optional `queueNames`
  parameter on the interface instead. See **Design Decision** below.

**Design Decision — `listen()` signature**:

Option A (recommended): Extend the `BackendAdapter.listen()` signature:

```typescript
listen(
  handler: (payload: JobPayload) => Promise<void>,
  options?: { queueNames?: string[] }
): Promise<void>;
```

- Deno KV ignores `queueNames` (single global queue)
- BullMQ uses them to create per-queue workers
- No need for a separate `listenOnQueues()` method

Option B: Keep `listen()` simple and add `listenOnQueues()` as a BullMQ-specific
extension. This leaks backend details.

**Go with Option A.**

**Acceptance criteria**:

- [ ] `TBullMQBackend` implements `BackendAdapter`
- [ ] Exported as `BullMQBackend()` factory function (no `new`)
- [ ] Enqueue routes to the correct BullMQ queue based on `payload.queueName`
- [ ] Workers are created per queue name with configurable concurrency
- [ ] `close()` shuts down all queues and workers
- [ ] Connection options are configurable (host, port, password)
- [ ] Works with `npm:bullmq` specifier in Deno

---

### Task 4: Refactor `Job` Class — Remove Direct Deno KV Dependency

**Complexity**: Medium **Dependencies**: Task 0, Task 1 **Files to modify**:
`src/lib/job.ts`

The `Job` class currently calls `Deno.openKv()` directly in `performLater()`.
This must be decoupled. The job should delegate to a backend adapter, but jobs
should not need to know which backend is active.

**Approach**: Use a module-level backend registry. The `Hermes` orchestrator
sets the backend at startup; `Job.performLater()` reads from it.

Create a new file for the global backend registry:

```typescript
// src/lib/backend_registry.ts
import type { BackendAdapter } from "./backend.ts";

let _backend: BackendAdapter | null = null;

export function setBackend(backend: BackendAdapter): void {
  _backend = backend;
}

export function getBackend(): BackendAdapter {
  if (!_backend) {
    throw new Error(
      "No backend configured. Call Hermes.start() or setBackend() before enqueuing jobs.",
    );
  }
  return _backend;
}
```

Then refactor `Job.performLater()`:

```typescript
// src/lib/job.ts (modified)
import type { JobPayload, PerformLaterOptions } from "./types.ts";
import { getBackend } from "./backend_registry.ts";

export abstract class Job {
  abstract readonly jobName: string;
  abstract readonly queueName: string;
  readonly every?: number;
  readonly cron?: string;

  abstract perform(jobBody: unknown): Promise<unknown>;

  async performLater(
    jobBody?: unknown,
    opts: PerformLaterOptions = {},
  ): Promise<void> {
    const backend = getBackend();
    const payload: JobPayload = {
      jobName: this.jobName,
      queueName: this.queueName,
      jobBody,
    };
    await backend.enqueue(payload, { delay: opts.delay });
  }
}
```

**Acceptance criteria**:

- [ ] `Job.performLater()` no longer references `Deno.openKv()`
- [ ] Uses `getBackend()` to obtain the active backend adapter
- [ ] Throws a clear error if no backend is configured
- [ ] Public API for `Job` subclasses uses camelCase (`jobName`, `queueName`,
      `performLater`)

---

### Task 5: Refactor `Worker` Class — Use Backend Adapter

**Complexity**: Medium **Dependencies**: Task 0, Task 1, Task 4 **Files to
modify**: `src/lib/worker.ts`

Replace direct `kv.listenQueue()` usage with `backend.listen()`. Also integrate
the `Logger` (currently unused) and add error handling around `perform()`.

```typescript
// src/lib/worker.ts (modified)
import type { BackendAdapter } from "./backend.ts";
import type { JobPayload } from "./types.ts";
import { Logger } from "./logger.ts";

export interface WorkerStartOptions {
  jobsMap: Map<string, new () => import("./job.ts").Job>;
  backend: BackendAdapter;
  queueNames?: string[];
}

export class Worker {
  static async start(
    { jobsMap, backend, queueNames }: WorkerStartOptions,
  ): Promise<void> {
    Logger.workerStarted(jobsMap.size, { queueNames });

    await backend.listen(async (payload: JobPayload) => {
      const { jobName, jobBody, queueName } = payload;
      Logger.jobReceived(jobName, queueName);

      const jobClass = jobsMap.get(jobName);
      if (!jobClass) {
        Logger.unknownJob(jobName, queueName, payload);
        return; // Skip instead of throwing — don't crash the worker
      }

      const start = Date.now();
      Logger.jobStarted(jobName, queueName);

      try {
        const job = new jobClass();
        await job.perform(jobBody);
        Logger.jobSucceeded(jobName, queueName, Date.now() - start);
      } catch (error) {
        const errorMessage = error instanceof Error
          ? error.message
          : String(error);
        Logger.jobFailed(jobName, queueName, errorMessage, Date.now() - start);
        throw error; // Re-throw so the backend can retry
      }
    }, { queueNames });
  }
}
```

**Key improvements over current code**:

- Logger integration (was built but never wired in)
- Error handling with duration tracking
- Unknown jobs are skipped with a log instead of crashing
- Queue names passed through for BullMQ routing

**Acceptance criteria**:

- [ ] `Worker.start()` accepts a `BackendAdapter` instead of using
      `Deno.openKv()` directly
- [ ] Logger is integrated for all job lifecycle events
- [ ] Errors in `perform()` are caught, logged, and re-thrown for backend retry
- [ ] Unknown jobs are logged and skipped (no crash)
- [ ] Queue names are forwarded to `backend.listen()`

---

### Task 6: Refactor `ManifestLoader` — Fix the Broken Path Resolution

**Complexity**: Low **Dependencies**: None **Files to modify**:
`src/lib/manifest_loader.ts`

The current `ManifestLoader.load()` ignores the `manifestPath` parameter and
hardcodes `src/jobs/main.ts`. The `resolvePath()` method exists but is dead
code. Fix this.

```typescript
// src/lib/manifest_loader.ts (modified)
import { join } from "@std/path";
import { toFileUrl } from "@std/path";

export class ManifestLoader {
  static async load({ manifestPath }: { manifestPath: string }) {
    const resolved = this.resolvePath(manifestPath);
    const manifestModule = await import(resolved).catch(
      (error) => this.handleErrorNotFound(error, manifestPath),
    );
    this.validateManifestModule(manifestModule, manifestPath);

    const manifest = manifestModule.default || manifestModule.jobs;
    this.validateManifestType(manifest, manifestPath);

    return manifest;
  }

  // ... (resolvePath now actually called, same implementation as existing dead code)
}
```

**Acceptance criteria**:

- [ ] `manifestPath` parameter is actually used for dynamic import
- [ ] `resolvePath()` is called (no more dead code)
- [ ] Relative paths, absolute paths, and URLs all resolve correctly
- [ ] Error messages reference the user-provided path

---

### Task 7: Refactor `Hermes` Orchestrator — Backend Configuration

**Complexity**: High **Dependencies**: Tasks 0-6 **Files to modify**:
`src/lib/hermes.ts`, `src/lib/types.ts`

This is the central integration task. `Hermes` becomes the facade that wires
together the backend adapter, job registry, and worker.

**Update types** (`src/lib/types.ts`):

```typescript
import type { BackendAdapter } from "./backend.ts";

export type PerformLaterOptions = {
  delay?: number;
};

export type JobPayload = {
  jobName: string;
  queueName: string;
  jobBody: unknown;
  metadata?: Record<string, unknown>;
};

export type WorkerConfig = {
  concurrency?: number;
  gracefulShutdownTimeout?: number;
};

export type HermesParams = {
  manifest: string;
  backend: BackendAdapter;
  worker?: WorkerConfig;
};

export type WorkerParams = {
  jobsMap: Map<string, any>;
  backend: BackendAdapter;
  queueNames?: string[];
};
```

**Update Hermes** (`src/lib/hermes.ts`):

```typescript
import { JobLoader } from "./job_loader.ts";
import { ManifestLoader } from "./manifest_loader.ts";
import { Worker } from "./worker.ts";
import { setBackend } from "./backend_registry.ts";
import type { BackendAdapter } from "./backend.ts";
import type { HermesParams } from "./types.ts";

export interface HermesInstance {
  start(): Promise<void>;
  stop(): Promise<void>;
}

class THermes implements HermesInstance {
  private params: HermesParams;

  constructor(params: HermesParams) {
    this.params = params;
    // Register backend globally so Job.performLater() can use it
    setBackend(params.backend);
  }

  async start(): Promise<void> {
    const jobManifest = await ManifestLoader.load({
      manifestPath: this.params.manifest,
    });
    const jobsMap = await JobLoader(jobManifest).run();

    // Extract unique queue names from registered jobs for BullMQ routing
    const queueNames = [
      ...new Set(
        Array.from(jobsMap.values()).map((cls: any) =>
          new cls().queueName as string
        ),
      ),
    ];

    await Worker.start({
      jobsMap,
      backend: this.params.backend,
      queueNames,
    });
  }

  async stop(): Promise<void> {
    await this.params.backend.close();
  }
}

export const Hermes = (params: HermesParams): HermesInstance => {
  return new THermes(params);
};

export const configure = ({ backend }: { backend: BackendAdapter }): void => {
  setBackend(backend);
};
```

**Note**: `HermesInstance` is an exported interface — this is the return type of
the `Hermes()` factory. This solves both the "no `new`" requirement and JSR's
slow-types check (the return type is explicit, not a private class).

**Acceptance criteria**:

- [ ] `HermesParams` requires a `backend` adapter (no default — explicit is
      better than implicit)
- [ ] `Hermes()` factory returns `HermesInstance` (no `new`, explicit return
      type)
- [ ] `setBackend()` is called during construction so `performLater()` works
      immediately
- [ ] Queue names are extracted from the job registry and passed to the worker
- [ ] `stop()` method exists for graceful shutdown
- [ ] `configure()` exported for enqueue-only scripts
- [ ] The `worker` config is forwarded appropriately

---

### Task 8: Update Public API (Barrel Exports)

**Complexity**: Low **Dependencies**: Tasks 1-7 **Files to modify**:
`src/main.ts`

Export the new public surface:

```typescript
// src/main.ts
export { Job } from "./lib/job.ts";
export { configure, Hermes } from "./lib/hermes.ts";
export type { HermesInstance } from "./lib/hermes.ts";
export { DenoKvBackend } from "./lib/backends/deno_kv.ts";
export { BullMQBackend } from "./lib/backends/bullmq.ts";
export type { BackendAdapter, EnqueueOptions } from "./lib/backend.ts";
export type { BullMQBackendOptions } from "./lib/backends/bullmq.ts";
export type {
  HermesParams,
  JobPayload,
  PerformLaterOptions,
  WorkerConfig,
} from "./lib/types.ts";
```

**Key design decisions**:

- Export `BackendAdapter` as a type so users can implement custom backends
  (e.g., SQS, RabbitMQ). This is the extension point.
- All public API entries are **factory functions** (no `new`): `Hermes()`,
  `DenoKvBackend()`, `BullMQBackend()`, `configure()`. The only exception is
  `Job` (abstract class users extend).
- `HermesInstance` exported as a type for users who need to type-annotate the
  return value of `Hermes()`.

**Acceptance criteria**:

- [ ] All public types and classes are exported
- [ ] `BackendAdapter` interface is exported for custom backend implementations
- [ ] Both built-in backends (`DenoKvBackend`, `BullMQBackend`) exported as
      factory functions
- [ ] `configure()` exported for enqueue-only scripts
- [ ] No `new` keyword required by consumers (except when extending `Job`)
- [ ] No internal implementation details leak (no `Logger`, `ManifestLoader`,
      `JobLoader`, `Worker` exports)

---

### Task 9: Update `deno.json` — Dependencies and JSR Config

**Complexity**: Low **Dependencies**: Tasks 1-8 **Files to modify**: `deno.json`

```json
{
  "name": "@dafu/hermes",
  "version": "0.2.0",
  "license": "MIT",
  "exports": "./src/main.ts",
  "tasks": {
    "dev": "deno run --watch main.ts",
    "worker": "deno run --unstable-kv example/worker.ts",
    "worker:redis": "deno run example/worker_redis.ts",
    "enqueue": "deno run --unstable-kv example/enqueue.ts",
    "enqueue:redis": "deno run example/enqueue_redis.ts",
    "test": "deno test --unstable-kv",
    "lint": "deno lint",
    "fmt": "deno fmt",
    "check": "deno check src/main.ts"
  },
  "imports": {
    "@std/path": "jsr:@std/path@^1.1.2",
    "@std/assert": "jsr:@std/assert@^1",
    "@std/testing": "jsr:@std/testing@^1",
    "@hermes": "./src/main.ts"
  }
}
```

**Note**: Fix the missing comma in the current `imports` object. Bump version to
`0.2.0` for the breaking change (backend is now required).

**Acceptance criteria**:

- [ ] JSON syntax is valid (fix missing comma)
- [ ] npm dependencies (`bullmq`, `ioredis`) resolve via Deno's npm specifier
      support — no `imports` entry needed; `npm:bullmq` in source is sufficient
- [ ] Test task added
- [ ] Type-check task added (`deno check`)
- [ ] Lint task added (`deno lint`)
- [ ] Version bumped to `0.2.0`

---

### Task 10: Update Examples

**Complexity**: Low **Dependencies**: Tasks 1-9 **Files to modify**:
`example/worker.ts`, `example/enqueue.ts` **Files to create**:
`example/worker_redis.ts`, `example/enqueue_redis.ts`

**Update existing Deno KV examples**:

```typescript
// example/worker.ts
import { DenoKvBackend, Hermes } from "@hermes";

const hermes = Hermes({
  manifest: "./example/jobs/main.ts",
  backend: DenoKvBackend(),
  worker: { gracefulShutdownTimeout: 1000 },
});

await hermes.start();
console.log("Hermes is running (Deno KV backend)");
```

```typescript
// example/enqueue.ts
import { configure, DenoKvBackend } from "@hermes";
import { ExampleJob } from "./jobs/example_job.ts";

// Configure the backend without starting workers
configure({ backend: DenoKvBackend() });

const job = new ExampleJob();
await job.performLater({ message: "hello" });
```

**Design note on `enqueue.ts`**: After the refactor, a backend must be
configured before enqueuing. The `configure()` function sets the backend
globally without starting workers — this is the lightweight path for web servers
or scripts that only enqueue.

**Add Redis examples**:

```typescript
// example/worker_redis.ts
import { Hermes } from "@hermes";
import { BullMQBackend } from "@dafu/hermes/backends/bullmq";

const hermes = Hermes({
  manifest: "./example/jobs/main.ts",
  backend: BullMQBackend({
    connection: { host: "localhost", port: 6379 },
    concurrency: 5,
  }),
});

await hermes.start();
console.log("Hermes is running (Redis/BullMQ backend)");
```

```typescript
// example/enqueue_redis.ts
import { configure } from "@hermes";
import { BullMQBackend } from "@dafu/hermes/backends/bullmq";
import { ExampleJob } from "./jobs/example_job.ts";

configure({
  backend: BullMQBackend({
    connection: { host: "localhost", port: 6379 },
  }),
});

const job = new ExampleJob();
await job.performLater({ message: "hello from redis" });
console.log("Job enqueued to Redis");
```

**Acceptance criteria**:

- [ ] Deno KV examples updated to use new API
- [ ] Redis/BullMQ examples added and functional
- [ ] No `new` keyword used for library classes (only for user job instances)
- [ ] `configure()` function used for enqueue-only scripts
- [ ] Example job class updated to camelCase (`jobName`, `queueName`,
      `performLater`)

---

### Task 11: Write Tests

**Complexity**: High **Dependencies**: Tasks 1-10 **Files to create**:
`src/lib/tests/`, multiple test files

#### Test Structure

```
src/lib/tests/
  job_test.ts
  job_loader_test.ts
  manifest_loader_test.ts
  worker_test.ts
  hermes_test.ts
  backends/
    deno_kv_test.ts
    bullmq_test.ts
  backend_registry_test.ts
```

#### Test Strategy: In-Memory Backend for Unit Tests

Create a simple in-memory backend for testing that doesn't require Deno KV or
Redis:

```typescript
// src/lib/tests/helpers/mock_backend.ts
import type { BackendAdapter, EnqueueOptions } from "../../backend.ts";
import type { JobPayload } from "../../types.ts";

export class MockBackend implements BackendAdapter {
  enqueued: { payload: JobPayload; options?: EnqueueOptions }[] = [];
  private handler: ((payload: JobPayload) => Promise<void>) | null = null;

  async enqueue(payload: JobPayload, options?: EnqueueOptions): Promise<void> {
    this.enqueued.push({ payload, options });
    // Optionally process immediately if a handler is registered
    if (this.handler) {
      await this.handler(payload);
    }
  }

  async listen(
    handler: (payload: JobPayload) => Promise<void>,
    _options?: { queueNames?: string[] },
  ): Promise<void> {
    this.handler = handler;
  }

  async close(): Promise<void> {
    this.handler = null;
  }

  // Test helper: manually trigger processing
  async process(payload: JobPayload): Promise<void> {
    if (!this.handler) throw new Error("No handler registered");
    await this.handler(payload);
  }
}
```

#### Test Cases by File

**`backend_registry_test.ts`**:

- `getBackend()` throws if no backend configured
- `setBackend()` followed by `getBackend()` returns the backend
- `setBackend()` replaces previous backend

**`job_test.ts`**:

- Subclass can define `jobName` and `queueName`
- `performLater()` delegates to the configured backend
- `performLater()` throws if no backend configured
- `performLater()` passes delay option through
- `performLater()` constructs correct `JobPayload`

**`job_loader_test.ts`**:

- Loads jobs from an array of classes into a Map
- Detects duplicate `jobName` and throws
- Returns empty Map for empty manifest

**`manifest_loader_test.ts`**:

- Loads manifest from a file path (use a test fixture)
- Throws on missing file
- Throws if export is not an array
- Throws if no `default` or `jobs` export

**`worker_test.ts`**:

- Dispatches to correct job class based on `jobName`
- Logs unknown job and skips (does not throw)
- Catches and re-throws errors from `perform()`
- Measures duration for succeeded/failed jobs

**`backends/deno_kv_test.ts`** (requires `--unstable-kv`):

- Enqueues and dequeues a payload through Deno KV
- Respects delay option
- `close()` cleans up the KV handle

**`backends/bullmq_test.ts`** (requires running Redis):

- Enqueues to the correct queue based on `queueName`
- Worker processes jobs from the correct queue
- Concurrency configuration is respected
- `close()` shuts down queues and workers
- Mark as integration test (skip in CI if no Redis)

**Acceptance criteria**:

- [ ] All unit tests pass with `deno test`
- [ ] MockBackend enables testing without external dependencies
- [ ] Deno KV tests pass with `--unstable-kv` flag
- [ ] BullMQ tests are separated and can be skipped without Redis
- [ ] Test coverage for core logic: job dispatch, error handling, registry,
      manifest loading

---

### Task 12: JSR Publishing Readiness

**Complexity**: Low **Dependencies**: Tasks 1-11 **Files to modify**:
`deno.json`, `src/main.ts`

Ensure the package is publishable to JSR:

1. **No slow types**: All exported functions and classes must have explicit
   return types. JSR's publishing pipeline runs a "slow types" check. Verify
   with `deno publish --dry-run`.

2. **Exports field**: `deno.json` already has `"exports": "./src/main.ts"` —
   good.

3. **No `Deno` namespace in core path**: The core `Job` class and `Hermes`
   orchestrator should not reference `Deno.*` APIs directly. Only the
   `DenoKvBackend` should. This ensures the library's core is runtime-agnostic
   (future: could work in Node.js too).

4. **npm specifier compatibility**: `npm:bullmq` works in Deno but won't resolve
   in a pure JSR install. The BullMQ backend import should be a separate export
   path or clearly documented as requiring npm compatibility.

**Consider**: Split exports in `deno.json` so the BullMQ backend is a separate
entry point:

```json
{
  "exports": {
    ".": "./src/main.ts",
    "./backends/bullmq": "./src/lib/backends/bullmq.ts"
  }
}
```

This way, `import { BullMQBackend } from "@dafu/hermes/backends/bullmq"` — and
users who only want Deno KV don't pull in the npm dependency. **This is the
recommended approach.**

Update `src/main.ts` to NOT export BullMQ:

```typescript
// src/main.ts — core exports only (no npm dependencies)
export { Job } from "./lib/job.ts";
export { configure, Hermes } from "./lib/hermes.ts";
export type { HermesInstance } from "./lib/hermes.ts";
export { DenoKvBackend } from "./lib/backends/deno_kv.ts";
export type { BackendAdapter, EnqueueOptions } from "./lib/backend.ts";
export type {
  HermesParams,
  JobPayload,
  PerformLaterOptions,
  WorkerConfig,
} from "./lib/types.ts";
```

**Acceptance criteria**:

- [ ] `deno publish --dry-run` succeeds with no errors
- [ ] `deno check src/main.ts` passes (no type errors)
- [ ] `deno lint` passes
- [ ] `deno fmt --check` passes
- [ ] BullMQ backend is a separate export path (no npm dep in core)
- [ ] All exported symbols have explicit type annotations (no slow types)

---

## Implementation Details

### File Structure (Final)

```
src/
  main.ts                          # Barrel exports (public API)
  lib/
    backend.ts                     # BackendAdapter interface
    backend_registry.ts            # Global backend singleton
    hermes.ts                      # Orchestrator (facade)
    job.ts                         # Abstract Job class
    job_loader.ts                  # Job registry builder
    logger.ts                      # Structured JSON logger
    manifest_loader.ts             # Dynamic manifest importer
    types.ts                       # Shared type definitions
    worker.ts                      # Job dispatch/processing
    backends/
      deno_kv.ts                   # Deno KV adapter
      bullmq.ts                    # BullMQ/Redis adapter
    tests/
      helpers/
        mock_backend.ts            # In-memory backend for tests
        test_jobs.ts               # Test job fixtures
      backend_registry_test.ts
      job_test.ts
      job_loader_test.ts
      manifest_loader_test.ts
      worker_test.ts
      hermes_test.ts
      backends/
        deno_kv_test.ts
        bullmq_test.ts
example/
  jobs/
    example_job.ts                 # (unchanged)
    main.ts                        # (unchanged)
  worker.ts                        # Updated for new API
  worker_redis.ts                  # New: Redis example
  enqueue.ts                       # Updated for new API
  enqueue_redis.ts                 # New: Redis example
```

### Design Patterns Used

| Pattern             | Where                                                           | Why                                                                     |
| ------------------- | --------------------------------------------------------------- | ----------------------------------------------------------------------- |
| **Strategy**        | `BackendAdapter` interface                                      | Swap queue backends without changing business logic                     |
| **Template Method** | `Job.perform()`                                                 | Users implement the abstract method; framework handles lifecycle        |
| **Facade**          | `Hermes` orchestrator                                           | Single entry point hides manifest loading, job registry, worker startup |
| **Registry**        | `JobLoader` + `backend_registry`                                | Name-to-class dispatch; global backend singleton                        |
| **Factory**         | `Hermes()`, `DenoKvBackend()`, `BullMQBackend()`, `JobLoader()` | No `new` keyword for consumers; hide internal class construction        |
| **Adapter**         | `TDenoKvBackend`, `TBullMQBackend`                              | Adapt different queue APIs to a unified interface                       |

### Potential Gotchas

1. **BullMQ + Deno compatibility**: BullMQ is an npm package that depends on
   `ioredis`. Test that `npm:bullmq` works correctly in Deno. If there are
   compatibility issues, consider using `npm:bullmq@^5` with specific Deno
   compatibility flags or an alternative Redis client.

2. **Deno KV `listenQueue` is blocking**: `kv.listenQueue()` does not return a
   promise that resolves when listening starts — it registers a callback. The
   `Worker.start()` method should account for this (it returns `void`, not a
   long-lived promise).

3. **BullMQ Worker is event-based**: BullMQ workers emit events (`completed`,
   `failed`, etc.). Consider hooking into these for the Logger integration
   rather than wrapping the handler.

4. **JSR slow types**: Deno's JSR publishing requires all exports to have
   explicit type annotations. All factory functions must declare their return
   types explicitly. This is already handled: `Hermes()` returns
   `HermesInstance`, `DenoKvBackend()` and `BullMQBackend()` return
   `BackendAdapter`. Verify with `deno publish --dry-run`.

5. **Global backend state**: The `backend_registry` is module-level state. This
   works for single-backend scenarios (like Sidekiq) but means you can't run two
   Hermes instances with different backends simultaneously. This is an
   acceptable tradeoff for simplicity (Sidekiq has the same constraint).

6. **`performLater()` before `start()`**: Users might try to enqueue before
   configuring a backend. The `getBackend()` error message must be clear. The
   `configure()` function enables enqueue-only scripts without starting workers.

---

## Testing Strategy

### Unit Tests (no external deps)

Run with: `deno test` (most tests don't need `--unstable-kv`)

- Use `MockBackend` for all unit tests
- Test job dispatch logic, error handling, registry building
- Test manifest loading with fixture files

### Integration Tests — Deno KV

Run with: `deno test --unstable-kv`

- End-to-end: enqueue via `DenoKvBackend`, process via Worker, verify job
  executed
- Use `Deno.openKv(":memory:")` or a temp path for test isolation if possible

### Integration Tests — BullMQ/Redis

Run with: `deno test` (requires Redis on localhost:6379)

- End-to-end: enqueue via `BullMQBackend`, process via BullMQ Worker, verify job
  executed
- Use a unique queue prefix per test run for isolation
- Clean up queues after tests
- Skip gracefully if Redis is unavailable:
  ```typescript
  const redisAvailable = await checkRedis();
  Deno.test({ name: "BullMQ: enqueue and process", ignore: !redisAvailable, fn: async () => { ... } });
  ```

### Linting & Type Checking

| Check             | Command                  |
| ----------------- | ------------------------ |
| Lint              | `deno lint`              |
| Format            | `deno fmt --check`       |
| Type check        | `deno check src/main.ts` |
| JSR publish check | `deno publish --dry-run` |

---

## Definition of Done

- [ ] All subtasks (0-12) completed
- [ ] All identifiers migrated from snake_case to camelCase
- [ ] `BackendAdapter` interface defined and implemented for Deno KV and BullMQ
- [ ] `Job.performLater()` is backend-agnostic
- [ ] `Worker` dispatches via backend adapter with logging and error handling
- [ ] `ManifestLoader` uses the provided path (bug fixed)
- [ ] `Hermes` orchestrator accepts and configures backend
- [ ] `configure()` function enables enqueue-only scripts
- [ ] BullMQ backend is a separate export path (no npm dep pollution)
- [ ] All tests pass: `deno test --unstable-kv`
- [ ] `deno lint` passes
- [ ] `deno fmt --check` passes
- [ ] `deno check src/main.ts` passes
- [ ] `deno publish --dry-run` succeeds
- [ ] Examples updated and functional for both backends
- [ ] Code follows TypeScript conventions (camelCase identifiers, PascalCase
      types)
