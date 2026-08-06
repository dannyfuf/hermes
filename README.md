# Hermes

A backend-agnostic background job processing library for TypeScript. Define jobs
as classes, enqueue them from anywhere, and process them with pluggable backends
like **Deno KV** or **Redis (BullMQ)**.

## Features

- **Backend-agnostic**: Swap between Deno KV and Redis/BullMQ (or write your own
  adapter)
- **Job classes**: Encapsulate job logic in typed, reusable classes
- **Manifest-based registration**: Auto-discover jobs from a single manifest
  file
- **Delayed jobs**: Schedule jobs to run after a specified delay
- **Timeouts and cancellation**: Release stuck worker slots and cooperatively
  abort supported I/O
- **Priorities and retries**: BullMQ-native ordering, attempts, and backoff
- **Queue health stats**: Inspect counts and detect unusually old active jobs
- **Queue routing**: Route jobs by name; BullMQ processes queues independently
  while Deno KV uses one global queue
- **Structured logging**: JSON-formatted lifecycle events for every job
- **Graceful shutdown**: Clean worker shutdown with configurable timeouts
- **Deno Deploy compatible**: Works out of the box on Deno Deploy with the Deno
  KV backend

## Installation

```bash
# From JSR
deno add @dafu/hermes
```

Or import directly in your `deno.json`:

```json
{
  "imports": {
    "@dafu/hermes": "jsr:@dafu/hermes"
  }
}
```

## Quick Start

### 1. Define a Job

Create a job class that extends the base `Job` class. Each job must declare a
unique `jobName` and a `queueName`:

```typescript
// jobs/email_job.ts
import { Job } from "@dafu/hermes";

interface EmailPayload {
  to: string;
  subject: string;
  body: string;
}

export class EmailJob extends Job {
  jobName = "send_email";
  queueName = "emails";

  async perform(jobBody: unknown): Promise<void> {
    const { to, subject, body } = jobBody as EmailPayload;
    console.log(`Sending email to ${to}: ${subject}`);
    // Your email sending logic here
  }
}
```

### 2. Create a Manifest

Export all your job classes as an array. Hermes supports both `default` and
named `jobs` exports:

```typescript
// jobs/main.ts
import { EmailJob } from "./email_job.ts";
import { ReportJob } from "./report_job.ts";

// Default export
export default [EmailJob, ReportJob];

// OR named export
// export const jobs = [EmailJob, ReportJob];
```

### 3. Start a Worker

Choose a backend and point the worker at your manifest:

```typescript
// worker.ts
import { DenoKvBackend, Hermes } from "@dafu/hermes";

const hermes = Hermes({
  manifest: "./jobs/main.ts",
  backend: DenoKvBackend(),
});

await hermes.start();
console.log("Worker is running");

const shutdown = async () => {
  await hermes.stop();
  Deno.exit(0);
};
Deno.addSignalListener("SIGINT", shutdown);
Deno.addSignalListener("SIGTERM", shutdown);
```

### 4. Enqueue Jobs

Before enqueueing, you must configure the backend. You can either start a full
Hermes instance or use `configure()` for enqueue-only processes:

```typescript
// enqueue.ts
import { configure, DenoKvBackend } from "@dafu/hermes";
import { EmailJob } from "./jobs/email_job.ts";

// Configure the backend (required before calling performLater)
configure({ backend: DenoKvBackend() });

const job = new EmailJob();
await job.performLater({
  to: "user@example.com",
  subject: "Welcome!",
  body: "Thanks for signing up!",
});

console.log("Job enqueued");
```

## Backends

Hermes ships with two built-in backends. You can also implement the
`BackendAdapter` interface to create your own.

### Deno KV

Zero-configuration backend using Deno's built-in KV store. Works on Deno Deploy
out of the box.

```typescript
import { DenoKvBackend } from "@dafu/hermes";

// Default (uses Deno's default KV store)
const backend = DenoKvBackend();

// Custom KV path (local development)
const backend = DenoKvBackend({ path: "./my-data.sqlite" });
```

Run local workers with `--unstable-kv`. Workers that register recurring Deno KV
jobs also require `--unstable-cron` at runtime (these flags are not needed on
Deno Deploy):

```bash
deno run --unstable-kv --unstable-cron worker.ts
```

Deno cron registration names use a readable form of `jobName` plus a stable hash
of the raw name. The total is capped at 64 characters for compatibility with the
local Deno runtime, so punctuation collisions and long names are safe. Hermes
validates the complete set before registering any cron.

### Redis / BullMQ

Production-grade backend powered by [BullMQ](https://docs.bullmq.io/). It
exposes concurrency, priorities, attempts/backoff, bounded retention, and queue
statistics. Requires a running Redis instance.

```typescript
import { BullMQBackend } from "@dafu/hermes/backends/bullmq";

const backend = BullMQBackend({
  connection: {
    host: "localhost",
    port: 6379,
    // password: "secret",
  },
  concurrency: 5, // Process up to 5 jobs concurrently per queue
  defaultQueueName: "default", // Fallback queue name
  defaultJobOptions: {
    attempts: 3,
    backoff: { type: "exponential", delay: 1000 },
    removeOnComplete: { count: 1000 },
    removeOnFail: { count: 5000 },
  },
});
```

BullMQ retains the most recent 1,000 completed and 5,000 failed jobs by default.
This is a behavior change in 0.3.0 that prevents unbounded Redis growth.
Override either value with `defaultJobOptions`; those options apply to ordinary
and recurring jobs, and the option accepts BullMQ's full `DefaultJobOptions`
type. Properties explicitly set to `undefined` are ignored so they cannot erase
the bounded defaults; explicit values such as `false` and `0` remain valid
overrides.

```bash
deno run worker.ts
```

### Custom Backend

Implement the `BackendAdapter` interface to use any queue system:

```typescript
import type { BackendAdapter, EnqueueOptions } from "@dafu/hermes";
import type { JobPayload } from "@dafu/hermes";

class MyCustomBackend implements BackendAdapter {
  async enqueue(payload: JobPayload, options?: EnqueueOptions): Promise<void> {
    // Add the job to your queue system
  }

  async listen(
    handler: (payload: JobPayload) => Promise<void>,
    options?: { queueNames?: string[]; concurrency?: number },
  ): Promise<void> {
    // Start consuming jobs and call handler() for each one
  }

  async close(options?: { force?: boolean }): Promise<void> {
    // Clean up connections
  }
}
```

## Recurring Jobs

Define jobs that run on a schedule using `every` (interval) or `cron`
(expression) properties. Recurring jobs are registered automatically when
`hermes.start()` is called.

### Interval-based (`every`)

Use `[number][unit]` format where unit is `s` (seconds), `m` (minutes), `h`
(hours), or `d` (days). Any positive integer amount is accepted by the core:

```typescript
export class HealthCheckJob extends Job {
  jobName = "health_check";
  queueName = "default";
  every = "5m"; // Run every 5 minutes

  async perform(): Promise<void> {
    console.log("Running health check...");
  }
}
```

### Cron-based (`cron`)

Use standard 5 or 6 field cron expressions:

```typescript
export class DailyReportJob extends Job {
  jobName = "daily_report";
  queueName = "reports";
  cron = "0 9 * * 1-5"; // 9 AM Monday-Friday

  async perform(): Promise<void> {
    console.log("Generating daily report...");
  }
}
```

### Backend Differences

| Feature            | Deno KV                                                                                                      | BullMQ (Redis)                                   |
| ------------------ | ------------------------------------------------------------------------------------------------------------ | ------------------------------------------------ |
| `every` support    | Minute divisors of 60, hour divisors of 24, and exactly `1d`; exact multiples convert upward (`120s` → `2m`) | Any safe positive `s`, `m`, `h`, or `d` interval |
| `cron` support     | Yes                                                                                                          | Yes                                              |
| Deduplication      | Hashed, 64-character-capped names via `Deno.cron`                                                            | Automatic via `upsertJobScheduler`               |
| Overlap prevention | Built-in                                                                                                     | Built-in                                         |
| Priorities         | Ignored                                                                                                      | Lower number runs first (`1`–`2^21`)             |
| Worker concurrency | Single global KV listener; setting ignored                                                                   | Configurable per queue worker                    |
| Local runtime flag | Recurrence requires `--unstable-cron` in addition to `--unstable-kv`                                         | None                                             |

Intervals such as `7m`, `5h`, `2d`, `90s`, `90m`, and `25h` cannot preserve a
true elapsed cadence through resetting cron fields and fail during registration
with guidance to use BullMQ. BullMQ uses millisecond intervals and accepts these
values directly.

## Delayed Jobs

Schedule a job to execute after a delay (in milliseconds):

```typescript
const job = new EmailJob();

// Send the welcome email 5 minutes from now
await job.performLater(
  { to: "user@example.com", subject: "Welcome!", body: "Hi!" },
  { delay: 5 * 60 * 1000 },
);
```

## Priorities

Declare a default on the job or override it per enqueue. BullMQ uses lower
numbers as higher priority (`1` through `2^21`):

```typescript
export class PaymentJob extends Job {
  jobName = "process_payment";
  queueName = "payments";
  priority = 5;

  async perform(jobBody: unknown): Promise<void> {
    // ...
  }
}

await new PaymentJob().performLater(payload, { priority: 1 });
```

The priority is also applied to recurring jobs. Deno KV accepts the portable API
but ignores priority.

## Execution Timeouts and Cooperative Cancellation

> **Production recommendation:** always set `worker.defaultJobTimeout` or a
> per-job `timeout`. Without one, a permanently pending `perform()` can occupy a
> worker slot forever.

Timeouts may be milliseconds or duration strings using the `every` grammar:

```typescript
import type { JobContext } from "@dafu/hermes";

export class FetchReportJob extends Job {
  jobName = "fetch_report";
  queueName = "reports";
  timeout = "30s";

  async perform(_body: unknown, context?: JobContext): Promise<void> {
    await fetch("https://example.com/report", { signal: context?.signal });
  }
}

const hermes = Hermes({
  manifest: "./jobs/main.ts",
  backend,
  worker: { defaultJobTimeout: "2m" },
});
```

Resolved timeouts must not exceed `2_147_483_647` milliseconds (about 24.8
days), the maximum reliable JavaScript timer delay. Hermes validates every job
timeout before registering recurring schedules or starting listeners.

A job's `timeout` overrides `defaultJobTimeout`. On expiry Hermes aborts the
context signal, logs `job_failed`, and throws an error whose `name` is
`"JobTimeoutError"`; the backend then applies its normal retry/failure policy.
The error class is intentionally internal, so detect it by name.

The timeout always releases the Hermes/BullMQ worker slot. It cannot forcibly
stop JavaScript already running inside `perform()`: pass `context.signal` to
APIs such as `fetch` or database clients that support `AbortSignal` so the
underlying I/O is actually cancelled. With retries enabled, a timed-out body
that ignores the signal can overlap its retry, so timeout-enabled jobs must be
abort-safe and idempotent. Existing jobs that implement only `perform(jobBody)`
remain valid.

## Monitoring Queue Health

After `start()`, BullMQ-backed instances expose one stats entry per manifest
queue:

```typescript
const queues = await hermes.stats();
const unhealthy = queues.find(
  (queue) => (queue.oldestActiveJobAgeMs ?? 0) > 5 * 60_000,
);
if (unhealthy) throw new Error(`Stuck queue: ${unhealthy.queueName}`);
```

Each entry includes `waiting`, `active`, `delayed`, `failed`, and `completed`
counts plus `oldestActiveJobAgeMs` when an active job has a processing
timestamp. For BullMQ, `waiting` is the full ready backlog: ordinary waiting
jobs plus prioritized jobs. The optional `prioritized` count exposes the raw
prioritized subset. Calling `stats()` before `start()`, after `stop()`, or with
a backend such as Deno KV that does not implement the optional capability throws
a clear error.

## Graceful Shutdown

`hermes.stop()` stops intake and gives in-flight work up to
`worker.gracefulShutdownTimeout` milliseconds to finish (default: `30_000`). If
that deadline expires, Hermes logs `worker_force_closed` and asks the backend to
force-close. For BullMQ this returns without waiting for in-flight handlers, so
their locks can expire and another worker can recover the jobs. The graceful
deadline starts when `stop()` is requested, including while `start()` is still
settling. Hermes waits at most another fixed 5 seconds for the force-close
attempt, so a stuck backend connection cannot make `stop()` unbounded. `stop()`
is idempotent. The configured shutdown timeout must be a positive safe integer
no greater than `2_147_483_647` milliseconds (about 24.8 days).

On graceful close, Deno KV drains tracked queue handlers before closing its KV
handle. BullMQ pauses workers, drains active handlers, and closes Redis
connections. Hermes also waits for timed-out `perform()` bodies that are still
running. On force close, Deno KV closes its handle immediately and BullMQ
disconnects without waiting for handlers; neither path waits for orphaned job
bodies beyond the graceful deadline.

Install both termination handlers in worker processes:

```typescript
const shutdown = async () => {
  await hermes.stop();
  Deno.exit(0);
};
Deno.addSignalListener("SIGINT", shutdown);
Deno.addSignalListener("SIGTERM", shutdown);
```

## Multi-Queue Architecture

Jobs declare which queue they belong to via `queueName`. This lets you run
specialized workers that only process specific queues, or a single worker that
handles everything.

```typescript
// A high-priority job
export class PaymentJob extends Job {
  jobName = "process_payment";
  queueName = "payments";

  async perform(jobBody: unknown): Promise<void> {
    // ...
  }
}

// A low-priority job
export class ReportJob extends Job {
  jobName = "generate_report";
  queueName = "reports";

  async perform(jobBody: unknown): Promise<void> {
    // ...
  }
}
```

Hermes automatically extracts all unique queue names from the manifest and
listens on each one. With the BullMQ backend, each queue gets its own dedicated
BullMQ Worker for true multi-queue processing. Deno KV has one global queue and
cannot filter by `queueName`.

## API Reference

### `Hermes(params)`

Creates a Hermes instance. Returns an object with `start()`, `stop()`, and
`stats()` methods.

```typescript
const hermes = Hermes({
  manifest: "./jobs/main.ts", // Path to your jobs manifest file
  backend: DenoKvBackend(), // A BackendAdapter instance
  worker: {
    concurrency: 5,
    defaultJobTimeout: "2m",
    gracefulShutdownTimeout: 10000, // Shutdown timeout in ms (default: 30000)
  },
});

await hermes.start(); // Load manifest, register jobs, start worker
await hermes.stop(); // Gracefully shut down
```

Calling `start()` consumes the instance's single startup attempt. If startup
fails, later `start()` calls report the original failure and require creating a
new Hermes instance; retrying the same instance is unsafe because startup may
already have registered durable schedules.

Calling `stop()` before the first `start()` makes that later `start()` reject
and requires a new instance. If `stop()` is requested while `start()` is already
in progress, startup cancellation is cooperative: `start()` may resolve without
a live worker after the current startup step settles, while `stop()` remains
bounded by the shutdown deadline.

### `configure({ backend })`

Sets the global backend for enqueuing jobs without starting a worker. Use this
in processes that only enqueue (e.g., a web server):

```typescript
import { configure, DenoKvBackend } from "@dafu/hermes";

configure({ backend: DenoKvBackend() });
// Now you can call job.performLater() anywhere in this process
```

### `Job` (abstract class)

Base class for all jobs.

| Property / Method               | Type                          | Description                                                   |
| ------------------------------- | ----------------------------- | ------------------------------------------------------------- |
| `jobName`                       | `string` (abstract)           | Unique identifier for the job type                            |
| `queueName`                     | `string` (abstract)           | Queue this job is dispatched to                               |
| `every`                         | `string?`                     | Interval schedule, e.g. `"5m"`, `"1h"`, `"7d"`                |
| `cron`                          | `string?`                     | Cron expression, e.g. `"0 9 * * 1-5"`                         |
| `timeout`                       | `string \| number?`           | Per-job execution timeout; overrides the worker default       |
| `priority`                      | `number?`                     | Default BullMQ priority (lower is higher)                     |
| `perform(jobBody, context?)`    | `Promise<unknown>` (abstract) | The work the job does; `context.signal` supports cancellation |
| `performLater(jobBody?, opts?)` | `Promise<void>`               | Enqueue the job for async processing                          |
| `isRecurring()`                 | `boolean`                     | Whether the job has a recurring schedule                      |

### `DenoKvBackend(options?)`

| Option | Type     | Default     | Description                       |
| ------ | -------- | ----------- | --------------------------------- |
| `path` | `string` | `undefined` | Custom path for the KV store file |

### `BullMQBackend(options)`

| Option                               | Type                                                | Default           | Description                           |
| ------------------------------------ | --------------------------------------------------- | ----------------- | ------------------------------------- |
| `connection.host`                    | `string`                                            | `undefined`       | Redis host                            |
| `connection.port`                    | `number`                                            | `undefined`       | Redis port                            |
| `connection.password`                | `string`                                            | `undefined`       | Redis password                        |
| `connection.url`                     | `string`                                            | `undefined`       | Redis connection URL                  |
| `concurrency`                        | `number`                                            | `1`               | Max concurrent jobs per queue worker  |
| `defaultQueueName`                   | `string`                                            | `"default"`       | Fallback queue when none is specified |
| `defaultJobOptions.attempts`         | `number`                                            | `1`               | Total BullMQ processing attempts      |
| `defaultJobOptions.backoff`          | `{ type: "fixed" \| "exponential"; delay: number }` | `undefined`       | Retry delay policy                    |
| `defaultJobOptions.removeOnComplete` | `boolean \| number \| { age?, count? }`             | `{ count: 1000 }` | Completed-job retention               |
| `defaultJobOptions.removeOnFail`     | `boolean \| number \| { age?, count? }`             | `{ count: 5000 }` | Failed-job retention                  |

`worker.concurrency` passed to `Hermes()` takes precedence over the backend's
`concurrency`; the fallback is `1`.

### Types

```typescript
// The payload structure sent through the queue
type JobPayload = {
  jobName: string;
  queueName: string;
  jobBody: unknown;
  metadata?: Record<string, unknown>;
};

// Options for performLater
type PerformLaterOptions = {
  delay?: number; // Delay in milliseconds
  priority?: number; // BullMQ: lower values run first
};

type JobContext = {
  signal: AbortSignal;
};

// Worker configuration
type WorkerConfig = {
  concurrency?: number;
  defaultJobTimeout?: string | number; // Maximum: 2_147_483_647ms
  gracefulShutdownTimeout?: number; // Default: 30000ms; same maximum
};

interface QueueStats {
  queueName: string;
  counts: {
    waiting: number;
    active: number;
    delayed: number;
    failed: number;
    completed: number;
    prioritized?: number; // Raw BullMQ subset included in waiting
  };
  oldestActiveJobAgeMs?: number;
}

// Main configuration
type HermesParams = {
  manifest: string; // Path to the manifest file
  backend: BackendAdapter; // Backend instance
  worker?: WorkerConfig;
};
```

## Logging

Hermes emits structured JSON logs for all job lifecycle events:

```json
{"timestamp":"2026-01-15T12:00:00.000Z","event":"worker_started","registeredJobs":3,"config":{"queueNames":["emails","reports"]}}
{"timestamp":"2026-01-15T12:00:01.000Z","event":"job_received","jobName":"send_email","queueName":"emails"}
{"timestamp":"2026-01-15T12:00:01.001Z","event":"job_started","jobName":"send_email","queueName":"emails"}
{"timestamp":"2026-01-15T12:00:01.050Z","event":"job_succeeded","jobName":"send_email","queueName":"emails","durationMs":49}
```

### Event Types

| Event                      | Description                                                            |
| -------------------------- | ---------------------------------------------------------------------- |
| `worker_started`           | Worker is listening for jobs                                           |
| `job_received`             | A job payload was dequeued                                             |
| `job_started`              | Job `perform()` is being called                                        |
| `job_succeeded`            | Job completed without errors                                           |
| `job_failed`               | Job threw an error (includes error message and duration)               |
| `worker_job_failed`        | BullMQ marked a job attempt failed (includes job ID and attempts made) |
| `job_stalled`              | BullMQ detected a stalled job                                          |
| `job_skipped`              | Job was skipped (e.g., queue filtering)                                |
| `unknown_job`              | Received a job with an unregistered `jobName`                          |
| `recurring_job_registered` | A recurring job schedule was registered at startup                     |
| `recurring_job_skipped`    | A recurring job schedule registration was skipped                      |
| `worker_error`             | A BullMQ worker connection/runtime error occurred                      |
| `queue_error`              | A BullMQ enqueue-side queue connection error occurred                  |
| `worker_closed`            | A BullMQ queue worker closed                                           |
| `worker_stopping`          | `hermes.stop()` began                                                  |
| `worker_force_closed`      | The graceful shutdown deadline elapsed                                 |
| `worker_stopped`           | Worker has fully shut down                                             |

## Error Handling

- **Duplicate `jobName`**: Hermes throws at startup if two job classes share the
  same `jobName`.
- **Invalid manifest**: Throws if the manifest file does not export an array via
  `default` or `jobs`.
- **Manifest not found**: Throws with a clear message if the manifest path is
  wrong.
- **Unknown job**: If a queued message references an unregistered `jobName`, the
  worker logs an `unknown_job` event and skips it.
- **Job execution failure**: If `perform()` throws, the error is logged with the
  `job_failed` event (including duration) and re-thrown to the backend, which
  can handle retries if supported.
- **Job timeout**: Hermes aborts the job context, logs and rethrows a
  `JobTimeoutError`, and frees the worker slot. Configure attempts/backoff on
  BullMQ if timed-out work should retry.

## Running the Examples

The repository includes working examples for both backends:

```bash
# Deno KV backend
deno task worker          # Start the worker
deno task enqueue         # Enqueue a job

# Redis/BullMQ backend (requires a running Redis instance)
deno task worker:redis    # Start the worker
deno task enqueue:redis   # Enqueue a job
```

## Running Tests

```bash
deno task test
```

## License

MIT
