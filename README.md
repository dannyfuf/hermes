# Hermes

A backend-agnostic background job processing library for Deno. Define jobs as
classes, enqueue them from anywhere, and process them with pluggable backends
like **Deno KV** or **Redis (BullMQ)**.

## Features

- **Backend-agnostic**: Swap between Deno KV and Redis/BullMQ (or write your
  own adapter)
- **Job classes**: Encapsulate job logic in typed, reusable classes
- **Manifest-based registration**: Auto-discover jobs from a single manifest
  file
- **Delayed jobs**: Schedule jobs to run after a specified delay
- **Multi-queue support**: Route jobs to different queues and process them
  independently
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

Export all your job classes as an array. Hermes supports both `default` and named
`jobs` exports:

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

// Graceful shutdown
Deno.addSignalListener("SIGINT", async () => {
  await hermes.stop();
  Deno.exit(0);
});
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

Run workers with the `--unstable-kv` flag (not needed on Deno Deploy):

```bash
deno run --unstable-kv worker.ts
```

### Redis / BullMQ

Production-grade backend powered by [BullMQ](https://docs.bullmq.io/). Supports
concurrency, retries, and all BullMQ features. Requires a running Redis
instance.

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
});
```

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
    options?: { queueNames?: string[] },
  ): Promise<void> {
    // Start consuming jobs and call handler() for each one
  }

  async close(): Promise<void> {
    // Clean up connections
  }
}
```

## Recurring Jobs

Define jobs that run on a schedule using `every` (interval) or `cron` (expression) properties. Recurring jobs are registered automatically when `hermes.start()` is called.

### Interval-based (`every`)

Use `[number][unit]` format where unit is `s` (seconds), `m` (minutes), `h` (hours), or `d` (days):

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

| Feature | Deno KV | BullMQ (Redis) |
| --- | --- | --- |
| `every` support | Minutes and above (`m`, `h`, `d`) | All units including seconds (`s`) |
| `cron` support | Yes | Yes |
| Deduplication | Automatic via `Deno.cron` | Automatic via `upsertJobScheduler` |
| Overlap prevention | Built-in | Built-in |

> **Note**: Seconds-level granularity (`s`) is only supported on BullMQ. Deno KV's
> minimum granularity is 1 minute since `Deno.cron` has no seconds field.

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
BullMQ Worker for true multi-queue processing.

## API Reference

### `Hermes(params)`

Creates a Hermes instance. Returns an object with `start()` and `stop()`
methods.

```typescript
const hermes = Hermes({
  manifest: "./jobs/main.ts", // Path to your jobs manifest file
  backend: DenoKvBackend(), // A BackendAdapter instance
  worker: {
    gracefulShutdownTimeout: 10000, // Shutdown timeout in ms (default: 30000)
  },
});

await hermes.start(); // Load manifest, register jobs, start worker
await hermes.stop(); // Gracefully shut down
```

### `configure({ backend })`

Sets the global backend for enqueuing jobs without starting a worker. Use this in
processes that only enqueue (e.g., a web server):

```typescript
import { configure, DenoKvBackend } from "@dafu/hermes";

configure({ backend: DenoKvBackend() });
// Now you can call job.performLater() anywhere in this process
```

### `Job` (abstract class)

Base class for all jobs.

| Property / Method | Type | Description |
| --- | --- | --- |
| `jobName` | `string` (abstract) | Unique identifier for the job type |
| `queueName` | `string` (abstract) | Queue this job is dispatched to |
| `every` | `string?` | Interval schedule, e.g. `"5m"`, `"1h"`, `"7d"` |
| `cron` | `string?` | Cron expression, e.g. `"0 9 * * 1-5"` |
| `perform(jobBody)` | `Promise<unknown>` (abstract) | The work the job does |
| `performLater(jobBody?, opts?)` | `Promise<void>` | Enqueue the job for async processing |
| `isRecurring()` | `boolean` | Whether the job has a recurring schedule |

### `DenoKvBackend(options?)`

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `path` | `string` | `undefined` | Custom path for the KV store file |

### `BullMQBackend(options)`

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `connection.host` | `string` | `undefined` | Redis host |
| `connection.port` | `number` | `undefined` | Redis port |
| `connection.password` | `string` | `undefined` | Redis password |
| `connection.url` | `string` | `undefined` | Redis connection URL |
| `concurrency` | `number` | `1` | Max concurrent jobs per queue worker |
| `defaultQueueName` | `string` | `"default"` | Fallback queue when none is specified |

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
};

// Worker configuration
type WorkerConfig = {
  concurrency?: number;
  gracefulShutdownTimeout?: number; // Default: 30000ms
};

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

| Event | Description |
| --- | --- |
| `worker_started` | Worker is listening for jobs |
| `job_received` | A job payload was dequeued |
| `job_started` | Job `perform()` is being called |
| `job_succeeded` | Job completed without errors |
| `job_failed` | Job threw an error (includes error message and duration) |
| `job_skipped` | Job was skipped (e.g., queue filtering) |
| `unknown_job` | Received a job with an unregistered `jobName` |
| `recurring_job_registered` | A recurring job schedule was registered at startup |
| `recurring_job_skipped` | A recurring job schedule registration was skipped |
| `worker_stopping` | Shutdown signal received |
| `worker_stopped` | Worker has fully shut down |

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
  `job_failed` event (including duration) and re-thrown to the backend, which can
  handle retries if supported.

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
