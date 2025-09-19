# Hermes

A Deno Deploy-compatible jobs library that dispatches enqueued jobs to their corresponding Job classes, with support for multiple queue names.

## Features

- **Job Registry**: Automatic loading and validation of job classes from a manifest
- **Queue Filtering**: Support for multiple queues with include/exclude filtering
- **Deno KV Integration**: Built-in support for Deno KV as the job queue backend
- **Graceful Shutdown**: Proper handling of worker lifecycle with configurable timeouts
- **Structured Logging**: Comprehensive job lifecycle event logging
- **Type Safety**: Full TypeScript support with proper type definitions

## Quick Start

### 1. Define Your Jobs

Create job classes that extend the base `Job` class:

```typescript
// src/jobs/email_job.ts
import { Job } from "hermes";

export class EmailJob extends Job {
  readonly job_name = "send_email";
  readonly queue_name = "emails";

  async perform(job_body: unknown): Promise<void> {
    const { to, subject, body } = job_body as {
      to: string;
      subject: string;
      body: string;
    };

    // Your email sending logic here
    console.log(`Sending email to ${to}: ${subject}`);
  }
}
```

### 2. Create a Jobs Manifest

Export all your job classes from `src/jobs/main.ts`:

```typescript
// src/jobs/main.ts
import { EmailJob } from "./email_job.ts";
import { ReportJob } from "./report_job.ts";

export default [
  EmailJob,
  ReportJob,
];
```

### 3. Start the Worker

```typescript
// worker.ts
import { createAndStartHermes } from "hermes";

const hermes = await createAndStartHermes({
  manifest: "./src/jobs/main.ts",
  worker: {
    includeQueues: ["emails", "reports"],
    concurrency: 2,
    gracefulShutdownTimeout: 10000,
  },
});

// Graceful shutdown handling
Deno.addSignalListener("SIGINT", async () => {
  await hermes.stop();
  Deno.exit(0);
});
```

### 4. Enqueue Jobs

```typescript
// enqueue.ts
import { EmailJob } from "./src/jobs/main.ts";

const emailJob = new EmailJob();

await emailJob.perform_later({
  to: "user@example.com",
  subject: "Welcome!",
  body: "Thanks for signing up!",
});
```

## Configuration

### HermesConfig

```typescript
interface HermesConfig {
  manifest: JobManifest | string;  // Array of Job classes or path to manifest module
  worker?: WorkerConfig;
}
```

### WorkerConfig

```typescript
interface WorkerConfig {
  includeQueues?: string[];        // Only process these queues
  excludeQueues?: string[];        // Skip these queues
  concurrency?: number;            // Max concurrent jobs (default: 1)
  gracefulShutdownTimeout?: number; // Shutdown timeout in ms (default: 30000)
}
```

## Job Payload Schema

All enqueued jobs include:

```typescript
interface JobPayload {
  job_name: string;      // Unique identifier for the job type
  queue_name: string;    // Queue to process the job in
  job_body: unknown;     // The actual job data
  metadata?: Record<string, unknown>; // Optional metadata
}
```

## Error Handling

The library handles various error scenarios:

- **Duplicate job_name**: Startup fails with clear error
- **Invalid manifest**: Startup fails with validation errors
- **Unknown job_name**: Logs error and skips processing
- **Queue filtering**: Skips jobs not matching worker configuration
- **Job execution failures**: Logs error details without crashing worker

## Logging

Structured JSON logs are emitted for all job lifecycle events:

```json
{
  "timestamp": "2024-01-01T12:00:00.000Z",
  "event": "job_started",
  "job_name": "send_email",
  "queue_name": "emails"
}
```

Events include: `job_received`, `job_started`, `job_succeeded`, `job_failed`, `job_skipped`, `unknown_job`, `worker_started`, `worker_stopping`, `worker_stopped`.

## Deno Permissions

Required permissions for Deno Deploy:

```bash
deno run --allow-read --allow-write worker.ts
```

## API Reference

### Classes

- `Job`: Abstract base class for all jobs
- `Hermes`: Main library class for managing workers
- `Logger`: Structured logging utilities

### Functions

- `createHermes(config)`: Initialize Hermes instance
- `createAndStartHermes(config)`: Initialize and start Hermes in one call

### Types

- `JobPayload`: Structure of enqueued job data
- `WorkerConfig`: Worker configuration options
- `JobManifest`: Array of Job class constructors
- `HermesConfig`: Main configuration interface

## Examples

See the `example/` directory for complete working examples of:

- Job class definitions
- Worker setup and configuration
- Job enqueueing
- Graceful shutdown handling

## License

MIT
