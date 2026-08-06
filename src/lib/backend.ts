import type { JobPayload } from "./types.ts";

/** Options passed when enqueueing a job. */
export interface EnqueueOptions {
  delay?: number;
  queueName?: string;
  priority?: number;
}

/** Configuration for registering a recurring job schedule. */
export interface RecurringJobConfig {
  jobName: string;
  queueName: string;
  every?: string;
  cron?: string;
  jobBody?: unknown;
  priority?: number;
}

/** Queue activity counts and the age of the oldest active job, when available. */
export interface QueueStats {
  queueName: string;
  counts: {
    waiting: number;
    active: number;
    delayed: number;
    failed: number;
    completed: number;
    prioritized?: number;
  };
  oldestActiveJobAgeMs?: number;
}

/** Interface that all Hermes backend adapters must implement. */
export interface BackendAdapter {
  /**
   * Enqueue a job payload for async processing.
   */
  enqueue(payload: JobPayload, options?: EnqueueOptions): Promise<void>;

  /**
   * Start listening for jobs and dispatch them to the handler.
   * The handler receives the raw JobPayload.
   */
  listen(
    handler: (payload: JobPayload) => Promise<void>,
    options?: { queueNames?: string[]; concurrency?: number },
  ): Promise<void>;

  /** Shut down the backend, optionally without waiting for in-flight work. */
  close(options?: { force?: boolean }): Promise<void>;

  /**
   * Register a recurring job schedule. Optional — not all backends must implement.
   */
  registerRecurringJob?(config: RecurringJobConfig): Promise<void>;

  /**
   * Remove a recurring job schedule. Optional — not all backends must implement.
   */
  removeRecurringJob?(jobName: string): Promise<void>;

  /** Read queue health statistics. Optional — not all backends support it. */
  getQueueStats?(queueName: string): Promise<QueueStats>;
}

/** Internal capability for validating a complete recurring-job manifest. */
export interface RecurringJobValidationBackend extends BackendAdapter {
  validateRecurringJobs(configs: readonly RecurringJobConfig[]): Promise<void>;
}
