import type { JobPayload } from "./types.ts";

/** Options passed when enqueueing a job. */
export interface EnqueueOptions {
  delay?: number;
  queueName?: string;
}

/** Configuration for registering a recurring job schedule. */
export interface RecurringJobConfig {
  jobName: string;
  queueName: string;
  every?: string;
  cron?: string;
  jobBody?: unknown;
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
    options?: { queueNames?: string[] },
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
}
