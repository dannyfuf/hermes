import type { JobPayload } from "./types.ts";

export interface EnqueueOptions {
  delay?: number;
  queueName?: string;
}

export interface RecurringJobConfig {
  jobName: string;
  queueName: string;
  every?: string;
  cron?: string;
  jobBody?: unknown;
}

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

  /**
   * Gracefully shut down the backend connection.
   */
  close(): Promise<void>;

  /**
   * Register a recurring job schedule. Optional — not all backends must implement.
   */
  registerRecurringJob?(config: RecurringJobConfig): Promise<void>;

  /**
   * Remove a recurring job schedule. Optional — not all backends must implement.
   */
  removeRecurringJob?(jobName: string): Promise<void>;
}
