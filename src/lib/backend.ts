import type { JobPayload } from "./types.ts";

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
   */
  listen(
    handler: (payload: JobPayload) => Promise<void>,
    options?: { queueNames?: string[] },
  ): Promise<void>;

  /**
   * Gracefully shut down the backend connection.
   */
  close(): Promise<void>;
}
