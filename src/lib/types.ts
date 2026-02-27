import type { BackendAdapter } from "./backend.ts";

/** Options for scheduling a job to run later. */
export type PerformLaterOptions = {
  delay?: number;
};

/** The serialized payload passed through the queue for each job execution. */
export type JobPayload = {
  jobName: string;
  queueName: string;
  jobBody: unknown;
  metadata?: Record<string, unknown>;
};

/** Configuration options for the job worker. */
export type WorkerConfig = {
  concurrency?: number;
  gracefulShutdownTimeout?: number;
};

/** Parameters for creating a Hermes instance. */
export type HermesParams = {
  manifest: string;
  backend: BackendAdapter;
  worker?: WorkerConfig;
};

export type WorkerParams = {
  // deno-lint-ignore no-explicit-any
  jobsMap: Map<string, any>;
  backend: BackendAdapter;
  queueNames?: string[];
};
