export type PerformLaterOptions = {
  delay?: number;
};

export type JobPayload = {
  job_name: string;
  queue_name: string;
  job_body: unknown;
  metadata?: Record<string, unknown>;
};

export type WorkerConfig = {
  gracefulShutdownTimeout?: number;
};

export type HermesParams = {
  manifest: string;
  worker: WorkerConfig;
};

export type WorkerParams = {
  // deno-lint-ignore no-explicit-any
  jobs_hash: Map<string, any>;
};
