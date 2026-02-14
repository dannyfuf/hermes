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
  // deno-lint-ignore no-explicit-any
  jobsMap: Map<string, any>;
  backend: BackendAdapter;
  queueNames?: string[];
};
