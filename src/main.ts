/**
 * Hermes - A background job framework for Deno with pluggable backends.
 *
 * @example
 * ```ts
 * import { Hermes, DenoKvBackend } from "@dafu/hermes";
 *
 * const hermes = Hermes({
 *   manifest: "./jobs/main.ts",
 *   backend: DenoKvBackend(),
 * });
 * await hermes.start();
 * ```
 *
 * @module
 */

export { Job } from "./lib/job.ts";
export { configure, Hermes } from "./lib/hermes.ts";
export type { HermesInstance } from "./lib/hermes.ts";
export { DenoKvBackend } from "./lib/backends/deno_kv.ts";
export type { DenoKvBackendOptions } from "./lib/backends/deno_kv.ts";
export type {
  BackendAdapter,
  EnqueueOptions,
  QueueStats,
  RecurringJobConfig,
} from "./lib/backend.ts";
export type {
  HermesParams,
  JobContext,
  JobPayload,
  PerformLaterOptions,
  WorkerConfig,
} from "./lib/types.ts";
export type {
  EveryInterval,
  RecurringSchedule,
  TimeUnit,
} from "./lib/schedule.ts";
export {
  intervalToMs,
  parseEveryInterval,
  validateCronExpression,
} from "./lib/schedule.ts";
