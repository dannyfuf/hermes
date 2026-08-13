import type { BackendAdapter } from "./backend.ts";
import type { LogEvent } from "./logger.ts";

/** Options for scheduling a job to run later. */
export type PerformLaterOptions = {
  delay?: number;
  priority?: number;
  /**
   * Opaque metadata stored on the payload envelope. Merged over
   * hook-contributed metadata (explicit per-call keys win). Never interpreted
   * by Hermes; must be JSON/structured-clone serializable like `jobBody`.
   */
  metadata?: Record<string, unknown>;
};

/** Context supplied to a running job. */
export type JobContext = {
  signal: AbortSignal;
  /**
   * `payload.metadata`, verbatim. `undefined` for scheduled runs and for
   * payloads enqueued by older versions — the signal for "no ambient
   * context, start fresh".
   */
  metadata?: Record<string, unknown>;
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
  defaultJobTimeout?: string | number;
};

/** Observability hooks. Deliberately outcome-inert — a buggy hook cannot
 * change whether a job succeeds, fails, or retries. */
export type HermesHooks = {
  /**
   * Runs synchronously inside `performLater()`, before enqueue. Its only
   * power is contributing metadata; it cannot mutate the payload, block the
   * enqueue, or change routing. Per-call `opts.metadata` keys win on
   * collision. A throw propagates to the `performLater()` caller and nothing
   * is enqueued.
   */
  enqueueMetadata?: (
    payload: Readonly<JobPayload>,
  ) => Record<string, unknown> | undefined;

  /**
   * Wraps every execution. MUST invoke `next()` exactly once and SHOULD
   * await it. The job's outcome is `next()`'s outcome — nothing the wrapper
   * does changes it; wrapper faults are logged as `hook_error` events.
   * Return value is ignored.
   */
  aroundPerform?: (
    payload: Readonly<JobPayload>,
    next: () => Promise<unknown>,
  ) => Promise<void>;
};

/** Log transport. Called synchronously with each structured event. Must not
 * throw (Hermes guards and falls back to `console.log` anyway). */
export type LoggerSink = (event: LogEvent) => void;

/** Parameters for creating a Hermes instance. */
export type HermesParams = {
  manifest: string;
  backend: BackendAdapter;
  worker?: WorkerConfig;
  hooks?: HermesHooks;
  logger?: LoggerSink;
};

export type WorkerParams = {
  // deno-lint-ignore no-explicit-any
  jobsMap: Map<string, any>;
  backend: BackendAdapter;
  queueNames?: string[];
  concurrency?: number;
  timeoutByJobName: ReadonlyMap<string, number | undefined>;
};
