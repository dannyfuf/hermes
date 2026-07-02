/**
 * BullMQ backend adapter for Hermes, using Redis-backed queues.
 *
 * @example
 * ```ts
 * import { Hermes } from "@dafu/hermes";
 * import { BullMQBackend } from "@dafu/hermes/backends/bullmq";
 *
 * const hermes = Hermes({
 *   manifest: "./jobs/manifest.json",
 *   backend: BullMQBackend({ connection: { host: "localhost", port: 6379 } }),
 * });
 * ```
 *
 * @module
 */

import {
  type DefaultJobOptions,
  type Job as BullMQJob,
  Queue,
  Worker,
} from "bullmq";
import type {
  BackendAdapter,
  EnqueueOptions,
  RecurringJobConfig,
} from "../backend.ts";
import type { JobPayload } from "../types.ts";
import { intervalToMs, parseEveryInterval } from "../schedule.ts";

/** Options for the BullMQ backend adapter. */
export interface BullMQBackendOptions {
  connection: {
    host?: string;
    port?: number;
    password?: string;
    url?: string;
  };
  defaultQueueName?: string;
  concurrency?: number;
  /**
   * Default options applied to every enqueued and recurring job, e.g. retention
   * (`removeOnComplete`/`removeOnFail`), attempts and backoff.
   *
   * BullMQ keeps completed and failed jobs in Redis indefinitely by default, so
   * recurring jobs pile up one record per run forever. To avoid unbounded
   * growth this backend applies bounded retention by default (see
   * {@link DEFAULT_JOB_OPTIONS}); whatever you pass here is merged on top and
   * takes precedence.
   */
  defaultJobOptions?: DefaultJobOptions;
}

/**
 * Bounded retention applied when the caller does not override it, so Redis does
 * not grow without limit. Keeps a recent window of completed/failed jobs for
 * inspection while discarding the rest. Override via
 * {@link BullMQBackendOptions.defaultJobOptions}.
 */
const DEFAULT_JOB_OPTIONS: DefaultJobOptions = {
  removeOnComplete: { count: 1000 },
  removeOnFail: { count: 5000 },
};

class TBullMQBackend implements BackendAdapter {
  private queues: Map<string, Queue> = new Map();
  private workers: Map<string, Worker> = new Map();
  private options: BullMQBackendOptions;
  private jobOptions: DefaultJobOptions;

  constructor(options: BullMQBackendOptions) {
    this.options = options;
    this.jobOptions = {
      ...DEFAULT_JOB_OPTIONS,
      ...options.defaultJobOptions,
    };
  }

  private getOrCreateQueue(queueName: string): Queue {
    if (!this.queues.has(queueName)) {
      this.queues.set(
        queueName,
        new Queue(queueName, {
          connection: this.options.connection,
          defaultJobOptions: this.jobOptions,
        }),
      );
    }
    return this.queues.get(queueName)!;
  }

  async enqueue(payload: JobPayload, options?: EnqueueOptions): Promise<void> {
    const queueName = payload.queueName ||
      this.options.defaultQueueName || "default";
    const queue = this.getOrCreateQueue(queueName);
    await queue.add(payload.jobName, payload, {
      delay: options?.delay,
    });
  }

  // deno-lint-ignore require-await
  async listen(
    handler: (payload: JobPayload) => Promise<void>,
    options?: { queueNames?: string[] },
  ): Promise<void> {
    const queueNames = options?.queueNames ?? [
      this.options.defaultQueueName ?? "default",
    ];

    for (const queueName of queueNames) {
      const worker = new Worker(
        queueName,
        async (job: BullMQJob) => {
          await handler(job.data as JobPayload);
        },
        {
          connection: this.options.connection,
          concurrency: this.options.concurrency ?? 1,
        },
      );
      this.workers.set(queueName, worker);
    }
  }

  async registerRecurringJob(config: RecurringJobConfig): Promise<void> {
    const queue = this.getOrCreateQueue(config.queueName);
    const schedulerId = `hermes:${config.jobName}`;

    const repeatOpts: { every?: number; pattern?: string } = {};
    if (config.every) {
      repeatOpts.every = intervalToMs(parseEveryInterval(config.every));
    } else if (config.cron) {
      repeatOpts.pattern = config.cron;
    }

    const payload: JobPayload = {
      jobName: config.jobName,
      queueName: config.queueName,
      jobBody: config.jobBody,
    };

    // Pass retention on the scheduler's job template too: jobs produced by a
    // scheduler do not inherit the queue's defaultJobOptions, so without this
    // each recurring run would leave a job record in Redis forever.
    await queue.upsertJobScheduler(schedulerId, repeatOpts, {
      name: config.jobName,
      data: payload,
      opts: this.jobOptions,
    });
  }

  async close(): Promise<void> {
    const closePromises: Promise<void>[] = [];
    for (const [_, queue] of this.queues) {
      closePromises.push(queue.close());
    }
    for (const [_, worker] of this.workers) {
      closePromises.push(worker.close());
    }
    await Promise.all(closePromises);
    this.queues.clear();
    this.workers.clear();
  }
}

/** Create a BullMQ (Redis) backend adapter for Hermes. */
export const BullMQBackend = (
  options: BullMQBackendOptions,
): BackendAdapter => {
  return new TBullMQBackend(options);
};
