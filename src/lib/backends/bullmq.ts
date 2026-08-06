/**
 * BullMQ backend adapter for Hermes, using Redis-backed queues.
 *
 * @example
 * ```ts
 * import { Hermes } from "@dafu/hermes";
 * import { BullMQBackend } from "@dafu/hermes/backends/bullmq";
 *
 * const hermes = Hermes({
 *   manifest: "./jobs/main.ts",
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
  QueueStats,
  RecurringJobConfig,
} from "../backend.ts";
import type { JobPayload } from "../types.ts";
import { intervalToMs, parseEveryInterval } from "../schedule.ts";
import { Logger } from "../logger.ts";

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
   * Defaults for ordinary and recurring jobs. Bounded completed/failed job
   * retention is applied when these fields are not overridden.
   */
  defaultJobOptions?: DefaultJobOptions;
}

const DEFAULT_JOB_OPTIONS: DefaultJobOptions = {
  removeOnComplete: { count: 1000 },
  removeOnFail: { count: 5000 },
};

function withoutUndefinedJobOptions(
  options?: DefaultJobOptions,
): DefaultJobOptions {
  if (!options) return {};

  return Object.fromEntries(
    Object.entries(options).filter(([, value]) => value !== undefined),
  );
}

function validateConcurrency(concurrency: number): void {
  if (!Number.isFinite(concurrency) || concurrency < 1) {
    throw new Error(
      "Invalid BullMQ concurrency: it must be a finite number greater than or equal to 1.",
    );
  }
}

class TBullMQBackend implements BackendAdapter {
  private queues: Map<string, Queue> = new Map();
  private workers: Map<string, Worker> = new Map();
  private options: BullMQBackendOptions;
  private jobOptions: DefaultJobOptions;
  private activeJobs = 0;
  private activeJobsFinishedResolvers: Array<() => void> = [];
  private closed = false;
  private gracefulClosePromise?: Promise<void>;
  private forceClosePromise?: Promise<void>;

  constructor(options: BullMQBackendOptions) {
    this.options = options;
    this.jobOptions = {
      ...DEFAULT_JOB_OPTIONS,
      ...withoutUndefinedJobOptions(options.defaultJobOptions),
    };
  }

  private getOrCreateQueue(queueName: string): Queue {
    if (!this.queues.has(queueName)) {
      const queue = new Queue(queueName, {
        connection: this.options.connection,
        defaultJobOptions: this.jobOptions,
      });
      queue.on("error", (error: Error) => Logger.queueError(queueName, error));
      this.queues.set(queueName, queue);
    }
    return this.queues.get(queueName)!;
  }

  async enqueue(payload: JobPayload, options?: EnqueueOptions): Promise<void> {
    const queueName = payload.queueName ||
      this.options.defaultQueueName || "default";
    const queue = this.getOrCreateQueue(queueName);
    await queue.add(payload.jobName, payload, {
      delay: options?.delay,
      priority: options?.priority,
    });
  }

  async listen(
    handler: (payload: JobPayload) => Promise<void>,
    options?: { queueNames?: string[]; concurrency?: number },
  ): Promise<void> {
    const queueNames = options?.queueNames ?? [
      this.options.defaultQueueName ?? "default",
    ];
    const concurrency = options?.concurrency ?? this.options.concurrency ?? 1;
    validateConcurrency(concurrency);

    for (const queueName of queueNames) {
      const existingWorker = this.workers.get(queueName);
      if (existingWorker) {
        await existingWorker.close();
      }

      const worker = new Worker(
        queueName,
        async (job: BullMQJob) => {
          this.activeJobs += 1;
          try {
            await handler(job.data as JobPayload);
          } finally {
            this.activeJobs -= 1;
            if (this.activeJobs === 0) {
              for (
                const resolve of this.activeJobsFinishedResolvers.splice(0)
              ) {
                resolve();
              }
            }
          }
        },
        {
          connection: this.options.connection,
          concurrency,
        },
      );
      worker.on("error", (error: Error) => {
        Logger.workerError(queueName, error);
      });
      worker.on("failed", (job: BullMQJob | undefined, error: Error) => {
        Logger.workerJobFailed(
          queueName,
          job?.name,
          job?.id,
          job?.attemptsMade,
          error,
        );
      });
      worker.on("stalled", (jobId: string) => {
        Logger.jobStalled(queueName, jobId);
      });
      worker.on("closed", () => {
        Logger.workerClosed(queueName);
      });
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

    await queue.upsertJobScheduler(schedulerId, repeatOpts, {
      name: config.jobName,
      data: payload,
      opts: {
        ...this.jobOptions,
        ...(config.priority === undefined ? {} : { priority: config.priority }),
      },
    });
  }

  close(options?: { force?: boolean }): Promise<void> {
    if (options?.force) {
      this.forceClosePromise ??= this.closeBackend(true);
      return this.forceClosePromise;
    }

    if (this.gracefulClosePromise) return this.gracefulClosePromise;
    if (this.forceClosePromise) return this.forceClosePromise;
    this.gracefulClosePromise ??= this.closeBackend(false);
    return this.gracefulClosePromise;
  }

  private async closeBackend(force: boolean): Promise<void> {
    this.closed = true;
    const workers = Array.from(this.workers.values());
    const queues = Array.from(this.queues.values());

    if (force) {
      const workersClosed = Promise.all(
        workers.map((worker) => worker.close(true)),
      );
      const queuesClosed = Promise.all(
        queues.map((queue) => queue.disconnect()),
      );
      this.resolveActiveJobsFinished();
      await Promise.all([workersClosed, queuesClosed]);
    } else {
      await Promise.all(workers.map((worker) => worker.pause(true)));
      await this.waitForActiveJobs();

      if (this.forceClosePromise) {
        await this.forceClosePromise;
        return;
      }

      await Promise.all(workers.map((worker) => worker.close(false)));
      await Promise.all(queues.map((queue) => queue.close()));
    }

    this.workers.clear();
    this.queues.clear();
  }

  private waitForActiveJobs(): Promise<void> {
    if (this.activeJobs === 0) return Promise.resolve();
    return new Promise((resolve) => {
      this.activeJobsFinishedResolvers.push(resolve);
    });
  }

  private resolveActiveJobsFinished(): void {
    for (const resolve of this.activeJobsFinishedResolvers.splice(0)) {
      resolve();
    }
  }

  async getQueueStats(queueName: string): Promise<QueueStats> {
    if (this.closed) {
      throw new Error("BullMQ backend is closed.");
    }

    const queue = this.getOrCreateQueue(queueName);
    const [counts, activeJobs] = await Promise.all([
      queue.getJobCounts(
        "waiting",
        "prioritized",
        "active",
        "delayed",
        "failed",
        "completed",
      ),
      queue.getActive(0, 50),
    ]);
    const processedOn = activeJobs
      .filter((job): job is BullMQJob => job !== undefined)
      .map((job) => job.processedOn)
      .filter((value): value is number => value !== undefined);
    const oldestProcessedOn = processedOn.length > 0
      ? Math.min(...processedOn)
      : undefined;

    return {
      queueName,
      counts: {
        waiting: (counts.waiting ?? 0) + (counts.prioritized ?? 0),
        active: counts.active ?? 0,
        delayed: counts.delayed ?? 0,
        failed: counts.failed ?? 0,
        completed: counts.completed ?? 0,
        prioritized: counts.prioritized ?? 0,
      },
      ...(oldestProcessedOn === undefined
        ? {}
        : { oldestActiveJobAgeMs: Date.now() - oldestProcessedOn }),
    };
  }
}

/** Create a BullMQ (Redis) backend adapter for Hermes. */
export const BullMQBackend = (
  options: BullMQBackendOptions,
): BackendAdapter => {
  return new TBullMQBackend(options);
};
