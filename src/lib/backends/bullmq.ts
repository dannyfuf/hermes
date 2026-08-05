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

import { type Job as BullMQJob, Queue, Worker } from "bullmq";
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
  defaultJobOptions?: {
    attempts?: number;
    backoff?: {
      type: "exponential" | "fixed";
      delay: number;
    };
    removeOnComplete?: boolean | number | { age?: number; count?: number };
    removeOnFail?: boolean | number | { age?: number; count?: number };
  };
}

type BullMQDefaultJobOptions = NonNullable<
  BullMQBackendOptions["defaultJobOptions"]
>;

const DEFAULT_JOB_OPTIONS: BullMQDefaultJobOptions = {
  removeOnComplete: { count: 1000 },
  removeOnFail: { count: 5000 },
};

class TBullMQBackend implements BackendAdapter {
  private queues: Map<string, Queue> = new Map();
  private workers: Map<string, Worker> = new Map();
  private options: BullMQBackendOptions;
  private jobOptions: BullMQDefaultJobOptions;
  private activeJobs = 0;
  private activeJobsFinishedResolvers: Array<() => void> = [];
  private closed = false;

  constructor(options: BullMQBackendOptions) {
    this.options = options;
    this.jobOptions = {
      ...DEFAULT_JOB_OPTIONS,
      ...options.defaultJobOptions,
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
          concurrency: options?.concurrency ?? this.options.concurrency ?? 1,
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
        priority: config.priority,
      },
    });
  }

  async close(options?: { force?: boolean }): Promise<void> {
    this.closed = true;
    const workers = Array.from(this.workers.values());
    await Promise.all(workers.map((worker) => worker.pause(true)));
    if (!options?.force) {
      await this.waitForActiveJobs();
    }
    await Promise.all(
      workers.map((worker) => worker.close(options?.force)),
    );
    this.workers.clear();

    await Promise.all(
      Array.from(this.queues.values()).map((queue) => queue.close()),
    );
    this.queues.clear();
  }

  private waitForActiveJobs(): Promise<void> {
    if (this.activeJobs === 0) return Promise.resolve();
    return new Promise((resolve) => {
      this.activeJobsFinishedResolvers.push(resolve);
    });
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
