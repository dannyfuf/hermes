import { type Job as BullMQJob, Queue, Worker } from "bullmq";
import type { BackendAdapter, EnqueueOptions } from "../backend.ts";
import type { JobPayload } from "../types.ts";

export interface BullMQBackendOptions {
  connection: {
    host?: string;
    port?: number;
    password?: string;
    url?: string;
  };
  defaultQueueName?: string;
  concurrency?: number;
}

class TBullMQBackend implements BackendAdapter {
  private queues: Map<string, Queue> = new Map();
  private workers: Map<string, Worker> = new Map();
  private options: BullMQBackendOptions;

  constructor(options: BullMQBackendOptions) {
    this.options = options;
  }

  private getOrCreateQueue(queueName: string): Queue {
    if (!this.queues.has(queueName)) {
      this.queues.set(
        queueName,
        new Queue(queueName, {
          connection: this.options.connection,
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

export const BullMQBackend = (
  options: BullMQBackendOptions,
): BackendAdapter => {
  return new TBullMQBackend(options);
};
