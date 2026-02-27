import type {
  BackendAdapter,
  EnqueueOptions,
  RecurringJobConfig,
} from "../backend.ts";
import type { JobPayload } from "../types.ts";
import { intervalToCronSchedule, parseEveryInterval } from "../schedule.ts";

/** Options for the Deno KV backend adapter. */
export interface DenoKvBackendOptions {
  path?: string;
}

class TDenoKvBackend implements BackendAdapter {
  private kv: Deno.Kv | null = null;
  private path?: string;

  constructor(options?: DenoKvBackendOptions) {
    this.path = options?.path;
  }

  private async getKv(): Promise<Deno.Kv> {
    if (!this.kv) {
      this.kv = await Deno.openKv(this.path);
    }
    return this.kv;
  }

  async enqueue(payload: JobPayload, options?: EnqueueOptions): Promise<void> {
    const kv = await this.getKv();
    await kv.enqueue(payload, { delay: options?.delay });
  }

  async listen(
    handler: (payload: JobPayload) => Promise<void>,
    _options?: { queueNames?: string[] },
  ): Promise<void> {
    const kv = await this.getKv();
    kv.listenQueue(async (message: unknown) => {
      await handler(message as JobPayload);
    });
  }

  // deno-lint-ignore require-await
  async registerRecurringJob(config: RecurringJobConfig): Promise<void> {
    const payload: JobPayload = {
      jobName: config.jobName,
      queueName: config.queueName,
      jobBody: config.jobBody,
    };

    let schedule: string | Record<string, { every: number }>;
    if (config.cron) {
      schedule = config.cron;
    } else if (config.every) {
      const interval = parseEveryInterval(config.every);
      schedule = intervalToCronSchedule(interval);
    } else {
      throw new Error("Recurring job must have either 'every' or 'cron'");
    }

    Deno.cron(`hermes:${config.jobName}`, schedule, async () => {
      await this.enqueue(payload);
    });
  }

  close(): Promise<void> {
    if (this.kv) {
      this.kv.close();
      this.kv = null;
    }
    return Promise.resolve();
  }
}

/** Create a Deno KV backend adapter for Hermes. */
export const DenoKvBackend = (options?: DenoKvBackendOptions): BackendAdapter => {
  return new TDenoKvBackend(options);
};
