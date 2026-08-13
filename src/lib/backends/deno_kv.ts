import type {
  BackendAdapter,
  EnqueueOptions,
  RecurringJobConfig,
  RecurringJobValidationBackend,
} from "../backend.ts";
import type { JobPayload } from "../types.ts";
import { intervalToCronSchedule, parseEveryInterval } from "../schedule.ts";
import { Logger } from "../logger.ts";

const MAX_CRON_NAME_LENGTH = 64;
const CRON_NAME_PREFIX = "hermes-";
const HASH_LENGTH = 8;
const MAX_READABLE_PREFIX_LENGTH = MAX_CRON_NAME_LENGTH -
  CRON_NAME_PREFIX.length - HASH_LENGTH - 1;

function fnv1a32(value: string): string {
  let hash = 0x811c9dc5;
  for (const byte of new TextEncoder().encode(value)) {
    hash ^= byte;
    hash = Math.imul(hash, 0x01000193) >>> 0;
  }
  return hash.toString(16).padStart(HASH_LENGTH, "0");
}

function cronNameFor(jobName: string): string {
  const readablePrefix = jobName.replace(/[^A-Za-z0-9_-]/g, "-")
    .slice(0, MAX_READABLE_PREFIX_LENGTH);
  return `${CRON_NAME_PREFIX}${readablePrefix}-${fnv1a32(jobName)}`;
}

/** Options for the Deno KV backend adapter. */
export interface DenoKvBackendOptions {
  path?: string;
}

class TDenoKvBackend implements RecurringJobValidationBackend {
  private kv: Deno.Kv | null = null;
  private openingKv?: Promise<Deno.Kv>;
  private path?: string;
  private closed = false;
  private readonly inFlightHandlers = new Set<Promise<void>>();

  constructor(options?: DenoKvBackendOptions) {
    this.path = options?.path;
  }

  private async getKv(): Promise<Deno.Kv> {
    this.ensureOpen();
    if (this.kv) return this.kv;

    this.openingKv ??= Deno.openKv(this.path).then((kv) => {
      if (this.closed) {
        kv.close();
        throw this.closedError();
      }
      this.kv = kv;
      return kv;
    }).finally(() => {
      this.openingKv = undefined;
    });
    return await this.openingKv;
  }

  private ensureOpen(): void {
    if (this.closed) throw this.closedError();
  }

  private closedError(): Error {
    return new Error("Deno KV backend is closed.");
  }

  async enqueue(payload: JobPayload, options?: EnqueueOptions): Promise<void> {
    const kv = await this.getKv();
    await kv.enqueue(payload, { delay: options?.delay });
  }

  async listen(
    handler: (payload: JobPayload) => Promise<void>,
    options?: { queueNames?: string[]; concurrency?: number },
  ): Promise<void> {
    const kv = await this.getKv();
    const configuredQueueNames = options?.queueNames;
    const queueNames = configuredQueueNames && configuredQueueNames.length > 0
      ? new Set(configuredQueueNames)
      : undefined;

    kv.listenQueue((message: unknown) => {
      const payload = message as JobPayload;
      const handlerPromise = Promise.resolve().then(() => {
        if (queueNames && !queueNames.has(payload.queueName)) {
          Logger.jobSkipped(
            payload.jobName,
            payload.queueName,
            "queue filtering",
          );
          throw new Error(
            `Queue "${payload.queueName}" is not handled by this worker`,
          );
        }

        return handler(payload);
      });
      this.inFlightHandlers.add(handlerPromise);
      handlerPromise.then(
        () => this.inFlightHandlers.delete(handlerPromise),
        () => this.inFlightHandlers.delete(handlerPromise),
      );
      return handlerPromise;
    });
  }

  // deno-lint-ignore require-await
  async validateRecurringJobs(
    configs: readonly RecurringJobConfig[],
  ): Promise<void> {
    const jobNameByCronName = new Map<string, string>();
    for (const config of configs) {
      const cronName = cronNameFor(config.jobName);
      if (cronName.length > MAX_CRON_NAME_LENGTH) {
        throw new Error(
          `Derived Deno cron name for job "${config.jobName}" exceeds ${MAX_CRON_NAME_LENGTH} characters.`,
        );
      }

      const existingJobName = jobNameByCronName.get(cronName);
      if (existingJobName !== undefined) {
        throw new Error(
          `Duplicate derived Deno cron name "${cronName}" for jobs "${existingJobName}" and "${config.jobName}".`,
        );
      }
      jobNameByCronName.set(cronName, config.jobName);
    }
  }

  // deno-lint-ignore require-await
  async registerRecurringJob(config: RecurringJobConfig): Promise<void> {
    this.ensureOpen();
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

    const cronName = cronNameFor(config.jobName);
    const cronPromise = Deno.cron(cronName, schedule, async () => {
      if (this.closed) return;
      await this.enqueue(payload);
    });
    cronPromise.catch((error: unknown) => {
      Logger.error(
        "Deno cron registration failed",
        error instanceof Error ? error.message : String(error),
        { jobName: config.jobName, cronName },
      );
    });
  }

  async close(options?: { force?: boolean }): Promise<void> {
    this.closed = true;

    if (!options?.force) {
      while (this.inFlightHandlers.size > 0) {
        await Promise.allSettled([...this.inFlightHandlers]);
      }
    }

    if (this.openingKv) {
      await this.openingKv.catch(() => {});
    }

    if (this.kv) {
      this.kv.close();
      this.kv = null;
    }
  }
}

/** Create a Deno KV backend adapter for Hermes. */
export const DenoKvBackend = (
  options?: DenoKvBackendOptions,
): BackendAdapter => {
  return new TDenoKvBackend(options);
};
