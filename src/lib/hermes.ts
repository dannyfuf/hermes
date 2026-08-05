import { JobLoader } from "./job_loader.ts";
import { ManifestLoader } from "./manifest_loader.ts";
import { MAX_TIMEOUT_MS, resolveJobTimeouts, Worker } from "./worker.ts";
import { setBackend } from "./backend_registry.ts";
import type { BackendAdapter, QueueStats } from "./backend.ts";
import type { HermesParams } from "./types.ts";
import { Logger } from "./logger.ts";

/** A running Hermes instance that manages job workers. */
export interface HermesInstance {
  start(): Promise<void>;
  stop(): Promise<void>;
  stats(): Promise<QueueStats[]>;
}

class THermes implements HermesInstance {
  private params: HermesParams;
  private queueNames: string[] = [];
  private started = false;
  private startPromise?: Promise<void>;
  private stopPromise?: Promise<void>;
  private stopRequested = false;

  constructor(params: HermesParams) {
    this.params = params;
    setBackend(params.backend);
  }

  start(): Promise<void> {
    if (this.startPromise) {
      return Promise.reject(new Error("Hermes has already started."));
    }

    this.startPromise = this.startBackend();
    return this.startPromise;
  }

  private async startBackend(): Promise<void> {
    const jobManifest = await ManifestLoader.load({
      manifestPath: this.params.manifest,
    });
    const jobsMap = await JobLoader(jobManifest).run();
    this.validateWorkerConfig();
    const timeoutByJobName = resolveJobTimeouts(
      jobsMap,
      this.params.worker?.defaultJobTimeout,
    );

    // Extract unique queue names from registered jobs for BullMQ routing
    const queueNames = [
      ...new Set(
        // deno-lint-ignore no-explicit-any
        Array.from(jobsMap.values()).map((cls: any) =>
          new cls().queueName as string
        ),
      ),
    ];

    if (this.stopRequested) return;

    // Register recurring jobs if backend supports it
    if (this.params.backend.registerRecurringJob) {
      for (const [_, jobClass] of jobsMap) {
        if (this.stopRequested) return;
        const instance = new jobClass();
        if (instance.isRecurring()) {
          if (instance.every && instance.cron) {
            throw new Error(
              `Job "${instance.jobName}" cannot have both 'every' and 'cron'`,
            );
          }
          await this.params.backend.registerRecurringJob({
            jobName: instance.jobName,
            queueName: instance.queueName,
            every: instance.every,
            cron: instance.cron,
            priority: instance.priority,
          });
          Logger.recurringJobRegistered(
            instance.jobName,
            instance.every || instance.cron!,
          );
        }
      }
    }

    if (this.stopRequested) return;

    await Worker.start({
      jobsMap,
      backend: this.params.backend,
      queueNames,
      concurrency: this.params.worker?.concurrency,
      timeoutByJobName,
    });

    this.queueNames = queueNames;
    this.started = true;
  }

  stop(): Promise<void> {
    this.stopRequested = true;
    if (!this.stopPromise) {
      this.stopPromise = this.stopAfterStart();
    }
    return this.stopPromise;
  }

  async stats(): Promise<QueueStats[]> {
    if (!this.started) {
      throw new Error("Hermes stats are only available after start().");
    }

    const getQueueStats = this.params.backend.getQueueStats;
    if (!getQueueStats) {
      throw new Error("The configured backend does not support queue stats.");
    }

    const stats: QueueStats[] = [];
    for (const queueName of this.queueNames) {
      stats.push(await getQueueStats.call(this.params.backend, queueName));
    }
    return stats;
  }

  private async stopBackend(): Promise<void> {
    Logger.workerStopping();
    const gracefulShutdownTimeout = Math.min(
      this.params.worker?.gracefulShutdownTimeout ?? 30_000,
      MAX_TIMEOUT_MS,
    );
    let timeoutId: ReturnType<typeof setTimeout> | undefined;

    try {
      const closedGracefully = await Promise.race([
        this.params.backend.close().then(() => true),
        new Promise<false>((resolve) => {
          timeoutId = setTimeout(() => resolve(false), gracefulShutdownTimeout);
        }),
      ]);

      if (!closedGracefully) {
        Logger.workerForceClosed(gracefulShutdownTimeout);
        await this.params.backend.close({ force: true });
      }
    } finally {
      clearTimeout(timeoutId);
    }

    this.started = false;
    Logger.workerStopped();
  }

  private async stopAfterStart(): Promise<void> {
    if (this.startPromise) {
      try {
        await this.startPromise;
      } catch {
        // start() reports its own failure; stop() still closes partial resources.
      }
    }
    await this.stopBackend();
  }

  private validateWorkerConfig(): void {
    const gracefulShutdownTimeout = this.params.worker?.gracefulShutdownTimeout;
    if (
      gracefulShutdownTimeout !== undefined &&
      (!Number.isSafeInteger(gracefulShutdownTimeout) ||
        gracefulShutdownTimeout <= 0 ||
        gracefulShutdownTimeout > MAX_TIMEOUT_MS)
    ) {
      throw new Error(
        `Invalid worker.gracefulShutdownTimeout: it must be a positive safe-integer number of milliseconds no greater than ${MAX_TIMEOUT_MS} (~24.8 days).`,
      );
    }
  }
}

/** Create a new Hermes instance with the given backend and job manifest. */
export const Hermes = (params: HermesParams): HermesInstance => {
  return new THermes(params);
};

/** Configure the global backend adapter for standalone job enqueueing without starting a worker. */
export const configure = (
  { backend }: { backend: BackendAdapter },
): void => {
  setBackend(backend);
};
