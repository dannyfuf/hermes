import { JobLoader } from "./job_loader.ts";
import { ManifestLoader } from "./manifest_loader.ts";
import { MAX_TIMEOUT_MS, resolveJobTimeouts, Worker } from "./worker.ts";
import { setBackend } from "./backend_registry.ts";
import {
  clearHooks,
  clearLoggerSink,
  setHooks,
  setLoggerSink,
} from "./hooks_registry.ts";
import type {
  BackendAdapter,
  QueueStats,
  RecurringJobConfig,
  RecurringJobValidationBackend,
} from "./backend.ts";
import type { HermesHooks, HermesParams, LoggerSink } from "./types.ts";
import { Logger } from "./logger.ts";

const FORCE_CLOSE_TIMEOUT_MS = 5_000;

// Every registration call fully replaces the previous one: omitted hooks or
// logger clears any earlier registration, mirroring how setBackend overwrites.
function registerObservability(hooks?: HermesHooks, logger?: LoggerSink): void {
  if (hooks) setHooks(hooks);
  else clearHooks();
  if (logger) setLoggerSink(logger);
  else clearLoggerSink();
}

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
  private startFailed = false;
  private startFailure?: unknown;
  private stopPromise?: Promise<void>;
  private stopRequested = false;
  private worker?: Worker;

  constructor(params: HermesParams) {
    this.params = params;
    setBackend(params.backend);
    registerObservability(params.hooks, params.logger);
  }

  start(): Promise<void> {
    if (this.stopRequested && !this.startPromise) {
      return Promise.reject(
        new Error(
          "Hermes cannot start because stop() was already requested; create a new Hermes instance.",
        ),
      );
    }

    if (this.startPromise) {
      if (this.startFailed) {
        const failureMessage = this.startFailure instanceof Error
          ? this.startFailure.message
          : String(this.startFailure);
        return Promise.reject(
          new Error(
            `A previous Hermes start() failed (${failureMessage}); create a new Hermes instance.`,
          ),
        );
      }
      return Promise.reject(new Error("Hermes has already started."));
    }

    this.startPromise = this.startBackend().catch((error: unknown) => {
      this.startFailed = true;
      this.startFailure = error;
      throw error;
    });
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

    // Extract unique queue names from registered jobs for backend routing
    const queueNames = [
      ...new Set(
        // deno-lint-ignore no-explicit-any
        Array.from(jobsMap.values()).map((cls: any) =>
          new cls().queueName as string
        ),
      ),
    ];

    if (await this.cancelStartupIfStopped()) return;

    // Register recurring jobs if backend supports it
    if (this.params.backend.registerRecurringJob) {
      const recurringJobs: RecurringJobConfig[] = [];
      for (const [_, jobClass] of jobsMap) {
        if (await this.cancelStartupIfStopped()) return;
        const instance = new jobClass();
        if (instance.isRecurring()) {
          if (instance.every && instance.cron) {
            throw new Error(
              `Job "${instance.jobName}" cannot have both 'every' and 'cron'`,
            );
          }
          recurringJobs.push({
            jobName: instance.jobName,
            queueName: instance.queueName,
            every: instance.every,
            cron: instance.cron,
            priority: instance.priority,
          });
        }
      }

      const validationBackend = this.params.backend as
        & BackendAdapter
        & Partial<RecurringJobValidationBackend>;
      await validationBackend.validateRecurringJobs?.(recurringJobs);

      for (const recurringJob of recurringJobs) {
        if (await this.cancelStartupIfStopped()) return;
        await this.params.backend.registerRecurringJob(recurringJob);
        if (await this.cancelStartupIfStopped()) return;
        Logger.recurringJobRegistered(
          recurringJob.jobName,
          recurringJob.every || recurringJob.cron!,
        );
      }
    }

    if (await this.cancelStartupIfStopped()) return;

    const worker = await Worker.start({
      jobsMap,
      backend: this.params.backend,
      queueNames,
      concurrency: this.params.worker?.concurrency,
      timeoutByJobName,
    });

    if (await this.cancelStartupIfStopped(worker)) return;

    this.worker = worker;
    this.queueNames = queueNames;
    this.started = true;
  }

  private async cancelStartupIfStopped(worker?: Worker): Promise<boolean> {
    if (!this.stopRequested) return false;

    // A backend startup operation may settle after stop() has already closed
    // the resources known at that point. Close again before allowing start()
    // itself to settle so late-created listeners cannot survive shutdown.
    await Promise.all([
      this.params.backend.close({ force: true }),
      worker?.awaitInFlight() ?? Promise.resolve(),
    ]);

    return true;
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

  private async closeGracefully(): Promise<void> {
    const backendClose = this.params.backend.close();
    if (!this.worker) {
      await backendClose;
      return;
    }

    await Promise.all([backendClose, this.worker.awaitInFlight()]);
    // A perform body may begin while the backend is finishing intake shutdown.
    await this.worker.awaitInFlight();
  }

  private async closeForcefully(): Promise<void> {
    let timeoutId: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([
        this.params.backend.close({ force: true }),
        new Promise<void>((resolve) => {
          timeoutId = setTimeout(resolve, FORCE_CLOSE_TIMEOUT_MS);
        }),
      ]);
    } finally {
      clearTimeout(timeoutId);
    }
  }

  private async stopAfterStart(): Promise<void> {
    Logger.workerStopping();
    const gracefulShutdownTimeout = Math.min(
      this.params.worker?.gracefulShutdownTimeout ?? 30_000,
      MAX_TIMEOUT_MS,
    );
    let timeoutId: ReturnType<typeof setTimeout> | undefined;
    const gracefulDeadline = new Promise<false>((resolve) => {
      timeoutId = setTimeout(() => resolve(false), gracefulShutdownTimeout);
    });

    try {
      let startSettled = true;
      if (this.startPromise) {
        startSettled = await Promise.race([
          this.startPromise.then(
            () => true,
            () => true,
          ),
          gracefulDeadline,
        ]);
      }

      const closedGracefully = startSettled
        ? await Promise.race([
          this.closeGracefully().then(() => true),
          gracefulDeadline,
        ])
        : false;

      if (!closedGracefully) {
        Logger.workerForceClosed(gracefulShutdownTimeout);
        await this.closeForcefully();
      }
    } finally {
      clearTimeout(timeoutId);
    }

    this.started = false;
    Logger.workerStopped();
  }

  private validateWorkerConfig(): void {
    const concurrency = this.params.worker?.concurrency;
    if (
      concurrency !== undefined &&
      (!Number.isFinite(concurrency) || concurrency < 1)
    ) {
      throw new Error(
        "Invalid worker.concurrency: it must be a finite number greater than or equal to 1.",
      );
    }

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

/** Configure the global backend adapter for standalone job enqueueing without
 * starting a worker. `aroundPerform` is excluded by type — enqueue-only
 * processes never execute jobs. Omitted `hooks`/`logger` clears any previous
 * registration. */
export const configure = (
  { backend, hooks, logger }: {
    backend: BackendAdapter;
    hooks?: Pick<HermesHooks, "enqueueMetadata">;
    logger?: LoggerSink;
  },
): void => {
  setBackend(backend);
  registerObservability(hooks, logger);
};
