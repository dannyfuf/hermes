import { JobRegistry } from "./registry.ts";
import { Logger } from "./logger.ts";
import { JobPayload, WorkerConfig, JobConstructor } from "./types.ts";

export class Worker {
  private kv: Deno.Kv | null = null;
  private isRunning = false;
  private abortController: AbortController | null = null;
  private activeJobs = new Set<Promise<void>>();

  constructor(
    private registry: JobRegistry,
    private config: WorkerConfig & {
      concurrency: number;
      gracefulShutdownTimeout: number;
    },
  ) {}

  async start(): Promise<void> {
    if (this.isRunning) {
      throw new Error("Worker is already running");
    }

    this.kv = await Deno.openKv();
    this.isRunning = true;
    this.abortController = new AbortController();

    Logger.workerStarted(this.registry.size(), {
      includeQueues: this.config.includeQueues,
      excludeQueues: this.config.excludeQueues,
      concurrency: this.config.concurrency,
    });

    this.processJobs();
  }

  async stop(): Promise<void> {
    if (!this.isRunning) {
      return;
    }

    Logger.workerStopping();
    this.isRunning = false;

    if (this.abortController) {
      this.abortController.abort();
    }

    const shutdownPromise = Promise.allSettled(Array.from(this.activeJobs));
    const timeoutPromise = new Promise<void>((resolve) => {
      setTimeout(resolve, this.config.gracefulShutdownTimeout);
    });

    await Promise.race([shutdownPromise, timeoutPromise]);

    if (this.kv) {
      this.kv.close();
      this.kv = null;
    }

    Logger.workerStopped();
  }

  private async processJobs(): Promise<void> {
    if (!this.kv || !this.isRunning) return;

    try {
      await this.kv.listenQueue(async (message) => {
        if (!this.isRunning || this.abortController?.signal.aborted) {
          return;
        }

        if (this.activeJobs.size >= this.config.concurrency) {
          await this.waitForJobSlot();
        }

        const jobPromise = this.handleMessage(message);
        this.activeJobs.add(jobPromise);

        jobPromise.finally(() => {
          this.activeJobs.delete(jobPromise);
        });
      });
    } catch (error) {
      if (this.isRunning) {
        const errorMessage = error instanceof Error
          ? error.message
          : String(error);
        Logger.error("Error in job processing loop", errorMessage);
      }
    }
  }

  private async waitForJobSlot(): Promise<void> {
    if (this.activeJobs.size === 0) return;

    await Promise.race(Array.from(this.activeJobs));
  }

  private async handleMessage(message: unknown): Promise<void> {
    let payload: JobPayload;

    try {
      payload = this.validatePayload(message);
    } catch (error) {
      const errorMessage = error instanceof Error
        ? error.message
        : String(error);
      Logger.error("Invalid job payload", errorMessage, { payload: message });
      return;
    }

    Logger.jobReceived(payload.job_name, payload.queue_name);

    if (!this.shouldProcessQueue(payload.queue_name)) {
      Logger.jobSkipped(
        payload.job_name,
        payload.queue_name,
        "Queue not in worker's processing list",
      );
      return;
    }

    const JobClass = this.registry.get(payload.job_name);
    if (!JobClass) {
      Logger.unknownJob(payload.job_name, payload.queue_name, payload);
      return;
    }

    await this.executeJob(JobClass, payload);
  }

  private async executeJob(
    JobClass: JobConstructor,
    payload: JobPayload,
  ): Promise<void> {
    const startTime = Date.now();

    try {
      Logger.jobStarted(payload.job_name, payload.queue_name);

      const job = new JobClass();
      await job.perform(payload.job_body);

      const duration = Date.now() - startTime;
      Logger.jobSucceeded(payload.job_name, payload.queue_name, duration);
    } catch (error) {
      const duration = Date.now() - startTime;
      const errorMessage = error instanceof Error
        ? error.message
        : String(error);
      Logger.jobFailed(
        payload.job_name,
        payload.queue_name,
        errorMessage,
        duration,
      );
    }
  }

  private validatePayload(message: unknown): JobPayload {
    if (!message || typeof message !== "object") {
      throw new Error("Payload must be an object");
    }

    const payload = message as Record<string, unknown>;

    if (!payload.job_name || typeof payload.job_name !== "string") {
      throw new Error("Payload must include a valid job_name string");
    }

    if (!payload.queue_name || typeof payload.queue_name !== "string") {
      throw new Error("Payload must include a valid queue_name string");
    }

    return {
      job_name: payload.job_name,
      queue_name: payload.queue_name,
      job_body: payload.job_body,
      metadata: payload.metadata as Record<string, unknown> | undefined,
    };
  }

  private shouldProcessQueue(queueName: string): boolean {
    if (this.config.includeQueues) {
      return this.config.includeQueues.includes(queueName);
    }

    if (this.config.excludeQueues) {
      return !this.config.excludeQueues.includes(queueName);
    }

    return true;
  }

  isActive(): boolean {
    return this.isRunning;
  }

  getActiveJobCount(): number {
    return this.activeJobs.size;
  }
}
