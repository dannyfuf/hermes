import type {
  BackendAdapter,
  EnqueueOptions,
  RecurringJobConfig,
} from "../../backend.ts";
import type { JobPayload } from "../../types.ts";

export class MockBackend implements BackendAdapter {
  enqueued: { payload: JobPayload; options?: EnqueueOptions }[] = [];
  registeredRecurringJobs: RecurringJobConfig[] = [];
  removedRecurringJobs: string[] = [];
  listenOptions: { queueNames?: string[] } | undefined = undefined;
  closeOptions: ({ force?: boolean } | undefined)[] = [];
  private handler: ((payload: JobPayload) => Promise<void>) | null = null;

  // deno-lint-ignore require-await
  async enqueue(payload: JobPayload, options?: EnqueueOptions): Promise<void> {
    this.enqueued.push({ payload, options });
  }

  // deno-lint-ignore require-await
  async listen(
    handler: (payload: JobPayload) => Promise<void>,
    options?: { queueNames?: string[] },
  ): Promise<void> {
    this.handler = handler;
    this.listenOptions = options;
  }

  close(options?: { force?: boolean }): Promise<void> {
    this.closeOptions.push(options);
    this.handler = null;
    return Promise.resolve();
  }

  // deno-lint-ignore require-await
  async registerRecurringJob(config: RecurringJobConfig): Promise<void> {
    this.registeredRecurringJobs.push(config);
  }

  // deno-lint-ignore require-await
  async removeRecurringJob(jobName: string): Promise<void> {
    this.removedRecurringJobs.push(jobName);
  }

  /** Test helper: manually trigger processing of a payload */
  async process(payload: JobPayload): Promise<void> {
    if (!this.handler) throw new Error("No handler registered");
    await this.handler(payload);
  }

  /** Test helper: check if a handler is registered */
  get isListening(): boolean {
    return this.handler !== null;
  }
}
