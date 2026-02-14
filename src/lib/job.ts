import type { JobPayload, PerformLaterOptions } from "./types.ts";
import { getBackend } from "./backend_registry.ts";

export abstract class Job {
  abstract readonly jobName: string;
  abstract readonly queueName: string;
  readonly every?: number;
  readonly cron?: string;

  abstract perform(jobBody: unknown): Promise<unknown>;

  async performLater(
    jobBody?: unknown,
    opts: PerformLaterOptions = {},
  ): Promise<void> {
    const backend = getBackend();
    const payload: JobPayload = {
      jobName: this.jobName,
      queueName: this.queueName,
      jobBody,
    };

    await backend.enqueue(payload, { delay: opts.delay });
  }
}
