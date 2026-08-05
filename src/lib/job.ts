import type { JobContext, JobPayload, PerformLaterOptions } from "./types.ts";
import { getBackend } from "./backend_registry.ts";
import { intervalToMs, parseEveryInterval } from "./schedule.ts";

/** Base class for defining background jobs. Subclasses must implement `jobName`, `queueName`, and `perform`. */
export abstract class Job {
  abstract readonly jobName: string;
  abstract readonly queueName: string;
  readonly every?: string;
  readonly cron?: string;
  readonly timeout?: string | number;

  abstract perform(
    jobBody: unknown,
    context?: JobContext,
  ): Promise<unknown>;

  isRecurring(): boolean {
    return this.every !== undefined || this.cron !== undefined;
  }

  getScheduleMs(): number | undefined {
    if (this.every) return intervalToMs(parseEveryInterval(this.every));
    return undefined;
  }

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
