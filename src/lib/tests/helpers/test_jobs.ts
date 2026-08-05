import { Job } from "../../job.ts";

/** Module-level call log for verifying perform() was called with the correct args */
export const performCalls: { jobName: string; jobBody: unknown }[] = [];

/** Reset the call log between tests */
export function clearPerformCalls(): void {
  performCalls.length = 0;
}

export class TestJob extends Job {
  readonly jobName = "test_job";
  readonly queueName = "default";

  async perform(jobBody: unknown): Promise<unknown> {
    performCalls.push({ jobName: this.jobName, jobBody });
    return await Promise.resolve({ ok: true });
  }
}

export class FailingJob extends Job {
  readonly jobName = "failing_job";
  readonly queueName = "default";

  perform(_jobBody: unknown): Promise<unknown> {
    throw new Error("Job failed intentionally");
  }
}

export class RecurringEveryJob extends Job {
  readonly jobName = "recurring_every_job";
  readonly queueName = "default";
  override readonly every = "5m";
  override readonly priority = 4;

  // deno-lint-ignore require-await
  async perform(jobBody: unknown): Promise<unknown> {
    performCalls.push({ jobName: this.jobName, jobBody });
    return { ok: true };
  }
}

export class RecurringCronJob extends Job {
  readonly jobName = "recurring_cron_job";
  readonly queueName = "default";
  override readonly cron = "0 9 * * 1-5";

  // deno-lint-ignore require-await
  async perform(jobBody: unknown): Promise<unknown> {
    performCalls.push({ jobName: this.jobName, jobBody });
    return { ok: true };
  }
}

export class CustomQueueJob extends Job {
  readonly jobName = "custom_queue_job";
  readonly queueName = "priority";

  // deno-lint-ignore require-await
  async perform(jobBody: unknown): Promise<unknown> {
    performCalls.push({ jobName: this.jobName, jobBody });
    return { processed: jobBody };
  }
}
