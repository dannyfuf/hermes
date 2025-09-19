import type { JobPayload, PerformLaterOptions } from "./types.ts";

export abstract class Job {
  abstract readonly job_name: string;
  abstract readonly queue_name: string;
  readonly every?: number;
  readonly cron?: string;

  abstract perform(job_body: unknown): Promise<unknown>;

  async perform_later(job_body: unknown, opts: PerformLaterOptions = {}) {
    const kv = await Deno.openKv();
    const payload: JobPayload = {
      job_name: this.job_name,
      queue_name: this.queue_name,
      job_body,
    };

    await kv.enqueue(payload, { delay: opts.delay });
  }
}
