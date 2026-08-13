import { Job } from "../../../../main.ts";

/** `7m` does not divide 60, so Deno KV recurrence must reject it at startup. */
class UnsupportedIntervalJob extends Job {
  readonly jobName = "unsupported_interval";
  readonly queueName = Deno.env.get("HERMES_INTEGRATION_QUEUE") ?? "default";
  override readonly every = "7m";

  perform(): Promise<void> {
    return Promise.resolve();
  }
}

export default [UnsupportedIntervalJob];
