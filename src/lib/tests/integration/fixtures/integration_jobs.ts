import { Job } from "../../../../main.ts";
import type { JobContext } from "../../../../main.ts";

type SinkPayload = {
  sinkPath: string;
  marker: string;
  durationMs?: number;
  childMarker?: string;
};

function requiredEnv(name: string): string {
  const value = Deno.env.get(name);
  if (!value) throw new Error(`${name} is required`);
  return value;
}

function queueName(): string {
  return requiredEnv("HERMES_INTEGRATION_QUEUE");
}

function secondaryQueueName(): string {
  return requiredEnv("HERMES_INTEGRATION_SECONDARY_QUEUE");
}

function recurringSinkPath(): string {
  return requiredEnv("HERMES_INTEGRATION_SINK");
}

async function appendLine(path: string, value: unknown): Promise<void> {
  await Deno.writeTextFile(path, `${JSON.stringify(value)}\n`, {
    append: true,
    create: true,
  });
}

async function sinkLines(path: string): Promise<unknown[]> {
  try {
    const contents = await Deno.readTextFile(path);
    return contents.trim().split("\n").filter(Boolean).map((line) =>
      JSON.parse(line)
    );
  } catch (error) {
    if (error instanceof Deno.errors.NotFound) return [];
    throw error;
  }
}

function sleep(durationMs: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, durationMs));
}

export class EchoJob extends Job {
  readonly jobName = "echo";
  readonly queueName = queueName();

  async perform(jobBody: unknown): Promise<void> {
    const body = jobBody as SinkPayload;
    await appendLine(body.sinkPath, body);
  }
}

export class TimedHangsJob extends Job {
  readonly jobName = "timed_hangs";
  readonly queueName = queueName();
  override readonly timeout = "1s";

  async perform(): Promise<never> {
    return await new Promise<never>(() => {});
  }
}

export class NumericTimedHangsJob extends Job {
  readonly jobName = "numeric_timed_hangs";
  readonly queueName = queueName();
  override readonly timeout = 800;

  async perform(): Promise<never> {
    return await new Promise<never>(() => {});
  }
}

export class RetryTimedHangsJob extends Job {
  readonly jobName = "retry_timed_hangs";
  readonly queueName = queueName();
  override readonly timeout = 800;

  async perform(jobBody: unknown): Promise<never> {
    const body = jobBody as SinkPayload;
    await appendLine(body.sinkPath, {
      event: "timeout_attempt",
      marker: body.marker,
    });
    return await new Promise<never>(() => {});
  }
}

export class AbortAwareJob extends Job {
  readonly jobName = "abort_aware";
  readonly queueName = queueName();
  override readonly timeout = "1s";

  async perform(jobBody: unknown, context?: JobContext): Promise<never> {
    const body = jobBody as SinkPayload;
    context?.signal.addEventListener(
      "abort",
      () => {
        void appendLine(body.sinkPath, {
          event: "aborted",
          marker: body.marker,
        });
      },
      { once: true },
    );
    return await new Promise<never>(() => {});
  }
}

export class SlowJob extends Job {
  readonly jobName = "slow";
  readonly queueName = queueName();

  async perform(jobBody: unknown): Promise<void> {
    const body = jobBody as SinkPayload;
    await sleep(1_500);
    await appendLine(body.sinkPath, { event: "slow", marker: body.marker });
  }
}

export class DelayTimedJob extends Job {
  readonly jobName = "delay_timed";
  readonly queueName = queueName();
  override readonly timeout = 800;

  async perform(jobBody: unknown): Promise<void> {
    const body = jobBody as SinkPayload;
    await sleep(300);
    await appendLine(body.sinkPath, {
      event: "delay_timed_completed",
      marker: body.marker,
    });
  }
}

export class MetadataEchoJob extends Job {
  readonly jobName = "metadata_echo";
  readonly queueName = queueName();

  async perform(jobBody: unknown, context?: JobContext): Promise<void> {
    const body = jobBody as SinkPayload;
    await appendLine(body.sinkPath, {
      event: "metadata_echo",
      marker: body.marker,
      metadata: context?.metadata ?? null,
    });
  }
}

export class ChainParentJob extends Job {
  readonly jobName = "chain_parent";
  readonly queueName = queueName();

  async perform(jobBody: unknown): Promise<void> {
    const body = jobBody as SinkPayload;
    // Job → job chain: enqueue the child from inside perform(). The child
    // enqueue goes through the same global registries, so a configured
    // enqueueMetadata hook must stamp it.
    await new MetadataEchoJob().performLater({
      sinkPath: body.sinkPath,
      marker: body.childMarker ?? `${body.marker}-child`,
    });
    await appendLine(body.sinkPath, {
      event: "chain_parent",
      marker: body.marker,
    });
  }
}

export class FlakyJob extends Job {
  readonly jobName = "flaky";
  readonly queueName = queueName();

  async perform(jobBody: unknown): Promise<void> {
    const body = jobBody as SinkPayload;
    const attempts =
      (await sinkLines(body.sinkPath)).filter((entry) =>
        (entry as { event?: unknown }).event === "flaky_attempt"
      ).length + 1;
    await appendLine(body.sinkPath, {
      event: "flaky_attempt",
      attempt: attempts,
    });

    if (attempts < 3) throw new Error(`flaky attempt ${attempts}`);
    await appendLine(body.sinkPath, { event: "flaky_success" });
  }
}

export class AlwaysFailJob extends Job {
  readonly jobName = "always_fail";
  readonly queueName = queueName();

  async perform(jobBody: unknown): Promise<never> {
    const body = jobBody as SinkPayload;
    await appendLine(body.sinkPath, {
      event: "always_fail_attempt",
      marker: body.marker,
    });
    throw new Error("always fails");
  }
}

export class LowPriorityJob extends Job {
  readonly jobName = "low_priority";
  readonly queueName = queueName();
  override readonly priority = 10;

  async perform(jobBody: unknown): Promise<void> {
    const body = jobBody as SinkPayload;
    await appendLine(body.sinkPath, { event: "priority", marker: body.marker });
  }
}

export class HighPriorityJob extends Job {
  readonly jobName = "high_priority";
  readonly queueName = queueName();
  override readonly priority = 1;

  async perform(jobBody: unknown): Promise<void> {
    const body = jobBody as SinkPayload;
    await appendLine(body.sinkPath, { event: "priority", marker: body.marker });
  }
}

export class DefaultPriorityJob extends Job {
  readonly jobName = "default_priority";
  readonly queueName = queueName();
  override readonly priority = 10;

  async perform(jobBody: unknown): Promise<void> {
    const body = jobBody as SinkPayload;
    await appendLine(body.sinkPath, { event: "priority", marker: body.marker });
  }
}

export class BlockingJob extends Job {
  readonly jobName = "blocking";
  readonly queueName = queueName();

  async perform(jobBody: unknown): Promise<void> {
    const body = jobBody as SinkPayload;
    await appendLine(body.sinkPath, {
      event: "blocking_started",
      marker: body.marker,
    });
    await sleep(body.durationMs ?? 800);
    await appendLine(body.sinkPath, {
      event: "blocking_completed",
      marker: body.marker,
    });
  }
}

export class UntimedHangsJob extends Job {
  readonly jobName = "untimed_hangs";
  readonly queueName = queueName();

  async perform(jobBody: unknown): Promise<never> {
    const body = jobBody as SinkPayload;
    await appendLine(body.sinkPath, {
      event: "untimed_hangs_started",
      marker: body.marker,
    });
    return await new Promise<never>(() => {});
  }
}

export class GracefulJob extends Job {
  readonly jobName = "graceful";
  readonly queueName = queueName();

  async perform(jobBody: unknown): Promise<void> {
    const body = jobBody as SinkPayload;
    await appendLine(body.sinkPath, {
      event: "graceful_started",
      marker: body.marker,
    });
    await sleep(1_000);
    await appendLine(body.sinkPath, {
      event: "graceful_completed",
      marker: body.marker,
    });
  }
}

export class ConcurrentJob extends Job {
  readonly jobName = "concurrent";
  readonly queueName = queueName();

  async perform(jobBody: unknown): Promise<void> {
    const body = jobBody as SinkPayload;
    const startedAt = Date.now();
    await appendLine(body.sinkPath, {
      event: "concurrency_started",
      marker: body.marker,
      at: startedAt,
    });
    await sleep(body.durationMs ?? 800);
    await appendLine(body.sinkPath, {
      event: "concurrency_ended",
      marker: body.marker,
      at: Date.now(),
    });
  }
}

export class PrimaryQueueJob extends Job {
  readonly jobName = "primary_queue";
  readonly queueName = queueName();

  async perform(jobBody: unknown): Promise<void> {
    const body = jobBody as SinkPayload;
    await appendLine(body.sinkPath, {
      event: "multi_queue",
      marker: body.marker,
      queueName: this.queueName,
    });
  }
}

export class SecondaryQueueJob extends Job {
  readonly jobName = "secondary_queue";
  readonly queueName = secondaryQueueName();

  async perform(jobBody: unknown): Promise<void> {
    const body = jobBody as SinkPayload;
    await appendLine(body.sinkPath, {
      event: "multi_queue",
      marker: body.marker,
      queueName: this.queueName,
    });
  }
}

export class RecurringTwoSecondsJob extends Job {
  readonly jobName = "recurring_2s";
  readonly queueName = queueName();
  override readonly every = "2s";

  async perform(): Promise<void> {
    await appendLine(recurringSinkPath(), {
      event: "recurring_2s",
      at: Date.now(),
    });
  }
}

export class RecurringOneHundredTwentySecondsJob extends Job {
  readonly jobName = "recurring_120s";
  readonly queueName = queueName();
  override readonly every = "120s";

  async perform(): Promise<void> {
    await appendLine(recurringSinkPath(), { event: "recurring_120s" });
  }
}

export class CronJob extends Job {
  readonly jobName = "cron_job";
  readonly queueName = queueName();
  override readonly cron = "*/2 * * * *";

  async perform(): Promise<void> {
    await appendLine(recurringSinkPath(), { event: "cron_job" });
  }
}

export class RecurringTimedHangsJob extends Job {
  readonly jobName = "recurring_timed_hangs";
  readonly queueName = queueName();
  override readonly every = "2s";
  override readonly timeout = "1s";

  async perform(): Promise<never> {
    await appendLine(recurringSinkPath(), {
      event: "recurring_timeout_started",
      at: Date.now(),
    });
    return await new Promise<never>(() => {});
  }
}

export class RecurringPriorityJob extends Job {
  readonly jobName = "recurring_priority";
  readonly queueName = queueName();
  override readonly every = "120s";
  override readonly priority = 3;

  async perform(): Promise<void> {
    await appendLine(recurringSinkPath(), { event: "recurring_priority" });
  }
}

let rescueCompletes = false;

export function setRescueCompletes(value: boolean): void {
  rescueCompletes = value;
}

export class RescueHangsJob extends Job {
  readonly jobName = "rescue_hangs";
  readonly queueName = queueName();

  async perform(jobBody: unknown): Promise<void> {
    const body = jobBody as SinkPayload;
    if (rescueCompletes) {
      await appendLine(body.sinkPath, { event: "rescue_completed" });
      return;
    }

    await appendLine(body.sinkPath, { event: "rescue_started" });
    await new Promise<never>(() => {});
  }
}
