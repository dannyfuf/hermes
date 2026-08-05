import type { JobContext, JobPayload, WorkerParams } from "./types.ts";
import { Logger } from "./logger.ts";
import { intervalToMs, parseEveryInterval } from "./schedule.ts";

export const MAX_TIMEOUT_MS: number = 2_147_483_647;

class JobTimeoutError extends Error {
  constructor(jobName: string, timeoutMs: number) {
    super(`Job "${jobName}" timed out after ${timeoutMs}ms`);
    this.name = "JobTimeoutError";
  }
}

export function resolveTimeoutMs(
  jobName: string,
  timeout: string | number | undefined,
): number | undefined {
  if (timeout === undefined) return undefined;

  let timeoutMs: number;
  if (typeof timeout === "string") {
    try {
      timeoutMs = intervalToMs(parseEveryInterval(timeout));
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      throw new Error(`Invalid timeout for job "${jobName}": ${message}`, {
        cause: error,
      });
    }
  } else if (!Number.isSafeInteger(timeout) || timeout <= 0) {
    throw new Error(
      `Invalid timeout for job "${jobName}": numeric timeouts must be positive safe-integer milliseconds.`,
    );
  } else {
    timeoutMs = timeout;
  }

  if (timeoutMs > MAX_TIMEOUT_MS) {
    throw new Error(
      `Invalid timeout for job "${jobName}": timeouts cannot exceed ${MAX_TIMEOUT_MS}ms (~24.8 days), the maximum supported timer delay.`,
    );
  }

  return timeoutMs;
}

export function resolveJobTimeouts(
  jobsMap: WorkerParams["jobsMap"],
  defaultJobTimeout?: string | number,
): Map<string, number | undefined> {
  const timeoutByJobName = new Map<string, number | undefined>();
  for (const [jobName, jobClass] of jobsMap) {
    const job = new jobClass();
    timeoutByJobName.set(
      jobName,
      resolveTimeoutMs(jobName, job.timeout ?? defaultJobTimeout),
    );
  }
  return timeoutByJobName;
}

export class Worker {
  static async start(
    {
      jobsMap,
      backend,
      queueNames,
      concurrency,
      timeoutByJobName,
    }: WorkerParams,
  ): Promise<void> {
    Logger.workerStarted(jobsMap.size, { queueNames, concurrency });

    await backend.listen(async (payload: JobPayload) => {
      const { jobName, jobBody, queueName } = payload;
      Logger.jobReceived(jobName, queueName);

      const jobClass = jobsMap.get(jobName);
      if (!jobClass) {
        Logger.unknownJob(jobName, queueName, payload);
        return;
      }

      const start = Date.now();
      Logger.jobStarted(jobName, queueName);

      try {
        const job = new jobClass();
        const controller = new AbortController();
        const context: JobContext = { signal: controller.signal };
        const performPromise = Promise.resolve().then(() =>
          job.perform(jobBody, context)
        );
        performPromise.catch(() => {});

        const timeoutMs = timeoutByJobName.get(jobName);
        if (timeoutMs === undefined) {
          await performPromise;
        } else {
          let timeoutId: ReturnType<typeof setTimeout> | undefined;
          try {
            await Promise.race([
              performPromise,
              new Promise<never>((_, reject) => {
                timeoutId = setTimeout(() => {
                  controller.abort();
                  reject(new JobTimeoutError(jobName, timeoutMs));
                }, timeoutMs);
              }),
            ]);
          } finally {
            clearTimeout(timeoutId);
          }
        }
        Logger.jobSucceeded(jobName, queueName, Date.now() - start);
      } catch (error) {
        const errorMessage = error instanceof Error
          ? error.message
          : String(error);
        Logger.jobFailed(jobName, queueName, errorMessage, Date.now() - start);
        throw error;
      }
    }, { queueNames, concurrency });
  }
}
