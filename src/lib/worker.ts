import type {
  HermesHooks,
  JobContext,
  JobPayload,
  WorkerParams,
} from "./types.ts";
import { getHooks } from "./hooks_registry.ts";
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

/**
 * Runs `perform` inside the `aroundPerform` wrapper while keeping the job's
 * outcome exactly `next()`'s outcome. The wrapper is a lens, not a valve:
 * its faults are logged as `hook_error` events, never substituted for the
 * perform result. The returned promise settles only after BOTH the wrapper
 * and (if invoked) the perform have settled, so timeout racing, in-flight
 * tracking, and graceful shutdown cover hung wrappers too.
 */
async function wrapPerform(
  wrapper: NonNullable<HermesHooks["aroundPerform"]>,
  payload: JobPayload,
  perform: () => Promise<unknown>,
): Promise<unknown> {
  const { jobName, queueName } = payload;

  // Hermes owns next: the first call creates the perform promise, every
  // later call returns the same one — a wrapper can never run a job twice.
  let performPromise: Promise<unknown> | undefined;
  const next = (): Promise<unknown> => {
    if (performPromise === undefined) {
      performPromise = Promise.resolve().then(perform);
      // The outcome is awaited below; keep an early rejection from surfacing
      // as unhandled while the wrapper is still running.
      performPromise.catch(() => {});
    }
    return performPromise;
  };

  let wrapperError: unknown;
  let wrapperRejected = false;
  try {
    await wrapper(payload, next);
  } catch (error) {
    wrapperError = error;
    wrapperRejected = true;
  }

  if (performPromise === undefined) {
    // next() never ran, so the job never executed: fail it with the most
    // informative error available.
    const error = wrapperRejected
      ? wrapperError
      : new Error("aroundPerform completed without invoking next()");
    Logger.hookError("aroundPerform", jobName, queueName, error);
    throw error;
  }

  try {
    const result = await performPromise;
    // The job did its work; a wrapper fault must not fail it and trigger a
    // phantom retry.
    if (wrapperRejected) {
      Logger.hookError("aroundPerform", jobName, queueName, wrapperError);
    }
    return result;
  } catch (error) {
    // The wrapper swallowed the failure; the backend still sees it.
    if (!wrapperRejected) {
      Logger.hookError(
        "aroundPerform",
        jobName,
        queueName,
        new Error("aroundPerform resolved after next() rejected"),
      );
    }
    throw error;
  }
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
  private readonly inFlightPerforms = new Set<Promise<unknown>>();

  static async start(
    {
      jobsMap,
      backend,
      queueNames,
      concurrency,
      timeoutByJobName,
    }: WorkerParams,
  ): Promise<Worker> {
    const worker = new Worker();
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
        const context: JobContext = {
          signal: controller.signal,
          metadata: payload.metadata,
        };
        const aroundPerform = getHooks()?.aroundPerform;
        const performPromise = aroundPerform
          ? wrapPerform(
            aroundPerform,
            payload,
            () => job.perform(jobBody, context),
          )
          : Promise.resolve().then(() => job.perform(jobBody, context));
        worker.inFlightPerforms.add(performPromise);
        performPromise.then(
          () => worker.inFlightPerforms.delete(performPromise),
          () => worker.inFlightPerforms.delete(performPromise),
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

    return worker;
  }

  async awaitInFlight(): Promise<void> {
    while (this.inFlightPerforms.size > 0) {
      await Promise.allSettled([...this.inFlightPerforms]);
    }
  }
}
