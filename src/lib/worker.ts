import type { JobPayload, WorkerParams } from "./types.ts";
import { Logger } from "./logger.ts";

export class Worker {
  static async start(
    { jobsMap, backend, queueNames }: WorkerParams,
  ): Promise<void> {
    Logger.workerStarted(jobsMap.size, { queueNames });

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
        await job.perform(jobBody);
        Logger.jobSucceeded(jobName, queueName, Date.now() - start);
      } catch (error) {
        const errorMessage = error instanceof Error
          ? error.message
          : String(error);
        Logger.jobFailed(jobName, queueName, errorMessage, Date.now() - start);
        throw error;
      }
    }, { queueNames });
  }
}
