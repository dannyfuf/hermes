import { Job } from "@hermes";

export class HealthCheckJob extends Job {
  jobName = "health_check";
  queueName = "default";
  override every = "5m";

  async perform() {
    await Promise.resolve();
    console.log(`Health check at ${new Date().toISOString()}`);
  }
}
