export interface LogEvent {
  timestamp: string;
  event: string;
  job_name?: string;
  queue_name?: string;
  error?: string;
  [key: string]: unknown;
}

export class Logger {
  private static formatTimestamp(): string {
    return new Date().toISOString();
  }

  static jobReceived(jobName: string, queueName: string): void {
    this.log({
      timestamp: this.formatTimestamp(),
      event: "job_received",
      job_name: jobName,
      queue_name: queueName,
    });
  }

  static jobStarted(jobName: string, queueName: string): void {
    this.log({
      timestamp: this.formatTimestamp(),
      event: "job_started",
      job_name: jobName,
      queue_name: queueName,
    });
  }

  static jobSucceeded(
    jobName: string,
    queueName: string,
    duration?: number,
  ): void {
    this.log({
      timestamp: this.formatTimestamp(),
      event: "job_succeeded",
      job_name: jobName,
      queue_name: queueName,
      duration_ms: duration,
    });
  }

  static jobFailed(
    jobName: string,
    queueName: string,
    error: string,
    duration?: number,
  ): void {
    this.log({
      timestamp: this.formatTimestamp(),
      event: "job_failed",
      job_name: jobName,
      queue_name: queueName,
      error,
      duration_ms: duration,
    });
  }

  static jobSkipped(jobName: string, queueName: string, reason: string): void {
    this.log({
      timestamp: this.formatTimestamp(),
      event: "job_skipped",
      job_name: jobName,
      queue_name: queueName,
      reason,
    });
  }

  static unknownJob(
    jobName: string,
    queueName: string,
    payload: unknown,
  ): void {
    this.log({
      timestamp: this.formatTimestamp(),
      event: "unknown_job",
      job_name: jobName,
      queue_name: queueName,
      payload,
    });
  }

  static workerStarted(registeredJobs: number, config: unknown): void {
    this.log({
      timestamp: this.formatTimestamp(),
      event: "worker_started",
      registered_jobs: registeredJobs,
      config,
    });
  }

  static workerStopping(): void {
    this.log({
      timestamp: this.formatTimestamp(),
      event: "worker_stopping",
    });
  }

  static workerStopped(): void {
    this.log({
      timestamp: this.formatTimestamp(),
      event: "worker_stopped",
    });
  }

  static info(message: string, data?: Record<string, unknown>): void {
    this.log({
      timestamp: this.formatTimestamp(),
      event: "info",
      message,
      ...data,
    });
  }

  static warn(message: string, data?: Record<string, unknown>): void {
    this.log({
      timestamp: this.formatTimestamp(),
      event: "warning",
      message,
      ...data,
    });
  }

  static error(
    message: string,
    error?: string,
    data?: Record<string, unknown>,
  ): void {
    this.log({
      timestamp: this.formatTimestamp(),
      event: "error",
      message,
      error,
      ...data,
    });
  }

  private static log(event: LogEvent): void {
    console.log(JSON.stringify(event));
  }
}
