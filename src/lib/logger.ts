import { getLoggerSink } from "./hooks_registry.ts";

export interface LogEvent {
  timestamp: string;
  event: string;
  jobName?: string;
  queueName?: string;
  error?: string;
  [key: string]: unknown;
}

export class Logger {
  private static formatTimestamp(): string {
    return new Date().toISOString();
  }

  static jobReceived(
    jobName: string,
    queueName: string,
    metadata?: Record<string, unknown>,
  ): void {
    this.log({
      timestamp: this.formatTimestamp(),
      event: "job_received",
      jobName,
      queueName,
      ...(metadata && { metadata }),
    });
  }

  static jobStarted(
    jobName: string,
    queueName: string,
    metadata?: Record<string, unknown>,
  ): void {
    this.log({
      timestamp: this.formatTimestamp(),
      event: "job_started",
      jobName,
      queueName,
      ...(metadata && { metadata }),
    });
  }

  static jobSucceeded(
    jobName: string,
    queueName: string,
    duration?: number,
    metadata?: Record<string, unknown>,
  ): void {
    this.log({
      timestamp: this.formatTimestamp(),
      event: "job_succeeded",
      jobName,
      queueName,
      durationMs: duration,
      ...(metadata && { metadata }),
    });
  }

  static jobFailed(
    jobName: string,
    queueName: string,
    error: string,
    duration?: number,
    metadata?: Record<string, unknown>,
  ): void {
    this.log({
      timestamp: this.formatTimestamp(),
      event: "job_failed",
      jobName,
      queueName,
      error,
      durationMs: duration,
      ...(metadata && { metadata }),
    });
  }

  static jobSkipped(jobName: string, queueName: string, reason: string): void {
    this.log({
      timestamp: this.formatTimestamp(),
      event: "job_skipped",
      jobName,
      queueName,
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
      jobName,
      queueName,
      payload,
    });
  }

  static workerStarted(registeredJobs: number, config: unknown): void {
    this.log({
      timestamp: this.formatTimestamp(),
      event: "worker_started",
      registeredJobs,
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

  static workerForceClosed(gracefulShutdownTimeoutMs: number): void {
    this.log({
      timestamp: this.formatTimestamp(),
      event: "worker_force_closed",
      gracefulShutdownTimeoutMs,
    });
  }

  static workerError(queueName: string, error: unknown): void {
    this.log({
      timestamp: this.formatTimestamp(),
      event: "worker_error",
      queueName,
      error: this.errorMessage(error),
    });
  }

  static workerJobFailed(
    queueName: string,
    jobName: string | undefined,
    jobId: string | undefined,
    attemptsMade: number | undefined,
    error: unknown,
  ): void {
    this.log({
      timestamp: this.formatTimestamp(),
      event: "worker_job_failed",
      queueName,
      jobName,
      jobId,
      attemptsMade,
      error: this.errorMessage(error),
    });
  }

  static jobStalled(queueName: string, jobId: string): void {
    this.log({
      timestamp: this.formatTimestamp(),
      event: "job_stalled",
      queueName,
      jobId,
    });
  }

  static workerClosed(queueName: string): void {
    this.log({
      timestamp: this.formatTimestamp(),
      event: "worker_closed",
      queueName,
    });
  }

  static queueError(queueName: string, error: unknown): void {
    this.log({
      timestamp: this.formatTimestamp(),
      event: "queue_error",
      queueName,
      error: this.errorMessage(error),
    });
  }

  static recurringJobRegistered(jobName: string, schedule: string): void {
    this.log({
      timestamp: this.formatTimestamp(),
      event: "recurring_job_registered",
      jobName,
      schedule,
    });
  }

  static recurringJobSkipped(jobName: string, reason: string): void {
    this.log({
      timestamp: this.formatTimestamp(),
      event: "recurring_job_skipped",
      jobName,
      reason,
    });
  }

  static hookError(
    hook: string,
    jobName: string,
    queueName: string,
    error: unknown,
  ): void {
    this.log({
      timestamp: this.formatTimestamp(),
      event: "hook_error",
      hook,
      jobName,
      queueName,
      error: this.errorMessage(error),
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
    const sink = getLoggerSink();
    if (!sink) {
      console.log(JSON.stringify(event));
      return;
    }

    try {
      sink(event);
    } catch (error) {
      // A broken sink must never take down dispatch: fall back to console
      // for the original event, then leave one logger_error breadcrumb —
      // written to console directly so a throwing sink cannot recurse.
      console.log(JSON.stringify(event));
      console.log(JSON.stringify({
        timestamp: this.formatTimestamp(),
        event: "logger_error",
        error: this.errorMessage(error),
      }));
    }
  }

  private static errorMessage(error: unknown): string {
    return error instanceof Error ? error.message : String(error);
  }
}
