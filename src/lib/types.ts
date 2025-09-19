export type PerformLaterOptions = {
  delay?: number;
};

export type JobPayload = {
  job_name: string;
  queue_name: string;
  job_body: unknown;
  metadata?: Record<string, unknown>;
};

export type WorkerConfig = {
  includeQueues?: string[];
  excludeQueues?: string[];
  concurrency?: number;
  gracefulShutdownTimeout?: number;
};

export type JobManifest = Array<new () => Job>;

export type JobConstructor = new () => Job;

export interface Job {
  readonly job_name: string;
  readonly queue_name: string;
  perform(job_body: unknown): Promise<unknown>;
  perform_later(job_body: unknown, opts?: PerformLaterOptions): Promise<void>;
}
