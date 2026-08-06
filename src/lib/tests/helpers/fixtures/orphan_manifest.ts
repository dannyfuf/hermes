import { Job } from "../../../job.ts";

let resolveStarted: () => void = () => {};
let releasePerform: () => void = () => {};
let started = Promise.resolve();
let release = Promise.resolve();

export function resetOrphanJob(): void {
  started = new Promise<void>((resolve) => resolveStarted = resolve);
  release = new Promise<void>((resolve) => releasePerform = resolve);
}

export function waitForOrphanJobStart(): Promise<void> {
  return started;
}

export function releaseOrphanJob(): void {
  releasePerform();
}

class OrphanJob extends Job {
  readonly jobName = "orphan_job";
  readonly queueName = "default";
  override readonly timeout = 10;

  async perform(): Promise<void> {
    resolveStarted();
    await release;
  }
}

export default [OrphanJob];
