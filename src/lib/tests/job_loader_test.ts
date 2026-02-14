import { assertEquals, assertRejects } from "@std/assert";
import { JobLoader } from "../job_loader.ts";
import { CustomQueueJob, TestJob } from "./helpers/test_jobs.ts";

Deno.test("JobLoader", async (t) => {
  await t.step("loads jobs from an array of classes into a Map", async () => {
    const jobsMap = await JobLoader([TestJob, CustomQueueJob]).run();

    assertEquals(jobsMap.size, 2);
    assertEquals(jobsMap.get("test_job"), TestJob);
    assertEquals(jobsMap.get("custom_queue_job"), CustomQueueJob);
  });

  await t.step("detects duplicate jobName and throws", async () => {
    await assertRejects(
      () => JobLoader([TestJob, TestJob]).run(),
      Error,
      'Duplicate jobName "test_job"',
    );
  });

  await t.step("returns empty Map for empty manifest", async () => {
    const jobsMap = await JobLoader([]).run();
    assertEquals(jobsMap.size, 0);
  });
});
