import { Job } from "../../src/main.ts";

export class ReportJob extends Job {
  readonly job_name = "generate_report";
  readonly queue_name = "reports";

  async perform(job_body: unknown): Promise<void> {
    const { reportType, userId } = job_body as {
      reportType: string;
      userId: string;
    };

    console.log(`Generating ${reportType} report for user ${userId}`);

    await new Promise((resolve) => setTimeout(resolve, 2000));

    console.log(
      `Report ${reportType} generated successfully for user ${userId}`,
    );
  }
}

