import { Job } from "../../src/main.ts";

export class EmailJob extends Job {
  readonly job_name = "send_email";
  readonly queue_name = "emails";

  async perform(job_body: unknown): Promise<void> {
    const { to, subject, body } = job_body as {
      to: string;
      subject: string;
      body: string;
    };

    console.log(`Sending email to ${to}: ${subject}`);
    
    await new Promise(resolve => setTimeout(resolve, 1000));
    
    console.log(`Email sent successfully to ${to}`);
  }
}