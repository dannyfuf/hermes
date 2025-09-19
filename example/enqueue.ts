import { EmailJob, ReportJob } from "./jobs/main.ts";

const emailJob = new EmailJob();
const reportJob = new ReportJob();

await emailJob.perform_later({
  to: "user@example.com",
  subject: "Welcome!",
  body: "Thanks for signing up!",
});

await reportJob.perform_later({
  reportType: "monthly",
  userId: "user123",
}, { delay: 5000 });

console.log("Jobs enqueued successfully!");