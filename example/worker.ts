import { Hermes } from "@hermes";

const hermes = Hermes({
  manifest: "./example/jobs/main.ts",
  worker: { gracefulShutdownTimeout: 1000 },
});

hermes.start().then(() => {
  console.log("Hermes is running");
});
