import { NativeConnection, Runtime, Worker } from "@temporalio/worker";
import * as activities from "./activities";

const temporalAddress = process.env.TEMPORAL_ADDRESS ?? "localhost:7233";
const namespace = process.env.TEMPORAL_NAMESPACE ?? "default";
const taskQueue = process.env.TEMPORAL_TASK_QUEUE ?? "benchmark-temporal";
const workerMetricsAddress = process.env.TEMPORAL_WORKER_METRICS_ADDRESS ?? "0.0.0.0:9465";

async function main(): Promise<void> {
  Runtime.install({
    telemetryOptions: {
      logging: {
        forward: {},
        filter: "INFO"
      },
      metrics: {
        prometheus: {
          bindAddress: workerMetricsAddress,
          countersTotalSuffix: true,
          unitSuffix: true
        }
      }
    }
  });

  const connection = await NativeConnection.connect({ address: temporalAddress });
  const worker = await Worker.create({
    connection,
    namespace,
    taskQueue,
    workflowsPath: require.resolve("./workflows"),
    activities,
    maxConcurrentActivityTaskExecutions: Number(process.env.TEMPORAL_MAX_ACTIVITIES ?? 200),
    maxConcurrentWorkflowTaskExecutions: Number(process.env.TEMPORAL_MAX_WORKFLOWS ?? 200)
  });

  console.log(`Temporal worker polling ${temporalAddress} namespace=${namespace} taskQueue=${taskQueue}`);
  console.log(`Temporal worker metrics listening on http://${workerMetricsAddress}/metrics`);
  await worker.run();
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
