import { randomUUID } from "node:crypto";
import { ConductorClient, type ConductorTask } from "./client";
import { conductorTaskTypes } from "./definitions";

const conductorUrl = process.env.CONDUCTOR_URL ?? "http://localhost:8080/api";
const workerId = process.env.CONDUCTOR_WORKER_ID ?? `benchmark-conductor-worker-${randomUUID()}`;
const pollCount = Number(process.env.CONDUCTOR_POLL_COUNT ?? 20);
const pollTimeoutMs = Number(process.env.CONDUCTOR_POLL_TIMEOUT_MS ?? 1000);

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function numberInput(task: ConductorTask, name: string, fallback: number): number {
  const value = task.inputData?.[name];
  return typeof value === "number" ? value : fallback;
}

async function handleTask(client: ConductorClient, task: ConductorTask): Promise<void> {
  const latencyMs = Math.max(0, numberInput(task, "latencyMs", 0));
  const failureRatePercent = Math.max(0, Math.min(100, numberInput(task, "failureRatePercent", 0)));

  if (latencyMs > 0) {
    await sleep(latencyMs);
  }

  if (failureRatePercent > 0 && Math.random() * 100 < failureRatePercent) {
    await client.updateTask({
      workflowInstanceId: task.workflowInstanceId,
      taskId: task.taskId,
      status: "FAILED",
      reasonForIncompletion: `Injected transient failure in ${String(task.inputData?.stepName ?? task.referenceTaskName)}`
    });
    return;
  }

  await client.updateTask({
    workflowInstanceId: task.workflowInstanceId,
    taskId: task.taskId,
    status: "COMPLETED",
    outputData: {
      stepName: task.inputData?.stepName ?? task.referenceTaskName,
      latencyMs,
      payloadBytes: numberInput(task, "payloadBytes", 0)
    }
  });
}

async function main(): Promise<void> {
  const client = new ConductorClient(conductorUrl);
  await client.health();
  console.log(`Conductor worker polling ${conductorUrl} workerId=${workerId}`);

  for (;;) {
    let processed = 0;
    for (const taskType of conductorTaskTypes) {
      const tasks = await client.pollBatch(taskType, pollCount, pollTimeoutMs, workerId);
      processed += tasks.length;
      await Promise.all(tasks.map((task) => handleTask(client, task)));
    }

    if (processed === 0) {
      await sleep(100);
    }
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
