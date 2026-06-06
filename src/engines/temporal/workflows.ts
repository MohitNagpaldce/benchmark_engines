import { proxyActivities, sleep } from "@temporalio/workflow";
import type { WorkflowInput, WorkflowResult } from "../../common/types";
import type * as activities from "./activities";

const defaultActivities = proxyActivities<typeof activities>({
  startToCloseTimeout: "30 seconds"
});

function numberParam(input: WorkflowInput, name: string, fallback: number): number {
  const value = input.params[name];
  return typeof value === "number" ? value : fallback;
}

function result(input: WorkflowInput, values: Omit<WorkflowResult, "problemId" | "workflowIndex">): WorkflowResult {
  return {
    problemId: input.problemId,
    workflowIndex: input.workflowIndex,
    ...values
  };
}

export async function lowLatencyWorkflow(input: WorkflowInput): Promise<WorkflowResult> {
  const steps = numberParam(input, "steps", 5);
  const latencyMs = numberParam(input, "activityLatencyMs", 10);
  const payloadBytes = numberParam(input, "payloadBytes", 256);

  for (let step = 0; step < steps; step += 1) {
    await defaultActivities.mockRpc({
      stepName: `low-latency-${step}`,
      latencyMs,
      payloadBytes
    });
  }

  return result(input, {
    completedSteps: steps,
    activityCalls: steps,
    timerCount: 0,
    retryableFailures: 0
  });
}

export async function highThroughputWorkflow(input: WorkflowInput): Promise<WorkflowResult> {
  const fanout = numberParam(input, "fanout", 100);
  const latencyMs = numberParam(input, "activityLatencyMs", 5);
  const payloadBytes = numberParam(input, "payloadBytes", 128);

  await Promise.all(
    Array.from({ length: fanout }, (_, index) =>
      defaultActivities.mockRpc({
        stepName: `fanout-${index}`,
        latencyMs,
        payloadBytes
      })
    )
  );

  await defaultActivities.mockRpc({
    stepName: "aggregate-results",
    latencyMs,
    payloadBytes
  });

  return result(input, {
    completedSteps: fanout + 1,
    activityCalls: fanout + 1,
    timerCount: 0,
    retryableFailures: 0
  });
}

export async function deepSequentialWorkflow(input: WorkflowInput): Promise<WorkflowResult> {
  const steps = numberParam(input, "steps", 50);
  const latencyMs = numberParam(input, "activityLatencyMs", 2);
  const payloadBytes = numberParam(input, "payloadBytes", 128);

  for (let step = 0; step < steps; step += 1) {
    await defaultActivities.mockRpc({
      stepName: `deep-step-${step}`,
      latencyMs,
      payloadBytes
    });
  }

  return result(input, {
    completedSteps: steps,
    activityCalls: steps,
    timerCount: 0,
    retryableFailures: 0
  });
}

export async function longRunningWorkflow(input: WorkflowInput): Promise<WorkflowResult> {
  const preWaitSteps = numberParam(input, "preWaitSteps", 2);
  const waitMs = numberParam(input, "waitMs", 5000);
  const postWaitSteps = numberParam(input, "postWaitSteps", 2);
  const latencyMs = numberParam(input, "activityLatencyMs", 5);

  for (let step = 0; step < preWaitSteps; step += 1) {
    await defaultActivities.mockRpc({ stepName: `long-pre-${step}`, latencyMs });
  }

  await sleep(waitMs);

  for (let step = 0; step < postWaitSteps; step += 1) {
    await defaultActivities.mockRpc({ stepName: `long-post-${step}`, latencyMs });
  }

  return result(input, {
    completedSteps: preWaitSteps + postWaitSteps,
    activityCalls: preWaitSteps + postWaitSteps,
    timerCount: 1,
    retryableFailures: 0
  });
}

export async function retryHeavyWorkflow(input: WorkflowInput): Promise<WorkflowResult> {
  const steps = numberParam(input, "steps", 5);
  const latencyMs = numberParam(input, "activityLatencyMs", 5);
  const failureRatePercent = numberParam(input, "failureRatePercent", 20);
  const maxAttempts = numberParam(input, "maxAttempts", 5);
  const retryActivities = proxyActivities<typeof activities>({
    startToCloseTimeout: "30 seconds",
    retry: {
      initialInterval: "100 milliseconds",
      backoffCoefficient: 2,
      maximumInterval: "5 seconds",
      maximumAttempts: maxAttempts
    }
  });

  for (let step = 0; step < steps; step += 1) {
    await retryActivities.mockRpc({
      stepName: `retry-step-${step}`,
      latencyMs,
      failureRatePercent
    });
  }

  return result(input, {
    completedSteps: steps,
    activityCalls: steps,
    timerCount: 0,
    retryableFailures: 0
  });
}

export async function timerIntensiveWorkflow(input: WorkflowInput): Promise<WorkflowResult> {
  const timers = numberParam(input, "timers", 10);
  const timerMs = numberParam(input, "timerMs", 1000);

  for (let timer = 0; timer < timers; timer += 1) {
    await sleep(timerMs);
  }

  return result(input, {
    completedSteps: timers,
    activityCalls: 0,
    timerCount: timers,
    retryableFailures: 0
  });
}

export async function failureRecoveryWorkflow(input: WorkflowInput): Promise<WorkflowResult> {
  const stepsBeforeWait = numberParam(input, "stepsBeforeWait", 2);
  const waitMs = numberParam(input, "waitMs", 30000);
  const stepsAfterWait = numberParam(input, "stepsAfterWait", 3);
  const latencyMs = numberParam(input, "activityLatencyMs", 10);

  for (let step = 0; step < stepsBeforeWait; step += 1) {
    await defaultActivities.mockRpc({ stepName: `recovery-before-${step}`, latencyMs });
  }

  await sleep(waitMs);

  for (let step = 0; step < stepsAfterWait; step += 1) {
    await defaultActivities.mockRpc({ stepName: `recovery-after-${step}`, latencyMs });
  }

  return result(input, {
    completedSteps: stepsBeforeWait + stepsAfterWait,
    activityCalls: stepsBeforeWait + stepsAfterWait,
    timerCount: 1,
    retryableFailures: 0
  });
}
