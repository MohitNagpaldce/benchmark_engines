import type { ProblemDefinition, WorkflowInput, WorkflowResult } from "../../common/types";

const MOCK_TASK = "benchmark_mock_rpc";
const RETRY_TASK = "benchmark_retry_rpc";

interface ConductorTaskDefinition {
  name: string;
  retryCount: number;
  retryLogic?: "FIXED" | "EXPONENTIAL_BACKOFF";
  retryDelaySeconds: number;
  timeoutSeconds: number;
  timeoutPolicy: "TIME_OUT_WF";
  responseTimeoutSeconds: number;
}

interface ConductorWorkflowTask {
  name: string;
  taskReferenceName: string;
  type: "SIMPLE" | "FORK_JOIN" | "JOIN" | "WAIT";
  inputParameters?: Record<string, unknown>;
  taskDefinition?: ConductorTaskDefinition;
  forkTasks?: ConductorWorkflowTask[][];
  joinOn?: string[];
}

interface ConductorWorkflowDef {
  ownerApp: string;
  ownerEmail: string;
  name: string;
  description: string;
  version: number;
  schemaVersion: 2;
  timeoutPolicy: "TIME_OUT_WF";
  timeoutSeconds: number;
  tasks: ConductorWorkflowTask[];
}

function numberParam(input: WorkflowInput, name: string, fallback: number): number {
  const value = input.params[name];
  return typeof value === "number" ? value : fallback;
}

function taskDefinition(taskName: string, maxAttempts = 1): ConductorTaskDefinition {
  return {
    name: taskName,
    retryCount: Math.max(0, maxAttempts - 1),
    retryLogic: "EXPONENTIAL_BACKOFF",
    retryDelaySeconds: 1,
    timeoutSeconds: 30,
    timeoutPolicy: "TIME_OUT_WF",
    responseTimeoutSeconds: 30
  };
}

function mockTask(ref: string, latencyMs: number, payloadBytes = 0): ConductorWorkflowTask {
  return {
    name: MOCK_TASK,
    taskReferenceName: ref,
    type: "SIMPLE",
    inputParameters: {
      stepName: ref,
      latencyMs,
      payloadBytes
    },
    taskDefinition: taskDefinition(MOCK_TASK)
  };
}

function retryTask(ref: string, latencyMs: number, failureRatePercent: number, maxAttempts: number): ConductorWorkflowTask {
  return {
    name: RETRY_TASK,
    taskReferenceName: ref,
    type: "SIMPLE",
    inputParameters: {
      stepName: ref,
      latencyMs,
      failureRatePercent
    },
    taskDefinition: taskDefinition(RETRY_TASK, maxAttempts)
  };
}

function waitTask(ref: string, waitMs: number): ConductorWorkflowTask {
  return {
    name: ref,
    taskReferenceName: ref,
    type: "WAIT",
    inputParameters: {
      duration: `${Math.max(1, Math.ceil(waitMs / 1000))} seconds`
    }
  };
}

function workflowDef(problem: ProblemDefinition, tasks: ConductorWorkflowTask[], timeoutSeconds = 600): ConductorWorkflowDef {
  return {
    ownerApp: "workflow-orchestration-benchmarks",
    ownerEmail: "benchmark@example.com",
    name: `benchmark_${problem.id.replaceAll("-", "_")}`,
    description: problem.objective,
    version: 1,
    schemaVersion: 2,
    timeoutPolicy: "TIME_OUT_WF",
    timeoutSeconds,
    tasks
  };
}

export function buildConductorStartRequest(problem: ProblemDefinition, input: WorkflowInput): Record<string, unknown> {
  const name = `benchmark_${problem.id.replaceAll("-", "_")}`;
  return {
    name,
    version: 1,
    correlationId: input.runId,
    input,
    workflowDef: buildWorkflowDef(problem, input)
  };
}

export function buildWorkflowDef(problem: ProblemDefinition, input: WorkflowInput): ConductorWorkflowDef {
  switch (problem.id) {
    case "low-latency": {
      const steps = numberParam(input, "steps", 5);
      const latencyMs = numberParam(input, "activityLatencyMs", 10);
      const payloadBytes = numberParam(input, "payloadBytes", 256);
      return workflowDef(
        problem,
        Array.from({ length: steps }, (_, index) => mockTask(`low_latency_${index}`, latencyMs, payloadBytes))
      );
    }
    case "high-throughput": {
      const fanout = numberParam(input, "fanout", 100);
      const latencyMs = numberParam(input, "activityLatencyMs", 5);
      const payloadBytes = numberParam(input, "payloadBytes", 128);
      const branchRefs = Array.from({ length: fanout }, (_, index) => `fanout_${index}`);
      return workflowDef(problem, [
        {
          name: "fanout",
          taskReferenceName: "fanout",
          type: "FORK_JOIN",
          forkTasks: branchRefs.map((ref) => [mockTask(ref, latencyMs, payloadBytes)])
        },
        {
          name: "join",
          taskReferenceName: "join",
          type: "JOIN",
          joinOn: branchRefs
        },
        mockTask("aggregate_results", latencyMs, payloadBytes)
      ]);
    }
    case "deep-sequential": {
      const steps = numberParam(input, "steps", 50);
      const latencyMs = numberParam(input, "activityLatencyMs", 2);
      const payloadBytes = numberParam(input, "payloadBytes", 128);
      return workflowDef(
        problem,
        Array.from({ length: steps }, (_, index) => mockTask(`deep_step_${index}`, latencyMs, payloadBytes))
      );
    }
    case "long-running": {
      const preWaitSteps = numberParam(input, "preWaitSteps", 2);
      const waitMs = numberParam(input, "waitMs", 5000);
      const postWaitSteps = numberParam(input, "postWaitSteps", 2);
      const latencyMs = numberParam(input, "activityLatencyMs", 5);
      return workflowDef(problem, [
        ...Array.from({ length: preWaitSteps }, (_, index) => mockTask(`long_pre_${index}`, latencyMs)),
        waitTask("long_wait", waitMs),
        ...Array.from({ length: postWaitSteps }, (_, index) => mockTask(`long_post_${index}`, latencyMs))
      ]);
    }
    case "retry-heavy": {
      const steps = numberParam(input, "steps", 5);
      const latencyMs = numberParam(input, "activityLatencyMs", 5);
      const failureRatePercent = numberParam(input, "failureRatePercent", 20);
      const maxAttempts = numberParam(input, "maxAttempts", 5);
      return workflowDef(
        problem,
        Array.from({ length: steps }, (_, index) =>
          retryTask(`retry_step_${index}`, latencyMs, failureRatePercent, maxAttempts)
        )
      );
    }
    case "timer-intensive": {
      const timers = numberParam(input, "timers", 10);
      const timerMs = numberParam(input, "timerMs", 1000);
      return workflowDef(
        problem,
        Array.from({ length: timers }, (_, index) => waitTask(`timer_${index}`, timerMs))
      );
    }
    case "failure-recovery": {
      const stepsBeforeWait = numberParam(input, "stepsBeforeWait", 2);
      const waitMs = numberParam(input, "waitMs", 30000);
      const stepsAfterWait = numberParam(input, "stepsAfterWait", 3);
      const latencyMs = numberParam(input, "activityLatencyMs", 10);
      return workflowDef(problem, [
        ...Array.from({ length: stepsBeforeWait }, (_, index) => mockTask(`recovery_before_${index}`, latencyMs)),
        waitTask("recovery_wait", waitMs),
        ...Array.from({ length: stepsAfterWait }, (_, index) => mockTask(`recovery_after_${index}`, latencyMs))
      ]);
    }
    default:
      throw new Error(`Unsupported Conductor problem: ${problem.id}`);
  }
}

export function expectedConductorResult(input: WorkflowInput): WorkflowResult {
  switch (input.problemId) {
    case "low-latency": {
      const steps = numberParam(input, "steps", 5);
      return { problemId: input.problemId, workflowIndex: input.workflowIndex, completedSteps: steps, activityCalls: steps, timerCount: 0, retryableFailures: 0 };
    }
    case "high-throughput": {
      const fanout = numberParam(input, "fanout", 100);
      return { problemId: input.problemId, workflowIndex: input.workflowIndex, completedSteps: fanout + 1, activityCalls: fanout + 1, timerCount: 0, retryableFailures: 0 };
    }
    case "deep-sequential": {
      const steps = numberParam(input, "steps", 50);
      return { problemId: input.problemId, workflowIndex: input.workflowIndex, completedSteps: steps, activityCalls: steps, timerCount: 0, retryableFailures: 0 };
    }
    case "long-running": {
      const preWaitSteps = numberParam(input, "preWaitSteps", 2);
      const postWaitSteps = numberParam(input, "postWaitSteps", 2);
      return { problemId: input.problemId, workflowIndex: input.workflowIndex, completedSteps: preWaitSteps + postWaitSteps, activityCalls: preWaitSteps + postWaitSteps, timerCount: 1, retryableFailures: 0 };
    }
    case "retry-heavy": {
      const steps = numberParam(input, "steps", 5);
      return { problemId: input.problemId, workflowIndex: input.workflowIndex, completedSteps: steps, activityCalls: steps, timerCount: 0, retryableFailures: 0 };
    }
    case "timer-intensive": {
      const timers = numberParam(input, "timers", 10);
      return { problemId: input.problemId, workflowIndex: input.workflowIndex, completedSteps: timers, activityCalls: 0, timerCount: timers, retryableFailures: 0 };
    }
    case "failure-recovery": {
      const stepsBeforeWait = numberParam(input, "stepsBeforeWait", 2);
      const stepsAfterWait = numberParam(input, "stepsAfterWait", 3);
      return { problemId: input.problemId, workflowIndex: input.workflowIndex, completedSteps: stepsBeforeWait + stepsAfterWait, activityCalls: stepsBeforeWait + stepsAfterWait, timerCount: 1, retryableFailures: 0 };
    }
  }
}

export const conductorTaskTypes = [MOCK_TASK, RETRY_TASK] as const;
