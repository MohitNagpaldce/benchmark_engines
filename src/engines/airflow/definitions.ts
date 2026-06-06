import type { ProblemDefinition, WorkflowInput, WorkflowResult } from "../../common/types";

export function airflowDagId(problem: ProblemDefinition): string {
  return `benchmark_${problem.id.replaceAll("-", "_")}`;
}

function numberParam(input: WorkflowInput, name: string, fallback: number): number {
  const value = input.params[name];
  return typeof value === "number" ? value : fallback;
}

export function expectedAirflowResult(input: WorkflowInput): WorkflowResult {
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
