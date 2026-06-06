import type { ProblemDefinition, ProblemId } from "./common/types";

export const problemDefinitions: Record<ProblemId, ProblemDefinition> = {
  "low-latency": {
    id: "low-latency",
    name: "Low-Latency Workflow Execution",
    objective: "Measure baseline orchestration overhead for short user-request workflows.",
    workflowType: "lowLatencyWorkflow",
    defaultTotal: 100,
    defaultConcurrency: 10,
    params: {
      steps: 5,
      activityLatencyMs: 10,
      payloadBytes: 256
    },
    metrics: ["start_latency", "task_scheduling_latency", "completion_latency", "p50", "p95", "p99"]
  },
  "high-throughput": {
    id: "high-throughput",
    name: "High-Throughput Parallel Workflows",
    objective: "Measure fan-out/fan-in throughput and worker saturation behavior.",
    workflowType: "highThroughputWorkflow",
    defaultTotal: 20,
    defaultConcurrency: 5,
    params: {
      fanout: 100,
      activityLatencyMs: 5,
      payloadBytes: 128
    },
    metrics: ["workflows_per_second", "tasks_per_second", "worker_utilization", "queue_backlog"]
  },
  "deep-sequential": {
    id: "deep-sequential",
    name: "Deep Multi-Step Workflows",
    objective: "Measure scheduler overhead and history growth for many sequential steps.",
    workflowType: "deepSequentialWorkflow",
    defaultTotal: 20,
    defaultConcurrency: 5,
    params: {
      steps: 50,
      activityLatencyMs: 2,
      payloadBytes: 128
    },
    metrics: ["scheduler_overhead_per_step", "completion_latency", "event_history_growth"]
  },
  "long-running": {
    id: "long-running",
    name: "Long-Running Workflows",
    objective: "Measure durable timer and resume behavior for workflows with persisted waits.",
    workflowType: "longRunningWorkflow",
    defaultTotal: 50,
    defaultConcurrency: 10,
    params: {
      preWaitSteps: 2,
      waitMs: 5000,
      postWaitSteps: 2,
      activityLatencyMs: 5
    },
    metrics: ["storage_overhead", "event_history_growth", "resume_latency", "database_load"]
  },
  "retry-heavy": {
    id: "retry-heavy",
    name: "Retry-Heavy Workflows",
    objective: "Measure behavior under transient activity failures and retry storms.",
    workflowType: "retryHeavyWorkflow",
    defaultTotal: 50,
    defaultConcurrency: 10,
    params: {
      steps: 5,
      activityLatencyMs: 5,
      failureRatePercent: 20,
      maxAttempts: 5
    },
    metrics: ["retry_latency", "retry_queue_buildup", "duplicate_execution_risk", "stability"]
  },
  "timer-intensive": {
    id: "timer-intensive",
    name: "Timer-Intensive Workflows",
    objective: "Measure timer scheduling accuracy and queue scalability.",
    workflowType: "timerIntensiveWorkflow",
    defaultTotal: 100,
    defaultConcurrency: 25,
    params: {
      timers: 10,
      timerMs: 1000
    },
    metrics: ["timer_scheduling_accuracy", "timer_fire_delay", "timer_queue_scalability"]
  },
  "failure-recovery": {
    id: "failure-recovery",
    name: "Failure Recovery and Resilience",
    objective: "Measure correctness and recovery latency after worker crashes or restarts.",
    workflowType: "failureRecoveryWorkflow",
    defaultTotal: 100,
    defaultConcurrency: 25,
    params: {
      stepsBeforeWait: 2,
      waitMs: 30000,
      stepsAfterWait: 3,
      activityLatencyMs: 10
    },
    metrics: ["recovery_time", "replay_overhead", "duplicate_task_execution_risk", "consistency"]
  }
};

export function getProblemDefinition(id: ProblemId): ProblemDefinition {
  const definition = problemDefinitions[id];
  if (!definition) {
    throw new Error(`Unknown problem: ${id}`);
  }
  return definition;
}

export function listProblemIds(): ProblemId[] {
  return Object.keys(problemDefinitions) as ProblemId[];
}
