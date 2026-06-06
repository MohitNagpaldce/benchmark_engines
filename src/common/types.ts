export type EngineName = "temporal" | "conductor";

export type ProblemId =
  | "low-latency"
  | "high-throughput"
  | "deep-sequential"
  | "long-running"
  | "retry-heavy"
  | "timer-intensive"
  | "failure-recovery";

export interface ProblemDefinition {
  id: ProblemId;
  name: string;
  objective: string;
  workflowType: string;
  defaultTotal: number;
  defaultConcurrency: number;
  params: Record<string, number | string | boolean>;
  metrics: string[];
}

export interface BenchmarkRunOptions {
  engine: EngineName;
  problem: ProblemId;
  total: number;
  concurrency: number;
  temporalAddress: string;
  taskQueue: string;
  namespace: string;
  resultsDir: string;
}

export interface WorkflowInput {
  runId: string;
  problemId: ProblemId;
  workflowIndex: number;
  params: Record<string, number | string | boolean>;
}

export interface WorkflowResult {
  problemId: ProblemId;
  workflowIndex: number;
  completedSteps: number;
  activityCalls: number;
  timerCount: number;
  retryableFailures: number;
}

export interface BenchmarkSample {
  workflowId: string;
  workflowIndex: number;
  ok: boolean;
  startedAt: string;
  completedAt: string;
  latencyMs: number;
  result?: WorkflowResult;
  error?: string;
}

export interface BenchmarkResultFile {
  runId: string;
  engine: EngineName;
  problem: ProblemDefinition;
  options: BenchmarkRunOptions;
  startedAt: string;
  completedAt: string;
  summary: {
    total: number;
    succeeded: number;
    failed: number;
    durationMs: number;
    workflowsPerSecond: number;
    p50LatencyMs: number;
    p95LatencyMs: number;
    p99LatencyMs: number;
  };
  samples: BenchmarkSample[];
}
