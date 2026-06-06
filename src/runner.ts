import { randomUUID } from "node:crypto";
import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { Command, InvalidArgumentError } from "commander";
import { Connection, Client } from "@temporalio/client";
import { ConductorClient, type ConductorWorkflowStatus } from "./engines/conductor/client";
import {
  buildConductorStartRequest,
  expectedConductorResult
} from "./engines/conductor/definitions";
import type {
  BenchmarkRunOptions,
  BenchmarkResultFile,
  BenchmarkSample,
  EngineName,
  ProblemId,
  WorkflowInput,
  WorkflowResult
} from "./common/types";
import {
  recordBenchmarkResult,
  recordWorkflowSample,
  recordWorkflowStarted,
  startMetricsServer
} from "./metrics";
import { getProblemDefinition, listProblemIds } from "./problems";

function parsePositiveInteger(value: string): number {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new InvalidArgumentError("must be a positive integer");
  }
  return parsed;
}

function parseNonNegativeInteger(value: string): number {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 0) {
    throw new InvalidArgumentError("must be a non-negative integer");
  }
  return parsed;
}

function percentile(values: number[], p: number): number {
  if (values.length === 0) {
    return 0;
  }
  const sorted = [...values].sort((a, b) => a - b);
  const index = Math.min(sorted.length - 1, Math.ceil((p / 100) * sorted.length) - 1);
  return Math.round(sorted[index]);
}

async function runWithConcurrency<T>(total: number, concurrency: number, task: (index: number) => Promise<T>): Promise<T[]> {
  const results: T[] = [];
  let nextIndex = 0;

  async function worker(): Promise<void> {
    for (;;) {
      const index = nextIndex;
      nextIndex += 1;
      if (index >= total) {
        return;
      }
      results[index] = await task(index);
    }
  }

  await Promise.all(Array.from({ length: Math.min(total, concurrency) }, () => worker()));
  return results;
}

async function runTemporalBenchmark(options: BenchmarkRunOptions): Promise<BenchmarkResultFile> {
  const problem = getProblemDefinition(options.problem);
  const runId = randomUUID();
  const connection = await Connection.connect({ address: options.temporalAddress });
  const client = new Client({ connection, namespace: options.namespace });
  const startedAtMs = Date.now();
  const startedAt = new Date(startedAtMs).toISOString();

  const samples = await runWithConcurrency(options.total, options.concurrency, async (workflowIndex) => {
    const workflowId = `${problem.id}-${runId}-${workflowIndex}`;
    const sampleStartedAtMs = Date.now();
    const sampleStartedAt = new Date(sampleStartedAtMs).toISOString();
    const input: WorkflowInput = {
      runId,
      problemId: problem.id,
      workflowIndex,
      params: problem.params
    };

    try {
      recordWorkflowStarted(options, runId);
      const handle = await client.workflow.start(problem.workflowType, {
        taskQueue: options.taskQueue,
        workflowId,
        args: [input]
      });
      const result = (await handle.result()) as WorkflowResult;
      const completedAtMs = Date.now();
      const sample = {
        workflowId,
        workflowIndex,
        ok: true,
        startedAt: sampleStartedAt,
        completedAt: new Date(completedAtMs).toISOString(),
        latencyMs: completedAtMs - sampleStartedAtMs,
        result
      } satisfies BenchmarkSample;
      recordWorkflowSample(options, runId, sample);
      return sample;
    } catch (error) {
      const completedAtMs = Date.now();
      const sample = {
        workflowId,
        workflowIndex,
        ok: false,
        startedAt: sampleStartedAt,
        completedAt: new Date(completedAtMs).toISOString(),
        latencyMs: completedAtMs - sampleStartedAtMs,
        error: error instanceof Error ? error.message : String(error)
      } satisfies BenchmarkSample;
      recordWorkflowSample(options, runId, sample);
      return sample;
    }
  });

  const completedAtMs = Date.now();
  const completedAt = new Date(completedAtMs).toISOString();
  const succeeded = samples.filter((sample) => sample.ok).length;
  const latencies = samples.map((sample) => sample.latencyMs);
  const durationMs = completedAtMs - startedAtMs;

  const result: BenchmarkResultFile = {
    runId,
    engine: "temporal",
    problem,
    options,
    startedAt,
    completedAt,
    summary: {
      total: options.total,
      succeeded,
      failed: options.total - succeeded,
      durationMs,
      workflowsPerSecond: Number((options.total / (durationMs / 1000)).toFixed(2)),
      p50LatencyMs: percentile(latencies, 50),
      p95LatencyMs: percentile(latencies, 95),
      p99LatencyMs: percentile(latencies, 99)
    },
    samples
  };
  recordBenchmarkResult(result);
  return result;
}

function isTerminalConductorStatus(status: ConductorWorkflowStatus): boolean {
  return status === "COMPLETED" || status === "FAILED" || status === "TERMINATED" || status === "TIMED_OUT";
}

async function waitForConductorWorkflow(
  client: ConductorClient,
  workflowId: string,
  timeoutMs: number
): Promise<ConductorWorkflowStatus> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const workflow = await client.getWorkflow(workflowId, false);
    if (isTerminalConductorStatus(workflow.status)) {
      return workflow.status;
    }
    if (Date.now() > deadline) {
      throw new Error(`Timed out waiting for Conductor workflow ${workflowId}`);
    }
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
}

async function runConductorBenchmark(options: BenchmarkRunOptions): Promise<BenchmarkResultFile> {
  const problem = getProblemDefinition(options.problem);
  const runId = randomUUID();
  const client = new ConductorClient(options.conductorUrl);
  await client.health();

  const startedAtMs = Date.now();
  const startedAt = new Date(startedAtMs).toISOString();

  const samples = await runWithConcurrency(options.total, options.concurrency, async (workflowIndex) => {
    const sampleStartedAtMs = Date.now();
    const sampleStartedAt = new Date(sampleStartedAtMs).toISOString();
    const input: WorkflowInput = {
      runId,
      problemId: problem.id,
      workflowIndex,
      params: problem.params
    };

    try {
      recordWorkflowStarted(options, runId);
      const workflowId = await client.startWorkflow(buildConductorStartRequest(problem, input));
      const status = await waitForConductorWorkflow(client, workflowId, 10 * 60 * 1000);
      const completedAtMs = Date.now();
      const ok = status === "COMPLETED";
      const sample = {
        workflowId,
        workflowIndex,
        ok,
        startedAt: sampleStartedAt,
        completedAt: new Date(completedAtMs).toISOString(),
        latencyMs: completedAtMs - sampleStartedAtMs,
        result: ok ? expectedConductorResult(input) : undefined,
        error: ok ? undefined : `Conductor workflow ended with status ${status}`
      } satisfies BenchmarkSample;
      recordWorkflowSample(options, runId, sample);
      return sample;
    } catch (error) {
      const completedAtMs = Date.now();
      const sample = {
        workflowId: `conductor-start-failed-${runId}-${workflowIndex}`,
        workflowIndex,
        ok: false,
        startedAt: sampleStartedAt,
        completedAt: new Date(completedAtMs).toISOString(),
        latencyMs: completedAtMs - sampleStartedAtMs,
        error: error instanceof Error ? error.message : String(error)
      } satisfies BenchmarkSample;
      recordWorkflowSample(options, runId, sample);
      return sample;
    }
  });

  const completedAtMs = Date.now();
  const completedAt = new Date(completedAtMs).toISOString();
  const succeeded = samples.filter((sample) => sample.ok).length;
  const latencies = samples.map((sample) => sample.latencyMs);
  const durationMs = completedAtMs - startedAtMs;

  const result: BenchmarkResultFile = {
    runId,
    engine: "conductor",
    problem,
    options,
    startedAt,
    completedAt,
    summary: {
      total: options.total,
      succeeded,
      failed: options.total - succeeded,
      durationMs,
      workflowsPerSecond: Number((options.total / (durationMs / 1000)).toFixed(2)),
      p50LatencyMs: percentile(latencies, 50),
      p95LatencyMs: percentile(latencies, 95),
      p99LatencyMs: percentile(latencies, 99)
    },
    samples
  };
  recordBenchmarkResult(result);
  return result;
}

async function main(): Promise<void> {
  const program = new Command()
    .option("--engine <engine>", "engine adapter to run", "temporal")
    .option("--problem <problem>", `problem to run: ${listProblemIds().join(", ")}`, "low-latency")
    .option("--total <count>", "total workflows to start")
    .option("--concurrency <count>", "concurrent workflow starts")
    .option("--temporal-address <address>", "Temporal frontend address", "localhost:7233")
    .option("--conductor-url <url>", "Conductor API base URL", "http://localhost:8080/api")
    .option("--namespace <namespace>", "Temporal namespace", "default")
    .option("--task-queue <taskQueue>", "Temporal task queue", "benchmark-temporal")
    .option("--results-dir <dir>", "directory for result files", "results")
    .option("--metrics-port <port>", "Prometheus metrics port for the benchmark runner", parsePositiveInteger, 9464)
    .option("--metrics-hold-ms <ms>", "keep metrics endpoint alive after completion for Prometheus scraping", parseNonNegativeInteger, 15000);

  program.parse();
  const parsed = program.opts<{
    engine: EngineName;
    problem: ProblemId;
    total?: string;
    concurrency?: string;
    temporalAddress: string;
    conductorUrl: string;
    namespace: string;
    taskQueue: string;
    resultsDir: string;
    metricsPort: number;
    metricsHoldMs: number;
  }>();

  if (parsed.engine !== "temporal" && parsed.engine !== "conductor") {
    throw new Error(`Engine ${parsed.engine} is not implemented.`);
  }

  const problem = getProblemDefinition(parsed.problem);
  const options: BenchmarkRunOptions = {
    engine: parsed.engine,
    problem: parsed.problem,
    total: parsed.total ? parsePositiveInteger(parsed.total) : problem.defaultTotal,
    concurrency: parsed.concurrency ? parsePositiveInteger(parsed.concurrency) : problem.defaultConcurrency,
    temporalAddress: parsed.temporalAddress,
    conductorUrl: parsed.conductorUrl,
    namespace: parsed.namespace,
    taskQueue: parsed.taskQueue,
    resultsDir: parsed.resultsDir,
    metricsPort: parsed.metricsPort,
    metricsHoldMs: parsed.metricsHoldMs
  };

  const metricsServer = startMetricsServer(options.metricsPort);
  const result = options.engine === "temporal" ? await runTemporalBenchmark(options) : await runConductorBenchmark(options);
  await mkdir(options.resultsDir, { recursive: true });
  const fileName = `${result.engine}-${result.problem.id}-${result.runId}.json`;
  const filePath = join(options.resultsDir, fileName);
  await writeFile(filePath, `${JSON.stringify(result, null, 2)}\n`, "utf8");

  console.log(JSON.stringify({ resultFile: filePath, summary: result.summary }, null, 2));
  if (options.metricsHoldMs > 0) {
    await new Promise((resolve) => setTimeout(resolve, options.metricsHoldMs));
  }
  metricsServer.close();
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
