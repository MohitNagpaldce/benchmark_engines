import { randomUUID } from "node:crypto";
import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { Command, InvalidArgumentError } from "commander";
import { Connection, Client } from "@temporalio/client";
import type {
  BenchmarkRunOptions,
  BenchmarkResultFile,
  BenchmarkSample,
  EngineName,
  ProblemId,
  WorkflowInput,
  WorkflowResult
} from "./common/types";
import { getProblemDefinition, listProblemIds } from "./problems";

function parsePositiveInteger(value: string): number {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new InvalidArgumentError("must be a positive integer");
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
      const handle = await client.workflow.start(problem.workflowType, {
        taskQueue: options.taskQueue,
        workflowId,
        args: [input]
      });
      const result = (await handle.result()) as WorkflowResult;
      const completedAtMs = Date.now();
      return {
        workflowId,
        workflowIndex,
        ok: true,
        startedAt: sampleStartedAt,
        completedAt: new Date(completedAtMs).toISOString(),
        latencyMs: completedAtMs - sampleStartedAtMs,
        result
      } satisfies BenchmarkSample;
    } catch (error) {
      const completedAtMs = Date.now();
      return {
        workflowId,
        workflowIndex,
        ok: false,
        startedAt: sampleStartedAt,
        completedAt: new Date(completedAtMs).toISOString(),
        latencyMs: completedAtMs - sampleStartedAtMs,
        error: error instanceof Error ? error.message : String(error)
      } satisfies BenchmarkSample;
    }
  });

  const completedAtMs = Date.now();
  const completedAt = new Date(completedAtMs).toISOString();
  const succeeded = samples.filter((sample) => sample.ok).length;
  const latencies = samples.map((sample) => sample.latencyMs);
  const durationMs = completedAtMs - startedAtMs;

  return {
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
}

async function main(): Promise<void> {
  const program = new Command()
    .option("--engine <engine>", "engine adapter to run", "temporal")
    .option("--problem <problem>", `problem to run: ${listProblemIds().join(", ")}`, "low-latency")
    .option("--total <count>", "total workflows to start")
    .option("--concurrency <count>", "concurrent workflow starts")
    .option("--temporal-address <address>", "Temporal frontend address", "localhost:7233")
    .option("--namespace <namespace>", "Temporal namespace", "default")
    .option("--task-queue <taskQueue>", "Temporal task queue", "benchmark-temporal")
    .option("--results-dir <dir>", "directory for result files", "results");

  program.parse();
  const parsed = program.opts<{
    engine: EngineName;
    problem: ProblemId;
    total?: string;
    concurrency?: string;
    temporalAddress: string;
    namespace: string;
    taskQueue: string;
    resultsDir: string;
  }>();

  if (parsed.engine !== "temporal") {
    throw new Error(`Engine ${parsed.engine} is not implemented yet. Next target: conductor.`);
  }

  const problem = getProblemDefinition(parsed.problem);
  const options: BenchmarkRunOptions = {
    engine: parsed.engine,
    problem: parsed.problem,
    total: parsed.total ? parsePositiveInteger(parsed.total) : problem.defaultTotal,
    concurrency: parsed.concurrency ? parsePositiveInteger(parsed.concurrency) : problem.defaultConcurrency,
    temporalAddress: parsed.temporalAddress,
    namespace: parsed.namespace,
    taskQueue: parsed.taskQueue,
    resultsDir: parsed.resultsDir
  };

  const result = await runTemporalBenchmark(options);
  await mkdir(options.resultsDir, { recursive: true });
  const fileName = `${result.engine}-${result.problem.id}-${result.runId}.json`;
  const filePath = join(options.resultsDir, fileName);
  await writeFile(filePath, `${JSON.stringify(result, null, 2)}\n`, "utf8");

  console.log(JSON.stringify({ resultFile: filePath, summary: result.summary }, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
