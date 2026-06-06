import http from "node:http";
import {
  collectDefaultMetrics,
  Counter,
  Gauge,
  Histogram,
  Registry
} from "prom-client";
import type { BenchmarkResultFile, BenchmarkRunOptions, BenchmarkSample } from "./common/types";

const registry = new Registry();

collectDefaultMetrics({
  register: registry,
  prefix: "benchmark_process_"
});

const workflowLatency = new Histogram({
  name: "benchmark_workflow_latency_ms",
  help: "End-to-end workflow latency measured by the benchmark runner.",
  labelNames: ["engine", "problem", "run_id", "status"],
  buckets: [10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000, 60000],
  registers: [registry]
});

const workflowCompleted = new Counter({
  name: "benchmark_workflows_completed_total",
  help: "Total workflows completed by status.",
  labelNames: ["engine", "problem", "run_id", "status"],
  registers: [registry]
});

const workflowStarted = new Counter({
  name: "benchmark_workflows_started_total",
  help: "Total workflows started by the benchmark runner.",
  labelNames: ["engine", "problem", "run_id"],
  registers: [registry]
});

const runInfo = new Gauge({
  name: "benchmark_run_info",
  help: "Benchmark run metadata. Value is always 1 for the active or last run.",
  labelNames: ["engine", "problem", "run_id", "total", "concurrency"],
  registers: [registry]
});

const runDuration = new Gauge({
  name: "benchmark_run_duration_ms",
  help: "Total benchmark run duration in milliseconds.",
  labelNames: ["engine", "problem", "run_id"],
  registers: [registry]
});

const runThroughput = new Gauge({
  name: "benchmark_workflows_per_second",
  help: "Benchmark-level workflow throughput.",
  labelNames: ["engine", "problem", "run_id"],
  registers: [registry]
});

const activityCalls = new Counter({
  name: "benchmark_activity_calls_total",
  help: "Total mock activity calls reported by workflow results.",
  labelNames: ["engine", "problem", "run_id"],
  registers: [registry]
});

const timersCompleted = new Counter({
  name: "benchmark_timers_completed_total",
  help: "Total workflow timers reported by workflow results.",
  labelNames: ["engine", "problem", "run_id"],
  registers: [registry]
});

export function startMetricsServer(port: number): http.Server {
  const server = http.createServer(async (request, response) => {
    if (request.url !== "/metrics") {
      response.statusCode = 404;
      response.end("not found\n");
      return;
    }

    response.setHeader("Content-Type", registry.contentType);
    response.end(await registry.metrics());
  });

  server.listen(port, () => {
    console.log(`Benchmark metrics listening on http://localhost:${port}/metrics`);
  });

  return server;
}

export function recordWorkflowStarted(options: BenchmarkRunOptions, runId: string): void {
  workflowStarted.inc({
    engine: options.engine,
    problem: options.problem,
    run_id: runId
  });
}

export function recordWorkflowSample(options: BenchmarkRunOptions, runId: string, sample: BenchmarkSample): void {
  const status = sample.ok ? "success" : "failure";
  const labels = {
    engine: options.engine,
    problem: options.problem,
    run_id: runId,
    status
  };

  workflowLatency.observe(labels, sample.latencyMs);
  workflowCompleted.inc(labels);

  if (sample.result) {
    activityCalls.inc(
      {
        engine: options.engine,
        problem: options.problem,
        run_id: runId
      },
      sample.result.activityCalls
    );
    timersCompleted.inc(
      {
        engine: options.engine,
        problem: options.problem,
        run_id: runId
      },
      sample.result.timerCount
    );
  }
}

export function recordBenchmarkResult(result: BenchmarkResultFile): void {
  runInfo.set(
    {
      engine: result.engine,
      problem: result.problem.id,
      run_id: result.runId,
      total: String(result.options.total),
      concurrency: String(result.options.concurrency)
    },
    1
  );
  runDuration.set(
    {
      engine: result.engine,
      problem: result.problem.id,
      run_id: result.runId
    },
    result.summary.durationMs
  );
  runThroughput.set(
    {
      engine: result.engine,
      problem: result.problem.id,
      run_id: result.runId
    },
    result.summary.workflowsPerSecond
  );
}
