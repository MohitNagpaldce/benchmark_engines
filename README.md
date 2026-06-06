# Workflow Orchestration Benchmarks

This repository is the implementation starting point for benchmarking workflow orchestration engines under realistic workload shapes.

The first implemented engine is Temporal. Netflix Conductor is the next adapter target.

## Temporal Local Setup

Temporal is configured through `docker-compose.temporal.yml` using the official `temporalio/temporal` development server image.

```bash
npm install
npm run temporal:up
```

Temporal endpoints:

- gRPC frontend: `localhost:7233`
- Web UI: `http://localhost:8233`
- Server Prometheus metrics: `http://localhost:8000/metrics`

Start a worker in one terminal:

```bash
npm run temporal:worker
```

The worker exposes Temporal TypeScript SDK metrics at:

- Worker SDK Prometheus metrics: `http://localhost:9465/metrics`

Run a benchmark in another terminal:

```bash
npm run temporal:bench -- --problem low-latency --total 100 --concurrency 10
```

Results are written as JSON files under `results/`.

The benchmark runner also exposes Prometheus metrics while it is running:

- Benchmark runner metrics: `http://localhost:9464/metrics`

Short benchmark runs keep the metrics endpoint alive for 15 seconds after completion by default so Prometheus can scrape the final values. Override that with:

```bash
npm run temporal:bench -- --problem low-latency --metrics-hold-ms 60000
```

## Grafana and Prometheus

Grafana and Prometheus are configured through Docker Compose. Start Temporal plus observability:

```bash
npm run stack:up
```

Or start only Prometheus and Grafana after Temporal is already running:

```bash
npm run observability:up
```

Endpoints:

- Grafana: `http://localhost:3000`
- Prometheus: `http://localhost:9090`
- Prometheus targets: `http://localhost:9090/targets`

Grafana login:

- Username: `admin`
- Password: `admin`

Provisioned dashboard:

- `Workflow Benchmark Overview`

Prometheus scrapes:

- Temporal server metrics from `temporal:8000`
- Benchmark runner metrics from `host.docker.internal:9464`
- Temporal TypeScript worker SDK metrics from `host.docker.internal:9465`

## Temporal Output Metrics

Each Temporal benchmark run writes a JSON result file and emits Prometheus metrics.

JSON summary fields:

- `total`: workflows requested
- `succeeded`: workflows completed successfully
- `failed`: workflows that failed
- `durationMs`: benchmark wall-clock duration
- `workflowsPerSecond`: completed benchmark throughput
- `p50LatencyMs`: median end-to-end workflow latency
- `p95LatencyMs`: 95th percentile end-to-end workflow latency
- `p99LatencyMs`: 99th percentile end-to-end workflow latency

JSON per-workflow sample fields:

- `workflowId`
- `workflowIndex`
- `ok`
- `startedAt`
- `completedAt`
- `latencyMs`
- `result.completedSteps`
- `result.activityCalls`
- `result.timerCount`
- `error`, when failed

Prometheus benchmark metrics:

- `benchmark_workflows_started_total`
- `benchmark_workflows_completed_total`
- `benchmark_workflow_latency_ms`
- `benchmark_workflows_per_second`
- `benchmark_run_duration_ms`
- `benchmark_run_info`
- `benchmark_activity_calls_total`
- `benchmark_timers_completed_total`
- `benchmark_process_*`

Temporal worker SDK metrics are exposed with the `temporal_` prefix from the TypeScript SDK runtime. Temporal server metrics are exposed by the local Temporal server on port `8000`.

## Implemented Workload Definitions

- `low-latency`: short user-facing orchestration flow
- `high-throughput`: fan-out/fan-in workload
- `deep-sequential`: 50+ step workflow depth test
- `long-running`: durable timer and resume test
- `retry-heavy`: transient service failure and retry behavior
- `timer-intensive`: timer scheduling accuracy test
- `failure-recovery`: active workflow recovery after worker restart

## Current Limitation

Docker is required to run Temporal locally. This Codex environment does not currently have `docker` installed, so the implementation can be type-checked here but the Temporal server cannot be started from this session.
