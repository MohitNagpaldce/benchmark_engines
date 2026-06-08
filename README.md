# Workflow Orchestration Benchmarks

This repository contains an open-source benchmark harness for comparing workflow orchestration engines under representative distributed-system workload shapes.

Implemented engines:

- Temporal, through the Temporal TypeScript SDK and local development server.
- Conductor OSS, through REST workflow definitions and an external TypeScript task worker.
- Apache Airflow, through static DAGs and the stable REST API.

The harness normalizes result files across engines so each run reports workflow success, wall-clock duration, throughput, P50/P95/P99 latency, activity/task counts, and timer counts where applicable.

## Repository Layout

```text
src/
  engines/
    temporal/     Temporal workflows, activities, and worker
    conductor/    Conductor REST client, workflow definitions, and worker
    airflow/      Airflow REST client and benchmark definitions
  problems.ts     Shared workload definitions
  runner.ts       Benchmark runner CLI
  metrics.ts      Prometheus metric exports
airflow/dags/     Airflow DAG definitions
observability/    Prometheus and Grafana provisioning
outputs/          Human-readable benchmark reports
```

## Quick Start

Install Node dependencies:

```bash
npm install
```

Type-check the project:

```bash
npm run typecheck
```

Start the desired engine stack, start any required worker, then run the benchmark command for that engine. Result JSON files are written under `results/`.

## Workloads

The same seven workload definitions are used across supported engines:

| Workload | Purpose |
| --- | --- |
| `low-latency` | Baseline request-style orchestration with short sequential RPC-like steps. |
| `high-throughput` | Fan-out/fan-in workload with many parallel tasks and an aggregate step. |
| `deep-sequential` | Long ordered workflow with 50 sequential steps. |
| `long-running` | Work, durable wait/timer, then resumed work. |
| `retry-heavy` | Transient task failures with retry policy enabled. |
| `timer-intensive` | Ten sequential timer/wait steps. |
| `failure-recovery` | Recovery-shaped baseline with a long wait and post-wait continuation; crash injection is not yet included. |

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

## Conductor Local Setup

Conductor is configured through `docker-compose.conductor.yml`.

```bash
npm run conductor:up
```

Conductor endpoint:

- API: `http://localhost:8080/api`

Start the Conductor external task worker in one terminal:

```bash
npm run conductor:worker
```

Run the same benchmark cases with the Conductor adapter:

```bash
npm run conductor:bench -- --problem low-latency --total 100 --concurrency 10
```

The Conductor adapter uses dynamic workflow definitions and polls `SIMPLE` tasks through the Conductor Task API. It writes the same JSON schema as Temporal, so the results can be compared directly.

## Airflow Local Setup

Airflow is configured through `docker-compose.airflow.yml` using Apache Airflow 2.10.4 with `LocalExecutor`.

```bash
npm run airflow:up
```

Airflow endpoints:

- UI: `http://localhost:8081`
- Stable REST API: `http://localhost:8081/api/v1`

Default credentials:

- Username: `airflow`
- Password: `airflow`

Run the same benchmark cases with the Airflow adapter:

```bash
npm run airflow:bench -- --problem low-latency --total 5 --concurrency 5
```

The Airflow adapter triggers DAG runs through the stable REST API and writes the same JSON result schema as Temporal and Conductor.

## Current Limitation

Docker is required to run Conductor locally through the provided Compose file. Temporal can also run through Docker, but the Temporal CLI dev server works without Docker.

The published reports under `outputs/` include local pilot measurements, historical bootstrap-style reports, and a live cross-engine benchmark archive generated on 2026-06-07. For publication-grade claims, prefer the live archive, run future suites on fixed hardware with equal run sizes where practical, and archive the raw `results/` files.

## Citation

Citation metadata is provided in `CITATION.cff`.

## License

This project is released under the GPL-3.0-only license. See `LICENSE`.
