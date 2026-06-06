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

Start a worker in one terminal:

```bash
npm run temporal:worker
```

Run a benchmark in another terminal:

```bash
npm run temporal:bench -- --problem low-latency --total 100 --concurrency 10
```

Results are written as JSON files under `results/`.

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
