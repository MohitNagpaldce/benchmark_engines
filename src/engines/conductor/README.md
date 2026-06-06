# Conductor Adapter Plan

This is the next engine target after Temporal.

## Local Setup

The repository includes `docker-compose.conductor.yml` using the current Conductor OSS image:

```bash
npm run conductor:up
```

Expected local API:

- Conductor API: `http://localhost:8080/api`

Docker is not installed in this Codex environment, so this has not been started here.

## Local Run Commands

Start Conductor:

```bash
npm run conductor:up
```

Start the external task worker:

```bash
npm run conductor:worker
```

Run a benchmark:

```bash
npm run conductor:bench -- --problem low-latency --total 100 --concurrency 10
```

The Conductor runner emits the same JSON result schema and Prometheus benchmark metrics as the Temporal runner.

## Adapter Shape

The Conductor implementation reuses the shared problem definitions in `src/problems.ts`.

Implemented pieces:

- Dynamic workflow definitions for each benchmark problem.
- External task worker for `benchmark_mock_rpc` and `benchmark_retry_rpc`.
- Workflow execution through the Conductor REST API.
- Normalized `BenchmarkResultFile` JSON output.
- Shared benchmark Prometheus metrics.

The adapter uses dynamic workflow start requests instead of pre-registering static workflow definitions. This keeps the benchmark parameters colocated with the run and avoids stale metadata between runs.

## Problem Mapping

### Low Latency

Use a short workflow with 3 to 5 `SIMPLE` tasks:

- validate request
- authorize payment
- check inventory
- confirm transaction
- return response

### High Throughput

Use a `FORK_JOIN` workflow with configurable fan-out. Each fork branch should execute a mock RPC task, followed by a join and aggregate task.

### Deep Sequential

Generate a workflow definition with `N` sequential `SIMPLE` tasks, starting with 50 steps.

### Long Running

Use `WAIT`, `HUMAN`, or timer-equivalent behavior where supported by the chosen OSS version. If the local OSS image has limitations, model this with a delayed task worker and document the difference.

### Retry Heavy

Use Conductor task retry configuration with controlled worker-side failure injection.

### Timer Intensive

Use wait/timer tasks if available. Otherwise, use delayed task completion and clearly mark it as a model approximation.

### Failure Recovery

Start active workflows, stop task workers, restart them, then measure completion and duplicate execution behavior.

## Next Implementation Steps

1. Run the adapter against a live Conductor server.
2. Validate the OSS image endpoint paths and UI behavior.
3. Tune Conductor worker polling count and benchmark concurrency.
4. Add controlled worker-stop/restart automation for the failure-recovery scenario.
5. Generate the Conductor performance report next to the Temporal report.
