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

## Adapter Shape

The Conductor implementation should reuse the shared problem definitions in `src/problems.ts`.

Planned pieces:

- Register workflow definitions for each benchmark problem.
- Register task definitions for mock RPC tasks.
- Start workflow executions through the Conductor REST API.
- Run a polling worker loop for `SIMPLE` tasks.
- Normalize Conductor run results into the same `BenchmarkResultFile` schema used by Temporal.

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

1. Confirm the local Conductor image API paths and UI behavior.
2. Add a `ConductorClient` wrapper around REST calls.
3. Add task and workflow definition registration.
4. Implement the mock RPC task worker.
5. Add `--engine conductor` support in `src/runner.ts`.
6. Run parity tests for `low-latency` and `deep-sequential` before implementing fan-out and timer cases.
