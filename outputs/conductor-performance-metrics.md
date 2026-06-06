# Conductor Performance Metrics

Run timestamp: 2026-06-06 PDT

Environment:

- Conductor image: `conductoross/conductor:latest`
- Runtime: Docker through Colima
- Docker CLI: 29.5.3
- Docker Compose: 5.1.4
- Conductor API: `http://localhost:8080/api`
- Worker: TypeScript external task worker polling `benchmark_mock_rpc` and `benchmark_retry_rpc`

Notes:

- These are local development-container pilot results from one machine, not production capacity numbers.
- The Conductor adapter uses dynamic workflow definitions sent through the Start Workflow API.
- Totals are intentionally smaller than the Temporal run because the local Conductor container had materially higher orchestration latency.
- `failure-recovery` is still a baseline durable-wait workflow shape. It does not yet inject an actual worker crash during the run.

## Summary

| Problem | Total | Succeeded | Failed | Duration ms | Workflows/sec | P50 ms | P95 ms | P99 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| low-latency | 10 | 10 | 0 | 11,593 | 0.86 | 11,385 | 11,591 | 11,591 |
| high-throughput | 5 | 5 | 0 | 36,851 | 0.14 | 30,433 | 36,849 | 36,849 |
| deep-sequential | 5 | 5 | 0 | 108,121 | 0.05 | 108,116 | 108,120 | 108,120 |
| long-running | 5 | 5 | 0 | 28,307 | 0.18 | 28,304 | 28,306 | 28,306 |
| retry-heavy | 5 | 5 | 0 | 14,272 | 0.35 | 12,265 | 14,270 | 14,270 |
| timer-intensive | 5 | 5 | 0 | 25,544 | 0.20 | 24,778 | 25,543 | 25,543 |
| failure-recovery | 5 | 5 | 0 | 39,833 | 0.13 | 39,830 | 39,831 | 39,831 |

## Observations

- All seven pilot workloads completed with 100% final workflow success.
- Conductor showed substantially higher local orchestration latency than Temporal in this development setup.
- `deep-sequential` was the slowest case because each workflow schedules 50 ordered external tasks.
- `high-throughput` completed 500 forked tasks across 5 workflows, but tail latency reached about 36.8 seconds.
- `timer-intensive` used Conductor `WAIT` tasks and completed around 25 seconds for ten one-second timers.
- `long-running` and `failure-recovery` were dominated by Conductor `WAIT` task scheduling plus configured waits.
- `retry-heavy` completed successfully with the same 20% injected failure rate pattern used for Temporal.

## Raw Result Files

- `results/conductor-low-latency-08004e1c-ea7f-4182-8d05-cba1f8ce18f6.json`
- `results/conductor-high-throughput-9d340130-4867-4ea0-a204-8f74a421ddd5.json`
- `results/conductor-deep-sequential-92bac01e-44bd-4ea1-a191-2bccb0761612.json`
- `results/conductor-long-running-d89c58eb-bd4f-40e6-93de-bc3f54886dcd.json`
- `results/conductor-retry-heavy-f29d31ec-8bde-40f2-99d5-a3b50cd8642a.json`
- `results/conductor-timer-intensive-129915cb-ad00-4332-8f41-e2ffb32fe778.json`
- `results/conductor-failure-recovery-e591db2b-514e-4e2e-b787-7adb1580769f.json`

## Implementation Notes

- Workflow definitions are generated in `src/engines/conductor/definitions.ts`.
- External task polling is implemented in `src/engines/conductor/worker.ts`.
- REST calls are implemented in `src/engines/conductor/client.ts`.
- The shared runner supports Conductor with `npm run conductor:bench -- --problem <problem>`.

