# Temporal Performance Metrics

Run timestamp: 2026-06-05 17:31:53 PDT

Environment:

- Temporal CLI: 1.7.1
- Temporal server: 1.31.0
- Temporal UI: 2.49.1
- Node.js: v26.0.0
- Worker task queue: `benchmark-temporal`
- Namespace: `default`

Notes:

- These are local development-server results from one machine, not production capacity numbers.
- Docker was not installed, so Temporal was run through `temporal server start-dev`.
- The `failure-recovery` case below is the baseline durable-wait workflow shape. It does not yet inject an actual worker crash during the run.

## Summary

| Problem | Total | Succeeded | Failed | Duration ms | Workflows/sec | P50 ms | P95 ms | P99 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| low-latency | 100 | 100 | 0 | 5,480 | 18.25 | 550 | 559 | 565 |
| high-throughput | 20 | 20 | 0 | 6,223 | 3.21 | 451 | 1,293 | 4,900 |
| deep-sequential | 20 | 20 | 0 | 20,021 | 1.00 | 5,042 | 5,063 | 5,065 |
| long-running | 50 | 50 | 0 | 27,289 | 1.83 | 5,433 | 5,516 | 5,519 |
| retry-heavy | 50 | 50 | 0 | 12,385 | 4.04 | 1,549 | 4,549 | 5,498 |
| timer-intensive | 100 | 100 | 0 | 42,233 | 2.37 | 10,547 | 10,571 | 10,574 |
| failure-recovery | 100 | 100 | 0 | 122,332 | 0.82 | 30,569 | 30,628 | 30,633 |

## Observations

- All seven workloads completed with 100% final workflow success.
- `low-latency` finished at 18.25 workflows/sec with tight latency distribution around 550-565 ms.
- `high-throughput` showed a wide tail, with p99 at 4.9 seconds. This is expected because each workflow fans out to 100 mock activities.
- `deep-sequential` was stable around 5.05 seconds per workflow for 50 sequential activities.
- `long-running` was stable around 5.5 seconds, dominated by the configured 5-second durable timer.
- `retry-heavy` completed without final workflow failures, but p95 and p99 grew because the 20% injected activity failure rate triggered retries and backoff.
- `timer-intensive` was stable around 10.55 seconds, matching ten sequential one-second timers plus orchestration overhead.
- `failure-recovery` was stable around 30.6 seconds, matching the configured 30-second durable wait plus activity overhead.

## Raw Result Files

- `results/temporal-low-latency-840e249f-618e-4dd5-9457-0f19ab6cbcd6.json`
- `results/temporal-high-throughput-52b30dcf-fb99-4099-b8e0-6b290735b2b1.json`
- `results/temporal-deep-sequential-5053c254-f180-4794-9699-06d483df188c.json`
- `results/temporal-long-running-1512ec97-e4cc-440a-9773-627ffbf51c29.json`
- `results/temporal-retry-heavy-0c16d9b3-8e0b-460d-a148-18a48dadd733.json`
- `results/temporal-timer-intensive-6417fc3e-d2f5-4dbe-b125-d53cf7f7a8b6.json`
- `results/temporal-failure-recovery-14dbce9b-c5a1-4236-bfcb-0f14a455ee50.json`

## Live Metrics Endpoints Verified

- Temporal server metrics: `http://localhost:51045/metrics`
- Temporal TypeScript worker SDK metrics: `http://localhost:9465/metrics`

Worker SDK scrape included:

- `temporal_activity_execution_failed`
- `temporal_activity_execution_latency_milliseconds`

Temporal server scrape included:

- `ack_level_update`
- `acquire_shards_count`
- `acquire_shards_latency`

