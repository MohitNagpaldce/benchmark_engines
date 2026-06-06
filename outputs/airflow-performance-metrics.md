# Airflow Performance Metrics

Run timestamp: 2026-06-06 PDT

Environment:

- Apache Airflow image: `apache/airflow:2.10.4`
- Executor: `LocalExecutor`
- Metadata DB: PostgreSQL 16 Alpine
- Runtime: Docker through Colima
- Airflow API: `http://localhost:8081/api/v1`
- DAGs: `airflow/dags/benchmark_workloads.py`

Notes:

- These are local development-container pilot results from one machine, not production capacity numbers.
- Airflow is a DAG scheduler/executor, not a request/response workflow engine. These numbers mainly reflect DAG run scheduling, task instance scheduling, executor throughput, and Python task execution.
- Totals are intentionally smaller than the Temporal run because Airflow DAG/task scheduling overhead is much higher for these workflow-style cases.
- Timer-style tests use Python sleep tasks. Those occupy executor slots and are not equivalent to Temporal durable timers or Conductor WAIT tasks.
- `failure-recovery` is a baseline long-wait DAG shape. It does not yet inject an actual scheduler/worker crash during the run.

## Summary

| Problem | Total | Succeeded | Failed | Duration ms | Workflows/sec | P50 ms | P95 ms | P99 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| low-latency | 5 | 5 | 0 | 6,153 | 0.81 | 5,644 | 6,152 | 6,152 |
| high-throughput | 3 | 3 | 0 | 33,076 | 0.09 | 32,267 | 33,076 | 33,076 |
| deep-sequential | 3 | 3 | 0 | 50,526 | 0.06 | 50,495 | 50,526 | 50,526 |
| long-running | 3 | 3 | 0 | 11,352 | 0.26 | 11,312 | 11,351 | 11,351 |
| retry-heavy | 3 | 3 | 0 | 8,429 | 0.36 | 8,427 | 8,428 | 8,428 |
| timer-intensive | 3 | 3 | 0 | 20,152 | 0.15 | 20,142 | 20,151 | 20,151 |
| failure-recovery | 3 | 3 | 0 | 36,628 | 0.08 | 36,612 | 36,626 | 36,626 |

## Observations

- All seven Airflow pilot workloads completed with 100% final DAG-run success.
- Airflow showed much higher latency than Temporal for workflow-style cases, especially sequential and fan-out task graphs.
- `high-throughput` triggered 3 DAG runs with 100 parallel Python tasks each and completed around 33 seconds.
- `deep-sequential` triggered 3 DAG runs with 50 ordered task instances each and completed around 50.5 seconds.
- `long-running`, `timer-intensive`, and `failure-recovery` are dominated by sleep/wait tasks plus scheduler overhead.
- `retry-heavy` completed successfully with Airflow task retries enabled.

## Raw Result Files

- `results/airflow-low-latency-152c1511-b4ab-4e45-937b-326925fee621.json`
- `results/airflow-high-throughput-4dd0ff8e-6bdf-44ca-826d-7c0fb09ac30a.json`
- `results/airflow-deep-sequential-0dcdcad0-867a-4e83-a27b-0f0b4402e788.json`
- `results/airflow-long-running-1830263d-6f46-4e48-b672-8c2584805135.json`
- `results/airflow-retry-heavy-93a0a42c-a3ea-4adf-a317-a9af9a1a2f1e.json`
- `results/airflow-timer-intensive-6cf9e42b-f41c-4978-9665-739e50780b29.json`
- `results/airflow-failure-recovery-2618bf66-2f8a-441c-a36d-621e131cca8c.json`

## Implementation Notes

- DAG definitions are implemented in `airflow/dags/benchmark_workloads.py`.
- REST API calls are implemented in `src/engines/airflow/client.ts`.
- DAG ID and expected result mapping are implemented in `src/engines/airflow/definitions.ts`.
- The shared runner supports Airflow with `npm run airflow:bench -- --problem <problem>`.

