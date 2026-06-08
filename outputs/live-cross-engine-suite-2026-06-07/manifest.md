# Live Cross-Engine Benchmark Suite - 2026-06-07

This archive contains live local benchmark result JSON files for Temporal, Conductor, and Airflow across the same seven workload definitions. The suite was run on 2026-06-07 in the local development environment.

## Environment

- Temporal CLI: 1.7.1
- Temporal server: 1.31.0
- Temporal UI: 2.49.1
- Conductor image: conductoross/conductor:latest
- Airflow image: apache/airflow:2.10.4
- Airflow executor: LocalExecutor
- Docker CLI: 29.5.3
- Docker Compose: 5.1.4
- Node.js benchmark harness: package version 1.0.0

## Summary

| Workload | Engine | Total | Succeeded | Failed | Workflows/sec | P50 ms | P95 ms | P99 ms | Raw file |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| low-latency | temporal | 100 | 100 | 0 | 17.31 | 552 | 678 | 727 | temporal/raw-results/temporal-low-latency-3a34d8a2-94c1-4ebf-b6dc-4bd04869da86.json |
| low-latency | conductor | 100 | 100 | 0 | 0.78 | 11,801 | 13,711 | 18,267 | conductor/raw-results/conductor-low-latency-79d55d13-f3df-43da-9f44-7062b9b59457.json |
| low-latency | airflow | 100 | 100 | 0 | 0.36 | 27,384 | 41,523 | 42,521 | airflow/raw-results/airflow-low-latency-c998cd4e-18c9-42ab-8095-c19ea0467a2a.json |
| high-throughput | temporal | 20 | 20 | 0 | 8.51 | 544 | 719 | 802 | temporal/raw-results/temporal-high-throughput-3eda4422-fa5c-4dee-8271-bf56f2052b8b.json |
| high-throughput | conductor | 20 | 20 | 0 | 0.15 | 32,286 | 41,940 | 62,113 | conductor/raw-results/conductor-high-throughput-a66ee510-f0f6-490e-940c-fac2bc370535.json |
| high-throughput | airflow | 20 | 20 | 0 | 0.01 | 566,047 | 604,577 | 604,840 | airflow/raw-results/airflow-high-throughput-097be4fe-d7e6-40ac-9ffb-7783721bef57.json |
| deep-sequential | temporal | 20 | 20 | 0 | 0.99 | 5,044 | 5,055 | 5,055 | temporal/raw-results/temporal-deep-sequential-3637e4f7-12d8-4a59-957f-0cd6576d7ebf.json |
| deep-sequential | conductor | 20 | 20 | 0 | 0.04 | 113,038 | 115,868 | 115,872 | conductor/raw-results/conductor-deep-sequential-4d40c1c4-9517-4fb3-bee4-e7f2af70a479.json |
| deep-sequential | airflow | 20 | 20 | 0 | 0.03 | 160,461 | 204,140 | 204,423 | airflow/raw-results/airflow-deep-sequential-471a269e-e3df-4919-b150-91c503e094f4.json |
| long-running | temporal | 50 | 50 | 0 | 1.83 | 5,424 | 5,534 | 5,540 | temporal/raw-results/temporal-long-running-fd4c5577-9764-45d1-9767-36643a7c304a.json |
| long-running | conductor | 50 | 50 | 0 | 0.64 | 15,724 | 17,965 | 18,145 | conductor/raw-results/conductor-long-running-1d424152-c0f9-4657-bd44-a2ed7f793236.json |
| long-running | airflow | 50 | 50 | 0 | 0.2 | 48,553 | 69,385 | 71,202 | airflow/raw-results/airflow-long-running-59c61ea1-a1f7-4883-806b-97787bf5093a.json |
| retry-heavy | temporal | 50 | 50 | 0 | 4.51 | 1,547 | 4,435 | 5,502 | temporal/raw-results/temporal-retry-heavy-442ba230-a9ad-45b0-867d-25f5486b1a63.json |
| retry-heavy | conductor | 50 | 49 | 1 | 0.61 | 14,308 | 24,018 | 34,049 | conductor/raw-results/conductor-retry-heavy-356b8876-6fd5-4b84-9bb8-a4f6f7d549f5.json |
| retry-heavy | airflow | 50 | 50 | 0 | 0.15 | 68,508 | 87,938 | 103,707 | airflow/raw-results/airflow-retry-heavy-588640d4-8caf-4dfb-b931-e34aa4f44152.json |
| timer-intensive | temporal | 100 | 100 | 0 | 2.37 | 10,548 | 10,566 | 10,619 | temporal/raw-results/temporal-timer-intensive-8e72c54f-861f-4278-983b-b76478417b1a.json |
| timer-intensive | conductor | 100 | 100 | 0 | 0.62 | 32,377 | 55,953 | 56,955 | conductor/raw-results/conductor-timer-intensive-106db1b2-2aa3-4de8-88e4-c943bf54b6ec.json |
| timer-intensive | airflow | 100 | 100 | 0 | 0.1 | 264,225 | 283,911 | 286,809 | airflow/raw-results/airflow-timer-intensive-1f0bb288-962f-48d7-9751-a69526a32d1e.json |
| failure-recovery | temporal | 100 | 100 | 0 | 0.81 | 30,626 | 30,898 | 30,909 | temporal/raw-results/temporal-failure-recovery-6149f6f3-9b2c-4a4a-95af-7278d5eb6fd0.json |
| failure-recovery | conductor | 100 | 100 | 0 | 0.55 | 42,000 | 49,208 | 52,216 | conductor/raw-results/conductor-failure-recovery-0ddda56a-a9b8-4c74-b80d-e66a31d033e5.json |
| failure-recovery | airflow | 100 | 100 | 0 | 0.15 | 148,861 | 258,499 | 276,889 | airflow/raw-results/airflow-failure-recovery-bf62385e-37a0-4348-9eff-a2cc7a5cc95a.json |

## Notes

- These are live local executions, not bootstrap-style simulations.
- The Conductor retry-heavy workload completed 49 of 50 workflows successfully in this run; that failure is preserved in the raw result file and summary.
- The failure-recovery workload remains a recovery-shaped baseline with durable wait/resume behavior; it does not inject an actual crash yet.
