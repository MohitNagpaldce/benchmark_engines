# 1000-Execution Simulation Metrics

Run date: 2026-06-06 PDT

## Methodology

This report is a deterministic bootstrap-style simulation using the latest raw benchmark result file for each engine and workload. For every engine/test case, 1000 workflow executions were sampled with replacement from the observed per-workflow samples, then average latency and percentile metrics were recalculated.

The `measuredWorkflowsPerSecond` column is the throughput from the source benchmark run, not a simulated value.

## Final Metrics

| Test case | Description | Engine | Source run size | Simulated executions | Avg latency | P50 | P95 | P99 | Measured workflows/sec |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| low-latency | Short request-style workflow with small sequential RPC-like steps. | temporal | 100 | 1000 | 547 ms | 550 ms | 561 ms | 565 ms | 18.25 |
| low-latency | Short request-style workflow with small sequential RPC-like steps. | conductor | 10 | 1000 | 11,436 ms | 11,385 ms | 11,591 ms | 11,591 ms | 0.86 |
| low-latency | Short request-style workflow with small sequential RPC-like steps. | airflow | 5 | 1000 | 5,841 ms | 5,644 ms | 6,152 ms | 6,152 ms | 0.81 |
| high-throughput | Fan-out/fan-in workflow with many parallel tasks. | temporal | 20 | 1000 | 837 ms | 452 ms | 4,900 ms | 4,900 ms | 3.21 |
| high-throughput | Fan-out/fan-in workflow with many parallel tasks. | conductor | 5 | 1000 | 31,549 ms | 30,433 ms | 36,849 ms | 36,849 ms | 0.14 |
| high-throughput | Fan-out/fan-in workflow with many parallel tasks. | airflow | 3 | 1000 | 32,503 ms | 32,267 ms | 33,076 ms | 33,076 ms | 0.09 |
| deep-sequential | Long ordered workflow with 50 sequential steps. | temporal | 20 | 1000 | 5,001 ms | 5,042 ms | 5,065 ms | 5,065 ms | 1.00 |
| deep-sequential | Long ordered workflow with 50 sequential steps. | conductor | 5 | 1000 | 108,117 ms | 108,116 ms | 108,120 ms | 108,120 ms | 0.05 |
| deep-sequential | Long ordered workflow with 50 sequential steps. | airflow | 3 | 1000 | 50,499 ms | 50,495 ms | 50,526 ms | 50,526 ms | 0.06 |
| long-running | Work, wait/timer, then resume more work. | temporal | 50 | 1000 | 5,358 ms | 5,433 ms | 5,516 ms | 5,519 ms | 1.83 |
| long-running | Work, wait/timer, then resume more work. | conductor | 5 | 1000 | 28,304 ms | 28,304 ms | 28,306 ms | 28,306 ms | 0.18 |
| long-running | Work, wait/timer, then resume more work. | airflow | 3 | 1000 | 11,323 ms | 11,312 ms | 11,351 ms | 11,351 ms | 0.26 |
| retry-heavy | Injected task failures with retry policy enabled. | temporal | 50 | 1000 | 2,227 ms | 1,556 ms | 4,549 ms | 5,498 ms | 4.04 |
| retry-heavy | Injected task failures with retry policy enabled. | conductor | 5 | 1000 | 11,704 ms | 12,265 ms | 14,270 ms | 14,270 ms | 0.35 |
| retry-heavy | Injected task failures with retry policy enabled. | airflow | 3 | 1000 | 8,426 ms | 8,427 ms | 8,428 ms | 8,428 ms | 0.36 |
| timer-intensive | Ten sequential timer/wait steps. | temporal | 100 | 1000 | 10,456 ms | 10,546 ms | 10,574 ms | 10,574 ms | 2.37 |
| timer-intensive | Ten sequential timer/wait steps. | conductor | 5 | 1000 | 24,734 ms | 24,778 ms | 25,543 ms | 25,543 ms | 0.20 |
| timer-intensive | Ten sequential timer/wait steps. | airflow | 3 | 1000 | 20,144 ms | 20,142 ms | 20,151 ms | 20,151 ms | 0.15 |
| failure-recovery | Baseline recovery-shaped workflow: work, long wait, resume work; no crash injection yet. | temporal | 100 | 1000 | 30,553 ms | 30,570 ms | 30,629 ms | 30,633 ms | 0.82 |
| failure-recovery | Baseline recovery-shaped workflow: work, long wait, resume work; no crash injection yet. | conductor | 5 | 1000 | 39,830 ms | 39,830 ms | 39,831 ms | 39,831 ms | 0.13 |
| failure-recovery | Baseline recovery-shaped workflow: work, long wait, resume work; no crash injection yet. | airflow | 3 | 1000 | 36,612 ms | 36,612 ms | 36,626 ms | 36,626 ms | 0.08 |

## Source Result Files

- temporal / low-latency: `results/temporal-low-latency-840e249f-618e-4dd5-9457-0f19ab6cbcd6.json`
- conductor / low-latency: `results/conductor-low-latency-08004e1c-ea7f-4182-8d05-cba1f8ce18f6.json`
- airflow / low-latency: `results/airflow-low-latency-152c1511-b4ab-4e45-937b-326925fee621.json`
- temporal / high-throughput: `results/temporal-high-throughput-52b30dcf-fb99-4099-b8e0-6b290735b2b1.json`
- conductor / high-throughput: `results/conductor-high-throughput-9d340130-4867-4ea0-a204-8f74a421ddd5.json`
- airflow / high-throughput: `results/airflow-high-throughput-4dd0ff8e-6bdf-44ca-826d-7c0fb09ac30a.json`
- temporal / deep-sequential: `results/temporal-deep-sequential-5053c254-f180-4794-9699-06d483df188c.json`
- conductor / deep-sequential: `results/conductor-deep-sequential-92bac01e-44bd-4ea1-a191-2bccb0761612.json`
- airflow / deep-sequential: `results/airflow-deep-sequential-0dcdcad0-867a-4e83-a27b-0f0b4402e788.json`
- temporal / long-running: `results/temporal-long-running-1512ec97-e4cc-440a-9773-627ffbf51c29.json`
- conductor / long-running: `results/conductor-long-running-d89c58eb-bd4f-40e6-93de-bc3f54886dcd.json`
- airflow / long-running: `results/airflow-long-running-1830263d-6f46-4e48-b672-8c2584805135.json`
- temporal / retry-heavy: `results/temporal-retry-heavy-0c16d9b3-8e0b-460d-a148-18a48dadd733.json`
- conductor / retry-heavy: `results/conductor-retry-heavy-f29d31ec-8bde-40f2-99d5-a3b50cd8642a.json`
- airflow / retry-heavy: `results/airflow-retry-heavy-93a0a42c-a3ea-4adf-a317-a9af9a1a2f1e.json`
- temporal / timer-intensive: `results/temporal-timer-intensive-6417fc3e-d2f5-4dbe-b125-d53cf7f7a8b6.json`
- conductor / timer-intensive: `results/conductor-timer-intensive-129915cb-ad00-4332-8f41-e2ffb32fe778.json`
- airflow / timer-intensive: `results/airflow-timer-intensive-6cf9e42b-f41c-4978-9665-739e50780b29.json`
- temporal / failure-recovery: `results/temporal-failure-recovery-14dbce9b-c5a1-4236-bfcb-0f14a455ee50.json`
- conductor / failure-recovery: `results/conductor-failure-recovery-e591db2b-514e-4e2e-b787-7adb1580769f.json`
- airflow / failure-recovery: `results/airflow-failure-recovery-2618bf66-2f8a-441c-a36d-621e131cca8c.json`
