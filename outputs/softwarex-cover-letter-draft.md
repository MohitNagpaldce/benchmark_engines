# Cover Letter — SoftwareX Submission

Dear SoftwareX Editors,

Please find enclosed our manuscript, "A Reproducible Benchmark Harness for Comparing Workflow Orchestration Engines: Temporal, Conductor, and Apache Airflow," submitted as an Original Software Publication to SoftwareX.

The submitted software is an open-source benchmark harness that enables reproducible, workload-specific comparison of workflow orchestration engines. The framework supports Temporal, Conductor OSS, and Apache Airflow through engine-specific adapters sharing a common TypeScript CLI runner, seven parameterized workload definitions, containerized PostgreSQL-backed infrastructure (Docker Compose), and a normalized JSON result schema. A Prometheus metrics endpoint supports live monitoring and Grafana integration.

The seven workloads — low-latency sequential execution, high-throughput fan-out, deep sequential task chains, long-running persisted waits, retry-heavy fault injection, timer-intensive scheduling, and failure-recovery baselines — cover the principal patterns that distinguish orchestration engine behavior in production. These workloads are defined engine-agnostically and translated to engine-specific API calls by each adapter, making the framework extensible to additional engines.

The repository is publicly available at:

  https://github.com/MohitNagpaldce/benchmark_engines

The code is released under the GPL-3.0-only license. A permanent archived version with a Zenodo DOI will be provided at acceptance. This manuscript has not been submitted elsewhere and is not under consideration at any other venue.

We believe this software will be valuable to researchers benchmarking orchestration systems, to engineers evaluating engine selection for production workloads, and to engine maintainers identifying performance regressions across releases.

Sincerely,

Mohit Nagpal
mht.nagpal@gmail.com
https://github.com/MohitNagpaldce/benchmark_engines
