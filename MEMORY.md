# Project Memory

- [2026-06-22] (Mem-0001) The initial workshop platform uses Spark 4.1.2, Delta Lake 4.2.0, sparkMeasure 0.28.0, and MinIO; ClickHouse and Go are intentionally out of scope.
- [2026-06-22] (Mem-0002) MinIO is organized into `lakehouse`, `tests`, and `observability` buckets; lakehouse layers are prefixes inside the `lakehouse` bucket.
- [2026-06-22] (Mem-0003) The first integration test collects stage-level metrics and persists them as Delta under the `observability` bucket.
- [2026-06-22] (Mem-0004) Spark 4.1.2 remains functional despite a secondary SLF4J binding warning from the resolved dependency set; dependency pruning is deferred until it can be validated across Delta and S3A.
- [2026-06-22] (Mem-0005) Workshop workloads implement SparkExperiment; ExperimentRunner measures only workload(), while prepare, validation, metric persistence, and cleanup remain outside the measurement boundary.
- [2026-06-22] (Mem-0006) Named experiment configuration controls app identity, separate application/Spark log levels, stage/task collection, persistence, and named input/output artifacts.
