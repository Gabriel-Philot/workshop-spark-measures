# Project Memory

- [2026-06-22] (Mem-0001) The initial workshop platform uses Spark 4.1.2, Delta Lake 4.2.0, sparkMeasure 0.28.0, and MinIO; ClickHouse and Go are intentionally out of scope.
- [2026-06-22] (Mem-0002) MinIO is organized into `lakehouse`, `tests`, and `observability` buckets; lakehouse layers are prefixes inside the `lakehouse` bucket.
- [2026-06-22] (Mem-0003) The first integration test collects stage-level metrics and persists them as Delta under the `observability` bucket.
- [2026-06-22] (Mem-0004) Spark 4.1.2 remains functional despite a secondary SLF4J binding warning from the resolved dependency set; dependency pruning is deferred until it can be validated across Delta and S3A.
- [2026-06-22] (Mem-0005) Workshop workloads implement SparkExperiment; ExperimentRunner measures only workload(), while prepare, validation, metric persistence, and cleanup remain outside the measurement boundary.
- [2026-06-22] (Mem-0006) Named experiment configuration controls app identity, separate application/Spark log levels, stage/task collection, persistence, and named input/output artifacts.
- [2026-06-23] (Mem-0007) Data generator research recommends a Spark-native one-shot Docker generator as the first implementation path, with Polars/delta-rs and dbldatagen kept as benchmark candidates; generated data should be Delta in bronze and generator manifests should be written to observability.
- [2026-06-23] (Mem-0008) Generator design is now schema-first and relationship-first: own the YAML contract and validation locally, POC dbldatagen for Spark-native relational materialization, fallback to internal Spark expressions if needed, and keep Polars/delta-rs as secondary benchmark tooling.
- [2026-06-23] (Mem-0009) Implemented the first schema-first retail data generator slice: Spark-native materialization writes related vendors/products/customers/sales Delta tables to bronze, validates FK coverage and hot-vendor skew, emits a generator manifest to observability, and exposes `make generate`.
- [2026-06-24] (Mem-0010) Local Spark topology now defaults to two explicit workers and exposes `make compose-three-workers` for a WSL-friendly three-worker demo; readiness validates alive workers from Spark Master UI.
- [2026-06-25] (Mem-0011) Lab 0 source observability compares the same generated bronze-source profiling workload first without sparkMeasure and then with stage-level sparkMeasure metrics persisted to observability.
- [2026-06-26] (Mem-0012) Lab 0 submit output now uses terminal section dividers and native-mode Spark explain plans before the sparkMeasure run, making the workshop comparison easier to follow.
- [2026-06-26] (Mem-0013) Lab 0 is split into `source_inventory` for generated-source readiness and `sparkmeasure_presentation` for a focused Bronze-to-Silver sparkMeasure comparison with metric persistence disabled.
- [2026-06-26] (Mem-0014) Lab 0 source inventory reports physical source volume (`rows`, files, total bytes, and min/avg/max file bytes) and only keeps vendor imbalance as a short source characteristic note.
