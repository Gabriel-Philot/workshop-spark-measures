<div align="center">

# 🏗️ Data Platform Tradeoffs

### A lightweight runtime contract designed for teaching Spark and sparkMeasure

`shared conventions` · `less boilerplate` · `controlled escape hatches`

</div>

> [!IMPORTANT]
> This repository contains infrastructure and reusable design patterns, but it
> does not attempt to present a production-grade data platform. The platform is
> teaching infrastructure: it keeps repeated concerns consistent so the Labs
> can focus on Spark behavior and sparkMeasure evidence.

---

## 🎯 Design intent

The platform exists to make different Spark workloads easy to build, submit,
observe, and compare without repeating session setup, storage paths, logging,
and sparkMeasure integration in every lesson.

| Design question | Workshop answer |
| --- | --- |
| What should remain consistent? | Configuration, SparkSession lifecycle, artifact IO, logging, collector setup, metadata, and cleanup. |
| What should remain visible? | The business transformation, diagnostic question, evidence, and limitations of each Lab. |
| What is the main tradeoff? | Accept a lightweight shared contract and some Lab-local duplication instead of building a generic framework. |
| When is the design sufficient? | When a new experiment is easy to implement and students can reach the Spark concept without infrastructure boilerplate. |

The shared contract provides enough structure to run many workloads against
named sources, persist business outputs and observability metadata, and resemble
a standard `spark-submit` data-processing flow. At the same time, each Lab keeps
its transformation and diagnostic question visible.

> [!NOTE]
> There is room for improvement. Those improvements were not required once the
> contract satisfied the workshop's teaching and development goals.

---

## 🧩 Shared execution contract

The common execution path is intentionally small:

```text
named YAML experiment
        │
        ▼
typed configuration ──► process-local SparkSession singleton
        │
        ▼
named input artifacts
        │
        ▼
extract ──► transform ──► load
        │
        ├──► named business output
        └──► optional StageMetrics or TaskMetrics
                         │
                         └──► optional metrics metadata output
        │
        ▼
validation ──► cleanup ──► SparkSession stop
```

Most of this behavior lives under `src/spark_workshop/`:

| Package | Workshop responsibility |
| --- | --- |
| `config/` | Loads named experiments, merges defaults, expands environment values, and validates typed settings. |
| `session/` | Creates and stops one process-local SparkSession with the shared Delta configuration. |
| `jobs/` | Exposes the readable `extract -> transform -> load -> validate` contract used by the main Lab scripts. |
| `runtime/` | Owns the measured workload boundary, collector lifecycle, validation, metric persistence, and cleanup. |
| `metrics/` | Adapts the sparkMeasure `StageMetrics` and `TaskMetrics` APIs and normalizes aggregate values. |
| `artifacts/` | Reads and writes named Delta or JSON artifacts without scattering physical paths through workload code. |
| `utils/` | Provides deterministic logs, classroom terminal blocks, and Spark job descriptions for History Server navigation. |

The exact lifecycle and measurement boundary are documented in
[Runtime Pattern](runtime-pattern.md). This document explains why that pattern
exists and where the workshop intentionally stops extending it.

---

## 🎓 Why this helps the classroom

The contract connects the project to familiar data-platform practices without
making platform engineering the subject of the class.

### The platform keeps

- normal `spark-submit` execution and SparkSession behavior;
- explicit Delta inputs and outputs;
- event logs available to Spark History Server;
- named applications, artifacts, and observability outputs;
- configurable `StageMetrics`, `TaskMetrics`, or disabled collection;
- logs and Spark job descriptions that help correlate executions.

### The Lab keeps

- the transformation students need to understand;
- the metric relationship being investigated;
- the operational evidence being compared;
- the conclusion allowed by that evidence;
- the limitations and edge cases of the experiment.

```text
platform concerns stay consistent
                +
Lab logic stays visible
                =
more classroom time spent on Spark and sparkMeasure concepts
```

---

## 🛣️ Standard path and controlled exceptions

### Standard path — Lab 0C

Lab 0C uses one `SparkWorkshopComparisonJob` to execute the same
Bronze-to-Silver transformation with `native` and `observed` configurations.
The workload contract remains unchanged while YAML controls whether
sparkMeasure is enabled.

```text
same transformation
  ├──► native config   ──► Spark UI and native evidence
  └──► observed config ──► Spark UI + aggregated StageMetrics
```

This comparison does not duplicate SparkSession, artifact, or collector setup
inside the lesson script.

### Controlled exception — Lab 3

Lab 3 cannot use the default measurement boundary unchanged. Its benchmark
separates:

```text
workload wall time
collector begin/end
report generation
metric aggregation
validation
application timing
```

It therefore owns a Lab-local runtime while reusing shared configuration,
session, metrics, artifact, and logging components.

> [!TIP]
> Other Labs use the same type of local escape hatch when task distributions,
> multiple contract outputs, or per-date orchestration require a different
> boundary. These extensions remain under each Lab's `*_utils/` directory.

The project accepts some local duplication so one edge case does not force all
workloads through a larger global abstraction.

---

## ⚖️ Deliberate tradeoffs

| What the workshop gains | What the workshop accepts |
| --- | --- |
| Centralized paths and Spark settings | YAML introduces some indirection. |
| Readable main Lab scripts | Lab-local runtimes may repeat small lifecycle sections. |
| One process-local session lifecycle | The singleton is not a cluster session service. |
| Named artifact access | The catalog is not a lineage or governance system. |
| Repeatable observability persistence | There are no production retries, retention policies, access controls, or SLAs. |
| Flexible diagnostic boundaries | Fingerprints, policies, contracts, and history remain separate teaching applications. |

The alternative would have been a generic plugin or orchestration framework.
That could reduce some duplication, but it would add interfaces, configuration,
and lifecycle rules that students would need to understand before reaching the
Spark and sparkMeasure concepts.

> [!CAUTION]
> The patterns in this repository should not be interpreted as a complete
> production control plane. They are intentionally scoped to reproducible local
> workshop workloads.

---

## 🚀 Where a production platform could evolve

A production implementation could add:

- stronger interfaces for custom runtimes;
- schema and configuration versioning;
- idempotent writes and retry policies;
- lineage, retention, and environment separation;
- access control and operational ownership;
- orchestration and telemetry promotion;
- one governed pipeline connecting the independent Lab concepts.

These are valid evolutions, not missing workshop requirements.

---

## 🧭 Decision summary

```text
The workshop standardizes what would otherwise be repetitive,
keeps the Spark workload visible,
and allows unusual diagnostic boundaries to remain local.
```

The contract meets its intended tradeoff: it makes workloads easier to develop
and compare while keeping the classroom focused on Spark behavior and
sparkMeasure evidence rather than infrastructure boilerplate.
