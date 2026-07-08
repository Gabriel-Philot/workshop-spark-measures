# Lab 0 contract rationale

## Why show sparkMeasure directly first

The natural API makes the library mechanics explicit:

```python
stage_metrics = StageMetrics(spark)
stage_metrics.begin()
# Spark action
stage_metrics.end()
stage_metrics.print_report()
stage_metrics.aggregate_stagemetrics()
```

This is the shortest path for students to understand what sparkMeasure is doing. It also makes the later workshop contract easier to justify: the contract is not a different measurement model; it wraps the same begin/end/report sequence around a named workload.

## Why use `SparkWorkshopJob`

Workshop scripts should highlight the data engineering work:

```text
extract -> transform -> load -> validate_result
```

The shared contract keeps repeated SRE concerns out of each lab script:

- loading experiment config;
- creating and stopping SparkSession;
- applying Spark log level;
- wrapping the measured block with sparkMeasure when enabled;
- printing consistent sections and run summaries;
- keeping metric persistence outside the measured workload.

## Why configs live next to the lab

Lab-specific experiments live in `src/apps/labs/lab_0/experiments.yaml` so the script, README, and runnable configs stay together. The loader still inherits global defaults from `src/config/experiments.yaml`, so Spark/Delta defaults remain centralized.

This gives us local lab ownership without duplicating global Spark settings.

## Why observability stays in YAML

The script should describe the transformation. YAML controls runtime observability:

```yaml
observability:
  enabled: true
  collector: stage
  persist: false
```

That keeps `stage` vs `task`, enable/disable, and persistence choices changeable without editing Python. For Lab 0, `persist=false` prevents metric Delta writes from adding extra jobs to the History Server comparison.

## Why the Silver table is `sales_enriched`

The previous `vendor_sales_summary` was closer to a Gold aggregate. Lab 0 now builds a Silver table by joining `sales`, `vendors`, and `products`, normalizing names, and adding `sale_year_month`.

That is a better teaching workload because it demonstrates a realistic Bronze-to-Silver refinement while still being small enough for a local cluster.

## Log namespaces

- `SPARK_*`: Spark execution and artifact IO.
- `SPARKMEASURE_*`: direct or wrapped sparkMeasure output.
- `WORKSHOP_*`: SRE/workshop narrative and run summaries.
- `LAB0_*`: Lab-specific validation and teaching markers.
