# Lab 1 plan: global sort diagnosis with sparkMeasure

## Purpose

Lab 1 teaches sparkMeasure as a diagnostic tool, not as a generic Spark
tuning recipe. The story is intentionally common: a business-facing ranking
job runs, but one part of the execution is heavier than expected. Students use
stage-level sparkMeasure metrics to isolate the expensive stage before
discussing possible redesigns.

## Story

A stakeholder asks for a global sales ranking. The implementation reads the
generated bronze retail data, builds an enriched sales dataset, globally orders
all rows by sales amount, and writes a Delta result for reporting.

The code looks reasonable at first glance, but the global ordering is a wide
operation. The native Spark UI and physical plan expose the details, but the
signal is verbose. The sparkMeasure run gives a compact summary that highlights
stage count, task count, executor runtime, and shuffle bytes.

## Planned workload

- Inputs:
  - `s3a://lakehouse/bronze/retail/sales`
  - `s3a://lakehouse/bronze/retail/vendors`
  - `s3a://lakehouse/bronze/retail/products`
- Transform:
  - Build `sales_enriched` from the bronze inputs.
  - Apply a global sort by `sale_amount` descending and `sale_id` ascending.
- Output:
  - `s3a://lakehouse/gold/lab1/top_sales_global_sort`
- Observability:
  - Native run without sparkMeasure.
  - Observed run with sparkMeasure `collector: stage`.
  - Metrics persistence disabled so History Server stays focused on the
    workload jobs.

## Teaching checkpoints

1. Run the native job and inspect Spark History.
2. Show that the physical plan and History UI are useful, but verbose.
3. Run the observed job and inspect the sparkMeasure summary.
4. Identify the expensive stage through stage-level metrics.
5. Explain why global sort introduces shuffle and ordering work.
6. Discuss fixes as follow-up options only: aggregate before sorting,
   partition-aware sorting, top-N strategies, or reducing columns before the
   sort.

## Explicit non-goals

- Do not use task-level metrics in Lab 1.
- Do not turn the lab into a full performance tuning exercise.
- Do not require Lab 0 output to exist; Lab 1 reads bronze directly.
- Do not change the data generator for this lab.
- Keep Lab 1 transformations inside `lab_1_utils` for didactic readability, even if this duplicates the Lab 0 enrichment shape.
