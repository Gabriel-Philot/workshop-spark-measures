# Lab 7A class notes: temporal source generator

## Main idea

Lab 7 starts by creating a controlled temporal source.

The later backfill labs need a known fact before any Spark metrics are
collected:

```text
Which business dates are supposed to be normal, medium-spike, or large-spike
days?
```

Without that known volume plan, the class would look at Spark metrics without a
business-time reference point.

## Why a new source instead of reusing retail sales

The existing retail data is intentionally shared by Labs 0-6. Changing it would
make earlier labs less stable.

Lab 7 therefore adds a new bronze entity:

```text
s3a://lakehouse/bronze/lab7/source_events_temporal
```

This keeps the temporal-backfill problem isolated. Previous labs keep reading
the same retail paths.

## Why Spark-native generation

The generator uses Spark expressions over `spark.range`.

This avoids the slow patterns that make synthetic data generation unstable:

- Python loops;
- Faker row-by-row generation;
- Python UDFs;
- tiny append writes;
- uncontrolled cross joins.

The goal is not realistic fake identities. The goal is deterministic volume,
partitioning, and business attributes that are good enough for observability
lessons.

## Volume plan

The default plan has 14 business dates with visible spikes:

```text
normal day   -> 1x
medium spike -> 10x
large spike  -> 100x
```

The first candidate default is:

```text
base_rows_per_day = 10,000
large spike       = 1,000,000 rows
medium spike      = 100,000 rows
```

This is intentionally calibratable. The important point is that spike days must
be visible later in StageMetrics.

## Non-destructive behavior

The generator is scoped to Lab 7 paths.

It must not write to:

```text
s3a://lakehouse/bronze/retail/...
```

It must not rewrite Lab 0-6 output or observability tables.

If calibration requires a reset, use:

```bash
LAB7_REPLACE_SOURCE=true \
bash src/apps/labs/lab_7/run_temporal_source_generator.sh
```

That reset is intentionally scoped to:

```text
s3a://lakehouse/bronze/lab7/source_events_temporal
```

## What students should notice

At this point, sparkMeasure is not the focus yet.

The teaching point is:

```text
Good observability starts with reproducible workload evidence.
```

Once the source has known temporal volume, the next lab can process one date at
a time and persist StageMetrics by `processing_date`.
