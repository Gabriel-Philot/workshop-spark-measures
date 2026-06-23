# Data generator research

Research date: 2026-06-23.

This research supports the Spark Measures workshop data generator decision. The question was whether Polars plus a Faker-like framework should be used to generate multiple bronze Delta tables quickly, including intentionally skewed datasets.

## Sources checked

- Polars `DataFrame.write_delta`: <https://docs.pola.rs/api/python/stable/reference/api/polars.DataFrame.write_delta.html>
- Polars `LazyFrame.sink_delta`: <https://docs.pola.rs/api/python/stable/reference/api/polars.LazyFrame.sink_delta.html>
- Polars streaming concepts: <https://docs.pola.rs/user-guide/concepts/streaming/>
- delta-rs writing docs: <https://delta-io.github.io/delta-rs/usage/writing/>
- DuckDB Delta extension: <https://duckdb.org/docs/current/core_extensions/delta>
- Spark `SparkSession.range`: <https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.range.html>
- Spark random functions: <https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html>
- Spark `DataFrameWriter.partitionBy`: <https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.partitionBy.html>
- Delta Lake batch writes: <https://docs.delta.io/delta-batch/>
- Databricks Labs `dbldatagen`: <https://github.com/databrickslabs/dbldatagen>
- Databricks Labs `dbldatagen` docs: <https://databrickslabs.github.io/dbldatagen/public_docs/index.html>
- Faker docs: <https://faker.readthedocs.io/en/master/>
- Mimesis docs: <https://mimesis.name/master/>
- Mimesis structured data docs: <https://mimesis.name/master/schema.html>
- ShadowTraffic `lookup`: <https://docs.shadowtraffic.io/functions/lookup/>
- ShadowTraffic `cache`: <https://docs.shadowtraffic.io/functions/cache/>
- ShadowTraffic `weightedOneOf`: <https://docs.shadowtraffic.io/functions/weightedOneOf/>
- Databricks Labs `dbldatagen` multi-table docs: <https://databrickslabs.github.io/dbldatagen/public_docs/multi_table_data.html>
- Databricks Labs `dbldatagen` distributions docs: <https://databrickslabs.github.io/dbldatagen/public_docs/DISTRIBUTIONS.html>

## Findings

### Polars + delta-rs

Polars can write Delta tables through the `deltalake` package. `DataFrame.write_delta` supports modes such as append and overwrite, plus S3-style storage options. `LazyFrame.sink_delta` is more interesting for large data because it can sink a lazy computation instead of fully materializing a DataFrame first.

Important caveat: Polars marks `LazyFrame.sink_delta` as unstable. That is acceptable for a research path, but not ideal as the only foundation for workshop data creation.

Polars streaming is useful because it processes batches and reduces memory pressure, but it is still a single-node generator unless we explicitly shard the workload across multiple containers/processes. For 1 TB-class fact tables, object storage and disk throughput will likely dominate.

Assessment: viable for small/medium tables and benchmark experiments; not the default path for the first generator.

### delta-rs directly

delta-rs `write_deltalake` accepts Pandas, PyArrow Table, or an iterator of PyArrow RecordBatches. The iterator path is relevant because it lets us generate and write in batches without holding the full dataset in memory.

Assessment: useful as a low-level writer if Polars becomes awkward. It gives control, but we would need to build more of the generation framework ourselves.

### Spark-native generation

Spark has the primitives we need:

- `spark.range(..., numPartitions=...)` creates distributed row IDs;
- `rand(seed)` and `randn(seed)` create seeded random columns;
- `partitionBy(...)` controls filesystem layout;
- Delta writes use the same writer path as the labs;
- Delta write options such as `maxRecordsPerFile` help control file count and file size.

This aligns best with sparkMeasure labs. We can generate bad layouts and then run separate measured experiments over those layouts.

Assessment: best first implementation path.

### Databricks Labs dbldatagen

`dbldatagen` is a Spark-based synthetic data generator. Its README explicitly targets large synthetic data, repeatable data, multiple tables, relationships, weighted values, distributions, SQL expressions, and plugins for libraries like Faker.

This is close to what the workshop needs. The concern is compatibility and ownership: it is Databricks Labs, Databricks-oriented, and not formally supported with SLAs. It says it targets recent Databricks Runtime / PySpark combinations, but this repo uses local Spark 4.1.2. We need a local proof before adopting it.

Assessment: strong POC candidate before building too much custom DSL.

### DuckDB

DuckDB has a Delta extension with read and write support for local and remote Delta tables. It can also generate synthetic data through SQL patterns. This is attractive for local data generation, especially for users comfortable with SQL.

However, the workshop goal is Spark diagnosis. DuckDB would introduce a second execution engine whose behavior is not what participants are measuring. It may still be useful for quick dimensions or comparison benchmarks.

Assessment: research candidate, not first path.

### Faker and Mimesis

Faker is feature-rich and supports locales, providers, seeding, and customization. It also has a `use_weighting` performance option.

Mimesis is designed for fake data and provides structured schemas, relational schemas, lazy iteration, seeds, weighted choice, and DataFrame integration. Its docs claim it is much faster than Faker in their benchmarks.

Neither library should generate the large fact table row by row. They are better used to generate dictionaries/dimensions that are then joined or mapped into vectorized fact generation.

Assessment: use for small dimensions and dictionaries only.

## Specific answer to “Polars + Faker-like framework?”

Yes, it is possible, but it should not be the primary generator path yet.

A practical Polars path would be:

1. Use Mimesis or Faker to generate small dimensions.
2. Use Polars expressions for larger vectorized tables.
3. Write with `LazyFrame.sink_delta` or `DataFrame.write_delta`.
4. Validate the resulting Delta table with Spark 4.1.2.
5. Benchmark MinIO write throughput.

The blocker is not whether Polars can write Delta. It can. The blocker is whether a single-node Polars generator can produce the workshop-scale fact tables faster and more reliably than Spark while still creating layouts Spark will diagnose naturally.

## Recommended benchmark before committing

Run the same scenario through at least two engines:

- Spark-native writer;
- Polars + delta-rs writer;
- optionally `dbldatagen`.

Benchmark target:

- 10 GB physical Delta output;
- same schema;
- same skew distribution;
- same MinIO endpoint;
- same compression where possible;
- same file-count target.

Capture:

- elapsed seconds;
- rows generated;
- Delta physical bytes;
- number of data files;
- average, min, and max file size;
- Spark read compatibility;
- Spark query time for a simple groupBy/join;
- whether sparkMeasure can read the resulting workload symptoms clearly.

Only after this benchmark should Polars become a first-class generator engine.

## Suggested generator scenarios

### 1. Skewed vendor sales

Purpose: demonstrate join skew and uneven task duration.

Data:

- `vendors`: small table;
- `sales`: large fact table;
- one vendor receives a configurable share of rows.

Controls:

- `hot_vendor_id`;
- `hot_vendor_share`;
- `vendor_count`;
- `sale_rows` or `target_bytes`;
- `partition_count`;
- `maxRecordsPerFile`.

Expected Spark symptoms:

- uneven shuffle task durations;
- possible spill under larger scales;
- stage/task skew visible in sparkMeasure task metrics.

### 2. Small files

Purpose: demonstrate metadata/file scan overhead.

Data:

- same fact table, but written with many tiny files.

Controls:

- high partition count;
- low `maxRecordsPerFile`;
- optional partition-by date/vendor.

Expected Spark symptoms:

- high task scheduling overhead;
- many input files;
- worse scan time despite modest bytes.

### 3. Wide shuffle

Purpose: demonstrate expensive aggregation/groupBy.

Data:

- fact table with high-cardinality keys and wide rows.

Controls:

- group key cardinality;
- column count;
- string width;
- shuffle partitions.

Expected Spark symptoms:

- high shuffle read/write;
- high executor run time;
- possible spill.

### 4. Bad partitioning

Purpose: demonstrate partition pruning and partition skew.

Data:

- sales partitioned by a skewed column or over-partitioned by too many columns.

Controls:

- partition columns;
- date/vendor distribution;
- records per partition.

Expected Spark symptoms:

- file skipping/pruning differences;
- uneven file sizes;
- slow scans for queries that do not align with partition layout.

## Risks

- Physical 1 TB/min is probably unrealistic in local Docker.
- MinIO S3 API throughput may dominate generation time.
- Polars Delta write compatibility must be validated with Spark + Delta 4.2.0.
- DuckDB Delta write support is promising but adds another engine.
- `dbldatagen` may save time, but it must pass Spark 4.1.2 compatibility checks.
- Skew diagnosis needs task-level metrics for the clearest workshop story.


## Relationship-generation addendum

The generator needs to create join-ready data, not isolated synthetic tables. This changes the main design requirement.

Relevant tool patterns:

- ShadowTraffic has `lookup`, which retrieves previously generated events from another collection and is explicitly meant to create relationships through shared identifiers. It also has `cache` for stable identity-derived values and `weightedOneOf` for skewed categorical selection.
- `dbldatagen` has a multi-table example focused on consistent primary/foreign keys for join and merge scenarios. It uses repeatable generation, sequences, hashes, base columns, weighted values, and distributions to create related Spark DataFrames.
- Mimesis 19 has `SchemaBuilder` and relational schema support through `ctx.pick_from`, useful for small/medium relational dictionaries.

For this workshop, the best pattern is not to copy one tool directly. The right pattern is:

1. Define relationship contracts in our config.
2. Generate dimensions first.
3. Generate fact-table keys from known domains with deterministic weighted distributions.
4. Validate the generated Delta tables with Spark.
5. Persist a run manifest that captures actual row counts, key distributions, and file layout.

This gives us the ShadowTraffic-style guarantee of related data while staying inside a Spark/Delta lakehouse workflow.

## Scale addendum

The revised classroom target is 1-5 minutes of generation for useful local datasets, not a hard 1 TB/minute benchmark. The generator should support large presets such as 500 GB, but the default workshop flow should use the smallest dataset that makes the Spark issue obvious.

Recommended local scale strategy:

- `demo`: sub-GB to a few GB, used to verify the flow quickly;
- `xs`: a few GB, visible in Spark UI and History Server;
- `s`: 10-50 GB, good for most skew/small-file labs;
- `m`: 50-150 GB, useful for wide shuffle and more visible IO;
- `large-local`: 250-500 GB, optional if the machine has enough disk/time;
- `tb`: supported as a configuration target, not a default expectation.

The local machine has limited RAM, so the generator must avoid driver collection and Python row loops for fact tables. It should generate fact data through Spark expressions or a compatible Spark-native generator and write directly to Delta.

## Updated research verdict

The most defensible solution is a hybrid:

- contract: custom schema-first YAML owned by this repo;
- first engine candidate: `dbldatagen` wrapped by our contract, if it works with Spark 4.1.2 locally;
- fallback engine: internal Spark-native generator using deterministic expressions;
- writer: Spark Delta to MinIO/S3A;
- providers: Mimesis/Faker only for small dimensions or dictionaries;
- benchmark-only: Polars/delta-rs for optional comparison and small tables.

Polars is not rejected. It is just not the right primary layer for relational, join-heavy, Spark-diagnostic datasets on a local cluster.

## Research conclusion

Start with a schema-first relational contract and one retail skew scenario. POC `dbldatagen` first because it already targets Spark multi-table generation and distributions. If compatibility is clean, wrap it. If not, implement the minimal internal Spark-native generator. Keep Polars/delta-rs as a secondary benchmark path, not the primary generator.
