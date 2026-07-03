"""Lab 7 temporal source generator utilities."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping
from uuid import uuid4

import yaml

from spark_workshop.config import load_experiment_config
from spark_workshop.runtime import ExperimentContext
from spark_workshop.session import SparkSessionSingleton
from spark_workshop.utils import logger, spark_job_description


FULL = "full"
APPEND_DAY = "append_day"
SUPPORTED_GENERATE_MODES = frozenset({FULL, APPEND_DAY})


@dataclass(frozen=True)
class TemporalDimensions:
    """Deterministic dimensions used to generate semi-real temporal events."""

    accounts: int
    customers: int
    vendors: int
    products: int
    regions: tuple[str, ...]
    channels: tuple[str, ...]
    event_types: tuple[str, ...]


@dataclass(frozen=True)
class DateVolume:
    """Expected source volume for one event_date."""

    event_date: str
    volume_multiplier: int
    rows: int
    spike_label: str


@dataclass(frozen=True)
class TemporalVolumePlan:
    """Classroom volume plan for the Lab 7 temporal bronze source."""

    source_name: str
    seed: int
    start_date: str
    end_date: str
    base_rows_per_day: int
    spike_days: Mapping[str, int]
    target_rows_per_partition: int
    max_partitions_per_day: int
    dimensions: TemporalDimensions

    @property
    def date_volumes(self) -> tuple[DateVolume, ...]:
        return tuple(
            self.date_volume(day.isoformat())
            for day in _date_range(
                _parse_date(self.start_date, "date_range.start"),
                _parse_date(self.end_date, "date_range.end"),
            )
        )

    @property
    def total_rows(self) -> int:
        return sum(item.rows for item in self.date_volumes)

    def date_volume(self, event_date: str) -> DateVolume:
        multiplier = int(self.spike_days.get(event_date, 1))
        rows = self.base_rows_per_day * multiplier
        if multiplier >= 100:
            label = "VOLUME_SPIKE"
        elif multiplier >= 10:
            label = "MEDIUM_SPIKE"
        else:
            label = "NORMAL"
        return DateVolume(
            event_date=event_date,
            volume_multiplier=multiplier,
            rows=rows,
            spike_label=label,
        )

    def partitions_for_rows(self, rows: int) -> int:
        by_target = max(1, _ceil_div(rows, self.target_rows_per_partition))
        return min(self.max_partitions_per_day, by_target)


@dataclass(frozen=True)
class TemporalGeneratorSettings:
    """Lab 7 generator settings loaded from experiments.yaml."""

    workload_name: str = "temporal_source_generator"
    success_marker: str = "LAB7_TEMPORAL_SOURCE_GENERATOR_OK"


@dataclass(frozen=True)
class TemporalGenerationResult:
    """Summary of one temporal source generation run."""

    run_id: str
    mode: str
    source_path: str
    volume_plan_path: str
    planned_rows: int
    generated_rows: int
    skipped_dates: int
    validated_dates: int


def load_temporal_volume_plan(path: Path) -> TemporalVolumePlan:
    """Read and validate the Lab 7 temporal volume plan."""

    raw = _load_yaml(path)
    source = raw.get("source") or {}
    generation = raw.get("generation") or {}
    dimensions = raw.get("dimensions") or {}
    date_range = raw.get("date_range") or {}

    source_name = str(source.get("name", "source_events_temporal"))
    start_date = _parse_date_value(date_range.get("start"), "date_range.start")
    end_date = _parse_date_value(date_range.get("end"), "date_range.end")
    if end_date < start_date:
        raise ValueError("Lab 7 volume plan end date must be >= start date")

    spike_days = {
        _parse_date_value(key, "spike_days key").isoformat(): _positive_int(
            value,
            f"spike_days.{key}",
        )
        for key, value in (raw.get("spike_days") or {}).items()
    }
    for spike_day in spike_days:
        parsed = _parse_date(spike_day, "spike_days key")
        if parsed < start_date or parsed > end_date:
            raise ValueError(
                f"Lab 7 spike day {spike_day} is outside the configured date range"
            )

    return TemporalVolumePlan(
        source_name=source_name,
        seed=_non_negative_int(source.get("seed", 77), "source.seed"),
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        base_rows_per_day=_positive_int(
            raw.get("base_rows_per_day", 10000),
            "base_rows_per_day",
        ),
        spike_days=spike_days,
        target_rows_per_partition=_positive_int(
            generation.get("target_rows_per_partition", 250000),
            "generation.target_rows_per_partition",
        ),
        max_partitions_per_day=_positive_int(
            generation.get("max_partitions_per_day", 16),
            "generation.max_partitions_per_day",
        ),
        dimensions=TemporalDimensions(
            accounts=_positive_int(dimensions.get("accounts", 10000), "dimensions.accounts"),
            customers=_positive_int(
                dimensions.get("customers", 50000),
                "dimensions.customers",
            ),
            vendors=_positive_int(dimensions.get("vendors", 500), "dimensions.vendors"),
            products=_positive_int(
                dimensions.get("products", 5000),
                "dimensions.products",
            ),
            regions=_non_empty_string_tuple(dimensions.get("regions"), "dimensions.regions"),
            channels=_non_empty_string_tuple(
                dimensions.get("channels"),
                "dimensions.channels",
            ),
            event_types=_non_empty_string_tuple(
                dimensions.get("event_types"),
                "dimensions.event_types",
            ),
        ),
    )


def load_temporal_generator_settings(
    config_name: str,
    config_path: Path,
) -> TemporalGeneratorSettings:
    """Read Lab 7 generator settings from experiments.yaml."""

    raw = _load_yaml(config_path)
    experiments = raw.get("experiments") or {}
    if config_name not in experiments:
        raise KeyError(
            f"Unknown Lab 7 experiment '{config_name}'. "
            f"Available experiments: {sorted(experiments)}"
        )
    workload = (experiments[config_name] or {}).get("workload") or {}
    return TemporalGeneratorSettings(
        workload_name=str(workload.get("workload_name", "temporal_source_generator")),
        success_marker=str(
            workload.get("success_marker", "LAB7_TEMPORAL_SOURCE_GENERATOR_OK")
        ),
    )


def build_temporal_events_dataframe(
    spark: Any,
    plan: TemporalVolumePlan,
    date_volumes: Iterable[DateVolume],
) -> Any:
    """Build deterministic temporal events with Spark-native expressions only."""

    from pyspark.sql import functions as F

    frames = [
        _build_one_date_dataframe(spark, F, plan, date_volume)
        for date_volume in date_volumes
        if date_volume.rows > 0
    ]
    if not frames:
        raise ValueError("Lab 7 temporal source generation received no rows to build")

    result = frames[0]
    for frame in frames[1:]:
        result = result.unionByName(frame)
    return result


def build_volume_plan_records(
    *,
    run_id: str,
    mode: str,
    plan: TemporalVolumePlan,
    planned_dates: Iterable[DateVolume],
    generated_dates: Iterable[str],
    source_path: str,
) -> list[dict[str, Any]]:
    """Build auditable volume-plan records for the generated or skipped dates."""

    created_at = _utc_now()
    generated = set(generated_dates)
    records: list[dict[str, Any]] = []
    for item in planned_dates:
        records.append(
            {
                "run_id": run_id,
                "generation_mode": mode,
                "source_name": plan.source_name,
                "event_date": item.event_date,
                "volume_multiplier": item.volume_multiplier,
                "expected_rows": item.rows,
                "spike_label": item.spike_label,
                "write_status": (
                    "generated" if item.event_date in generated else "already_exists"
                ),
                "source_path": source_path,
                "created_at": created_at,
            }
        )
    return records


def run_temporal_source_generator(
    *,
    config_name: str,
    config_path: Path,
    volume_plan_path: Path,
    mode: str,
    append_date: str | None,
    append_volume_multiplier: int,
    replace_lab7_source: bool,
) -> TemporalGenerationResult:
    """Run the Lab 7 temporal source generator."""

    normalized_mode = mode.strip().lower()
    if normalized_mode not in SUPPORTED_GENERATE_MODES:
        raise ValueError(
            f"Unsupported Lab 7 generation mode '{mode}'. "
            f"Expected one of {sorted(SUPPORTED_GENERATE_MODES)}"
        )
    if replace_lab7_source and normalized_mode != FULL:
        raise ValueError("LAB7_REPLACE_SOURCE is supported only with full mode")

    config = load_experiment_config(config_name, config_path=config_path)
    logger.set_level(config.log_level)
    settings = load_temporal_generator_settings(config_name, config_path)
    plan = load_temporal_volume_plan(volume_plan_path)
    run_id = str(uuid4())

    logger.info(
        "WORKSHOP_EXPERIMENT_STARTED "
        f"experiment={config.name} app_name={config.app_name}"
    )
    logger.info(
        "LAB7_TEMPORAL_GENERATOR_CONFIG "
        f"config_name={config.name} mode={normalized_mode} "
        f"source_name={plan.source_name} replace_lab7_source={replace_lab7_source}"
    )
    logger.info(
        "LAB7_TEMPORAL_VOLUME_PLAN_OK "
        f"source_name={plan.source_name} start_date={plan.start_date} "
        f"end_date={plan.end_date} dates={len(plan.date_volumes)} "
        f"planned_rows={plan.total_rows}"
    )

    spark = SparkSessionSingleton.get_or_create(config.app_name, config.spark_config)
    reused = SparkSessionSingleton.get_or_create(config.app_name, config.spark_config)
    if reused is not spark:
        raise RuntimeError("SparkSession singleton returned different instances")
    logger.info("SPARK_SESSION_SINGLETON_OK")
    spark.sparkContext.setLogLevel(config.spark_log_level.upper())

    context = ExperimentContext(spark=spark, config=config)
    source_path = config.artifacts.output("source_events_temporal").path
    volume_plan_output_path = config.artifacts.output("temporal_volume_plan").path

    try:
        if replace_lab7_source:
            _delete_path(spark, source_path)
            logger.info(f"LAB7_TEMPORAL_SOURCE_REPLACED path={source_path}")

        existing_dates = _existing_event_dates(spark, source_path)
        planned_dates = _date_volumes_for_mode(
            plan=plan,
            mode=normalized_mode,
            append_date=append_date,
            append_volume_multiplier=append_volume_multiplier,
        )

        if normalized_mode == APPEND_DAY:
            append_item = planned_dates[0]
            if append_item.event_date in existing_dates:
                raise RuntimeError(
                    "Lab 7 append_day would duplicate an existing event_date: "
                    f"{append_item.event_date}. Use a new append date or explicitly "
                    "reset the scoped Lab 7 source before regenerating."
                )
            dates_to_generate = planned_dates
        else:
            dates_to_generate = tuple(
                item for item in planned_dates if item.event_date not in existing_dates
            )

        generated_rows = sum(item.rows for item in dates_to_generate)
        skipped_dates = len(planned_dates) - len(dates_to_generate)

        if dates_to_generate:
            with spark_job_description(
                spark,
                "LAB7 | temporal_source_generator | write_temporal_source",
            ):
                context.write(
                    "source_events_temporal",
                    build_temporal_events_dataframe(spark, plan, dates_to_generate),
                )

        plan_records = build_volume_plan_records(
            run_id=run_id,
            mode=normalized_mode,
            plan=plan,
            planned_dates=planned_dates,
            generated_dates=(item.event_date for item in dates_to_generate),
            source_path=source_path,
        )
        with spark_job_description(
            spark,
            "LAB7 | temporal_source_generator | write_volume_plan",
        ):
            context.write(
                "temporal_volume_plan",
                spark.createDataFrame(plan_records),
            )

        validation = validate_temporal_source(
            spark,
            source_path=source_path,
            expected_dates=planned_dates,
        )
        logger.info(
            "LAB7_TEMPORAL_SOURCE_GENERATED_OK "
            f"mode={normalized_mode} generated_rows={generated_rows} "
            f"generated_dates={len(dates_to_generate)} skipped_dates={skipped_dates} "
            f"source_path={source_path}"
        )
        logger.info(
            "LAB7_TEMPORAL_SOURCE_VALIDATION_OK "
            f"validated_dates={validation['validated_dates']} "
            f"validated_rows={validation['validated_rows']}"
        )
        logger.info(
            render_temporal_generation_block(
                result=TemporalGenerationResult(
                    run_id=run_id,
                    mode=normalized_mode,
                    source_path=source_path,
                    volume_plan_path=volume_plan_output_path,
                    planned_rows=sum(item.rows for item in planned_dates),
                    generated_rows=generated_rows,
                    skipped_dates=skipped_dates,
                    validated_dates=int(validation["validated_dates"]),
                ),
                plan=plan,
                planned_dates=planned_dates,
            )
        )
        logger.info(settings.success_marker)
        logger.info(
            "WORKSHOP_EXPERIMENT_COMPLETED "
            f"experiment={config.name} run_id={run_id} source_path={source_path}"
        )
        return TemporalGenerationResult(
            run_id=run_id,
            mode=normalized_mode,
            source_path=source_path,
            volume_plan_path=volume_plan_output_path,
            planned_rows=sum(item.rows for item in planned_dates),
            generated_rows=generated_rows,
            skipped_dates=skipped_dates,
            validated_dates=int(validation["validated_dates"]),
        )
    finally:
        SparkSessionSingleton.stop()


def validate_temporal_source(
    spark: Any,
    *,
    source_path: str,
    expected_dates: Iterable[DateVolume],
) -> dict[str, int]:
    """Validate source row counts by event_date against the volume plan."""

    from pyspark.sql import functions as F

    expected = {item.event_date: item.rows for item in expected_dates}
    if not expected:
        raise ValueError("Lab 7 validation requires at least one expected date")
    if not _path_exists(spark, source_path):
        raise RuntimeError(f"Lab 7 temporal source does not exist: {source_path}")

    actual_rows = (
        spark.read.format("delta")
        .load(source_path)
        .where(F.col("event_date").cast("string").isin(sorted(expected)))
        .groupBy(F.col("event_date").cast("string").alias("event_date"))
        .count()
        .collect()
    )
    actual = {row.event_date: int(row["count"]) for row in actual_rows}
    mismatches = {
        event_date: {"expected": rows, "actual": actual.get(event_date, 0)}
        for event_date, rows in expected.items()
        if actual.get(event_date, 0) != rows
    }
    if mismatches:
        raise RuntimeError(
            "Lab 7 temporal source validation failed. "
            f"Row-count mismatches by date: {mismatches}. "
            "For calibration, use LAB7_REPLACE_SOURCE=true to reset only the "
            "scoped Lab 7 source path."
        )
    return {
        "validated_dates": len(expected),
        "validated_rows": sum(expected.values()),
    }


def render_temporal_generation_block(
    *,
    result: TemporalGenerationResult,
    plan: TemporalVolumePlan,
    planned_dates: Iterable[DateVolume],
    width: int = 104,
) -> str:
    """Render a prominent classroom-friendly generation summary."""

    spike_lines = [
        f"{item.event_date}: rows={item.rows} multiplier={item.volume_multiplier}x label={item.spike_label}"
        for item in planned_dates
        if item.volume_multiplier > 1
    ]
    lines = [
        "## LAB 7 TEMPORAL SOURCE GENERATOR",
        "",
        "### Source isolation",
        "status: scoped to Lab 7 bronze and observability paths",
        f"source_path: {result.source_path}",
        f"volume_plan_path: {result.volume_plan_path}",
        "",
        "### Generation result",
        f"mode: {result.mode}",
        f"planned_rows: {result.planned_rows}",
        f"generated_rows: {result.generated_rows}",
        f"skipped_dates: {result.skipped_dates}",
        f"validated_dates: {result.validated_dates}",
        "",
        "### Volume shape",
        f"base_rows_per_day: {plan.base_rows_per_day}",
        f"date_range: {plan.start_date} -> {plan.end_date}",
        "spike_days:",
        *(spike_lines or ["none"]),
        "",
        "### Next classroom step",
        "Run the daily backfill stage-metrics lab after this source is available.",
    ]
    return _boxed_lines(lines, width=width)


def _date_volumes_for_mode(
    *,
    plan: TemporalVolumePlan,
    mode: str,
    append_date: str | None,
    append_volume_multiplier: int,
) -> tuple[DateVolume, ...]:
    if mode == FULL:
        return plan.date_volumes
    if not append_date:
        raise ValueError("append_day mode requires LAB7_APPEND_DATE or --append-date")
    event_date = _parse_date_value(append_date, "append_date").isoformat()
    multiplier = _positive_int(append_volume_multiplier, "append_volume_multiplier")
    rows = plan.base_rows_per_day * multiplier
    if multiplier >= 100:
        label = "VOLUME_SPIKE"
    elif multiplier >= 10:
        label = "MEDIUM_SPIKE"
    else:
        label = "NORMAL"
    return (
        DateVolume(
            event_date=event_date,
            volume_multiplier=multiplier,
            rows=rows,
            spike_label=label,
        ),
    )


def _build_one_date_dataframe(
    spark: Any,
    F: Any,
    plan: TemporalVolumePlan,
    date_volume: DateVolume,
) -> Any:
    dimensions = plan.dimensions
    partitions = plan.partitions_for_rows(date_volume.rows)
    base_timestamp = f"{date_volume.event_date} 00:00:00"
    quantity = (_hash_mod(F, F.col("event_offset"), plan.seed + 8, 5) + F.lit(1)).cast(
        "int"
    )
    unit_price = F.round(
        (_hash_mod(F, F.col("event_offset"), plan.seed + 9, 20000).cast("double") / 100.0)
        + F.lit(5.0),
        2,
    )

    return (
        spark.range(0, date_volume.rows, 1, numPartitions=partitions)
        .withColumnRenamed("id", "event_offset")
        .withColumn(
            "event_id",
            F.concat(
                F.regexp_replace(F.lit(date_volume.event_date), "-", ""),
                F.lit("-"),
                F.lpad(F.col("event_offset").cast("string"), 12, "0"),
            ),
        )
        .withColumn("event_date", F.to_date(F.lit(date_volume.event_date)))
        .withColumn(
            "event_ts",
            F.from_unixtime(
                F.unix_timestamp(F.lit(base_timestamp))
                + F.pmod(F.col("event_offset"), F.lit(86400)).cast("long")
            ).cast("timestamp"),
        )
        .withColumn(
            "account_id",
            (_hash_mod(F, F.col("event_offset"), plan.seed + 1, dimensions.accounts) + 1).cast(
                "long"
            ),
        )
        .withColumn(
            "customer_id",
            (
                _hash_mod(F, F.col("event_offset"), plan.seed + 2, dimensions.customers)
                + 1
            ).cast("long"),
        )
        .withColumn(
            "vendor_id",
            (_hash_mod(F, F.col("event_offset"), plan.seed + 3, dimensions.vendors) + 1).cast(
                "long"
            ),
        )
        .withColumn(
            "product_id",
            (_hash_mod(F, F.col("event_offset"), plan.seed + 4, dimensions.products) + 1).cast(
                "long"
            ),
        )
        .withColumn(
            "region",
            _array_value(
                F,
                dimensions.regions,
                _hash_mod(F, F.col("event_offset"), plan.seed + 5, len(dimensions.regions)),
            ),
        )
        .withColumn(
            "channel",
            _array_value(
                F,
                dimensions.channels,
                _hash_mod(F, F.col("event_offset"), plan.seed + 6, len(dimensions.channels)),
            ),
        )
        .withColumn(
            "event_type",
            _array_value(
                F,
                dimensions.event_types,
                _hash_mod(
                    F,
                    F.col("event_offset"),
                    plan.seed + 7,
                    len(dimensions.event_types),
                ),
            ),
        )
        .withColumn("quantity", quantity)
        .withColumn("gross_amount", F.round(F.col("quantity") * unit_price, 2))
        .withColumn(
            "payload_size_bucket",
            _array_value(
                F,
                ("SMALL", "MEDIUM", "LARGE"),
                _hash_mod(F, F.col("event_offset"), plan.seed + 10, 3),
            ),
        )
        .withColumn("created_at", F.current_timestamp())
        .select(
            "event_id",
            "event_date",
            "event_ts",
            "account_id",
            "customer_id",
            "vendor_id",
            "product_id",
            "region",
            "channel",
            "event_type",
            "quantity",
            "gross_amount",
            "payload_size_bucket",
            "created_at",
        )
    )


def _existing_event_dates(spark: Any, source_path: str) -> set[str]:
    if not _path_exists(spark, source_path):
        return set()

    from pyspark.sql import functions as F

    rows = (
        spark.read.format("delta")
        .load(source_path)
        .select(F.col("event_date").cast("string").alias("event_date"))
        .distinct()
        .collect()
    )
    return {str(row.event_date) for row in rows}


def _hash_mod(F: Any, column: Any, seed: int, modulo: int) -> Any:
    if modulo <= 0:
        raise ValueError("modulo must be positive")
    return F.pmod(F.xxhash64(column, F.lit(seed)), F.lit(modulo)).cast("long")


def _array_value(F: Any, values: tuple[str, ...], zero_based_index: Any) -> Any:
    return F.element_at(
        F.array(*[F.lit(value) for value in values]),
        (zero_based_index + F.lit(1)).cast("int"),
    )


def _path_exists(spark: Any, path: str) -> bool:
    jvm = spark.sparkContext._jvm
    hconf = spark.sparkContext._jsc.hadoopConfiguration()
    target = jvm.org.apache.hadoop.fs.Path(path)
    fs = target.getFileSystem(hconf)
    return bool(fs.exists(target))


def _delete_path(spark: Any, path: str) -> None:
    jvm = spark.sparkContext._jvm
    hconf = spark.sparkContext._jsc.hadoopConfiguration()
    target = jvm.org.apache.hadoop.fs.Path(path)
    fs = target.getFileSystem(hconf)
    if fs.exists(target):
        logger.info(f"Deleting scoped Lab 7 path before regeneration path={path}")
        fs.delete(target, True)


def _boxed_lines(lines: list[str], *, width: int) -> str:
    normalized_width = max(width, 60)
    content_width = normalized_width - 4
    border = "═" * (normalized_width - 2)
    rendered = []
    for line in lines:
        trimmed = line[: content_width - 1] + "…" if len(line) > content_width else line
        rendered.append(f"║ {trimmed.ljust(content_width)} ║")
    return f"\n╔{border}╗\n" + "\n".join(rendered) + f"\n╚{border}╝"


def _date_range(start: date, end: date) -> Iterable[date]:
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def _parse_date_value(value: Any, name: str) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    return _parse_date(str(value), name)


def _parse_date(value: str, name: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"{name} must be formatted as YYYY-MM-DD, got {value!r}") from exc


def _positive_int(value: Any, name: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise ValueError(f"{name} must be > 0")
    return parsed


def _non_negative_int(value: Any, name: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise ValueError(f"{name} must be >= 0")
    return parsed


def _non_empty_string_tuple(value: Any, name: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise ValueError(f"{name} must be a non-empty list")
    parsed = tuple(str(item) for item in value)
    if any(not item.strip() for item in parsed):
        raise ValueError(f"{name} cannot contain empty values")
    return parsed


def _ceil_div(value: int, divisor: int) -> int:
    return -(-value // divisor)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as config_file:
        return yaml.safe_load(config_file) or {}
