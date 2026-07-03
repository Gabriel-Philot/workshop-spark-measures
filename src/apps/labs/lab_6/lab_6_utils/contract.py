"""Lab 6 contract settings, StageMetrics normalization, and rule evaluation."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from functools import reduce
from pathlib import Path
from typing import Any, Mapping

import yaml


PASS = "PASS"
FAIL = "FAIL"

SCHEMA = "SCHEMA"
SEMANTIC = "SEMANTIC"
CORRELATION = "CORRELATION"

REQUIRED_STAGE_METRICS = ("numStages", "numTasks", "executorRunTime")


@dataclass(frozen=True)
class StageMetricsContractSettings:
    """Classroom workload settings loaded from the selected Lab 6 experiment."""

    workload_name: str = "retail_stage_metrics_contract_gate"
    workload_variant: str = "contract_ready_metrics"
    success_marker: str = "LAB6_STAGE_METRICS_CONTRACT_GATE_OK"
    shuffle_partitions: int = 64


@dataclass(frozen=True)
class NormalizedStageMetrics:
    """Normalized sparkMeasure StageMetrics fields persisted by Lab 6."""

    executor_run_time_ms: int
    shuffle_bytes_written: int
    shuffle_bytes_read: int
    jvm_gc_time_ms: int
    memory_bytes_spilled: int
    disk_bytes_spilled: int
    input_bytes: int
    num_stages: int
    num_tasks: int


@dataclass(frozen=True)
class SchemaRules:
    """Schema-level contract options."""

    require_columns: bool = True


@dataclass(frozen=True)
class SemanticRules:
    """Semantic-level contract options."""

    num_stages_gt_zero: bool = True
    num_tasks_gt_zero: bool = True
    created_at_not_null: bool = True
    non_negative_metrics: tuple[str, ...] = (
        "executor_run_time_ms",
        "shuffle_bytes_written",
        "shuffle_bytes_read",
        "jvm_gc_time_ms",
        "memory_bytes_spilled",
        "disk_bytes_spilled",
        "input_bytes",
    )


@dataclass(frozen=True)
class CorrelationRules:
    """Correlation-level contract options."""

    required_identity_columns: tuple[str, ...] = (
        "run_id",
        "app_name",
        "lab_id",
        "workload_name",
        "workload_variant",
        "collector_name",
        "metric_scope",
        "created_at",
    )
    expected_values: Mapping[str, str] | None = None
    uniqueness_key: tuple[str, ...] = (
        "run_id",
        "workload_name",
        "workload_variant",
        "metric_scope",
    )


@dataclass(frozen=True)
class ContractRules:
    """All Lab 6 contract rules loaded from contract_rules.yaml."""

    version: str
    required_columns: tuple[str, ...]
    schema_rules: SchemaRules
    semantic_rules: SemanticRules
    correlation_rules: CorrelationRules
    severity: Mapping[str, str]


@dataclass(frozen=True)
class ContractRuleResult:
    """One rule-level contract result row."""

    validation_run_id: str
    source_path: str
    contract_version: str
    rule_id: str
    rule_name: str
    rule_type: str
    severity: str
    decision: str
    failed_count: int
    sample_failed_keys: str
    recommendation: str
    created_at: str

    def as_record(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ContractSummary:
    """Final contract summary row."""

    validation_run_id: str
    source_path: str
    contract_version: str
    total_rules: int
    passed_rules: int
    failed_rules: int
    decision: str
    created_at: str

    @property
    def final_marker(self) -> str:
        return f"LAB6_STAGE_METRICS_CONTRACT_{self.decision}"

    def as_record(self) -> dict[str, Any]:
        return asdict(self)


def load_stage_metrics_contract_settings(
    config_name: str,
    config_path: Path,
) -> StageMetricsContractSettings:
    """Read Lab 6 workload settings from the local YAML config."""

    raw = _load_yaml(config_path, label="Lab 6 experiments")
    experiments = raw.get("experiments") or {}
    if config_name not in experiments:
        raise KeyError(
            f"Unknown Lab 6 experiment '{config_name}'. "
            f"Available experiments: {sorted(experiments)}"
        )

    workload = (experiments[config_name] or {}).get("workload") or {}
    return StageMetricsContractSettings(
        workload_name=str(
            workload.get("workload_name", "retail_stage_metrics_contract_gate")
        ),
        workload_variant=str(workload.get("workload_variant", "contract_ready_metrics")),
        success_marker=str(
            workload.get("success_marker", "LAB6_STAGE_METRICS_CONTRACT_GATE_OK")
        ),
        shuffle_partitions=_positive_int(
            workload.get("shuffle_partitions", 64),
            "shuffle_partitions",
        ),
    )


def load_contract_rules(rules_path: Path) -> ContractRules:
    """Read Lab 6 contract rules from YAML."""

    raw = _load_yaml(rules_path, label="Lab 6 contract rules")
    contract = raw.get("contract") or {}
    required_columns = raw.get("required_columns") or ()
    schema_rules = raw.get("schema_rules") or {}
    semantic_rules = raw.get("semantic_rules") or {}
    correlation_rules = raw.get("correlation_rules") or {}
    severity = raw.get("severity") or {}

    if not isinstance(required_columns, list | tuple):
        raise ValueError(f"Lab 6 required_columns must be a sequence: {rules_path}")
    if not isinstance(correlation_rules.get("expected_values") or {}, Mapping):
        raise ValueError(f"Lab 6 expected_values must be a mapping: {rules_path}")

    return ContractRules(
        version=str(contract.get("version", "1.0.0")),
        required_columns=tuple(str(column) for column in required_columns),
        schema_rules=SchemaRules(
            require_columns=bool(schema_rules.get("require_columns", True)),
        ),
        semantic_rules=SemanticRules(
            num_stages_gt_zero=bool(
                semantic_rules.get("num_stages_gt_zero", True)
            ),
            num_tasks_gt_zero=bool(semantic_rules.get("num_tasks_gt_zero", True)),
            created_at_not_null=bool(
                semantic_rules.get("created_at_not_null", True)
            ),
            non_negative_metrics=tuple(
                str(metric)
                for metric in (
                    semantic_rules.get("non_negative_metrics")
                    or SemanticRules().non_negative_metrics
                )
            ),
        ),
        correlation_rules=CorrelationRules(
            required_identity_columns=tuple(
                str(column)
                for column in (
                    correlation_rules.get("required_identity_columns")
                    or CorrelationRules().required_identity_columns
                )
            ),
            expected_values={
                str(key): str(value)
                for key, value in (
                    correlation_rules.get("expected_values") or {}
                ).items()
            },
            uniqueness_key=tuple(
                str(column)
                for column in (
                    correlation_rules.get("uniqueness_key")
                    or CorrelationRules().uniqueness_key
                )
            ),
        ),
        severity={str(key): str(value) for key, value in severity.items()},
    )


def normalize_stage_metrics(metrics: Mapping[str, int | float]) -> NormalizedStageMetrics:
    """Map actual sparkMeasure StageMetrics aggregate names to Lab 6 fields."""

    missing = tuple(key for key in REQUIRED_STAGE_METRICS if key not in metrics)
    if missing:
        raise ValueError(
            "Lab 6 received an unsupported stage metrics schema. "
            f"Missing required metrics: {', '.join(missing)}"
        )

    normalized = NormalizedStageMetrics(
        executor_run_time_ms=_metric_int(metrics, "executorRunTime"),
        shuffle_bytes_written=_metric_int(metrics, "shuffleBytesWritten"),
        shuffle_bytes_read=_metric_int(metrics, "shuffleTotalBytesRead"),
        jvm_gc_time_ms=_metric_int(metrics, "jvmGCTime"),
        memory_bytes_spilled=_metric_int(metrics, "memoryBytesSpilled"),
        disk_bytes_spilled=_metric_int(metrics, "diskBytesSpilled"),
        input_bytes=_metric_int(metrics, "bytesRead"),
        num_stages=_metric_int(metrics, "numStages"),
        num_tasks=_metric_int(metrics, "numTasks"),
    )
    if normalized.num_stages < 1 or normalized.num_tasks < 1:
        raise ValueError(
            "Lab 6 captured no useful stage-level metrics: "
            f"numStages={normalized.num_stages}, numTasks={normalized.num_tasks}"
        )
    return normalized


def build_stage_metrics_record(
    *,
    run_id: str,
    app_name: str,
    application_id: str,
    settings: StageMetricsContractSettings,
    contract_version: str,
    metrics: NormalizedStageMetrics,
) -> dict[str, Any]:
    """Build one contract-ready raw StageMetrics row."""

    return {
        "run_id": run_id,
        "app_name": app_name,
        "application_id": application_id,
        "lab_id": "lab_6",
        "workload_name": settings.workload_name,
        "workload_variant": settings.workload_variant,
        "collector_name": "sparkmeasure_stage_metrics",
        "metric_scope": "stage",
        "contract_version": contract_version,
        "created_at": _utc_now(),
        "num_stages": metrics.num_stages,
        "num_tasks": metrics.num_tasks,
        "executor_run_time_ms": metrics.executor_run_time_ms,
        "shuffle_bytes_written": metrics.shuffle_bytes_written,
        "shuffle_bytes_read": metrics.shuffle_bytes_read,
        "jvm_gc_time_ms": metrics.jvm_gc_time_ms,
        "memory_bytes_spilled": metrics.memory_bytes_spilled,
        "disk_bytes_spilled": metrics.disk_bytes_spilled,
        "input_bytes": metrics.input_bytes,
    }


def build_invalid_demo_records(base_record: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Build controlled invalid rows without mutating the clean raw metrics row."""

    base = dict(base_record)

    def invalid(label: str, **overrides: Any) -> dict[str, Any]:
        row = dict(base)
        row["run_id"] = f"{base['run_id']}-{label}"
        row.update(overrides)
        return row

    duplicate = dict(base)

    return [
        base,
        invalid("null-run-id", run_id=None),
        invalid("zero-stages", num_stages=0),
        invalid("zero-tasks", num_tasks=0),
        invalid("negative-shuffle", shuffle_bytes_written=-1),
        invalid("null-created-at", created_at=None),
        invalid("invalid-scope", metric_scope="task"),
        duplicate,
    ]


def validate_stage_metrics_contract(
    dataframe: Any,
    *,
    rules: ContractRules,
    validation_run_id: str,
    source_path: str,
) -> tuple[list[ContractRuleResult], ContractSummary]:
    """Evaluate schema, semantic, and correlation rules against a metrics DataFrame."""

    created_at = _utc_now()
    results = [
        _schema_required_columns_result(
            dataframe,
            rules=rules,
            validation_run_id=validation_run_id,
            source_path=source_path,
            created_at=created_at,
        ),
        _positive_metric_result(
            dataframe,
            rules=rules,
            validation_run_id=validation_run_id,
            source_path=source_path,
            created_at=created_at,
            metric_column="num_stages",
            rule_id="SEMANTIC_NUM_STAGES_GT_ZERO",
            rule_name="num_stages must be greater than zero",
            severity_key="zero_stage_or_task",
            recommendation="Check whether the StageMetrics collector captured a real Spark action.",
        ),
        _positive_metric_result(
            dataframe,
            rules=rules,
            validation_run_id=validation_run_id,
            source_path=source_path,
            created_at=created_at,
            metric_column="num_tasks",
            rule_id="SEMANTIC_NUM_TASKS_GT_ZERO",
            rule_name="num_tasks must be greater than zero",
            severity_key="zero_stage_or_task",
            recommendation="Check whether the workload created executable Spark tasks.",
        ),
        _non_negative_metrics_result(
            dataframe,
            rules=rules,
            validation_run_id=validation_run_id,
            source_path=source_path,
            created_at=created_at,
        ),
        _created_at_not_null_result(
            dataframe,
            rules=rules,
            validation_run_id=validation_run_id,
            source_path=source_path,
            created_at=created_at,
        ),
        _identity_columns_not_null_result(
            dataframe,
            rules=rules,
            validation_run_id=validation_run_id,
            source_path=source_path,
            created_at=created_at,
        ),
        _expected_values_result(
            dataframe,
            rules=rules,
            validation_run_id=validation_run_id,
            source_path=source_path,
            created_at=created_at,
        ),
        _uniqueness_key_result(
            dataframe,
            rules=rules,
            validation_run_id=validation_run_id,
            source_path=source_path,
            created_at=created_at,
        ),
    ]

    failed_rules = sum(1 for result in results if result.decision == FAIL)
    summary = ContractSummary(
        validation_run_id=validation_run_id,
        source_path=source_path,
        contract_version=rules.version,
        total_rules=len(results),
        passed_rules=len(results) - failed_rules,
        failed_rules=failed_rules,
        decision=FAIL if failed_rules else PASS,
        created_at=created_at,
    )
    return results, summary


def layer_decision(
    results: list[ContractRuleResult],
    rule_type: str,
) -> str:
    """Return PASS only when every rule in a contract layer passed."""

    layer_results = [result for result in results if result.rule_type == rule_type]
    if not layer_results:
        return PASS
    return FAIL if any(result.decision == FAIL for result in layer_results) else PASS


def render_contract_gate_block(
    *,
    summary: ContractSummary,
    results: list[ContractRuleResult],
    raw_metrics_path: str,
    validation_input_path: str,
    results_output_path: str,
    summary_output_path: str,
    demo_mode: bool,
    width: int = 104,
) -> str:
    """Render a prominent classroom-friendly final contract block."""

    failed = [result for result in results if result.decision == FAIL]
    lines = [
        "## LAB 6 STAGE METRICS CONTRACT GATE",
        "",
        "### Final contract decision",
        f"decision: {summary.decision}",
        f"demo_mode: {str(demo_mode).lower()}",
        f"total_rules: {summary.total_rules}",
        f"passed_rules: {summary.passed_rules}",
        f"failed_rules: {summary.failed_rules}",
        "",
        "### Contract layers",
        f"schema: {layer_decision(results, SCHEMA)}",
        f"semantic: {layer_decision(results, SEMANTIC)}",
        f"correlation: {layer_decision(results, CORRELATION)}",
        "",
        "### Failed rule details",
    ]
    if failed:
        lines.extend(
            f"{result.rule_id}: failed_count={result.failed_count} "
            f"sample={result.sample_failed_keys or 'none'}"
            for result in failed
        )
    else:
        lines.append("none")

    lines.extend(
        [
            "",
            "### Delta outputs",
            f"raw_metrics_path: {raw_metrics_path}",
            f"validation_input_path: {validation_input_path}",
            f"rule_results_path: {results_output_path}",
            f"summary_path: {summary_output_path}",
        ]
    )
    return _boxed_lines(lines, width=width)


def _schema_required_columns_result(
    dataframe: Any,
    *,
    rules: ContractRules,
    validation_run_id: str,
    source_path: str,
    created_at: str,
) -> ContractRuleResult:
    missing = [
        column
        for column in rules.required_columns
        if column not in set(dataframe.columns)
    ]
    return _result(
        validation_run_id=validation_run_id,
        source_path=source_path,
        rules=rules,
        rule_id="SCHEMA_REQUIRED_COLUMNS",
        rule_name="required columns must exist",
        rule_type=SCHEMA,
        severity_key="missing_required_column",
        failed_count=len(missing) if rules.schema_rules.require_columns else 0,
        sample_failed_keys=",".join(missing),
        recommendation="Keep the StageMetrics data product schema stable for downstream automation.",
        created_at=created_at,
    )


def _positive_metric_result(
    dataframe: Any,
    *,
    rules: ContractRules,
    validation_run_id: str,
    source_path: str,
    created_at: str,
    metric_column: str,
    rule_id: str,
    rule_name: str,
    severity_key: str,
    recommendation: str,
) -> ContractRuleResult:
    from pyspark.sql import functions as F

    if metric_column not in dataframe.columns:
        failed_count = 0
        sample = ""
    else:
        condition = F.col(metric_column).isNull() | (F.col(metric_column) <= 0)
        failed_count = _count_failed(dataframe, condition)
        sample = _sample_failed_keys(dataframe, condition, rules)

    return _result(
        validation_run_id=validation_run_id,
        source_path=source_path,
        rules=rules,
        rule_id=rule_id,
        rule_name=rule_name,
        rule_type=SEMANTIC,
        severity_key=severity_key,
        failed_count=failed_count,
        sample_failed_keys=sample,
        recommendation=recommendation,
        created_at=created_at,
    )


def _non_negative_metrics_result(
    dataframe: Any,
    *,
    rules: ContractRules,
    validation_run_id: str,
    source_path: str,
    created_at: str,
) -> ContractRuleResult:
    from pyspark.sql import functions as F

    conditions = [
        F.col(metric).isNull() | (F.col(metric) < 0)
        for metric in rules.semantic_rules.non_negative_metrics
        if metric in dataframe.columns
    ]
    condition = _or_conditions(conditions)
    failed_count = _count_failed(dataframe, condition) if condition is not None else 0
    sample = (
        _sample_failed_keys(dataframe, condition, rules)
        if condition is not None
        else ""
    )

    return _result(
        validation_run_id=validation_run_id,
        source_path=source_path,
        rules=rules,
        rule_id="SEMANTIC_NON_NEGATIVE_METRICS",
        rule_name="numeric metrics must be non-negative",
        rule_type=SEMANTIC,
        severity_key="non_negative_metric_violation",
        failed_count=failed_count,
        sample_failed_keys=sample,
        recommendation="Reject negative counters before they feed budgets, dashboards, or drift analysis.",
        created_at=created_at,
    )


def _created_at_not_null_result(
    dataframe: Any,
    *,
    rules: ContractRules,
    validation_run_id: str,
    source_path: str,
    created_at: str,
) -> ContractRuleResult:
    from pyspark.sql import functions as F

    if "created_at" not in dataframe.columns or not rules.semantic_rules.created_at_not_null:
        failed_count = 0
        sample = ""
    else:
        condition = F.col("created_at").isNull()
        failed_count = _count_failed(dataframe, condition)
        sample = _sample_failed_keys(dataframe, condition, rules)

    return _result(
        validation_run_id=validation_run_id,
        source_path=source_path,
        rules=rules,
        rule_id="SEMANTIC_CREATED_AT_NOT_NULL",
        rule_name="created_at must be present",
        rule_type=SEMANTIC,
        severity_key="null_created_at",
        failed_count=failed_count,
        sample_failed_keys=sample,
        recommendation="Timestamp every metrics row so it can support audit and historical analysis.",
        created_at=created_at,
    )


def _identity_columns_not_null_result(
    dataframe: Any,
    *,
    rules: ContractRules,
    validation_run_id: str,
    source_path: str,
    created_at: str,
) -> ContractRuleResult:
    from pyspark.sql import functions as F

    conditions = [
        F.col(column).isNull()
        for column in rules.correlation_rules.required_identity_columns
        if column in dataframe.columns
    ]
    condition = _or_conditions(conditions)
    failed_count = _count_failed(dataframe, condition) if condition is not None else 0
    sample = (
        _sample_failed_keys(dataframe, condition, rules)
        if condition is not None
        else ""
    )

    return _result(
        validation_run_id=validation_run_id,
        source_path=source_path,
        rules=rules,
        rule_id="CORRELATION_IDENTITY_NOT_NULL",
        rule_name="identity columns must not be null",
        rule_type=CORRELATION,
        severity_key="null_identity_column",
        failed_count=failed_count,
        sample_failed_keys=sample,
        recommendation="Preserve correlation keys so metrics can be joined, grouped, and audited.",
        created_at=created_at,
    )


def _expected_values_result(
    dataframe: Any,
    *,
    rules: ContractRules,
    validation_run_id: str,
    source_path: str,
    created_at: str,
) -> ContractRuleResult:
    from pyspark.sql import functions as F

    expected_values = rules.correlation_rules.expected_values or {}
    conditions = [
        F.col(column).isNull() | (F.col(column) != F.lit(expected))
        for column, expected in expected_values.items()
        if column in dataframe.columns
    ]
    condition = _or_conditions(conditions)
    failed_count = _count_failed(dataframe, condition) if condition is not None else 0
    sample = (
        _sample_failed_keys(dataframe, condition, rules)
        if condition is not None
        else ""
    )

    return _result(
        validation_run_id=validation_run_id,
        source_path=source_path,
        rules=rules,
        rule_id="CORRELATION_EXPECTED_VALUES",
        rule_name="collector identity fields must match expected values",
        rule_type=CORRELATION,
        severity_key="invalid_expected_value",
        failed_count=failed_count,
        sample_failed_keys=sample,
        recommendation="Do not mix stage metrics with task metrics or unknown collectors in the same contract.",
        created_at=created_at,
    )


def _uniqueness_key_result(
    dataframe: Any,
    *,
    rules: ContractRules,
    validation_run_id: str,
    source_path: str,
    created_at: str,
) -> ContractRuleResult:
    from pyspark.sql import functions as F

    key_columns = tuple(
        column
        for column in rules.correlation_rules.uniqueness_key
        if column in dataframe.columns
    )
    if len(key_columns) != len(rules.correlation_rules.uniqueness_key):
        failed_count = 0
        sample = ""
    else:
        duplicates = (
            dataframe.groupBy(*key_columns)
            .count()
            .filter(F.col("count") > 1)
        )
        failed_count = int(duplicates.count())
        sample_rows = duplicates.select(*key_columns).limit(5).collect()
        sample = ";".join(
            json.dumps(row.asDict(recursive=True), sort_keys=True, default=str)
            for row in sample_rows
        )

    return _result(
        validation_run_id=validation_run_id,
        source_path=source_path,
        rules=rules,
        rule_id="CORRELATION_UNIQUENESS_KEY",
        rule_name="metrics identity key must be unique",
        rule_type=CORRELATION,
        severity_key="duplicate_uniqueness_key",
        failed_count=failed_count,
        sample_failed_keys=sample,
        recommendation="Use unique run identity so automation does not double-count the same workload evidence.",
        created_at=created_at,
    )


def _result(
    *,
    validation_run_id: str,
    source_path: str,
    rules: ContractRules,
    rule_id: str,
    rule_name: str,
    rule_type: str,
    severity_key: str,
    failed_count: int,
    sample_failed_keys: str,
    recommendation: str,
    created_at: str,
) -> ContractRuleResult:
    return ContractRuleResult(
        validation_run_id=validation_run_id,
        source_path=source_path,
        contract_version=rules.version,
        rule_id=rule_id,
        rule_name=rule_name,
        rule_type=rule_type,
        severity=rules.severity.get(severity_key, "ERROR"),
        decision=FAIL if failed_count > 0 else PASS,
        failed_count=failed_count,
        sample_failed_keys=sample_failed_keys,
        recommendation=recommendation,
        created_at=created_at,
    )


def _count_failed(dataframe: Any, condition: Any) -> int:
    return int(dataframe.filter(condition).count())


def _sample_failed_keys(
    dataframe: Any,
    condition: Any,
    rules: ContractRules,
) -> str:
    key_columns = [
        column
        for column in rules.correlation_rules.uniqueness_key
        if column in dataframe.columns
    ]
    if not key_columns:
        return ""
    rows = dataframe.filter(condition).select(*key_columns).limit(5).collect()
    return ";".join(
        json.dumps(row.asDict(recursive=True), sort_keys=True, default=str)
        for row in rows
    )


def _or_conditions(conditions: list[Any]) -> Any | None:
    if not conditions:
        return None
    return reduce(lambda left, right: left | right, conditions)


def _load_yaml(path: Path, *, label: str) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as config_file:
            loaded = yaml.safe_load(config_file) or {}
    except yaml.YAMLError as exc:
        raise ValueError(f"Invalid {label} YAML: {path}") from exc
    if not isinstance(loaded, dict):
        raise ValueError(f"{label} YAML must contain a mapping: {path}")
    return loaded


def _metric_int(metrics: Mapping[str, int | float], key: str) -> int:
    value = metrics.get(key, 0)
    if value is None:
        return 0
    return int(value)


def _positive_int(value: Any, field_name: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise ValueError(f"Lab 6 {field_name} must be >= 1")
    return parsed


def _boxed_lines(lines: list[str], *, width: int) -> str:
    normalized_width = max(width, 72)
    content_width = normalized_width - 4
    border = "═" * (normalized_width - 2)
    rendered = [f"\n╔{border}╗"]
    for line in lines:
        if not line:
            rendered.append(f"║ {' ' * content_width} ║")
            continue
        for wrapped in _wrap_line(line, content_width):
            rendered.append(f"║ {wrapped.ljust(content_width)} ║")
    rendered.append(f"╚{border}╝")
    return "\n".join(rendered)


def _wrap_line(line: str, width: int) -> list[str]:
    words = line.split()
    if not words:
        return [""]
    wrapped: list[str] = []
    current = words[0]
    for word in words[1:]:
        if len(current) + 1 + len(word) > width:
            wrapped.append(current)
            current = word
        else:
            current = f"{current} {word}"
    wrapped.append(current)
    return wrapped


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
