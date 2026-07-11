# Lab 6 guide: stage metrics contract gate

This guide is the classroom runbook for Lab 6.

Goal:

```text
run one Spark workload
  -> collect contract-ready StageMetrics
  -> persist the clean raw metrics row
  -> validate schema, semantic, and correlation contracts
  -> persist rule-level evidence and a summary decision
  -> demonstrate a controlled contract failure without corrupting clean data
```

Lab 5 used stage-level evidence as policy. Lab 6 inserts a trust boundary before
that policy: metrics must behave like a reliable operational data product.

Class notes:

[Stage metrics contract gate class
notes](docs/stage_metrics_contract_gate_class_notes.md)

Keep those notes open for metric availability, consumer assumptions, failure
types, and the production data-product rationale.

## 0. Confirm the shared workshop prerequisites

Start from the repository root:

```bash
cd workshop-spark-measures
```

This guide assumes that the workshop images and shared Bronze retail sources
already exist.

If this is the first workshop run, or project images and MinIO data were
removed, follow [Lab 0 guide: bootstrap through Bronze data
generation](../lab_0/guide_lab0.md). Follow sections 1 through 5 there; the
full bootstrap sequence is intentionally not duplicated here.

If only the containers were stopped and MinIO data remains, restart the stack:

```bash
make compose
```

Expected shared sources:

```text
s3a://lakehouse/bronze/retail/sales
s3a://lakehouse/bronze/retail/vendors
s3a://lakehouse/bronze/retail/products
s3a://lakehouse/bronze/retail/customers
```

Teacher notes:

```text
Labs 1-6 share the generated retail sources. `make down` preserves MinIO data.
Regenerate only after `make clean-data` or when a required Bronze table is
missing.
```

## 1. Move to the Lab 6 folder

The remaining commands assume this directory:

```bash
cd src/apps/labs/lab_6
```

Optional sanity check:

```bash
ls
```

Expected classroom entry points:

```text
lab_6_stage_metrics_contract_gate.py
run_stage_metrics_contract_gate.sh
```

## 2. Establish the trust question

Ask the class:

```text
Can we trust this metrics row before using it for automation?
```

The platform flow is:

```text
collect metrics
  -> validate the contract
  -> use trustworthy evidence in dashboards, alerts, reviews, and gates
```

Teacher notes:

```text
Observability metrics are data. Once automation consumes them, a wrong metric
row can produce a wrong engineering decision even when the Spark workload
itself completed successfully.
```

## 3. Understand the workload and collection boundary

Open:

```text
lab_6_stage_metrics_contract_gate.py
lab_6_utils/transformations.py
lab_6_utils/experiments.yaml
```

The workload performs:

```text
sales
  -> prune fact columns
  -> join vendor region
  -> join product category
  -> join customer context
  -> select the business fact
  -> repartition by month, region, and category
  -> aggregate revenue, orders, and customers
  -> write the Gold output
```

The business output is intentionally ordinary:

```text
order_month
region
category
gross_revenue
order_count
customer_count
```

StageMetrics wraps the executed workload and captures one aggregate metrics
map. This lab is not diagnosing a pathological job. It is validating the
observability record produced by a valid Spark action.

## 4. Inspect the contract before running it

Open:

```text
lab_6_utils/contract_rules.yaml
```

The contract version is explicit:

```yaml
contract:
  version: "1.0.0"
```

The contract validates three layers.

### 4.1 Schema contract

Question:

```text
Do the columns required by downstream automation exist?
```

Examples include `run_id`, `app_name`, `workload_name`, `num_stages`,
`num_tasks`, `executor_run_time_ms`, and `created_at`.

### 4.2 Semantic contract

Question:

```text
Do the values make operational sense?
```

Examples include positive stage/task counts, non-negative counters, and a
non-null creation timestamp.

### 4.3 Correlation contract

Question:

```text
Can the row be traced, joined, grouped, and audited safely?
```

The layer validates identity fields, expected collector/scope values, and the
configured uniqueness key.

Teacher notes:

```text
The lab is a focused reliability layer, not a generic data-quality framework.
Each rule exists because a downstream engineering consumer makes an assumption
about schema, meaning, or identity.
```

## 5. Understand required and optional metrics

Required metrics:

```text
num_stages
num_tasks
executor_run_time_ms
```

Optional collector-dependent metrics:

```text
shuffle_bytes_written
shuffle_bytes_read
jvm_gc_time_ms
memory_bytes_spilled
disk_bytes_spilled
input_bytes
```

Each optional metric has an explicit availability field:

```text
shuffle_bytes_written
shuffle_bytes_written_available
```

This preserves the difference between:

```text
value=0, available=true  -> the collector emitted a real zero
value=0, available=false -> the counter was unavailable; zero is a placeholder
```

Teacher notes:

```text
Metric absence and metric value zero are different facts. Collapsing them into
one number makes dashboards and automated decisions look confident when the
evidence is actually missing.
```

## 6. Run the clean passing scenario

Use the classroom runner:

```bash
bash run_stage_metrics_contract_gate.sh
```

Expected progress markers:

```text
LAB6_STAGE_METRICS_CAPTURED_OK
LAB6_STAGE_METRICS_INPUT_OK
LAB6_CONTRACT_RULES_LOADED_OK
LAB6_SCHEMA_CONTRACT_EVALUATED
LAB6_SEMANTIC_CONTRACT_EVALUATED
LAB6_CORRELATION_CONTRACT_EVALUATED
LAB6_CONTRACT_RESULTS_WRITTEN_OK
```

Expected final marker:

```text
LAB6_STAGE_METRICS_CONTRACT_PASS
```

The clean raw row should satisfy all three contract layers.

Optional expanded submit command from the Lab 6 folder:

```bash
docker compose --env-file ../../../../.env -f ../../../../build/docker-compose.yml exec -T spark-master \
  env PYTHONPATH=/opt/spark/src:/opt/spark/generator/src \
    LAB6_CONFIG_NAME=lab6-stage-metrics-contract-gate \
    LAB6_INJECT_INVALID_RECORDS=false \
    /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --conf spark.driver.host=spark-master \
    --conf spark.eventLog.dir=s3a://observability/event-logs \
    --conf spark.executorEnv.PYTHONPATH=/opt/spark/src:/opt/spark/generator/src \
    /opt/spark/src/apps/labs/lab_6/lab_6_stage_metrics_contract_gate.py \
    --inject-invalid-records false
```

## 7. Read the final contract block

Near the end of each submit, the app prints one boxed block:

```text
## LAB 6 STAGE METRICS CONTRACT GATE

### Final contract decision
decision: ...
demo_mode: ...
total_rules: ...
passed_rules: ...
failed_rules: ...

### Optional metric availability
status: ...

### Contract layers
schema: ...
semantic: ...
correlation: ...

### Failed rule details
rule_id: failed_count=... sample=...

### Delta outputs
...
```

Read it in this order:

1. confirm whether this is the clean or demo input;
2. read the overall decision and rule counts;
3. verify optional metric availability metadata;
4. identify which contract layer failed;
5. use rule IDs, failed counts, and sample keys to make the result actionable.

Teacher notes:

```text
The layer markers say EVALUATED because an evaluated layer may return PASS or
FAIL. The final contract marker communicates the overall decision.
```

## 8. Run the controlled failure demonstration

Run:

```bash
LAB6_INJECT_INVALID_RECORDS=true \
bash run_stage_metrics_contract_gate.sh
```

The application keeps the valid raw StageMetrics row in the clean table and
writes a separate demonstration input containing:

- the clean base row;
- null `run_id`;
- zero stages;
- zero tasks;
- negative shuffle bytes;
- null `created_at`;
- invalid `metric_scope=task`;
- a duplicate uniqueness key.

Expected final marker:

```text
LAB6_STAGE_METRICS_CONTRACT_FAIL
```

This is an expected policy result and the runner exits with status `0`. A
technical error—missing sources, invalid YAML, broken Spark session, or failed
Delta write—still exits non-zero.

Teacher notes:

```text
The demo does not mutate or corrupt the clean raw metrics table. Controlled bad
records are isolated so students can inspect contract failures safely and the
next clean run remains trustworthy.
```

## 9. Optional: inspect the persisted evidence

Business output:

```text
s3a://lakehouse/gold/lab6/stage_metrics_contract_gate/business_output
```

Clean raw StageMetrics:

```text
s3a://observability/lab6/stage_metrics_raw
```

Controlled demo validation input:

```text
s3a://observability/lab6/stage_metrics_contract_demo_input
```

One row per evaluated rule is appended to:

```text
s3a://observability/lab6/stage_metrics_contract_results
```

Each rule row includes its type, severity, decision, failed count, sample keys,
recommendation, contract version, and validation run ID.

One summary row per validation is appended to:

```text
s3a://observability/lab6/stage_metrics_contract_summary
```

Use the MinIO Console:

```text
http://127.0.0.1:29011
```

Default credentials:

```text
user:     sparkworkshop
password: sparkworkshop123
```

## 10. Connect contract fields to consumers

| Consumer | Contract assumption |
| --- | --- |
| Dashboard | Identity and timestamps exist for grouping and filtering. |
| Alerts | Runtime, shuffle, spill, and GC values are semantically valid. |
| Runtime budgets | Required metrics and availability flags are trustworthy. |
| PR review | Run and workload variant identify the evidence being compared. |
| Drift monitoring | Stable keys prevent duplicate inflation across history. |

Teacher notes:

```text
The contract is valuable because a consumer depends on it. Rules without a
known consumer assumption quickly become ceremonial checks with no operational
owner.
```

## 11. Optional: correlate with Spark History Server

Open:

```text
http://127.0.0.1:28090
```

The History Server explains the Spark application that produced the row. The
contract gate answers a different question: whether that row is safe for
downstream automation.

## 12. Classroom conclusion

End with:

```text
A mature Spark observability platform does not only collect metrics. It
validates that the evidence is reliable enough to support engineering
decisions.
```

The progression is:

```text
collect
  -> preserve identity and availability
  -> validate schema, meaning, and correlation
  -> persist rule-level evidence
  -> automate only after the contract passes
```

## 13. Optional cleanup after class

From the repository root:

```bash
make down
```

This stops the stack and preserves generated MinIO data. Use the project soft
cleanup drill only when the next workshop run must start from a clean state.
