# To-do: Lab 7C temporal backfill observability dashboard

## Goal

Create a small classroom dashboard that turns Lab 7B persisted StageMetrics into
an operational temporal view. The dashboard should make daily volume spikes,
shuffle growth, and normalized metrics visible without running Spark.

## Scope

- Add a Streamlit dashboard for Lab 7C.
- Read Lab 7B metrics from Delta on MinIO using DuckDB.
- Keep the dashboard stage-level only.
- Do not run Spark inside the dashboard container.
- Do not alter the Lab 7A generator or Lab 7B backfill workload.
- Keep the service optional in Docker Compose so the default workshop stack does
  not change.

## Expected inputs

- `s3a://observability/lab7/daily_backfill_stage_metrics`
- The dashboard should explain clearly when this input is missing and tell the
  instructor to run Lab 7A and Lab 7B first.

## Expected outputs

- Browser UI exposed on a local port.
- No new persistent Delta output is required for this slice.
- The dashboard is a read-only presentation layer over Lab 7B metrics.

## Visuals

Include at least:

- KPI cards for processed dates, expected rows, spike days, and max shuffle day.
- Timeline of expected source rows by processing date.
- Timeline of shuffle bytes written by processing date.
- Scatter plot of expected rows versus executor runtime.
- Scatter plot of expected rows versus shuffle bytes written.
- Normalized metric view for runtime and shuffle per million source rows.
- Data table with the metrics used by the visualizations.

## Docker and operational interface

- Add a lightweight Python dashboard image.
- Add an optional Docker Compose service, preferably behind a profile.
- Add a Makefile command to start the dashboard after the platform is up.
- Expose a stable local URL for classroom use.

## Documentation

- Update Lab 7 README with the dashboard flow.
- Add class commands for starting the dashboard.
- Document why DuckDB is used here: small analytical reader over persisted Delta
  metrics, not part of the Spark workload.
- Document the limitation: the dashboard depends on Lab 7B metrics being present.

## Validation

- Unit-test query/path/config helpers without requiring Streamlit.
- Validate shell/compose syntax.
- If local Docker supports it, build and start the dashboard service.
- If Lab 7B metrics are present, validate that DuckDB can read the Delta table.

## Teaching message

StageMetrics become more useful when they are persisted and viewed over time.
The purpose of Lab 7C is not to replace Spark UI. It is to show how a team can
turn stage-level evidence into a small operational dashboard for backfill review.
