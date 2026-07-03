"""Lab 7C Streamlit dashboard for temporal backfill StageMetrics."""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

try:  # pragma: no cover - package import path
    from .config import load_dashboard_config
    from .data import load_metrics_dataframe
except ImportError:  # pragma: no cover - Streamlit script path
    from config import load_dashboard_config
    from data import load_metrics_dataframe


st.set_page_config(
    page_title="Lab 7C - Temporal Backfill Observability",
    page_icon="📈",
    layout="wide",
)


@st.cache_data(ttl=15)
def cached_metrics(metrics_uri: str, endpoint: str, access_key: str, secret_key: str, region: str):
    config = load_dashboard_config(
        {
            "LAB7_DASHBOARD_METRICS_URI": metrics_uri,
            "LAB7_DASHBOARD_MINIO_ENDPOINT": endpoint,
            "MINIO_ROOT_USER": access_key,
            "MINIO_ROOT_PASSWORD": secret_key,
            "AWS_REGION": region,
        }
    )
    return load_metrics_dataframe(config)


def human_bytes(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    units = ("B", "KB", "MB", "GB", "TB")
    number = float(value)
    for unit in units:
        if abs(number) < 1024.0 or unit == units[-1]:
            return f"{number:,.1f} {unit}"
        number /= 1024.0
    return f"{number:,.1f} TB"


def human_number(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    return f"{float(value):,.0f}"


def prepare_metrics(frame: pd.DataFrame) -> pd.DataFrame:
    data = frame.copy()
    data["processing_date"] = pd.to_datetime(data["processing_date"])
    data["created_at"] = pd.to_datetime(data["created_at"])
    data["processing_date_label"] = data["processing_date"].dt.strftime("%Y-%m-%d")
    data["spike_label"] = data["spike_label"].fillna("UNKNOWN")
    return data.sort_values("processing_date")


def latest_run_id(data: pd.DataFrame) -> str:
    latest_by_run = data.groupby("run_id", as_index=False)["created_at"].max()
    latest = latest_by_run.sort_values("created_at").iloc[-1]
    return str(latest["run_id"])


def render_empty_state(error: Exception) -> None:
    st.error("Lab 7B metrics are not available yet.")
    st.code(
        """
make compose
make generate-lab7
bash src/apps/labs/lab_7/run_daily_backfill_stage_metrics.sh
make lab7-dashboard
""".strip(),
        language="bash",
    )
    st.caption(f"Reader error: {error}")


def render_dashboard(data: pd.DataFrame) -> None:
    spike_days = data[data["spike_label"].isin(["MEDIUM_SPIKE", "VOLUME_SPIKE"])]
    max_shuffle = data.loc[data["shuffle_bytes_written"].idxmax()]
    max_volume = data.loc[data["source_rows_for_date"].idxmax()]

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Processed dates", human_number(len(data)))
    col2.metric("Expected source rows", human_number(data["source_rows_for_date"].sum()))
    col3.metric("Spike days", human_number(len(spike_days)))
    col4.metric("Max shuffle day", str(max_shuffle["processing_date_label"]))

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Max expected rows", human_number(max_volume["source_rows_for_date"]))
    col2.metric("Max records read", human_number(data["records_read"].max()))
    col3.metric("Max shuffle written", human_bytes(data["shuffle_bytes_written"].max()))
    col4.metric("Max executor runtime", f"{data['executor_run_time_ms'].max():,.0f} ms")

    color_map = {
        "NORMAL": "#4C78A8",
        "MEDIUM_SPIKE": "#F58518",
        "VOLUME_SPIKE": "#E45756",
        "UNKNOWN": "#9D9D9D",
    }

    st.subheader("Daily source volume plan")
    st.plotly_chart(
        px.bar(
            data,
            x="processing_date_label",
            y="source_rows_for_date",
            color="spike_label",
            color_discrete_map=color_map,
            labels={
                "processing_date_label": "processing date",
                "source_rows_for_date": "expected rows",
                "spike_label": "volume class",
            },
            title="Expected source rows by business date",
        ),
        use_container_width=True,
    )

    left, right = st.columns(2)
    with left:
        st.subheader("Records read")
        st.plotly_chart(
            px.line(
                data,
                x="processing_date_label",
                y="records_read",
                markers=True,
                color="spike_label",
                color_discrete_map=color_map,
                labels={"records_read": "records read", "processing_date_label": "processing date"},
                title="StageMetrics recordsRead follows the temporal volume plan",
            ),
            use_container_width=True,
        )
    with right:
        st.subheader("Shuffle written")
        st.plotly_chart(
            px.line(
                data,
                x="processing_date_label",
                y="shuffle_bytes_written",
                markers=True,
                color="spike_label",
                color_discrete_map=color_map,
                labels={
                    "shuffle_bytes_written": "shuffle bytes written",
                    "processing_date_label": "processing date",
                },
                title="Shuffle grows on larger business dates",
            ),
            use_container_width=True,
        )

    left, right = st.columns(2)
    with left:
        st.subheader("Volume versus executor runtime")
        st.plotly_chart(
            px.scatter(
                data,
                x="source_rows_for_date",
                y="executor_run_time_ms",
                color="spike_label",
                size="shuffle_bytes_written",
                hover_name="processing_date_label",
                color_discrete_map=color_map,
                labels={
                    "source_rows_for_date": "expected rows",
                    "executor_run_time_ms": "executor runtime ms",
                },
                title="Runtime signal is useful, but local startup overhead matters",
            ),
            use_container_width=True,
        )
    with right:
        st.subheader("Volume versus shuffle")
        st.plotly_chart(
            px.scatter(
                data,
                x="source_rows_for_date",
                y="shuffle_bytes_written",
                color="spike_label",
                size="records_read",
                hover_name="processing_date_label",
                color_discrete_map=color_map,
                labels={
                    "source_rows_for_date": "expected rows",
                    "shuffle_bytes_written": "shuffle bytes written",
                },
                title="Shuffle is the strongest local execution-shape signal",
            ),
            use_container_width=True,
        )

    st.subheader("Normalized execution view")
    normalized = data[
        [
            "processing_date_label",
            "spike_label",
            "runtime_per_million_rows",
            "shuffle_per_million_rows",
            "tasks_per_million_rows",
        ]
    ].melt(
        id_vars=["processing_date_label", "spike_label"],
        var_name="metric",
        value_name="value",
    )
    st.plotly_chart(
        px.line(
            normalized,
            x="processing_date_label",
            y="value",
            color="metric",
            line_dash="spike_label",
            markers=True,
            labels={
                "processing_date_label": "processing date",
                "value": "metric value",
                "metric": "normalized metric",
            },
            title="Normalize metrics before comparing dates with different volumes",
        ),
        use_container_width=True,
    )

    st.subheader("Classroom interpretation")
    st.info(
        """
Read the dashboard from left to right:

1. Confirm the temporal volume plan.
2. Check whether StageMetrics records read follows the expected volume.
3. Compare shuffle growth against spike days.
4. Treat runtime carefully because this local lab starts one Spark application per date.
5. Use normalized metrics to make fairer comparisons across dates.
""".strip()
    )

    st.subheader("Metrics table")
    st.dataframe(
        data[
            [
                "run_id",
                "processing_date_label",
                "spike_label",
                "filter_strategy",
                "volume_multiplier",
                "source_rows_for_date",
                "records_read",
                "records_read_to_expected_ratio",
                "executor_run_time_ms",
                "shuffle_bytes_written",
                "shuffle_written_per_source_row",
                "num_stages",
                "num_tasks",
                "jvm_gc_time_ms",
                "memory_bytes_spilled",
                "disk_bytes_spilled",
            ]
        ],
        use_container_width=True,
        hide_index=True,
    )


config = load_dashboard_config()

st.title("Lab 7C - Temporal Backfill Observability")
st.caption("A read-only Streamlit view over Lab 7B sparkMeasure StageMetrics persisted as Delta.")

with st.sidebar:
    st.header("Input")
    st.code(config.metrics_uri, language="text")
    st.caption("DuckDB reads this Delta table directly from MinIO.")
    if st.button("Refresh metrics"):
        st.cache_data.clear()

try:
    raw = cached_metrics(
        config.metrics_uri,
        config.minio_endpoint,
        config.access_key,
        config.secret_key,
        config.region,
    )
    if raw.empty:
        raise RuntimeError("Lab 7B metrics Delta table is empty.")
    prepared = prepare_metrics(raw)
    run_ids = sorted(prepared["run_id"].dropna().unique().tolist())
    default_run_id = latest_run_id(prepared)
    with st.sidebar:
        selected_run_id = st.selectbox(
            "Batch run_id",
            run_ids,
            index=run_ids.index(default_run_id),
            help="Lab 7B appends metrics. Select one batch to avoid mixing reruns.",
        )
    selected = prepared[prepared["run_id"] == selected_run_id]
    st.caption(f"Showing batch run_id: `{selected_run_id}`")
    render_dashboard(selected)
except Exception as exc:
    render_empty_state(exc)
