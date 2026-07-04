"""Lab 7 Streamlit dashboard for temporal backfill StageMetrics."""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

try:  # pragma: no cover - package import path
    from .config import load_dashboard_config
    from .data import load_metrics_dataframe
except ImportError:  # pragma: no cover - Streamlit script path
    from config import load_dashboard_config
    from data import load_metrics_dataframe


st.set_page_config(
    page_title="Lab 7 - Temporal Backfill Observability",
    page_icon="📈",
    layout="wide",
)

CHART_BG = "#0E1117"
CHART_GRID = "rgba(148, 163, 184, 0.26)"
CHART_FONT = "#F8FAFC"
px.defaults.template = "plotly_dark"


def apply_dark_chart_theme(fig: go.Figure) -> go.Figure:
    """Make Plotly figures readable inside Streamlit's dark theme."""

    title_text = fig.layout.title.text
    fig.update_layout(
        template="plotly_dark",
        plot_bgcolor=CHART_BG,
        paper_bgcolor=CHART_BG,
        font={"color": CHART_FONT},
        legend={
            "orientation": "h",
            "yanchor": "bottom",
            "y": 1.08,
            "xanchor": "left",
            "x": 0,
            "font": {"color": CHART_FONT},
        },
        margin={"l": 60, "r": 60, "t": 80, "b": 70},
    )
    if title_text:
        fig.update_layout(title_font={"color": CHART_FONT})
    else:
        fig.update_layout(title_text="")
    fig.update_xaxes(
        showgrid=False,
        tickfont={"color": CHART_FONT},
        title_font={"color": CHART_FONT},
        linecolor=CHART_GRID,
        zerolinecolor=CHART_GRID,
    )
    fig.update_yaxes(
        gridcolor=CHART_GRID,
        tickfont={"color": CHART_FONT},
        title_font={"color": CHART_FONT},
        linecolor=CHART_GRID,
        zerolinecolor=CHART_GRID,
    )
    return fig


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
    st.error("Lab 7 metrics are not available yet.")
    st.code(
        """
bash src/apps/labs/lab_7/run_temporal_backfill_observability.sh
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

    st.subheader("Stage pressure signals by processing date")
    signal = data.copy()
    signal["shuffle_written_mb"] = signal["shuffle_bytes_written"] / (1024 * 1024)
    signal["shuffle_read_mb"] = signal["shuffle_bytes_read"] / (1024 * 1024)
    signal["source_rows_millions"] = signal["source_rows_for_date"] / 1_000_000
    signal["memory_spill_mb"] = signal["memory_bytes_spilled"] / (1024 * 1024)
    signal["disk_spill_mb"] = signal["disk_bytes_spilled"] / (1024 * 1024)
    signal["gc_ratio_pct"] = (
        signal["jvm_gc_time_ms"] / signal["executor_run_time_ms"].where(signal["executor_run_time_ms"] > 0)
    ).fillna(0) * 100

    st.markdown("**Chart input data**")
    st.dataframe(
        signal[
            [
                "processing_date_label",
                "spike_label",
                "source_rows_for_date",
                "records_read",
                "shuffle_bytes_written",
                "shuffle_bytes_read",
                "shuffle_written_mb",
                "shuffle_read_mb",
                "memory_bytes_spilled",
                "disk_bytes_spilled",
                "memory_spill_mb",
                "disk_spill_mb",
                "jvm_gc_time_ms",
                "executor_run_time_ms",
                "gc_ratio_pct",
                "runtime_per_million_rows",
                "shuffle_per_million_rows",
                "tasks_per_million_rows",
            ]
        ].rename(
            columns={
                "processing_date_label": "processing_date",
                "source_rows_for_date": "expected_source_rows",
                "shuffle_written_mb": "shuffle_written_mb",
                "shuffle_read_mb": "shuffle_read_mb",
                "memory_spill_mb": "memory_spill_mb",
                "disk_spill_mb": "disk_spill_mb",
                "gc_ratio_pct": "gc_ratio_pct",
            }
        ),
        use_container_width=True,
        hide_index=True,
    )

    st.subheader("Shuffle timeline with source-volume context")
    shuffle_fig = make_subplots(specs=[[{"secondary_y": True}]])
    shuffle_customdata = signal[
        ["source_rows_for_date", "records_read", "shuffle_bytes_written", "shuffle_bytes_read", "spike_label"]
    ].to_numpy()
    shuffle_fig.add_trace(
        go.Bar(
            x=signal["processing_date_label"],
            y=signal["shuffle_written_mb"],
            customdata=shuffle_customdata,
            name="shuffle written MB",
            marker_color=[color_map.get(label, color_map["UNKNOWN"]) for label in signal["spike_label"]],
            marker_line={"width": 1, "color": "rgba(248, 250, 252, 0.55)"},
            hovertemplate=(
                "%{x}<br>shuffle written=%{customdata[2]:,.0f} B"
                "<br>records read=%{customdata[1]:,.0f}"
                "<br>expected rows=%{customdata[0]:,.0f}"
                "<br>class=%{customdata[4]}<extra>%{fullData.name}</extra>"
            ),
        ),
        secondary_y=False,
    )
    shuffle_fig.add_trace(
        go.Scatter(
            x=signal["processing_date_label"],
            y=signal["shuffle_read_mb"],
            customdata=shuffle_customdata,
            mode="lines+markers",
            name="shuffle read MB",
            line={"color": "#E45756", "width": 3},
            marker={"size": 9, "line": {"width": 1, "color": "white"}},
            hovertemplate=(
                "%{x}<br>shuffle read=%{customdata[3]:,.0f} B"
                "<br>records read=%{customdata[1]:,.0f}"
                "<br>expected rows=%{customdata[0]:,.0f}"
                "<br>class=%{customdata[4]}<extra>%{fullData.name}</extra>"
            ),
        ),
        secondary_y=False,
    )
    shuffle_fig.add_trace(
        go.Scatter(
            x=signal["processing_date_label"],
            y=signal["source_rows_millions"],
            mode="lines+markers",
            name="source rows M",
            line={"color": "#4C78A8", "width": 3, "dash": "dot"},
            marker={"size": 9, "symbol": "diamond", "line": {"width": 1, "color": "white"}},
            hovertemplate="%{x}<br>source rows=%{y:.3f}M<extra>%{fullData.name}</extra>",
        ),
        secondary_y=True,
    )
    shuffle_fig.update_layout(
        barmode="group",
        height=540,
        hovermode="x unified",
    )
    shuffle_fig.update_xaxes(type="category", title_text="processing date", tickangle=-35)
    shuffle_fig.update_yaxes(
        title_text="shuffle MB",
        rangemode="tozero",
        secondary_y=False,
    )
    shuffle_fig.update_yaxes(title_text="source rows M", rangemode="tozero", secondary_y=True)
    st.plotly_chart(apply_dark_chart_theme(shuffle_fig), use_container_width=True)

    st.subheader("Memory pressure: spills and GC ratio")
    memory_max = max(
        0.05,
        float(signal[["memory_spill_mb", "disk_spill_mb"]].max().max()),
    )
    memory_fig = make_subplots(specs=[[{"secondary_y": True}]])
    memory_customdata = signal[
        ["memory_bytes_spilled", "disk_bytes_spilled", "jvm_gc_time_ms", "executor_run_time_ms", "spike_label"]
    ].to_numpy()
    memory_fig.add_trace(
        go.Bar(
            x=signal["processing_date_label"],
            y=signal["memory_spill_mb"],
            customdata=memory_customdata,
            name="memory spill MB",
            marker_color="rgba(122, 81, 149, 0.72)",
            hovertemplate=(
                "%{x}<br>memory spill=%{customdata[0]:,.0f} B"
                "<br>GC time=%{customdata[2]:,.0f} ms"
                "<br>executor runtime=%{customdata[3]:,.0f} ms"
                "<br>class=%{customdata[4]}<extra>%{fullData.name}</extra>"
            ),
        ),
        secondary_y=False,
    )
    memory_fig.add_trace(
        go.Bar(
            x=signal["processing_date_label"],
            y=signal["disk_spill_mb"],
            customdata=memory_customdata,
            name="disk spill MB",
            marker_color="rgba(114, 183, 178, 0.72)",
            hovertemplate=(
                "%{x}<br>disk spill=%{customdata[1]:,.0f} B"
                "<br>GC time=%{customdata[2]:,.0f} ms"
                "<br>executor runtime=%{customdata[3]:,.0f} ms"
                "<br>class=%{customdata[4]}<extra>%{fullData.name}</extra>"
            ),
        ),
        secondary_y=False,
    )
    memory_fig.add_trace(
        go.Scatter(
            x=signal["processing_date_label"],
            y=signal["gc_ratio_pct"],
            customdata=memory_customdata,
            mode="lines+markers",
            name="GC ratio %",
            line={"color": "#7A5195", "width": 3, "dash": "dash"},
            marker={
                "size": 10,
                "symbol": "triangle-up",
                "color": [color_map.get(label, color_map["UNKNOWN"]) for label in signal["spike_label"]],
                "line": {"width": 1, "color": "white"},
            },
            hovertemplate=(
                "%{x}<br>GC ratio=%{y:.2f}%"
                "<br>GC time=%{customdata[2]:,.0f} ms"
                "<br>executor runtime=%{customdata[3]:,.0f} ms"
                "<br>class=%{customdata[4]}<extra>%{fullData.name}</extra>"
            ),
        ),
        secondary_y=True,
    )
    memory_fig.update_layout(
        barmode="group",
        height=520,
        hovermode="x unified",
    )
    memory_fig.update_xaxes(type="category", title_text="processing date", tickangle=-35)
    memory_fig.update_yaxes(
        title_text="spill MB",
        range=[0, memory_max * 1.15],
        secondary_y=False,
    )
    memory_fig.update_yaxes(title_text="GC ratio %", rangemode="tozero", secondary_y=True)
    st.plotly_chart(apply_dark_chart_theme(memory_fig), use_container_width=True)
    if signal[["memory_bytes_spilled", "disk_bytes_spilled"]].max().max() == 0:
        st.caption("No memory or disk spill detected in this selected run.")
    st.caption(
        "Shuffle uses StageMetrics shuffle counters. Memory pressure uses memory/disk spill counters plus "
        "JVM GC time divided by executor runtime. Zero spill is a valid low-pressure signal in this lab."
    )

    left, right = st.columns(2)
    with left:
        st.subheader("Volume versus executor runtime")
        runtime_fig = px.scatter(
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
        )
        st.plotly_chart(apply_dark_chart_theme(runtime_fig), use_container_width=True)
    with right:
        st.subheader("Volume versus shuffle")
        volume_shuffle_fig = px.scatter(
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
        )
        st.plotly_chart(apply_dark_chart_theme(volume_shuffle_fig), use_container_width=True)

    st.subheader("Normalized execution view")
    normal_days = data[data["spike_label"] == "NORMAL"]

    def normal_day_baseline(metric: str) -> float:
        baseline = normal_days[metric].replace(0, pd.NA).dropna().median()
        if pd.isna(baseline) or float(baseline) == 0:
            return 1.0
        return float(baseline)

    normalized_index = data.copy()
    index_metrics = {
        "runtime / M rows": "runtime_per_million_rows",
        "shuffle / M rows": "shuffle_per_million_rows",
        "tasks / M rows": "tasks_per_million_rows",
    }
    for label, metric in index_metrics.items():
        normalized_index[f"{metric}_index"] = normalized_index[metric] / normal_day_baseline(metric)

    normalized_customdata = normalized_index[
        [
            "spike_label",
            "source_rows_for_date",
            "runtime_per_million_rows",
            "shuffle_per_million_rows",
            "tasks_per_million_rows",
        ]
    ].to_numpy()
    normalized_fig = go.Figure()
    normalized_colors = {
        "runtime / M rows": "#636EFA",
        "shuffle / M rows": "#EF553B",
        "tasks / M rows": "#00CC96",
    }
    chronological_dates = normalized_index["processing_date_label"].tolist()
    marker_colors = [color_map.get(label, color_map["UNKNOWN"]) for label in normalized_index["spike_label"]]

    raw_normalized_fig = go.Figure()
    for label, metric in index_metrics.items():
        raw_normalized_fig.add_trace(
            go.Scatter(
                x=normalized_index["processing_date_label"],
                y=normalized_index[metric],
                customdata=normalized_customdata,
                mode="lines+markers",
                name=label,
                line={"color": normalized_colors[label], "width": 3},
                marker={
                    "size": 9,
                    "color": marker_colors,
                    "line": {"width": 1, "color": normalized_colors[label]},
                },
                hovertemplate=(
                    "%{x}<br>"
                    f"{label}=%{{y:,.1f}}"
                    "<br>class=%{customdata[0]}"
                    "<br>expected rows=%{customdata[1]:,.0f}"
                    "<extra>%{fullData.name}</extra>"
                ),
            )
        )
    raw_normalized_fig.update_layout(height=500, hovermode="x unified")
    raw_normalized_fig.update_xaxes(
        type="category",
        categoryorder="array",
        categoryarray=chronological_dates,
        title_text="processing date",
        tickangle=-35,
    )
    raw_normalized_fig.update_yaxes(
        title_text="value per 1M expected rows",
        type="log",
    )

    for label, metric in index_metrics.items():
        normalized_fig.add_trace(
            go.Scatter(
                x=normalized_index["processing_date_label"],
                y=normalized_index[f"{metric}_index"],
                customdata=normalized_customdata,
                mode="lines+markers",
                name=label,
                line={"color": normalized_colors[label], "width": 3},
                marker={
                    "size": 9,
                    "color": marker_colors,
                    "line": {"width": 1, "color": normalized_colors[label]},
                },
                hovertemplate=(
                    "%{x}<br>"
                    f"{label} index=%{{y:.2f}}x"
                    "<br>class=%{customdata[0]}"
                    "<br>expected rows=%{customdata[1]:,.0f}"
                    "<br>runtime/M rows=%{customdata[2]:,.1f}"
                    "<br>shuffle/M rows=%{customdata[3]:,.1f}"
                    "<br>tasks/M rows=%{customdata[4]:,.1f}"
                    "<extra>%{fullData.name}</extra>"
                ),
            )
        )
    normalized_fig.add_hline(
        y=1.0,
        line_dash="dot",
        line_color="rgba(248, 250, 252, 0.65)",
        annotation_text="normal-day median",
        annotation_position="top left",
        annotation_font_color=CHART_FONT,
    )
    normalized_fig.update_layout(height=520, hovermode="x unified")
    normalized_fig.update_xaxes(
        type="category",
        categoryorder="array",
        categoryarray=chronological_dates,
        title_text="processing date",
        tickangle=-35,
    )
    normalized_fig.update_yaxes(title_text="index vs normal-day median", rangemode="tozero")

    left, right = st.columns(2)
    with left:
        st.markdown("**Raw normalized metrics**")
        st.plotly_chart(apply_dark_chart_theme(raw_normalized_fig), use_container_width=True)
    with right:
        st.markdown("**Index versus normal-day median**")
        st.plotly_chart(apply_dark_chart_theme(normalized_fig), use_container_width=True)
    st.caption(
        "Both charts keep the processing dates in chronological order. "
        "The left chart uses a log scale because runtime, shuffle, and task density have different magnitudes. "
        "The right chart converts each metric to an index where 1.0 means the median NORMAL day."
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

st.title("Lab 7 - Temporal Backfill Observability")
st.caption("A read-only Streamlit view over Lab 7 sparkMeasure StageMetrics persisted as Delta.")

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
        raise RuntimeError("Lab 7 metrics Delta table is empty.")
    prepared = prepare_metrics(raw)
    run_ids = sorted(prepared["run_id"].dropna().unique().tolist())
    default_run_id = latest_run_id(prepared)
    with st.sidebar:
        selected_run_id = st.selectbox(
            "Batch run_id",
            run_ids,
            index=run_ids.index(default_run_id),
            help="Lab 7 appends metrics. Select one batch to avoid mixing reruns.",
        )
    selected = prepared[prepared["run_id"] == selected_run_id]
    st.caption(f"Showing batch run_id: `{selected_run_id}`")
    render_dashboard(selected)
except Exception as exc:
    render_empty_state(exc)
