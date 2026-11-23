"""Gold dashboard.

The main panel is the prediction chart: observed gage height against the
prediction the model issued for each target hour. The remaining panels are
supporting context (current level, recent error, hydrograph with rainfall).

Everything is read from the gold bucket; the dashboard never recomputes
features and never calls the model directly.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

TIMEZONE = "America/Los_Angeles"
CACHE_TTL_SECONDS = 300
DEFAULT_WINDOW_DAYS = 30


@st.cache_resource(show_spinner=False)
def _filesystem():
    """Return the filesystem, the table root and whether it is object storage."""

    import pyarrow.fs as pafs

    endpoint = os.getenv("S3_ENDPOINT_URL", "").strip()
    if not endpoint:
        return (
            pafs.LocalFileSystem(),
            os.getenv("GOLD_LOCAL_DIR", "/prefect/data/gold"),
            False,
        )

    parsed = urlparse(endpoint)
    filesystem = pafs.S3FileSystem(
        access_key=os.getenv("GARAGE_DEFAULT_ACCESS_KEY", ""),
        secret_key=os.getenv("GARAGE_DEFAULT_SECRET_KEY", ""),
        endpoint_override=parsed.netloc,
        scheme=parsed.scheme or "http",
        region=os.getenv("S3_REGION", "garage"),
    )
    return filesystem, os.getenv("S3_BUCKET_GOLD", "gold"), True


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def _read_table(
    name: str,
    since_column: str | None = None,
    since: str | None = None,
    columns: tuple[str, ...] | None = None,
) -> pd.DataFrame:
    """Read a gold table with filter pushdown, empty when it does not exist."""

    import pyarrow.dataset as ds
    import pyarrow.fs as pafs

    filesystem, base, _ = _filesystem()
    path = f"{base}/{name}"

    try:
        info = filesystem.get_file_info(path)
        if info.type == pafs.FileType.NotFound:
            return pd.DataFrame()
        dataset = ds.dataset(
            path, filesystem=filesystem, format="parquet", partitioning="hive"
        )
        expression = None
        if since_column and since:
            expression = ds.field(since_column) >= since
        frame = dataset.to_table(
            filter=expression, columns=list(columns) if columns else None
        ).to_pandas()
    except Exception:  # noqa: BLE001 - the dashboard must stay alive
        return pd.DataFrame()

    for column in ("station_id", "dataset_version", "discharge_grain", "status"):
        if column in frame.columns:
            frame[column] = frame[column].astype(str)
    for column in (
        "time_utc",
        "time_local",
        "issued_at",
        "target_time",
        "processed_at",
        "created_at",
    ):
        if column in frame.columns:
            frame[column] = pd.to_datetime(frame[column], utc=True, errors="coerce")
    return frame


HOURLY_COLUMNS = (
    "station_id",
    "time_utc",
    "time_local",
    "date_utc",
    "date_local",
    "stage_mean_ft",
    "stage_complete",
    "precip_mm",
    "discharge_grain",
    "dataset_version",
)

PREDICTION_COLUMNS = (
    "station_id",
    "issued_at",
    "target_time",
    "horizon",
    "y_pred",
    "y_obs",
    "model_version",
)


def load_hourly(since: str) -> pd.DataFrame:
    return _read_table(
        "station_hourly", "date_utc", since, HOURLY_COLUMNS
    )


def load_predictions(since: str) -> pd.DataFrame:
    return _read_table("predictions", "issue_date", since, PREDICTION_COLUMNS)


def load_runs() -> pd.DataFrame:
    return _read_table("dataset_versions")


def _local(frame: pd.DataFrame, column: str = "time_utc") -> pd.DataFrame:
    frame = frame.copy()
    stamp = pd.to_datetime(frame[column], utc=True, errors="coerce")
    frame[column] = stamp.dt.tz_convert(TIMEZONE)
    return frame


def _latest_predictions(predictions: pd.DataFrame, station: str, horizon: int) -> pd.DataFrame:
    if predictions.empty:
        return predictions
    subset = predictions[
        (predictions["station_id"] == station) & (predictions["horizon"] == horizon)
    ].dropna(subset=["target_time"])
    if subset.empty:
        return subset
    subset = subset.sort_values("issued_at").drop_duplicates(
        "target_time", keep="last"
    )
    return subset.sort_values("target_time")


def _prediction_chart(
    observed: pd.DataFrame,
    predictions: pd.DataFrame,
    station: str,
    horizon: int,
) -> go.Figure:
    figure = go.Figure()

    figure.add_trace(
        go.Scatter(
            x=_local(observed)["time_utc"],
            y=observed["stage_mean_ft"],
            mode="lines",
            name="Observasi",
            line={"color": "#334155", "width": 1.6},
        )
    )

    if not predictions.empty:
        local_predictions = _local(predictions, "target_time")
        figure.add_trace(
            go.Scatter(
                x=local_predictions["target_time"],
                y=predictions["y_pred"],
                mode="lines",
                name=f"Prediksi h+{horizon}",
                line={"color": "#2563eb", "width": 1.8},
            )
        )
        issued = local_predictions["issued_at"].max()
        if pd.notna(issued):
            figure.add_vline(
                x=issued.timestamp() * 1000,
                line={"color": "#f59e0b", "width": 1, "dash": "dot"},
                annotation_text="waktu terbit",
                annotation_position="top left",
            )

    figure.update_layout(
        height=430,
        margin={"l": 10, "r": 10, "t": 40, "b": 10},
        title=f"Prediksi vs observasi — stasiun {station}, horizon {horizon} jam",
        legend={"orientation": "h", "y": 1.02, "x": 0},
        hovermode="x unified",
    )
    figure.update_yaxes(title_text="ketinggian air (ft)")
    figure.update_xaxes(title_text=f"waktu lokal ({TIMEZONE})")
    return figure


def _hydrograph(station_hourly: pd.DataFrame, station: str) -> go.Figure:
    frame = station_hourly[station_hourly["station_id"] == station]
    local = _local(frame)
    figure = make_subplots(specs=[[{"secondary_y": True}]])
    figure.add_trace(
        go.Scatter(
            x=local["time_utc"],
            y=frame["stage_mean_ft"],
            name="ketinggian air",
            line={"color": "#0f766e", "width": 1.5},
        ),
        secondary_y=False,
    )
    if "precip_mm" in frame.columns:
        figure.add_trace(
            go.Bar(
                x=local["time_utc"],
                y=frame["precip_mm"],
                name="hujan (mm/jam)",
                marker={"color": "#94a3b8"},
                opacity=0.6,
            ),
            secondary_y=True,
        )
        figure.update_yaxes(title_text="hujan (mm/jam)", secondary_y=True, autorange="reversed")

    figure.update_layout(
        height=300,
        margin={"l": 10, "r": 10, "t": 30, "b": 10},
        legend={"orientation": "h", "y": 1.05, "x": 0},
        hovermode="x unified",
    )
    figure.update_yaxes(title_text="ketinggian air (ft)", secondary_y=False)
    return figure


def main() -> None:
    st.set_page_config(page_title="Monitoring Ketinggian Air", layout="wide")

    since = (
        datetime.now(timezone.utc).date() - timedelta(days=DEFAULT_WINDOW_DAYS * 4)
    ).isoformat()

    runs = load_runs()
    hourly = load_hourly(since)
    predictions = load_predictions(since)

    latest_run = None
    if not runs.empty and "processed_at" in runs.columns:
        latest_run = runs.sort_values("processed_at").iloc[-1]

    st.title("Monitoring Ketinggian Air")
    st.caption(
        "Sumber: gold layer (silver USGS + Open-Meteo yang digabung per jam). "
        "Panel utama adalah hasil prediksi model."
    )

    if latest_run is not None and str(latest_run.get("status", "")) != "committed":
        st.warning(
            "Run gold terakhir belum berstatus committed. Data bisa belum sinkron "
            "sampai run berikutnya selesai."
        )

    if hourly.empty:
        st.warning(
            "Tabel `gold/station_hourly` belum ada. Jalankan gold lebih dulu: "
            "`prefect deployment run 'gold-transform/gold-backfill'`."
        )
        return

    stations = sorted(hourly["station_id"].dropna().unique())
    with st.sidebar:
        st.header("Kontrol")
        station = st.selectbox("Stasiun", stations)
        horizons = sorted(predictions["horizon"].dropna().unique()) if not predictions.empty else [1, 6, 24]
        horizon = st.selectbox("Horizon prediksi (jam)", horizons, index=min(1, len(horizons) - 1))
        window_days = st.select_slider(
            "Rentang tampilan (hari)", options=[7, 14, 30, 90], value=DEFAULT_WINDOW_DAYS
        )

    station_hourly = hourly[hourly["station_id"] == station].sort_values("time_utc")
    latest_time = station_hourly["time_utc"].max()
    cutoff = latest_time - timedelta(days=window_days)
    observed = station_hourly[station_hourly["time_utc"] >= cutoff]

    window_predictions = _latest_predictions(predictions, station, horizon)
    if not window_predictions.empty:
        window_predictions = window_predictions[
            window_predictions["target_time"] >= cutoff
        ]

    current = station_hourly.dropna(subset=["stage_mean_ft"]).tail(1)
    current_stage = float(current["stage_mean_ft"].iloc[0]) if not current.empty else float("nan")

    upcoming = pd.DataFrame()
    if not predictions.empty:
        upcoming = predictions[
            (predictions["station_id"] == station) & (predictions["horizon"] == horizon)
        ]
    next_prediction = (
        upcoming.dropna(subset=["target_time"]).sort_values("target_time").tail(1)
        if not upcoming.empty
        else upcoming
    )
    predicted_delta = (
        float(next_prediction["y_pred"].iloc[0]) - current_stage
        if not next_prediction.empty and not pd.isna(current_stage)
        else float("nan")
    )

    columns = st.columns(4)
    columns[0].metric("Ketinggian air terkini", f"{current_stage:.2f} ft")
    columns[1].metric(
        f"Prediksi h+{horizon}",
        "—" if pd.isna(predicted_delta) else f"{next_prediction['y_pred'].iloc[0]:.2f} ft",
        None if pd.isna(predicted_delta) else f"{predicted_delta:+.2f} ft",
    )
    columns[2].metric(
        "Waktu data terakhir",
        latest_time.tz_convert(TIMEZONE).strftime("%d %b %H:%M") if pd.notna(latest_time) else "—",
    )
    version = (
        str(latest_run["dataset_version"])
        if latest_run is not None and "dataset_version" in latest_run
        else str(station_hourly["dataset_version"].dropna().max() or "-")
    )
    data_end = (
        str(latest_run["data_end_date"])
        if latest_run is not None and "data_end_date" in latest_run
        else "-"
    )
    columns[3].metric("Versi dataset", version, f"data s/d {data_end}")

    st.plotly_chart(
        _prediction_chart(observed, window_predictions, station, int(horizon)),
        use_container_width=True,
    )

    if window_predictions.empty:
        st.info(
            "Belum ada baris prediksi untuk stasiun dan horizon ini. Isi "
            "`gold/predictions` dari skrip inference model, lalu muat ulang."
        )

    st.subheader("Konteks hidrometeorologi")
    st.plotly_chart(_hydrograph(observed, station), use_container_width=True)

    with st.expander("Kualitas data per jam"):
        checks = {
            "Jam teramati": len(station_hourly),
            "Jam lengkap (≥3 dari 4 sampel)": int(station_hourly["stage_complete"].sum()),
            "Jam tanpa hujan tercatat": int(station_hourly["precip_mm"].isna().sum())
            if "precip_mm" in station_hourly.columns
            else 0,
            "Sumber debit": str(station_hourly["discharge_grain"].dropna().unique().tolist()),
        }
        st.write(pd.DataFrame([{"pemeriksaan": key, "nilai": value} for key, value in checks.items()]))


if __name__ == "__main__":
    main()
