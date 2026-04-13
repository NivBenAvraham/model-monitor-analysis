"""
Plot temperature scatter for one (group_id, date) — 4 panels side by side.

Panels (left to right): Ambient | Small | Medium | Large
  - Scatter markers for every sensor reading
  - Diamond markers for the hourly mean per bucket
  - Dashed reference lines per bucket:
      small  → 26°C
      medium → 26°C, 32°C
      large  → 28°C, 35°C

Usage:
    python skills/sensor_group_segment/scripts/plot_temperature_scatter.py
    python skills/sensor_group_segment/scripts/plot_temperature_scatter.py --group 491 --date 2026-02-16
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

REPO_ROOT = Path(__file__).resolve().parents[3]
DATA_DIR  = REPO_ROOT / "data/samples/temperature-export"

SIZE_MAP = [
    ("small",  "#ef5350", 2, [26]),
    ("medium", "#fb8c00", 3, [26, 32]),
    ("large",  "#43a047", 4, [28, 35]),
]


def load_data(group_id: int, date: str):
    base = DATA_DIR / f"group_{group_id}" / date
    sensor_files  = list(base.glob(f"{group_id}_*_sensor_temperature.parquet"))
    gateway_files = list(base.glob(f"{group_id}_*_gateway_temperature.parquet"))
    if not sensor_files or not gateway_files:
        raise FileNotFoundError(f"Missing parquet files in {base}")
    return pd.read_parquet(sensor_files[0]), pd.read_parquet(gateway_files[0])


def resample(sensor_df: pd.DataFrame, gateway_df: pd.DataFrame):
    sensor_hourly = (
        sensor_df
        .groupby(["hive_size_bucket", "sensor_mac_address",
                  pd.Grouper(key="timestamp", freq="1h")])["pcb_temperature_one"]
        .mean()
        .reset_index()
    )
    gateway_hourly = (
        gateway_df
        .groupby(["gateway_mac_address", pd.Grouper(key="timestamp", freq="1h")])["pcb_temperature_two"]
        .mean()
        .reset_index()
    )
    return sensor_hourly, gateway_hourly


def build_figure(sensor_hourly: pd.DataFrame, gateway_hourly: pd.DataFrame,
                 group_id: int, date: str) -> go.Figure:
    fig = make_subplots(
        rows=1, cols=4,
        shared_yaxes=True,
        subplot_titles=["Ambient (gateway)", "Small hives", "Medium hives", "Large hives"],
        horizontal_spacing=0.04,
    )

    # ── Col 1: Ambient ────────────────────────────────────────────────────
    for gw, grp in gateway_hourly.groupby("gateway_mac_address"):
        fig.add_trace(go.Scatter(
            x=grp["timestamp"], y=grp["pcb_temperature_two"],
            mode="markers",
            marker=dict(color="#1e88e5", size=4, opacity=0.5),
            name=gw, legendgroup="ambient", showlegend=False,
        ), row=1, col=1)

    mean_gw = gateway_hourly.groupby("timestamp")["pcb_temperature_two"].mean().reset_index()
    fig.add_trace(go.Scatter(
        x=mean_gw["timestamp"], y=mean_gw["pcb_temperature_two"],
        mode="markers",
        marker=dict(color="#1e88e5", size=7, symbol="diamond"),
        name="Mean ambient", legendgroup="ambient",
    ), row=1, col=1)

    # ── Cols 2-4: Internal temp by hive size ──────────────────────────────
    for size, color, col, ref_lines in SIZE_MAP:
        grp_size = sensor_hourly[sensor_hourly["hive_size_bucket"] == size]
        n = grp_size["sensor_mac_address"].nunique()

        for mac, sg in grp_size.groupby("sensor_mac_address"):
            fig.add_trace(go.Scatter(
                x=sg["timestamp"], y=sg["pcb_temperature_one"],
                mode="markers",
                marker=dict(color=color, size=4, opacity=0.35),
                name=mac, legendgroup=size, showlegend=False,
            ), row=1, col=col)

        mean_s = grp_size.groupby("timestamp")["pcb_temperature_one"].mean().reset_index()
        fig.add_trace(go.Scatter(
            x=mean_s["timestamp"], y=mean_s["pcb_temperature_one"],
            mode="markers",
            marker=dict(color=color, size=7, symbol="diamond"),
            name=f"{size} mean (n={n})", legendgroup=size,
        ), row=1, col=col)

        for y_val in ref_lines:
            fig.add_hline(
                y=y_val, row=1, col=col,
                line=dict(color="black", width=1.5, dash="dash"),
                annotation_text=f"{y_val}°C",
                annotation_position="top right",
                annotation_font=dict(color="black", size=10),
            )

    fig.update_yaxes(range=[0, 50], title_text="Temperature (°C)", col=1)
    for c in [2, 3, 4]:
        fig.update_yaxes(range=[0, 50], col=c)
    fig.update_xaxes(title_text="Time")

    fig.update_layout(
        title=dict(
            text=f"Group {group_id}  |  {date}  — Temperature scatter by hive size",
            font_size=15,
        ),
        height=500,
        width=1400,
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="left", x=0),
        template="plotly_white",
    )
    return fig


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Temperature scatter plot by hive size.")
    p.add_argument("--group", type=int, default=2805, help="group_id (default: 2805)")
    p.add_argument("--date",  type=str, default="2026-03-11", help="date YYYY-MM-DD (default: 2026-03-11)")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    sensor_df, gateway_df = load_data(args.group, args.date)
    sensor_hourly, gateway_hourly = resample(sensor_df, gateway_df)
    fig = build_figure(sensor_hourly, gateway_hourly, args.group, args.date)
    fig.show()


if __name__ == "__main__":
    main()
