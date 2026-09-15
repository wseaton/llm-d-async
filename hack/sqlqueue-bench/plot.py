# /// script
# requires-python = ">=3.12"
# dependencies = ["matplotlib>=3.9"]
# ///
"""Render the sql transport benchmark charts from sqlqueue-bench logs and a transport-bench CSV:
uv run hack/sqlqueue-bench/plot.py <store-log-dir> <transport-bench.csv>"""

import csv
import re
import sys
from dataclasses import dataclass
from pathlib import Path

import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib import font_manager
from matplotlib.ticker import FuncFormatter

ROOT = Path(__file__).resolve().parents[2]
STORE = Path(sys.argv[1])
E2E = Path(sys.argv[2])
OUT = ROOT / "docs/sql-transport"

for f in Path.home().joinpath("Library/Fonts").glob("TX-02*.otf"):
    font_manager.fontManager.addfont(str(f))

THEMES = {
    "light": dict(surface="#fcfcfb", ink="#0b0b0b", ink2="#52514e", grid="#e6e5e0",
                  series=["#2a78d6", "#eb6834", "#1baf7a"], redis="#4a3aa7", load=["#6da7ec", "#184f95"]),
    "dark": dict(surface="#1a1a19", ink="#ffffff", ink2="#c3c2b7", grid="#383835",
                 series=["#3987e5", "#d95926", "#199e70"], redis="#9085e9", load=["#9ec5f4", "#3987e5"]),
}


@dataclass
class Run:
    rate: float
    completion: float
    kept_up: bool


def field(text: str, pattern: str) -> str:
    m = re.search(pattern, text)
    if m is None:
        raise ValueError(f"missing {pattern!r} in bench log")
    return m.group(1)


def parse_log(path: Path) -> list[Run]:
    runs: list[Run] = []
    for block in path.read_text().split("=== sqlqueue-bench")[1:]:
        rate = float(field(block, r"target rate: (\d+)"))
        completion = float(field(block, r"achieved completion rate: ([\d.]+)"))
        kept_up = "drained=true" in block and completion >= 0.95 * rate
        runs.append(Run(rate, completion, kept_up))
    return sorted(runs, key=lambda r: r.rate)


def parse_result_batches(path: Path) -> dict[float, list[tuple[int, Run]]]:
    by_rate: dict[float, dict[int, list[Run]]] = {}
    for block in path.read_text().split("=== sqlqueue-bench")[1:]:
        rate = float(field(block, r"target rate: (\d+)"))
        batch = int(field(block, r"ack_batch=(\d+)"))
        completion = float(field(block, r"achieved completion rate: ([\d.]+)"))
        run = Run(rate, completion, "drained=true" in block and completion >= 0.95 * rate)
        by_rate.setdefault(rate, {}).setdefault(batch, []).append(run)
    medians: dict[float, list[tuple[int, Run]]] = {}
    for rate, batches in sorted(by_rate.items()):
        medians[rate] = [(b, sorted(runs, key=lambda r: r.completion)[len(runs) // 2])
                         for b, runs in sorted(batches.items())]
    return medians


def parse_e2e(transport: str) -> list[Run]:
    with E2E.open() as f:
        rows = [r for r in csv.DictReader(f) if r["transport"] == transport]
    runs = [Run(float(r["target_rate"]), float(r["completion_rate"]),
                r["drained"] == "true" and float(r["completion_rate"]) >= 0.95 * float(r["target_rate"]))
            for r in rows]
    return sorted(runs, key=lambda r: r.rate)


designs = [
    ("per-row claims", parse_log(STORE / "row-claims.log")),
    ("skip-locked batches", parse_log(STORE / "skip-locked.log")),
    ("partition leases", parse_log(STORE / "partition-leases.log")),
]

kfmt = FuncFormatter(lambda v, _: f"{v / 1000:g}k" if v >= 1000 else f"{v:g}")


def style(theme: dict) -> None:
    mpl.rcParams.update({
        "font.family": "TX-02", "font.size": 11,
        "figure.facecolor": theme["surface"], "axes.facecolor": theme["surface"],
        "savefig.facecolor": theme["surface"], "text.color": theme["ink"],
        "axes.labelcolor": theme["ink2"], "xtick.color": theme["ink2"], "ytick.color": theme["ink2"],
        "axes.edgecolor": theme["grid"], "axes.grid": True, "grid.color": theme["grid"],
        "grid.linewidth": 0.8, "axes.spines.top": False, "axes.spines.right": False,
        "axes.spines.left": False, "xtick.major.size": 0, "ytick.major.size": 0,
    })


def title(ax, text: str, sub: str, theme: dict) -> None:
    ax.set_title(text, loc="left", fontsize=15, color=theme["ink"], pad=26)
    ax.text(0, 1.02, sub, transform=ax.transAxes, fontsize=10.5, color=theme["ink2"], va="bottom")


def label(ax, x, y, text, theme, dy=0):
    ax.annotate(text, (x, y), xytext=(8, dy), textcoords="offset points", va="center",
                fontsize=10.5, color=theme["ink"])


def load_lines(ax, series, theme, top):
    ax.plot([0, top], [0, top], color=theme["ink2"], lw=1, ls=(0, (4, 4)), zorder=1)
    for name, runs, color, dy in series:
        runs = [r for r in runs if r.rate <= top]
        ax.plot([r.rate for r in runs], [r.completion for r in runs], color=color, lw=2, zorder=3)
        for r in runs:
            ax.scatter(r.rate, r.completion, s=64, zorder=4, linewidths=2,
                       color=color if r.kept_up else theme["surface"],
                       edgecolors=theme["surface"] if r.kept_up else color)
        label(ax, runs[-1].rate, runs[-1].completion, name, theme, dy)
    ax.xaxis.set_major_formatter(kfmt)
    ax.yaxis.set_major_formatter(kfmt)


def save(fig, name: str, mode: str) -> None:
    fig.tight_layout()
    fig.savefig(OUT / f"{name}-{mode}.png")
    plt.close(fig)


def throughput(theme: dict, mode: str) -> None:
    fig, ax = plt.subplots(figsize=(9, 5.2), dpi=200)
    series = [(n, runs, c, 10 if n == "per-row claims" else 0) for (n, runs), c in zip(designs, theme["series"])]
    load_lines(ax, series, theme, top=16500)
    ax.set_xlim(0, 20000)
    ax.set_ylim(0, 16000)
    ax.set_xlabel("offered load (req/s)")
    ax.set_ylabel("completed (req/s)")
    title(ax, "Store throughput on 2 vCPU Postgres 17",
          "dashed = offered load · filled = kept up · hollow = fell behind", theme)
    save(fig, "store-throughput", mode)


def transports(theme: dict, mode: str) -> None:
    fig, ax = plt.subplots(figsize=(9, 5.2), dpi=200)
    series = [("sql (Postgres 17)", parse_e2e("sql"), theme["series"][2], 0),
              ("redis-sortedset (Redis 7)", parse_e2e("redis-sortedset"), theme["redis"], 0)]
    load_lines(ax, series, theme, top=24500)
    ax.set_xlim(0, 32000)
    ax.set_ylim(0, 8000)
    ax.set_xlabel("offered load (req/s)")
    ax.set_ylabel("completed (req/s)")
    title(ax, "End-to-end dispatch: sql vs redis-sortedset",
          "4 flow replicas · 2 vCPU database pod · Kubernetes · hollow = fell behind", theme)
    save(fig, "transports", mode)


def result_batch(theme: dict, mode: str) -> None:
    fig, ax = plt.subplots(figsize=(9, 5.2), dpi=200)
    sweeps = parse_result_batches(STORE / "result-batch-sweep.log")
    batches = sorted({b for runs in sweeps.values() for b, _ in runs})
    for (rate, runs), color in zip(sweeps.items(), theme["load"]):
        ax.axhline(rate, color=color, lw=1, ls=(0, (4, 4)), zorder=1)
        xs = [b for b, _ in runs]
        ax.plot(xs, [r.completion for _, r in runs], color=color, lw=2, zorder=3)
        for b, r in runs:
            ax.scatter(b, r.completion, s=64, zorder=4, linewidths=2,
                       color=color if r.kept_up else theme["surface"],
                       edgecolors=theme["surface"] if r.kept_up else color)
        label(ax, xs[-1], runs[-1][1].completion, f"{rate / 1000:g}k req/s offered", theme)
    ax.set_xscale("log", base=2)
    ax.set_xticks(batches)
    ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:g}"))
    ax.xaxis.set_minor_locator(mpl.ticker.NullLocator())
    ax.yaxis.set_major_formatter(kfmt)
    ax.set_ylim(0, max(sweeps) * 1.15)
    ax.set_xlim(batches[0] / 1.5, batches[-1] * 4)
    ax.set_xlabel("result_batch_size = most requests redelivered if one result write fails")
    ax.set_ylabel("completed (req/s)")
    title(ax, "Result batch size vs throughput", "best of 2 runs per point · dashed = offered load · hollow = fell behind", theme)
    save(fig, "result-batch", mode)


OUT.mkdir(parents=True, exist_ok=True)
for mode, theme in THEMES.items():
    style(theme)
    throughput(theme, mode)
    transports(theme, mode)
    result_batch(theme, mode)
