"""Make the charts in the README from the exported query results.

The CSV files next to this script are the output of price_patterns.sql,
run on Databricks. Run from the repo root:

    python analysis/make_charts.py
"""

from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import pandas as pd

matplotlib.use("Agg")

HERE = Path(__file__).parent
CHARTS = HERE.parent / "charts"
INK = "#333333"
E5 = "#4a6fa5"
DIESEL = "#c27c3a"


def style(ax):
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.tick_params(colors=INK)
    ax.yaxis.label.set_color(INK)
    ax.xaxis.label.set_color(INK)
    ax.title.set_color(INK)


def save(fig, name):
    fig.tight_layout()
    fig.savefig(CHARTS / name, dpi=110)
    plt.close(fig)


def by_hour():
    d = pd.read_csv(HERE / "by_hour.csv")

    fig, ax = plt.subplots(figsize=(9, 4))
    ax.step(d["hour"], d["avg_diesel"], where="post", color=DIESEL, linewidth=2, label="Diesel")
    ax.step(d["hour"], d["avg_e5"], where="post", color=E5, linewidth=2, label="E5")
    ax.set_xlabel("hour of day (price at the full hour)")
    ax.set_ylabel("average price (EUR per litre)")
    ax.set_title("Prices jump once around noon, then fall until the next day")
    ax.set_xticks(range(0, 24, 2))
    ax.grid(axis="y", alpha=0.2)
    ax.legend(frameon=False)
    style(ax)
    save(fig, "price_by_hour.png")


def by_day():
    d = pd.read_csv(HERE / "by_day.csv", parse_dates=["date"])

    fig, ax = plt.subplots(figsize=(9, 4))
    ax.plot(d["date"], d["avg_diesel"], color=DIESEL, linewidth=2, label="Diesel")
    ax.plot(d["date"], d["avg_e5"], color=E5, linewidth=2, label="E5")
    ax.set_ylabel("average price (EUR per litre)")
    ax.set_title("Average price per day, August to October 2026")
    ax.grid(axis="y", alpha=0.2)
    ax.legend(frameon=False)
    fig.autofmt_xdate()
    style(ax)
    save(fig, "price_by_day.png")


def by_brand():
    d = pd.read_csv(HERE / "by_brand.csv").sort_values("avg_e5", ascending=False)
    short = {"BFT", "HEM", "JET"}
    names = [
        b if b in short else b.title().replace("Totalenergies", "TotalEnergies").replace("Eni", "ENI")
        for b in d["brand"]
    ]

    fig, ax = plt.subplots(figsize=(9, 4.5))
    ax.barh(names, d["avg_e5"], color=E5)
    ax.set_xlim(2.18, d["avg_e5"].max() + 0.02)
    ax.set_xlabel("average E5 price (EUR per litre, axis starts at 2.18)")
    ax.set_title("Brands with at least 200 stations")
    for i, v in enumerate(d["avg_e5"]):
        ax.text(v + 0.002, i, f"{v:.3f}", va="center", color=INK, fontsize=9)
    ax.grid(axis="x", alpha=0.2)
    style(ax)
    save(fig, "price_by_brand.png")


if __name__ == "__main__":
    CHARTS.mkdir(exist_ok=True)
    by_hour()
    by_day()
    by_brand()
