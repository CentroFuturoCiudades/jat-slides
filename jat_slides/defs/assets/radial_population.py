from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import numpy as np
import pandas as pd
from cfc_dagster_utils.partitions import zone_partitions
from matplotlib.figure import Figure

import dagster as dg
from jat_slides.defs.assets.remoteness import RADIUS_COLUMNS
from jat_slides.defs.resources import PathResource

REMOTENESS_LABELS = ("Central", "Intermedia", "Distante", "Periurbana")


def prepare_radial_change(
    radial: pd.DataFrame, brackets: pd.DataFrame, zone: str, start_year: int
) -> tuple[pd.DataFrame, np.ndarray, float]:
    columns = ["r", "ring_pop_1990", "ring_pop_2000", "ring_pop_2020"]
    if not set(columns).issubset(radial.columns) or not {
        "CVE_MET",
        *RADIUS_COLUMNS,
    }.issubset(brackets.columns):
        msg = f"Zone {zone}: missing required radial population or bracket columns"
        raise ValueError(msg)
    values = radial[columns].apply(pd.to_numeric, errors="coerce")
    if (
        values.empty
        or not np.isfinite(values.to_numpy()).all()
        or (values < 0).any().any()
    ):
        msg = f"Zone {zone}: radii and populations must be finite and nonnegative"
        raise ValueError(msg)
    rows = brackets.loc[brackets["CVE_MET"] == zone, list(RADIUS_COLUMNS)]
    if len(rows) != 1:
        msg = f"Zone {zone}: expected one remoteness row, found {len(rows)}"
        raise ValueError(msg)
    radii = rows.iloc[0].apply(pd.to_numeric, errors="coerce").to_numpy(dtype=float)
    if not np.isfinite(radii).all() or not (np.diff(np.r_[0, radii]) > 0).all():
        msg = f"Zone {zone}: bracket radii must be finite, positive and increasing"
        raise ValueError(msg)
    values = values.sort_values("r").reset_index(drop=True)
    cumulative = values["ring_pop_2020"].cumsum()
    if cumulative.iloc[-1] <= 0:
        msg = f"Zone {zone}: total 2020 population must be positive"
        raise ValueError(msg)
    values["r"] /= 1000
    xmax = float(values.loc[cumulative >= 0.9 * cumulative.iloc[-1], "r"].iloc[0])
    if xmax <= 0:
        msg = f"Zone {zone}: 90% population radius must be positive"
        raise ValueError(msg)
    values["pop_diff"] = values["ring_pop_2020"] - values[f"ring_pop_{start_year}"]
    values["remoteness_cat"] = pd.cut(
        values["r"],
        bins=np.r_[0, radii, np.inf],
        labels=list(REMOTENESS_LABELS),
        include_lowest=True,
    )
    return values, radii, xmax


def summarize_radial_change(radial: pd.DataFrame, start_year: int) -> pd.DataFrame:
    baseline_column = f"ring_pop_{start_year}"
    summary = (
        radial.groupby("remoteness_cat", observed=False)[["pop_diff", baseline_column]]
        .sum()
        .reset_index()
    )
    baseline = summary[baseline_column].where(summary[baseline_column] != 0)
    summary["pop_diff_pct"] = 100 * summary["pop_diff"] / baseline
    return summary.drop(columns=baseline_column)


def plot_radial_change(radial: pd.DataFrame, radii: np.ndarray, xmax: float) -> Figure:
    fig, ax = plt.subplots(figsize=(10, 5), layout="constrained")
    x = radial["r"].to_numpy()
    y = radial["pop_diff"].to_numpy()
    ax.plot(x, y, color="k", alpha=0.7)
    ax.fill_between(x, y, where=y > 0, interpolate=True, color="#1E90FF", alpha=0.7)
    ax.fill_between(x, y, where=y < 0, interpolate=True, color="#FF6347", alpha=0.7)
    ax.set_xlim(0, xmax)
    # Include both periods in autoscaling, even when materialized separately.
    changes = (
        radial[["ring_pop_1990", "ring_pop_2000"]]
        .rsub(radial["ring_pop_2020"], axis=0)
        .to_numpy()
    )
    ax.update_datalim([(0, min(0, changes.min())), (0, max(0, changes.max()))])
    ax.autoscale_view(scalex=False)
    ax.yaxis.set_major_formatter(mtick.StrMethodFormatter("{x:,.0f}"))
    ax.set_xlabel("Distancia al centro (km)", fontsize=14)
    ax.set_ylabel("Cambio en la población", fontsize=14)
    ax.tick_params(axis="both", labelsize=12)
    for radius in radii:
        if radius <= xmax:
            ax.axvline(radius, ls="--", c="k", alpha=0.5)
    boundaries = np.r_[0, radii, np.inf]
    for index, label in enumerate(REMOTENESS_LABELS):
        left, right = boundaries[index : index + 2]
        if left < xmax:
            ax.annotate(
                label,
                xy=((left + min(right, xmax)) / 2, 1.02),
                xycoords=ax.get_xaxis_transform(),
                ha="center",
                fontsize=12,
            )
    return fig


def radial_population_factory(start_year: int) -> dg.AssetsDefinition:
    name = f"radial_population_change_{start_year}_2020"

    @dg.multi_asset(
        name=name,
        partitions_def=zone_partitions,
        outs={
            "plot": dg.AssetOut(
                key=["plot_zone", name],
                io_manager_key="plot_manager",
                group_name="plot_zone",
            ),
            "table": dg.AssetOut(
                key=["stats_zone", name],
                io_manager_key="dataframe_manager",
                group_name="stats_zone",
            ),
        },
    )
    def radial_population(
        context: dg.AssetExecutionContext,
        path_resource: PathResource,
    ) -> tuple[Figure, pd.DataFrame]:
        zone = context.partition_key
        root = Path(path_resource.in_path)
        radial = pd.read_csv(root / "radial_f" / f"radial_functions_{zone}_rho.csv")
        brackets = pd.read_csv(root / "remoteness_brackets.csv", dtype={"CVE_MET": str})
        prepared, radii, xmax = prepare_radial_change(
            radial, brackets, zone, start_year
        )
        return plot_radial_change(prepared, radii, xmax), summarize_radial_change(
            prepared, start_year
        )

    return radial_population


radial_population_1990 = radial_population_factory(1990)
radial_population_2000 = radial_population_factory(2000)
