import itertools
from pathlib import Path

import geopandas as gpd
import jenkspy
import matplotlib as mpl
import numpy as np
import pandas as pd
from matplotlib.figure import Figure
from matplotlib.legend import Legend

import dagster as dg
from jat_slides.assets.maps.common import (
    add_overlay,
    generate_figure,
    get_bounds_base,
    get_bounds_mun,
    get_labels_mun,
    get_labels_zone,
    get_linewidth,
    get_overlay_config_mun,
    get_overlay_config_zone,
)
from jat_slides.partitions import mun_partitions, zone_partitions
from jat_slides.resources import PathResource


def add_categorical_column(
    df: gpd.GeoDataFrame,
    column: str,
    bins: int,
) -> tuple[gpd.GeoDataFrame, dict[int, str]]:
    breaks_orig = jenkspy.jenks_breaks(df[column], bins)  # pyright: ignore[reportArgumentType]
    breaks_middle = np.round(np.array(breaks_orig[1:-1]) / 100) * 100

    start = np.floor(breaks_orig[0] / 100) * 100
    start = np.max([1, start])

    breaks = np.insert(breaks_middle, 0, start)
    breaks = np.append(breaks, np.ceil(breaks_orig[-1] / 100) * 100)

    mask = pd.Series([0] * len(df), index=df.index, dtype=int)
    label_map = {}
    for i, (start, end) in enumerate(itertools.pairwise(breaks)):
        mask = mask + ((df[column] >= start) & (df[column] < end)) * (i + 1)
        label_map[i + 1] = f"{start:,.0f} - {end:,.0f}"

    df = df.assign(category=mask).sort_values("category")
    return df, label_map


def replace_categorical_legend(legend: Legend, label_map: dict[int, str]) -> None:
    for text in legend.texts:
        text.set_text(label_map[int(text.get_text())])


@dg.op(out=dg.Out(io_manager_key="plot_manager"))
def plot_jobs(
    context: dg.OpExecutionContext,
    path_resource: PathResource,
    df: gpd.GeoDataFrame,
    bounds: tuple[float, float, float, float],
    lw: float,
    labels: dict[str, bool],
    overlay_config: dict | None,
) -> Figure:
    state = int(context.partition_key.split(".")[0])

    cmap = mpl.colormaps["YlGn"]

    df = df.to_crs("EPSG:4326")
    df, label_map = add_categorical_column(df, "jobs", 6)

    fig, ax = generate_figure(
        *bounds,
        add_mun_bounds=True,
        add_mun_labels=labels["mun"],
        add_state_bounds=False,
        add_state_labels=labels["state"],
        state_poly_kwargs={
            "ls": "--",
            "linewidth": 1.5,
            "alpha": 1,
            "edgecolor": "#006400",
        },
        mun_poly_kwargs={"linewidth": 0.3, "alpha": 0.2},
        state_text_kwargs={"fontsize": 7, "color": "#006400", "alpha": 0.9},
        state=state,
        population_grids_path=path_resource.pg_path,
    )
    df.plot(
        column="category",
        legend=True,
        categorical=True,
        cmap=cmap,
        ax=ax,
        edgecolor="k",
        lw=lw,
        autolim=False,
        aspect=None,
        legend_kwds={"framealpha": 1, "title": "Número de empleos"},
    )
    leg = ax.get_legend()

    if leg is None:
        err = "Legend not found in jobs plot."
        raise ValueError(err)

    replace_categorical_legend(leg, label_map)
    leg.set_zorder(9999)

    overlay_dir = (
        Path(path_resource.data_path) / "overlays" / str(context.partition_key)
    )
    add_overlay(overlay_dir, ax=ax, config=overlay_config)

    return fig


def jobs_plot_factory(
    level: str,
    *,
    bounds_op: dg.OpDefinition,
    labels_op: dg.OpDefinition,
    overlay_config_op: dg.OpDefinition,
    partitions_def: dg.PartitionsDefinition,
) -> dg.AssetsDefinition:
    @dg.graph_asset(
        name="jobs",
        key_prefix=f"plot_{level}",
        ins={"df_jobs": dg.AssetIn([f"jobs_{level}", "reprojected"])},
        partitions_def=partitions_def,
        group_name=f"plot_{level}",
    )
    def _asset(df_jobs: gpd.GeoDataFrame) -> Figure:
        lw = get_linewidth()
        bounds = bounds_op()
        labels = labels_op()
        overlay_config = overlay_config_op()
        return plot_jobs(df_jobs, bounds, lw, labels, overlay_config)

    return _asset


jobs_plot_zone = jobs_plot_factory(
    "zone",
    bounds_op=get_bounds_base,
    labels_op=get_labels_zone,
    overlay_config_op=get_overlay_config_zone,
    partitions_def=zone_partitions,
)
jobs_plot_mun = jobs_plot_factory(
    "mun",
    bounds_op=get_bounds_mun,
    labels_op=get_labels_mun,
    partitions_def=mun_partitions,
    overlay_config_op=get_overlay_config_mun,
)
