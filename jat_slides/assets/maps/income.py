from pathlib import Path

import geopandas as gpd
import matplotlib as mpl
from matplotlib.figure import Figure

import dagster as dg
from jat_slides.assets.maps.common import (
    add_overlay,
    generate_figure,
    get_bounds_base,
    get_bounds_mun,
    get_bounds_trimmed,
    get_labels_mun,
    get_labels_zone,
    get_legend_pos_base,
    get_legend_pos_mun,
    get_linewidth,
    intersect_geometries,
    update_categorical_legend,
)
from jat_slides.partitions import mun_partitions, zone_partitions
from jat_slides.resources import (
    PathResource,
)


@dg.op(out=dg.Out(io_manager_key="plot_manager"))
def plot_income(
    context: dg.OpExecutionContext,
    path_resource: PathResource,
    df: gpd.GeoDataFrame,
    bounds: tuple[float, float, float, float],
    lw: float,
    labels: dict[str, bool],
    legend_pos: str,
) -> Figure:
    state = int(context.partition_key.split(".")[0])

    cmap = mpl.colormaps["RdBu"]

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

    if len(df) == 0:
        return fig

    df.plot(
        column="income_pc",
        scheme="natural_breaks",
        k=6,
        cmap=cmap,
        legend=True,
        ax=ax,
        edgecolor="k",
        lw=lw,
        autolim=False,
        aspect=None,
    )

    update_categorical_legend(
        ax,
        title="Ingreso anual per cápita\n(miles de USD)",
        fmt=".2f",
        cmap=cmap,
        legend_pos=legend_pos,
    )

    overlay_path = Path(path_resource.data_path) / "overlays"
    fpath = overlay_path / f"{context.partition_key}.gpkg"
    add_overlay(fpath, ax)

    return fig


@dg.graph_asset(
    name="income",
    key_prefix="plot_zone",
    ins={"df": dg.AssetIn(key=["income", "base"])},
    partitions_def=zone_partitions,
    group_name="plot_zone",
)
def income_plot(df: gpd.GeoDataFrame) -> Figure:
    lw = get_linewidth()
    bounds = get_bounds_base()
    labels = get_labels_zone()
    legend_pos = get_legend_pos_base()
    return plot_income(df, bounds, lw, labels, legend_pos)


@dg.graph_asset(
    name="income",
    key_prefix="plot_mun",
    ins={
        "agebs_mun": dg.AssetIn(key=["muns", "2020"]),
        "state_df": dg.AssetIn(key=["income", "state"]),
    },
    partitions_def=mun_partitions,
    group_name="plot_mun",
)
def income_plot_mun(agebs_mun: gpd.GeoDataFrame, state_df: gpd.GeoDataFrame) -> Figure:
    df = intersect_geometries(state_df, agebs_mun)
    lw = get_linewidth()
    bounds = get_bounds_mun()
    labels = get_labels_mun()
    legend_pos = get_legend_pos_mun()
    return plot_income(df, bounds, lw, labels, legend_pos)


@dg.graph_asset(
    name="income",
    key_prefix="plot_trimmed",
    ins={
        "agebs": dg.AssetIn(key=["agebs_trimmed", "2020"]),
        "df": dg.AssetIn(key=["income", "base"]),
    },
    partitions_def=zone_partitions,
    group_name="plot_trimmed",
)
def income_plot_trimmed(agebs: gpd.GeoDataFrame, df: gpd.GeoDataFrame) -> Figure:
    lw = get_linewidth()
    bounds = get_bounds_trimmed()
    df = intersect_geometries(df, agebs)
    return plot_income(df, bounds, lw)
