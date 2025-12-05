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
    get_labels_mun,
    get_labels_zone,
    get_legend_pos_base,
    get_legend_pos_mun,
    get_linewidth,
    get_overlay_config_mun,
    get_overlay_config_zone,
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
    overlay_config: dict | None,
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

    overlay_dir = (
        Path(path_resource.data_path) / "overlays" / str(context.partition_key)
    )
    add_overlay(overlay_dir, ax=ax, config=overlay_config)

    return fig


def income_plot_factory(
    level: str,
    *,
    bounds_op: dg.OpDefinition,
    labels_op: dg.OpDefinition,
    legend_pos_op: dg.OpDefinition,
    overlay_config_op: dg.OpDefinition,
    partitions_def: dg.PartitionsDefinition,
) -> dg.AssetsDefinition:
    @dg.graph_asset(
        name="income",
        key_prefix=f"plot_{level}",
        ins={"df": dg.AssetIn(key=["income", level])},
        partitions_def=partitions_def,
        group_name=f"plot_{level}",
    )
    def _asset(df: gpd.GeoDataFrame) -> Figure:
        lw = get_linewidth()
        bounds = bounds_op()
        labels = labels_op()
        legend_pos = legend_pos_op()
        overlay_config = overlay_config_op()
        return plot_income(
            df,
            bounds,
            lw,
            labels,
            legend_pos,
            overlay_config=overlay_config,
        )

    return _asset


income_plot_zone = income_plot_factory(
    "zone",
    bounds_op=get_bounds_base,
    labels_op=get_labels_zone,
    legend_pos_op=get_legend_pos_base,
    overlay_config_op=get_overlay_config_zone,
    partitions_def=zone_partitions,
)

income_plot_mun = income_plot_factory(
    "mun",
    bounds_op=get_bounds_mun,
    labels_op=get_labels_mun,
    legend_pos_op=get_legend_pos_mun,
    overlay_config_op=get_overlay_config_mun,
    partitions_def=mun_partitions,
)
