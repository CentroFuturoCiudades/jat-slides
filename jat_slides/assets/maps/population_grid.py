from pathlib import Path

import geopandas as gpd
import matplotlib.colors as mcol
from matplotlib.figure import Figure

import dagster as dg
from jat_slides.assets.maps.common import (
    add_overlay,
    add_pop_legend,
    cmap_rdbu,
    generate_figure,
    get_bounds_base,
    get_bounds_mun,
    get_cmap_bounds,
    get_labels_zone,
    get_legend_pos_base,
    get_linewidth,
    get_overlay_config_mun,
    get_overlay_config_zone,
)
from jat_slides.partitions import mun_partitions, zone_partitions
from jat_slides.resources import PathResource


@dg.op(out=dg.Out(io_manager_key="plot_manager"))
def plot_dataframe(
    context: dg.OpExecutionContext,
    path_resource: PathResource,
    bounds: tuple[float, float, float, float],
    df: gpd.GeoDataFrame,
    lw: float,
    labels: dict[str, bool],
    legend_pos: str,
    overlay_config: dict | None,
) -> Figure:
    state = int(context.partition_key.split(".")[0])

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

    cmap_bounds = get_cmap_bounds(df["difference"], 3)
    norm = mcol.BoundaryNorm(cmap_bounds, 256)

    df.to_crs("EPSG:4326").plot(
        column="difference",
        ax=ax,
        cmap=cmap_rdbu,
        ec="k",
        lw=lw,
        autolim=False,
        norm=norm,
        aspect=None,
    )

    add_pop_legend(cmap_bounds, ax=ax, cmap=cmap_rdbu, legend_pos=legend_pos)

    overlay_dir = (
        Path(path_resource.data_path) / "overlays" / str(context.partition_key)
    )
    add_overlay(overlay_dir, ax=ax, config=overlay_config)

    return fig


# pylint: disable=no-value-for-parameter
def population_grid_plot_factory(
    suffix: str,
    *,
    bounds_op: dg.OpDefinition,
    overlay_config_op: dg.OpDefinition,
    partitions_def: dg.PartitionsDefinition,
) -> dg.AssetsDefinition:
    @dg.graph_asset(
        name="population_grid",
        key_prefix=f"plot_{suffix}",
        ins={"df": dg.AssetIn(key=["cells", suffix])},
        partitions_def=partitions_def,
        group_name=f"plot_{suffix}",
    )
    def _asset(df: gpd.GeoDataFrame) -> Figure:
        bounds = bounds_op()
        lw = get_linewidth()
        labels = get_labels_zone()
        legend_pos = get_legend_pos_base()
        overlay_config = overlay_config_op()

        return plot_dataframe(
            bounds,
            df,
            lw,
            labels,
            legend_pos=legend_pos,
            overlay_config=overlay_config,
        )

    return _asset


population_grid_plot_zone = population_grid_plot_factory(
    "zone",
    partitions_def=zone_partitions,
    bounds_op=get_bounds_base,
    overlay_config_op=get_overlay_config_zone,
)


population_grid_plot_mun = population_grid_plot_factory(
    "mun",
    partitions_def=mun_partitions,
    bounds_op=get_bounds_mun,
    overlay_config_op=get_overlay_config_mun,
)
