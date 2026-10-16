import geopandas as gpd
import matplotlib.colors as mcol
from cfc_dagster_utils.partitions import zone_partitions
from cfc_dagster_utils.resources import PostgresResource
from matplotlib.figure import Figure

import dagster as dg
from jat_slides.defs.assets.maps.common import (
    add_pop_legend,
    add_remoteness_circles,
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
from jat_slides.defs.partitions import mun_partitions
from jat_slides.defs.resources import PathResource


@dg.op(
    out=dg.Out(io_manager_key="plot_manager"),
    config_schema={"start_year": dg.Field(int, default_value=2000)},
)
def plot_dataframe(  # noqa: PLR0913, PLR0917
    context: dg.OpExecutionContext,
    path_resource: PathResource,
    postgis_resource: PostgresResource,
    bounds: tuple[float, float, float, float],
    df: gpd.GeoDataFrame,
    lw: float,
    labels: dict[str, bool],
    legend_pos: str,
    overlay_config: dict | None,
    circles: gpd.GeoDataFrame | None = None,
) -> Figure:
    fig, ax = generate_figure(
        bounds,
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
        postgis_resource=postgis_resource,
    )

    cmap_bounds = get_cmap_bounds(df["difference"].to_numpy(), 3)  # ty: ignore[invalid-argument-type]
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

    add_pop_legend(
        cmap_bounds,
        ax=ax,
        cmap=cmap_rdbu,
        legend_pos=legend_pos,
        start_year=context.op_config["start_year"],
    )

    # overlay_dir = (
    #     Path(path_resource.data_path) / "overlays" / str(context.partition_key)
    # )
    # add_overlay(overlay_dir, ax=ax, config=overlay_config)

    add_remoteness_circles(circles, ax=ax)
    return fig


# pylint: disable=no-value-for-parameter
def population_grid_plot_factory(
    suffix: str,
    *,
    bounds_op: dg.OpDefinition,
    overlay_config_op: dg.OpDefinition,
    partitions_def: dg.PartitionsDefinition,
    start_year: int = 2000,
) -> dg.AssetsDefinition:
    plot = plot_dataframe
    if start_year != 2000:
        plot = plot_dataframe.configured(
            {"start_year": start_year}, name=f"plot_dataframe_{start_year}_2020"
        )

    def _asset(df: gpd.GeoDataFrame, circles: gpd.GeoDataFrame | None = None) -> Figure:
        bounds = bounds_op()
        lw = get_linewidth()
        labels = get_labels_zone()
        legend_pos = get_legend_pos_base()
        overlay_config = overlay_config_op()

        return plot(
            bounds,
            df,
            lw,
            labels,
            legend_pos=legend_pos,
            overlay_config=overlay_config,
            **({"circles": circles} if circles is not None else {}),
        )

    if suffix == "zone":

        @dg.graph_asset(
            name=(
                "population_grid"
                if start_year == 2000
                else f"population_grid_{start_year}_2020"
            ),
            key_prefix=f"plot_{suffix}",
            ins={
                "circles": dg.AssetIn("remoteness_circles_zone"),
                "df": dg.AssetIn(
                    key=[
                        "cells",
                        suffix if start_year == 2000 else f"{suffix}_{start_year}_2020",
                    ]
                ),
            },
            partitions_def=partitions_def,
            group_name=f"plot_{suffix}",
        )
        def _zone_asset(df: gpd.GeoDataFrame, circles: gpd.GeoDataFrame) -> Figure:
            return _asset(df, circles)

        return _zone_asset

    @dg.graph_asset(
        name="population_grid",
        key_prefix=f"plot_{suffix}",
        ins={"df": dg.AssetIn(key=["cells", suffix])},
        partitions_def=partitions_def,
        group_name=f"plot_{suffix}",
    )
    def _mun_asset(df: gpd.GeoDataFrame) -> Figure:
        return _asset(df)

    return _mun_asset


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


population_grid_1990_2020_plot_zone = population_grid_plot_factory(
    "zone",
    partitions_def=zone_partitions,
    bounds_op=get_bounds_base,
    overlay_config_op=get_overlay_config_zone,
    start_year=1990,
)
