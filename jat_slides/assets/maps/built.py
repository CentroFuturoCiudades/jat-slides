from pathlib import Path

import matplotlib as mpl
import numpy as np
import rasterio.plot as rio_plot
from affine import Affine
from matplotlib.axes import Axes
from matplotlib.colors import Colormap
from matplotlib.figure import Figure
from matplotlib.patches import Patch

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
    get_overlay_config_mun,
    get_overlay_config_zone,
)
from jat_slides.partitions import mun_partitions, zone_partitions
from jat_slides.resources import PathResource


def add_built_legend(cmap: Colormap, *, ax: Axes, loc: str | None) -> None:
    if loc is None:
        loc = "upper left"

    patches = []
    for i, year in enumerate(range(1975, 2021, 5)):
        label = "1975 o antes" if year == 1975 else str(year)
        patches.append(Patch(color=cmap(i), label=label))

    leg = ax.legend(
        handles=patches,
        title="Año de construcción",
        alignment="left",
        framealpha=1,
        loc=loc,
    )

    leg.set_zorder(9999)


@dg.op(
    ins={"data_and_transform": dg.In(input_manager_key="reprojected_raster_manager")},
    out=dg.Out(io_manager_key="plot_manager"),
)
def plot_raster(
    context: dg.OpExecutionContext,
    path_resource: PathResource,
    bounds: tuple[float, float, float, float],
    data_and_transform: tuple[np.ndarray, Affine],
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

    data, transform = data_and_transform

    data = data.astype(float)
    data[data == 0] = np.nan

    cmap = mpl.colormaps["magma_r"].resampled(10)
    rio_plot.show(data, transform=transform, ax=ax, cmap=cmap)
    add_built_legend(cmap, ax=ax, loc=legend_pos)

    overlay_dir = (
        Path(path_resource.data_path) / "overlays" / str(context.partition_key)
    )
    add_overlay(overlay_dir, ax=ax, config=overlay_config)

    return fig


def built_plot_factory(
    level: str,
    *,
    bounds_op: dg.OpDefinition,
    labels_op: dg.OpDefinition,
    legend_pos_op: dg.OpDefinition,
    overlay_config_op: dg.OpDefinition,
    partitions_def: dg.PartitionsDefinition,
) -> dg.AssetsDefinition:
    @dg.graph_asset(
        name="built",
        key_prefix=f"plot_{level}",
        ins={
            "data_and_transform": dg.AssetIn(
                key=f"built_{level}",
                input_manager_key="reprojected_raster_manager",
            ),
        },
        partitions_def=partitions_def,
        group_name=f"plot_{level}",
    )
    def _asset(data_and_transform: tuple[np.ndarray, Affine]) -> Figure:
        bounds = bounds_op()
        labels = labels_op()
        legend_pos = legend_pos_op()
        overlay_config = overlay_config_op()
        return plot_raster(
            bounds,
            data_and_transform,
            labels=labels,
            legend_pos=legend_pos,
            overlay_config=overlay_config,
        )

    return _asset


built_plot_zone = built_plot_factory(
    "zone",
    bounds_op=get_bounds_base,
    labels_op=get_labels_zone,
    legend_pos_op=get_legend_pos_base,
    overlay_config_op=get_overlay_config_zone,
    partitions_def=zone_partitions,
)

built_plot_mun = built_plot_factory(
    "mun",
    bounds_op=get_bounds_mun,
    labels_op=get_labels_mun,
    legend_pos_op=get_legend_pos_mun,
    overlay_config_op=get_overlay_config_mun,
    partitions_def=mun_partitions,
)
