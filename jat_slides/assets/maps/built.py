from pathlib import Path

import matplotlib as mpl
import numpy as np
import rasterio.plot as rio_plot
from affine import Affine
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from matplotlib.patches import Patch

import dagster as dg
from jat_slides.assets.maps.common import (
    add_overlay,
    generate_figure,
    get_bounds_base,
    get_bounds_mun,
    get_bounds_trimmed,
    get_labels_zone,
    get_legend_pos_base,
)
from jat_slides.partitions import mun_partitions, zone_partitions
from jat_slides.resources import PathResource


def add_built_legend(cmap, *, ax: Axes) -> None:
    patches = []
    for i, year in enumerate(range(1975, 2021, 5)):
        label = "1975 o antes" if year == 1975 else str(year)
        patches.append(Patch(color=cmap(i), label=label))

    leg = ax.legend(
        handles=patches,
        title="Año de construcción",
        alignment="left",
        framealpha=1,
        loc="upper left",
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
    add_built_legend(cmap, ax=ax)

    overlay_path = Path(path_resource.data_path) / "overlays"
    fpath = overlay_path / f"{context.partition_key}.gpkg"
    add_overlay(fpath, ax)

    return fig


@dg.graph_asset(
    name="built",
    key_prefix="plot",
    ins={
        "data_and_transform": dg.AssetIn(
            key="built",
            input_manager_key="reprojected_raster_manager",
        ),
    },
    partitions_def=zone_partitions,
    group_name="plot",
)
def built_plot(data_and_transform: tuple[np.ndarray, Affine]) -> Figure:
    bounds = get_bounds_base()
    labels = get_labels_zone()
    legend_pos = get_legend_pos_base()
    return plot_raster(bounds, data_and_transform, labels=labels, legend_pos=legend_pos)


@dg.graph_asset(
    name="built",
    key_prefix="plot_mun",
    ins={"data_and_transform": dg.AssetIn(key="built_mun")},
    partitions_def=mun_partitions,
    group_name="plot_mun",
)
def built_plot_mun(data_and_transform: tuple[np.ndarray, Affine]) -> Figure:
    bounds = get_bounds_mun()
    return plot_raster(bounds, data_and_transform)


@dg.graph_asset(
    name="built",
    key_prefix="plot_trimmed",
    ins={"data_and_transform": dg.AssetIn(key="built_trimmed")},
    partitions_def=zone_partitions,
    group_name="plot_trimmed",
)
def built_plot_trimmed(data_and_transform: tuple[np.ndarray, Affine]) -> Figure:
    bounds = get_bounds_trimmed()
    return plot_raster(bounds, data_and_transform)
