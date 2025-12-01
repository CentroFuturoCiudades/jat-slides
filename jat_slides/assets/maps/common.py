import os
from pathlib import Path
from typing import Literal

import contextily as cx
import geopandas as gpd
import matplotlib.colors as mcol
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import shapely
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from matplotlib.patches import Patch
from shapely.plotting import plot_line

import dagster as dg
from jat_slides.resources import (
    ConfigResource,
)

cmap_rdbu = mcol.LinearSegmentedColormap.from_list(
    "RdBu2",
    ["#67001f", "#c94741", "#f7b799", "#f6f7f7", "#5991e1", "#3340e2", "#090393"],
    N=255,
)


def get_cmap_bounds(differences, n_steps) -> np.ndarray:
    pos_step = differences.max() / n_steps
    neg_step = differences.min() / n_steps

    return np.array(
        [neg_step * i for i in range(1, n_steps + 1)][::-1]
        + [-0.001, 0.001]
        + [pos_step * i for i in range(1, n_steps + 1)],
    )


def add_pop_legend(bounds, *, ax, cmap: mcol.Colormap, legend_pos: str = "upper right"):
    cmap = cmap.resampled(7)

    patches = []
    for i, (lower, upper) in enumerate(zip(bounds, bounds[1:], strict=False)):
        if np.round(lower) == 0 and np.round(upper) == 0:
            label = "Sin cambio"
        else:
            label = f"{lower:,.0f} - {upper:,.0f}"
        patches.append(Patch(color=cmap(i), label=label))
    patches.reverse()

    ax.legend(
        handles=patches,
        title="Cambio de población\n(2020 - 2000)",
        alignment="left",
        framealpha=1,
        loc=legend_pos,
    ).set_zorder(9999)


def process_default_args(default_args: dict, kwargs: dict | None) -> dict:
    if kwargs is None:
        return default_args

    kwargs = kwargs.copy()
    for key, value in default_args.items():
        if key not in kwargs:
            kwargs[key] = value

    return kwargs


def add_polygon_bounds(
    path: os.PathLike,
    census_path: os.PathLike,
    *,
    xmin: float,
    ymin: float,
    xmax: float,
    ymax: float,
    ax: Axes,
    add_labels: bool,
    label_level: Literal["state", "mun"] | None = None,
    poly_kwargs: dict | None = None,
    text_kwargs: dict | None = None,
) -> None:
    default_poly_kwargs = {
        "linewidth": 0.5,
        "facecolor": "none",
        "edgecolor": "k",
        "zorder": 999,
        "alpha": 0.3,
    }

    default_text_kwargs = {
        "fontsize": 6,
        "color": "k",
        "alpha": 0.7,
        "weight": "bold",
    }

    poly_kwargs = process_default_args(default_poly_kwargs, poly_kwargs)
    text_kwargs = process_default_args(default_text_kwargs, text_kwargs)

    df_mun = gpd.read_file(path).to_crs("EPSG:4326").set_index("CVEGEO")
    bbox = shapely.box(xmin, ymin, xmax, ymax)
    df_mun = df_mun[df_mun.intersects(bbox)]

    if not isinstance(df_mun, gpd.GeoDataFrame):
        err = "df_mun must be a GeoDataFrame"
        raise TypeError(err)

    df_mun.plot(
        ax=ax,
        **poly_kwargs,
    )

    if add_labels:
        if label_level is None:
            err = "label_level must be 'state' or 'mun' if add_labels is True"
            raise ValueError(err)

        if label_level == "state":
            usecols = ["ENTIDAD", "NOM_ENT"]
            name_col = "NOM_ENT"
        elif label_level == "mun":
            usecols = ["ENTIDAD", "MUN", "NOM_MUN"]
            name_col = "NOM_MUN"
        else:
            err = f"label_level must be 'state' or 'mun', got {label_level}"
            raise ValueError(err)

        df_name = (
            pd.read_csv(census_path, usecols=usecols)
            .assign(CVEGEO="")
            .rename(columns={name_col: "name"})
        )

        if "ENTIDAD" in usecols:
            df_name = df_name.assign(
                CVEGEO=lambda df: df["CVEGEO"] + df["ENTIDAD"].astype(str).str.zfill(2),
            )
        if "MUN" in usecols:
            df_name = df_name.assign(
                CVEGEO=lambda df: df["CVEGEO"] + df["MUN"].astype(str).str.zfill(3),
            )

        df_name = (
            df_name.drop(columns=["ENTIDAD", "MUN"], errors="ignore")
            .drop_duplicates(subset=["CVEGEO"])
            .set_index("CVEGEO")
        )

        df_mun_trimmed = (
            df_mun[["geometry"]]
            .join(df_name, how="inner")
            .copy()
            .assign(
                geometry=lambda df: df["geometry"].intersection(bbox),
                coords=lambda df: df["geometry"].apply(
                    lambda x: x.representative_point().coords[:],
                ),
            )
        )
        df_mun_trimmed["coords"] = [c[0] for c in df_mun_trimmed["coords"]]

        df_mun_trimmed["name"] = df_mun_trimmed["name"].replace(
            {"México": "Estado de México"},
        )

        for _, row in df_mun_trimmed.iterrows():
            text = row["name"]
            if not isinstance(text, str):
                err = "text must be a string"
                raise TypeError(err)

            xy = row["coords"]
            if not isinstance(xy, tuple):
                err = "xy must be a tuple"
                raise TypeError(err)

            ax.annotate(
                text=text,
                xy=xy,
                horizontalalignment="center",
                **text_kwargs,
            )


def generate_figure(
    xmin: float,
    ymin: float,
    xmax: float,
    ymax: float,
    *,
    add_mun_bounds: bool = False,
    add_mun_labels: bool = False,
    add_state_bounds: bool = False,
    add_state_labels: bool = False,
    state_poly_kwargs: dict | None = None,
    state_text_kwargs: dict | None = None,
    mun_poly_kwargs: dict | None = None,
    mun_text_kwargs: dict | None = None,
    state: int | None = None,
    population_grids_path: os.PathLike | str | None = None,
) -> tuple[Figure, Axes]:
    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.axis("off")

    ax.set_xlim(xmin, xmax)
    ax.set_ylim(ymin, ymax)

    fig.subplots_adjust(bottom=0)
    fig.subplots_adjust(top=1)
    fig.subplots_adjust(right=1)
    fig.subplots_adjust(left=0)

    cx.add_basemap(ax, source=cx.providers.CartoDB.PositronNoLabels, crs="EPSG:4326")  # type: ignore

    if add_mun_bounds:
        if population_grids_path is None:
            err = "population_grids_path must be provided if add_mun_bounds is True"
            raise ValueError(err)

        population_grids_path = Path(population_grids_path)
        add_polygon_bounds(
            Path(population_grids_path / "final" / "framework" / "mun" / "2020.gpkg"),
            Path(
                population_grids_path
                / "initial"
                / "census"
                / "INEGI"
                / "2020"
                / f"conjunto_de_datos_ageb_urbana_{str(state).zfill(2)}_cpv2020.csv",
            ),
            xmin=xmin,
            ymin=ymin,
            xmax=xmax,
            ymax=ymax,
            ax=ax,
            add_labels=add_mun_labels,
            poly_kwargs=mun_poly_kwargs,
            text_kwargs=mun_text_kwargs,
            label_level="mun",
        )

    if add_state_bounds:
        if population_grids_path is None:
            err = "population_grids_path must be provided if add_state_bounds is True"
            raise ValueError(err)

        population_grids_path = Path(population_grids_path)

        add_polygon_bounds(
            Path(population_grids_path / "final" / "framework" / "state" / "2020.gpkg"),
            Path(
                population_grids_path
                / "initial"
                / "census"
                / "INEGI"
                / "2020"
                / f"conjunto_de_datos_ageb_urbana_{str(state).zfill(2)}_cpv2020.csv",
            ),
            xmin=xmin,
            ymin=ymin,
            xmax=xmax,
            ymax=ymax,
            ax=ax,
            add_labels=add_state_labels,
            poly_kwargs=state_poly_kwargs,
            text_kwargs=state_text_kwargs,
            label_level="state",
        )

    return fig, ax


def get_bounds_op_factory(level: str) -> dg.OpDefinition:
    @dg.op(
        name=f"get_bounds_{level}",
        required_resource_keys={f"{level}_config_resource"},
    )
    def _op(
        context: dg.OpExecutionContext,
    ) -> tuple[float, float, float, float]:
        config_resource = getattr(context.resources, f"{level}_config_resource", None)
        if config_resource is None:
            err = f"Resource '{level}_config_resource' not found in context.resources"
            raise ValueError(err)

        out = tuple(config_resource.bounds[context.partition_key])
        if len(out) != 4:
            err = f"Expected 4 bounds, got {len(out)}: {out}"
            raise ValueError(err)
        return out

    return _op


def get_legend_pos_op_factory(level: str) -> dg.OpDefinition:
    @dg.op(
        name=f"get_legend_pos_{level}",
        required_resource_keys={f"{level}_config_resource"},
    )
    def _op(context: dg.OpExecutionContext) -> str:
        config_resource = getattr(context.resources, f"{level}_config_resource", None)
        if config_resource is None:
            err = f"Resource '{level}_config_resource' not found in context.resources"
            raise ValueError(err)

        if context.partition_key in config_resource.legend_pos:
            return config_resource.legend_pos[context.partition_key]
        return "upper right"

    return _op


@dg.op
def get_labels_zone(
    context: dg.OpExecutionContext,
    zone_config_resource: ConfigResource,
) -> dict[str, bool]:
    if (
        zone_config_resource.add_labels is not None
        and context.partition_key in zone_config_resource.add_labels
    ):
        return {
            "state": "state" in zone_config_resource.add_labels[context.partition_key],
            "mun": "mun" in zone_config_resource.add_labels[context.partition_key],
        }
    return {
        "state": False,
        "mun": False,
    }


@dg.op
def get_labels_mun(
    context: dg.OpExecutionContext,
    mun_config_resource: ConfigResource,
) -> dict[str, bool]:
    if (
        mun_config_resource.add_labels is not None
        and context.partition_key in mun_config_resource.add_labels
    ):
        return {
            "state": "state" in mun_config_resource.add_labels[context.partition_key],
            "mun": "mun" in mun_config_resource.add_labels[context.partition_key],
        }
    return {
        "state": False,
        "mun": False,
    }


def update_categorical_legend(
    ax: Axes,
    title: str,
    fmt: str,
    cmap: mcol.Colormap,
    legend_pos: str,
) -> None:
    leg = ax.get_legend()

    if leg is None:
        err = "No legend found in the provided Axes"
        raise ValueError(err)

    leg.set_title(title)
    leg.set_alignment("left")

    texts = []
    for text_obj in leg.get_texts():
        start, end = text_obj.get_text().split(",")
        start = float(start.strip())
        end = float(end.strip())
        texts.append(f"{start:{fmt}} - {end:{fmt}}")

    steps = [i / (len(texts) - 1) for i in range(len(texts))]
    steps[-1] = 0.9999
    handles = [Patch(facecolor=cmap(x)) for x in reversed(steps)]

    ax.legend(
        handles=handles,
        labels=reversed(texts),
        title=title,
        alignment="left",
        framealpha=1,
        loc=legend_pos,
    ).set_zorder(9999)


@dg.op
def get_linewidth(
    context: dg.OpExecutionContext,
    zone_config_resource: ConfigResource,
) -> float:
    if (
        zone_config_resource.linewidths is not None
        and context.partition_key in zone_config_resource.linewidths
    ):
        return zone_config_resource.linewidths[context.partition_key]
    return 0.2


@dg.op
def intersect_geometries(
    sources: gpd.GeoDataFrame,
    targets: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    if sources.crs is None:
        err = "Sources must have a CRS"
        raise ValueError(err)

    overlay = sources.sjoin(targets[["geometry"]].to_crs(sources.crs))
    idx = overlay.index.unique()
    return sources.loc[idx]


def add_overlay(fpath: Path, ax: Axes) -> None:
    if fpath.exists():
        geom = gpd.read_file(fpath).to_crs("EPSG:4326")["geometry"].item()
        plot_line(geom, ax=ax, linewidth=3, color="k", add_points=False)


get_bounds_base = get_bounds_op_factory("zone")
get_bounds_mun = get_bounds_op_factory("mun")
get_bounds_trimmed = get_bounds_op_factory("trimmed")

get_legend_pos_base = get_legend_pos_op_factory("zone")
get_legend_pos_mun = get_legend_pos_op_factory("mun")
get_legend_pos_trimmed = get_legend_pos_op_factory("trimmed")
