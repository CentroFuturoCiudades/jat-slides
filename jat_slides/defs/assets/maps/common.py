import itertools
import os
from collections.abc import Sequence
from pathlib import Path
from typing import Literal

import contextily as cx
import geopandas as gpd
import matplotlib.colors as mcol
import matplotlib.patheffects as mpe
import matplotlib.pyplot as plt
import numpy as np
import shapely
from dagster_components.resources import PostGISResource
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from matplotlib.patches import Patch

import dagster as dg
from jat_slides.defs.resources import (
    ConfigResource,
)

cmap_rdbu = mcol.LinearSegmentedColormap.from_list(
    "RdBu2",
    ["#67001f", "#c94741", "#f7b799", "#f6f7f7", "#5991e1", "#3340e2", "#090393"],
    N=255,
)


def get_cmap_bounds(differences: Sequence[float], n_steps: int) -> np.ndarray:
    diff_arr = np.array(differences)

    pos_step = diff_arr.max() / n_steps
    neg_step = diff_arr.min() / n_steps

    return np.array(
        [neg_step * i for i in range(1, n_steps + 1)][::-1]
        + [-0.001, 0.001]
        + [pos_step * i for i in range(1, n_steps + 1)],
    )


def add_pop_legend(
    bounds: np.ndarray,
    *,
    ax: Axes,
    cmap: mcol.Colormap,
    legend_pos: str = "upper right",
) -> None:
    cmap = cmap.resampled(7)

    patches = []
    for i, (lower, upper) in enumerate(itertools.pairwise(bounds)):
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
    *,
    level: Literal["ent", "mun"],
    xmin: float,
    ymin: float,
    xmax: float,
    ymax: float,
    ax: Axes,
    add_labels: bool,
    postgis_resource: PostGISResource,
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

    if level == "ent":
        name_col = "NOM_ENT"
    elif level == "mun":
        name_col = "NOM_MUN"

    with postgis_resource.connect() as conn:
        df_mun = (
            gpd.read_postgis(
                f"""
            SELECT
                census_2020_{level}."CVEGEO",
                census_2020_{level}.geometry,
                census_2020_{level}."{name_col}"
            FROM census_2020_{level}
            WHERE ST_Intersects(
                census_2020_{level}.geometry,
                ST_Transform(
                    ST_MakeEnvelope(%(xmin)s, %(ymin)s, %(xmax)s, %(ymax)s, 4326),
                    6372
                )
            )
            """,  # noqa: S608
                conn,
                params={
                    "xmin": xmin,
                    "ymin": ymin,
                    "xmax": xmax,
                    "ymax": ymax,
                },
                geom_col="geometry",
            )
            .to_crs("EPSG:4326")
            .set_index("CVEGEO")
            .rename(columns={name_col: "name"})
        )

    df_mun.plot(
        ax=ax,
        **poly_kwargs,
    )

    if add_labels:
        if level is None:
            err = "level must be 'ent' or 'mun' if add_labels is True"
            raise ValueError(err)

        bbox = shapely.box(xmin, ymin, xmax, ymax)
        df_mun_trimmed = df_mun.assign(
            geometry=lambda df: df["geometry"].intersection(bbox),
            coords=lambda df: df["geometry"].apply(
                lambda x: x.representative_point().coords[:],
            ),
            name=lambda df: df["name"].replace({"México": "Estado de México"}),
        ).assign(coords=lambda df: df["coords"].apply(lambda x: x[0]))

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
    postgis_resource: PostGISResource,
    add_mun_bounds: bool = False,
    add_mun_labels: bool = False,
    add_state_bounds: bool = False,
    add_state_labels: bool = False,
    state_poly_kwargs: dict | None = None,
    state_text_kwargs: dict | None = None,
    mun_poly_kwargs: dict | None = None,
    mun_text_kwargs: dict | None = None,
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

    cx.add_basemap(ax, source=cx.providers.CartoDB.PositronNoLabels, crs="EPSG:4326")  # ty:ignore[unresolved-attribute]

    if add_mun_bounds:
        if population_grids_path is None:
            err = "population_grids_path must be provided if add_mun_bounds is True"
            raise ValueError(err)

        population_grids_path = Path(population_grids_path)
        add_polygon_bounds(
            xmin=xmin,
            ymin=ymin,
            xmax=xmax,
            ymax=ymax,
            ax=ax,
            add_labels=add_mun_labels,
            poly_kwargs=mun_poly_kwargs,
            text_kwargs=mun_text_kwargs,
            level="mun",
            postgis_resource=postgis_resource,
        )

    if add_state_bounds:
        if population_grids_path is None:
            err = "population_grids_path must be provided if add_state_bounds is True"
            raise ValueError(err)

        population_grids_path = Path(population_grids_path)

        add_polygon_bounds(
            xmin=xmin,
            ymin=ymin,
            xmax=xmax,
            ymax=ymax,
            ax=ax,
            add_labels=add_state_labels,
            poly_kwargs=state_poly_kwargs,
            text_kwargs=state_text_kwargs,
            level="ent",
            postgis_resource=postgis_resource,
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


def get_overlay_config_op_factory(level: str) -> dg.OpDefinition:
    @dg.op(
        name=f"get_overlay_config_{level}",
        required_resource_keys={f"{level}_config_resource"},
    )
    def _op(context: dg.OpExecutionContext) -> dict | None:
        config_resource = getattr(context.resources, f"{level}_config_resource", None)
        if config_resource is None:
            err = f"Resource '{level}_config_resource' not found in context.resources"
            raise ValueError(err)

        if (
            config_resource.overlays is not None
            and context.partition_key in config_resource.overlays
        ):
            return config_resource.overlays[context.partition_key]

        return None

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


def add_overlay(overlay_dir: Path, *, ax: Axes, config: dict | None) -> None:
    if overlay_dir.exists():
        for fpath in overlay_dir.glob("*.gpkg"):
            if config is None:
                subconfig: dict = {"linewidth": 3, "color": "k", "add_points": False}
            else:
                subconfig: dict = config[fpath.stem]

            if "patheffects" in subconfig:
                path_effects = subconfig.pop("patheffects")

                subconfig["path_effects"] = [
                    mpe.Stroke(
                        linewidth=path_effects["linewidth"],
                        foreground=path_effects["foreground"],
                    ),
                    mpe.Normal(),
                ]

            geom = gpd.read_file(fpath).to_crs("EPSG:4326")["geometry"]
            geom.plot(ax=ax, **subconfig)


get_bounds_base = get_bounds_op_factory("zone")
get_bounds_mun = get_bounds_op_factory("mun")

get_legend_pos_base = get_legend_pos_op_factory("zone")
get_legend_pos_mun = get_legend_pos_op_factory("mun")

get_overlay_config_zone = get_overlay_config_op_factory("zone")
get_overlay_config_mun = get_overlay_config_op_factory("mun")
