from pathlib import Path
from unittest.mock import patch

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pytest
from affine import Affine
from cfc_dagster_utils.partitions import zone_partitions
from cfc_dagster_utils.resources import PostgresResource
from matplotlib.collections import LineCollection
from shapely.geometry import Point, box

import dagster as dg
from jat_slides.defs.assets.maps import built, income, jobs, population_grid
from jat_slides.defs.assets.remoteness import (
    RADIUS_COLUMNS,
    build_remoteness_circles,
    remoteness_circles_zone,
)
from jat_slides.defs.resources import ConfigResource, PathResource

ZONE = "01.1.01"


def brackets() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "CVE_MET": ["02.1.01", ZONE],
            "r_3": [10, 3],
            "r_5": [20, 5],
            "r_9.3": [30, 9.3],
            "rmax": [100, 100],
        }
    )


def centroid_frame() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(geometry=[Point(-102.3, 21.9)], index=[ZONE], crs=4326)


@pytest.mark.parametrize("crs", [4326, 6372])
def test_circle_distances(crs: int) -> None:
    centroid = centroid_frame().to_crs(crs)
    circles = build_remoteness_circles(brackets(), centroid, ZONE)
    np.testing.assert_array_equal(circles.radius_column, RADIUS_COLUMNS)
    np.testing.assert_array_equal(circles.radius_km, [3, 5, 9.3])
    if circles.crs is None:
        pytest.fail("Circle CRS is missing")
    np.testing.assert_equal(circles.crs.to_epsg(), 4326)
    # Independent ellipsoidal distance check, using the CRS's geodesic calculator.
    geod = circles.crs.get_geod()
    if geod is None:
        pytest.fail("Geodesic calculator is missing")
    for ring, radius in zip(circles.geometry, circles.radius_km, strict=True):
        coords = np.asarray(ring.coords)
        _, _, distances = geod.inv(
            np.full(len(coords), -102.3),
            np.full(len(coords), 21.9),
            coords[:, 0],
            coords[:, 1],
        )
        np.testing.assert_allclose(distances, radius * 1000, atol=0.01)
        np.testing.assert_equal(len(coords), 361)


@pytest.mark.parametrize(
    "problem",
    ["missing", "duplicate", "column", "zero", "negative", "nan", "infinite", "text"],
)
def test_invalid_brackets(problem: str) -> None:
    frame = brackets()
    if problem == "missing":
        frame = frame.iloc[:1]
    elif problem == "duplicate":
        frame = pd.concat([frame, frame.iloc[1:]])
    elif problem == "column":
        frame = frame.drop(columns="r_3")
    else:
        values = {
            "zero": 0,
            "negative": -1,
            "nan": np.nan,
            "infinite": np.inf,
            "text": "bad",
        }
        frame["r_3"] = frame["r_3"].astype(object)
        frame.loc[1, "r_3"] = values[problem]
    with pytest.raises(ValueError, match=ZONE):
        build_remoteness_circles(frame, centroid_frame(), ZONE)


@pytest.mark.parametrize(
    "problem", ["missing", "multiple", "empty", "polygon", "null", "crs", "index"]
)
def test_invalid_centroid(problem: str) -> None:
    frame = centroid_frame()
    if problem == "missing":
        frame = frame.iloc[:0]
    elif problem == "multiple":
        frame = pd.concat([frame, frame])
    elif problem == "crs":
        frame = gpd.GeoDataFrame(geometry=[Point(-102.3, 21.9)], index=[ZONE])
    elif problem == "index":
        frame.index = ["02.1.01"]
    else:
        geometry = {"empty": Point(), "polygon": box(0, 0, 1, 1), "null": None}[problem]
        frame = gpd.GeoDataFrame(
            geometry=pd.Series([geometry], index=[ZONE]), index=[ZONE], crs=4326
        )
    with pytest.raises(ValueError, match=ZONE):
        build_remoteness_circles(brackets(), frame, ZONE)


@pytest.mark.parametrize(
    ("kind", "level"),
    [
        (kind, level)
        for kind in ["built", "jobs", "population_grid", "income", "empty_income"]
        for level in ["zone", "mun"]
    ]
    + [("population_grid_1990_2020", "zone")],
)
def test_map_graph_renders_circles(tmp_path: Path, kind: str, level: str) -> None:
    name = "income" if kind == "empty_income" else kind
    module = {
        "built": built,
        "jobs": jobs,
        "population_grid": population_grid,
        "population_grid_1990_2020": population_grid,
        "income": income,
    }[name]
    zone_asset = getattr(module, f"{name}_plot_zone")
    mun_asset = getattr(
        module,
        "population_grid_plot_mun"
        if name == "population_grid_1990_2020"
        else f"{name}_plot_mun",
    )
    circle_key = remoteness_circles_zone.key
    np.testing.assert_equal(
        circle_key in zone_asset.asset_deps[zone_asset.key], desired=True
    )
    np.testing.assert_equal(
        circle_key in mun_asset.asset_deps[mun_asset.key], desired=False
    )
    asset = zone_asset if level == "zone" else mun_asset
    partitions = asset.partitions_def
    if partitions is None:
        pytest.fail("Maps must remain partitioned")
    partition = ZONE if level == "zone" else partitions.get_partition_keys()[0]
    upstream = next(key for key in asset.asset_deps[asset.key] if key != circle_key)

    @dg.asset(
        key=upstream,
        partitions_def=partitions,
        io_manager_key="reprojected_raster_manager"
        if name == "built"
        else "io_manager",
    )
    def source() -> object:
        if name == "built":
            return np.array([[1975, 2000], [2010, 2020]]), Affine(
                0.01, 0, -102.31, 0, -0.01, 21.91
            )
        frame = gpd.GeoDataFrame(
            {
                "jobs": np.arange(1, 13) * 100 + 1,
                "income_pc": np.arange(1, 13) * 100,
                "difference": np.arange(-6, 6),
            },
            geometry=[
                box(-102.31 + i * 0.001, 21.89, -102.309 + i * 0.001, 21.91)
                for i in range(12)
            ],
            crs=4326,
        )
        return frame.iloc[:0] if kind == "empty_income" else frame

    @dg.asset(key="centroids_zone", partitions_def=zone_partitions)
    def centroid() -> gpd.GeoDataFrame:
        return centroid_frame().to_crs(6372)

    brackets().to_csv(tmp_path / "remoteness_brackets.csv", index=False)
    bounds = [-102.32, 21.88, -102.28, 21.92]
    fig, ax = plt.subplots()
    ax.set_xlim(bounds[0], bounds[2])
    ax.set_ylim(bounds[1], bounds[3])
    ax.set_aspect(1.2)
    paths = PathResource(
        in_path=str(tmp_path),
        out_path=str(tmp_path),
        pg_path="unused",
        job_estimation_path="unused",
    )
    assets = [asset, source]
    if level == "zone":
        assets.extend([centroid, remoteness_circles_zone])
    config = ConfigResource(bounds={partition: bounds}, legend_pos={})
    try:
        with patch.object(module, "generate_figure", return_value=(fig, ax)):
            result = dg.materialize(
                assets,
                partition_key=partition,
                resources={
                    "path_resource": paths,
                    "postgis_resource": PostgresResource(
                        host="unused",
                        port=5432,
                        user="unused",
                        password=str(tmp_path),
                        database="unused",
                    ),
                    "zone_config_resource": config,
                    "mun_config_resource": config,
                    "geodataframe_manager": dg.InMemoryIOManager(),
                    "reprojected_raster_manager": dg.InMemoryIOManager(),
                    "plot_manager": dg.InMemoryIOManager(),
                },
            )
        np.testing.assert_equal(result.success, desired=True)
        rings = [
            collection
            for collection in ax.collections
            if isinstance(collection, LineCollection)
            and collection.get_zorder() == 9998
        ]
        np.testing.assert_equal(len(rings), 1 if level == "zone" else 0)
        if level == "zone":
            np.testing.assert_equal(len(rings[0].get_segments()), 3)
            np.testing.assert_equal(rings[0].get_clip_on(), desired=True)
        np.testing.assert_allclose(ax.get_xlim(), [bounds[0], bounds[2]])
        np.testing.assert_allclose(ax.get_ylim(), [bounds[1], bounds[3]])
        np.testing.assert_equal(ax.get_aspect(), 1.0 if name == "built" else 1.2)
        if kind != "empty_income":
            legend = ax.get_legend()
            if legend is None:
                pytest.fail("Existing map legend was lost")
            np.testing.assert_equal(legend.get_zorder(), 9999)
            if name.startswith("population_grid"):
                np.testing.assert_equal(
                    legend.get_title().get_text(),
                    "Cambio de población\n(2020 - 1990)"
                    if name == "population_grid_1990_2020"
                    else "Cambio de población\n(2020 - 2000)",
                )
    finally:
        plt.close(fig)
