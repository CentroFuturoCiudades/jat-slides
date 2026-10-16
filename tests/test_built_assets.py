from pathlib import Path
from unittest.mock import patch

import geopandas as gpd
import numpy as np
import pytest
import rasterio as rio
from affine import Affine
from shapely.geometry import box

import dagster as dg
from jat_slides.defs.assets.built import built, built_mun
from jat_slides.defs.assets.stats.built_area import built_area_mun, built_area_zone
from jat_slides.defs.managers import RasterIOManager, ReprojectedRasterIOManager
from jat_slides.defs.resources import EarthEngineResource, PathResource


def boundary_asset(
    year: int, prefix: str, partitions: dg.PartitionsDefinition
) -> dg.AssetsDefinition:
    @dg.asset(key=[prefix, str(year)], partitions_def=partitions)
    def boundary() -> gpd.GeoDataFrame:
        return gpd.GeoDataFrame(
            geometry=[box(-103, 21, -102 + (year - 1990) / 100, 22)],
            crs="EPSG:4326",
        )

    return boundary


@pytest.mark.parametrize(
    ("raster_asset", "area_asset", "prefix"),
    [(built, built_area_zone, "agebs"), (built_mun, built_area_mun, "muns")],
)
def test_materialize_and_load_assets(
    tmp_path: Path,
    raster_asset: dg.AssetsDefinition,
    area_asset: dg.AssetsDefinition,
    prefix: str,
) -> None:
    partitions = raster_asset.partitions_def
    if partitions is None:
        pytest.fail("Built assets must retain their partitions")
    partition = partitions.get_partition_keys()[0]
    paths = PathResource(
        in_path=str(tmp_path), out_path=str(tmp_path), pg_path="unused"
    )
    raster_manager = RasterIOManager(path_resource=paths, extension=".tif")
    data = np.array([[0, 1975], [2000, 2020]], dtype=np.uint16)
    transform = Affine(100, 0, -9600000, 0, -100, 2700000)
    with (
        patch("jat_slides.defs.resources.ee.Initialize") as initialize,
        patch("jat_slides.ghsl.first_built_raster", return_value=(data, transform)),
        patch(
            "jat_slides.ghsl.built_surface_area",
            side_effect=lambda year, _: float(year),
        ) as area,
    ):
        resource = EarthEngineResource(project="test-project")
        initialize.assert_not_called()
        result = dg.materialize(
            [
                raster_asset,
                area_asset,
                *[
                    boundary_asset(year, prefix, partitions)
                    for year in (1990, 2000, 2010, 2020)
                ],
            ],
            partition_key=partition,
            resources={
                "earthengine_resource": resource,
                "raster_manager": raster_manager,
                "dataframe_manager": dg.InMemoryIOManager(),
            },
        )
    np.testing.assert_equal(result.success, desired=True)
    initialize.assert_called_once_with(project="test-project")
    np.testing.assert_equal(
        sorted(call.args[0] for call in area.call_args_list), [1990, 2000, 2010, 2020]
    )
    # Each area's query must retain its own year's boundaries.
    np.testing.assert_equal(
        len({call.args[1][0].wkt for call in area.call_args_list}), 4
    )
    frame = result.output_for_node(area_asset.node_def.name)
    np.testing.assert_equal(frame.columns.tolist(), ["year", "area"])
    np.testing.assert_equal(frame["area"].tolist(), [1990.0, 2000.0, 2010.0, 2020.0])

    path = tmp_path / raster_asset.key.to_user_string() / f"{partition}.tif"
    with rio.open(path) as dataset:
        np.testing.assert_array_equal(dataset.read(1), data)
        np.testing.assert_equal(dataset.transform, transform)
        np.testing.assert_equal(dataset.crs, rio.CRS.from_string("ESRI:54009"))
    manager = ReprojectedRasterIOManager(
        path_resource=paths, extension=".tif", crs="EPSG:4326"
    )
    with dg.build_input_context(
        asset_key=raster_asset.key,
        partition_key=partition,
        asset_partition_key_range=dg.PartitionKeyRange(partition, partition),
        asset_partitions_def=partitions,
    ) as context:
        loaded = manager.load_input(context)
    if isinstance(loaded, dict):
        pytest.fail("Expected a single raster partition")
    np.testing.assert_equal(np.isin(loaded[0], data).all(), desired=True)


def test_empty_project_fails_before_network() -> None:
    with (
        patch("jat_slides.defs.resources.ee.Initialize") as initialize,
        dg.build_init_resource_context() as context,
        pytest.raises(ValueError, match="EARTHENGINE_PROJECT"),
    ):
        EarthEngineResource(project=" ").setup_for_execution(context)
    initialize.assert_not_called()
