import os

import ee
import geopandas as gpd
import numpy as np
import pytest
from shapely.geometry import box

from jat_slides import ghsl


@pytest.mark.skipif(
    os.environ.get("GHSL_LIVE_TEST") != "1",
    reason="Set GHSL_LIVE_TEST=1 to query Earth Engine using configured credentials.",
)
def test_live_reduction_and_area() -> None:
    ee.Initialize(project=os.environ["EARTHENGINE_PROJECT"])
    geometries = ghsl.normalize_geometries(
        gpd.GeoDataFrame(
            geometry=[box(-102.30, 21.88, -102.29, 21.89)], crs="EPSG:4326"
        )
    )
    grid = ghsl.query_grid(geometries)
    expected = np.zeros(grid.mask.shape, dtype=np.uint16)
    area_1990 = 0
    for year in reversed(ghsl.YEARS):
        for rows, cols, tile in ghsl.download_tiles(ghsl.surface_image(year), grid):
            expected[rows, cols][tile >= 2000] = year
            if year == 1990:
                area_1990 += int(tile.sum(dtype=np.uint64))
    actual, transform = ghsl.first_built_raster(geometries)
    np.testing.assert_array_equal(actual, expected)
    np.testing.assert_equal(transform, grid.transform)
    np.testing.assert_equal(ghsl.built_surface_area(1990, geometries), float(area_1990))
