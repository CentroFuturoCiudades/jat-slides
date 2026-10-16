from unittest.mock import Mock, patch

import geopandas as gpd
import numpy as np
import pytest
import rasterio as rio
import shapely
from affine import Affine
from rasterio.io import MemoryFile
from rasterio.mask import mask
from shapely.geometry import Polygon, box

from jat_slides import ghsl


def array_image(data: np.ma.MaskedArray) -> Mock:
    """Evaluate EE image expressions against masked test pixels."""
    return Mock(
        data=data,
        select=lambda _band: array_image(data),
        rename=lambda _band: array_image(data),
        neq=lambda value: array_image(data != value),
        gte=lambda value: array_image(data >= value),
        multiply=lambda value: array_image(data * value),
        add=lambda value: array_image(data + value),
        updateMask=lambda other: array_image(
            np.ma.masked_where(~other.data.astype(bool), data)
        ),
        unmask=lambda value: array_image(np.ma.array(data.filled(value))),
        toUint16=lambda: array_image(np.ma.array(data.astype(np.uint16))),
        getInfo=data.tolist,
    )


def test_first_built_threshold_epochs_and_missing_pixels() -> None:
    years = ghsl.YEARS
    pixels = {year: np.ma.zeros((1, 7), dtype=np.uint16) for year in years}
    pixels[1975][0] = [2000, 1999, 65535, 0, 65535, 2001, 9000]
    pixels[1975].mask = [[False, False, False, False, False, False, True]]
    pixels[1980][0] = [0, 2000, 1999, 0, 65535, 3000, 2000]
    pixels[2020][0, 2] = 2000

    def image(asset: str) -> Mock:
        return array_image(pixels[int(asset.rsplit("/", 1)[1])])

    def collection(images: list[Mock]) -> Mock:
        return Mock(
            min=lambda: array_image(
                np.ma.min(np.ma.stack([image.data for image in images]), axis=0)
            )
        )

    with (
        patch.object(ghsl.ee, "Image", side_effect=image) as image_mock,
        patch.object(ghsl.ee.ImageCollection, "fromImages", side_effect=collection),
    ):
        actual = ghsl.first_built_image().getInfo()
    np.testing.assert_equal(actual, [[1975, 1980, 2020, 0, 0, 1975, 1980]])
    np.testing.assert_equal(image_mock.call_count, 10)


def test_unsupported_epoch() -> None:
    with pytest.raises(ValueError, match="Unsupported GHSL epoch"):
        ghsl.surface_image(2025)


@pytest.mark.parametrize("tile_size", [2, 3, 2048])
def test_tiled_crop_matches_rasterio(tile_size: int) -> None:
    transform = Affine(100, 0, -18041000, 0, -100, 9000000)
    outer = [(110, 110), (690, 130), (670, 690), (120, 650)]
    hole = [(300, 300), (500, 300), (500, 500), (300, 500)]
    local_geometries = [Polygon(outer, [hole]), box(810, 710, 990, 990)]
    geometries = [
        Polygon(
            [(transform.c + x, transform.f - y) for x, y in geom.exterior.coords],
            [
                [(transform.c + x, transform.f - y) for x, y in ring.coords]
                for ring in geom.interiors
            ],
        )
        for geom in local_geometries
    ]
    source = np.arange(100, dtype=np.uint16).reshape(10, 10) * 100
    source[2, 2] = ghsl.NODATA
    wkt = rio.CRS.from_string(ghsl.CRS).to_wkt()
    grid = ghsl.RasterGrid.from_geometries(geometries, transform, wkt)

    with (
        MemoryFile() as memory,
        memory.open(
            driver="GTiff",
            width=10,
            height=10,
            count=1,
            dtype="uint16",
            transform=transform,
            crs=ghsl.CRS,
        ) as dataset,
    ):
        dataset.write(source, 1)
        expected, expected_transform = mask(dataset, geometries, crop=True, nodata=0)
    expected[expected == ghsl.NODATA] = 0

    def compute_pixels(request: dict) -> np.ndarray:
        pixel_grid = request["grid"]
        affine = pixel_grid["affineTransform"]
        col, row = ~transform * (affine["translateX"], affine["translateY"])
        height = pixel_grid["dimensions"]["height"]
        width = pixel_grid["dimensions"]["width"]
        np.testing.assert_equal(pixel_grid["crsWkt"], wkt)
        result = np.zeros((height, width), dtype=[(ghsl.BAND, "uint16")])
        result[ghsl.BAND] = source[
            int(row) : int(row) + height, int(col) : int(col) + width
        ]
        return result

    with (
        patch.object(ghsl, "TILE_SIZE", tile_size),
        patch.object(ghsl, "query_grid", return_value=grid),
        patch.object(ghsl, "first_built_image", return_value=Mock()),
        patch.object(ghsl, "surface_image", return_value=Mock()),
        patch.object(ghsl.ee.data, "computePixels", side_effect=compute_pixels),
    ):
        actual, actual_transform = ghsl.first_built_raster(geometries)
        area = ghsl.built_surface_area(1990, geometries)
    np.testing.assert_array_equal(actual, expected[0])
    np.testing.assert_equal(actual_transform, expected_transform)
    np.testing.assert_equal(area, float(expected.sum(dtype=np.uint64)))


def test_normalize_geometry_crs_and_empty_inputs() -> None:
    frame = gpd.GeoDataFrame(geometry=[box(-103, 21, -102, 22)], crs="EPSG:4326")
    expected = frame.to_crs(ghsl.CRS).geometry.iloc[0]
    np.testing.assert_equal(
        shapely.equals(ghsl.normalize_geometries(frame)[0], expected), desired=True
    )
    with pytest.raises(ValueError, match="must have a CRS"):
        ghsl.normalize_geometries(gpd.GeoDataFrame(geometry=[box(0, 0, 1, 1)]))
    with pytest.raises(ValueError, match="non-empty"):
        ghsl.normalize_geometries(gpd.GeoDataFrame(geometry=[], crs=ghsl.CRS))
    with pytest.raises(ValueError, match="non-empty"):
        ghsl.RasterGrid.from_geometries([], Affine.identity(), "")


def test_download_failure_is_not_zero_data() -> None:
    grid = ghsl.RasterGrid(Affine.identity(), "test", np.ones((2, 2), dtype=bool))
    with (
        patch.object(ghsl.ee.data, "computePixels", side_effect=RuntimeError("failed")),
        pytest.raises(RuntimeError, match="failed"),
    ):
        list(ghsl.download_tiles(Mock(), grid))


def test_download_rejects_wrong_shape() -> None:
    grid = ghsl.RasterGrid(Affine.identity(), "test", np.ones((2, 2), dtype=bool))
    with (
        patch.object(
            ghsl.ee.data, "computePixels", return_value={ghsl.BAND: np.zeros((1, 1))}
        ),
        pytest.raises(ValueError, match="Unexpected GHSL tile shape"),
    ):
        list(ghsl.download_tiles(Mock(), grid))


def test_area_sum_does_not_overflow() -> None:
    grid = ghsl.RasterGrid(Affine.identity(), "test", np.ones((512, 512), dtype=bool))
    tile = np.full(grid.mask.shape, 30000, dtype=np.uint16)
    with (
        patch.object(ghsl, "query_grid", return_value=grid),
        patch.object(ghsl, "surface_image", return_value=Mock()),
        patch.object(ghsl.ee.data, "computePixels", return_value={ghsl.BAND: tile}),
    ):
        np.testing.assert_equal(ghsl.built_surface_area(2020, []), 30000.0 * 512 * 512)
