"""GHSL queries on the native 100 m grid, with Rasterio polygon masking."""

from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from math import ceil, floor

import ee
import geopandas as gpd
import numpy as np
import shapely
from affine import Affine
from rasterio.features import bounds as geometry_bounds
from rasterio.features import geometry_mask

COLLECTION = "JRC/GHSL/P2023A/GHS_BUILT_S"
BAND = "built_surface"
YEARS = tuple(range(1975, 2021, 5))
CRS = "ESRI:54009"
NODATA = 65535
TILE_SIZE = 2048


def normalize_geometries(frame: gpd.GeoDataFrame) -> list[shapely.Geometry]:
    if frame.crs is None:
        msg = "GHSL boundaries must have a CRS."
        raise ValueError(msg)
    geometries = [
        geom
        for geom in frame.to_crs(CRS).geometry
        if geom is not None and not shapely.is_empty(geom)
    ]
    if not geometries:
        msg = "GHSL boundaries must contain non-empty geometries."
        raise ValueError(msg)
    return geometries


def surface_image(year: int) -> ee.Image:
    if year not in YEARS:
        msg = f"Unsupported GHSL epoch: {year}. Expected one of {YEARS}."
        raise ValueError(msg)
    image = ee.Image(f"{COLLECTION}/{year}").select(BAND)
    return image.updateMask(image.neq(NODATA)).unmask(0)


def first_built_image() -> ee.Image:
    epochs = []
    for year in YEARS:
        image = surface_image(year)
        epochs.append(
            image.multiply(0)
            .add(year)
            .updateMask(image.gte(2000))
            .rename(BAND)
            .toUint16()
        )
    return ee.ImageCollection.fromImages(epochs).min().unmask(0).toUint16()


@dataclass(frozen=True)
class RasterGrid:
    transform: Affine
    wkt: str
    mask: np.ndarray

    @classmethod
    def from_geometries(
        cls,
        geometries: Sequence[shapely.Geometry],
        transform: Affine,
        wkt: str,
    ) -> "RasterGrid":
        if not geometries or all(shapely.is_empty(geom) for geom in geometries):
            msg = "GHSL boundaries must contain non-empty geometries."
            raise ValueError(msg)
        # Match Rasterio's geometry_window rounding on the source pixel grid.
        bounds = [
            geometry_bounds(geom, transform=~transform)
            for geom in geometries
            if not shapely.is_empty(geom)
        ]
        left = floor(min(bound[0] for bound in bounds))
        top = floor(min(bound[1] for bound in bounds))
        right = ceil(max(bound[2] for bound in bounds))
        bottom = ceil(max(bound[3] for bound in bounds))
        cropped = transform * Affine.translation(left, top)
        if right <= left or bottom <= top:
            msg = "GHSL boundaries must cover a non-empty raster window."
            raise ValueError(msg)
        mask = geometry_mask(
            geometries,
            out_shape=(bottom - top, right - left),
            transform=cropped,
            all_touched=False,
            invert=True,
        )
        return cls(cropped, wkt, mask)


def query_grid(geometries: Sequence[shapely.Geometry]) -> RasterGrid:
    projection = ee.Image(f"{COLLECTION}/1975").select(BAND).projection().getInfo()
    return RasterGrid.from_geometries(
        geometries, Affine(*projection["transform"]), projection["wkt"]
    )


def download_tiles(
    image: ee.Image, grid: RasterGrid
) -> Iterator[tuple[slice, slice, np.ndarray]]:
    height, width = grid.mask.shape
    for row in range(0, height, TILE_SIZE):
        for col in range(0, width, TILE_SIZE):
            rows = slice(row, min(row + TILE_SIZE, height))
            cols = slice(col, min(col + TILE_SIZE, width))
            mask = grid.mask[rows, cols]
            if not mask.any():
                continue
            transform = grid.transform * Affine.translation(col, row)
            result = ee.data.computePixels(
                {
                    "expression": image.toUint16(),
                    "fileFormat": "NUMPY_NDARRAY",
                    "bandIds": [BAND],
                    "grid": {
                        "dimensions": {"width": mask.shape[1], "height": mask.shape[0]},
                        "crsWkt": grid.wkt,
                        "affineTransform": {
                            "scaleX": transform.a,
                            "shearX": transform.b,
                            "translateX": transform.c,
                            "shearY": transform.d,
                            "scaleY": transform.e,
                            "translateY": transform.f,
                        },
                    },
                }
            )
            data = result[BAND]
            if data.shape != mask.shape:
                msg = (
                    f"Unexpected GHSL tile shape: {data.shape}; expected {mask.shape}."
                )
                raise ValueError(msg)
            yield rows, cols, np.where(mask & (data != NODATA), data, 0)


def first_built_raster(
    geometries: Sequence[shapely.Geometry],
) -> tuple[np.ndarray, Affine]:
    grid = query_grid(geometries)
    data = np.zeros(grid.mask.shape, dtype=np.uint16)
    for rows, cols, tile in download_tiles(first_built_image(), grid):
        data[rows, cols] = tile
    return data, grid.transform


def built_surface_area(year: int, geometries: Sequence[shapely.Geometry]) -> float:
    grid = query_grid(geometries)
    return float(
        sum(
            int(tile.sum(dtype=np.uint64))
            for _, _, tile in download_tiles(surface_image(year), grid)
        )
    )
