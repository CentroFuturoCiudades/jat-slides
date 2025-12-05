from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio as rio
import rasterio.mask as rio_mask
from affine import Affine

import dagster as dg
from jat_slides.partitions import mun_partitions, zone_partitions
from jat_slides.resources import PathResource

YEARS = (1990, 2000, 2010, 2020)


def load_built_area_rasters_factory(year: int) -> dg.OpDefinition:
    @dg.op(
        name=f"load_built_area_rasters_{year}",
        out={"data": dg.Out(), "transform": dg.Out()},
    )
    def _op(
        path_resource: PathResource,
        bounds: dict[int, list],
    ) -> tuple[np.ndarray, Affine]:
        fpath = Path(path_resource.ghsl_path) / f"BUILT_100/{year}.tif"
        with rio.open(fpath, nodata=65535) as ds:
            data, transform = rio_mask.mask(ds, bounds[year], crop=True, nodata=0)

        data[data == 65535] = 0
        return data, transform

    return _op


load_built_area_rasters_ops = {
    year: load_built_area_rasters_factory(year) for year in range(1975, 2021, 5)
}


# pylint: disable=unused-argument
@dg.op(out=dg.Out(io_manager_key="csv_manager"))
def reduce_area_rasters(
    rasters: list[np.ndarray],
    transforms: list[Affine],  # pyright: ignore[reportUnusedParameter]  # noqa: ARG001
) -> pd.DataFrame:
    out = []
    for year, arr in zip(YEARS, rasters, strict=False):
        out.append({"year": year, "area": arr.sum()})
    return pd.DataFrame(out)


@dg.op
def get_bounds(
    agebs_1990: gpd.GeoDataFrame,
    agebs_2000: gpd.GeoDataFrame,
    agebs_2010: gpd.GeoDataFrame,
    agebs_2020: gpd.GeoDataFrame,
) -> dict[int, list]:
    bounds = {}
    for year, agebs in zip(
        (1990, 2000, 2010, 2020),
        (agebs_1990, agebs_2000, agebs_2010, agebs_2020),
        strict=True,
    ):
        bounds[year] = agebs["geometry"].to_numpy().tolist()
    return bounds


@dg.op
def concat_transforms_and_rasters(
    bounds: dict[int, list],
) -> tuple[list[np.ndarray], list[Affine]]:
    rasters, transforms = [], []
    for year in YEARS:
        load_op = load_built_area_rasters_ops[year]
        data, transform = load_op(bounds)
        rasters.append(data)
        transforms.append(transform)

    return rasters, transforms


def built_area_factory(
    suffix: str, *, prefix: str, partitions_def: dg.PartitionsDefinition
) -> dg.AssetsDefinition:
    @dg.graph_asset(
        ins={
            "agebs_1990": dg.AssetIn(key=[prefix, "1990"]),
            "agebs_2000": dg.AssetIn(key=[prefix, "2000"]),
            "agebs_2010": dg.AssetIn(key=[prefix, "2010"]),
            "agebs_2020": dg.AssetIn(key=[prefix, "2020"]),
        },
        name="built_area",
        key_prefix=f"stats_{suffix}",
        partitions_def=partitions_def,
        group_name=f"stats_{suffix}",
    )
    def _asset(
        agebs_1990: gpd.GeoDataFrame,
        agebs_2000: gpd.GeoDataFrame,
        agebs_2010: gpd.GeoDataFrame,
        agebs_2020: gpd.GeoDataFrame,
    ) -> pd.DataFrame:
        bounds = get_bounds(agebs_1990, agebs_2000, agebs_2010, agebs_2020)
        rasters, transforms = concat_transforms_and_rasters(bounds)
        return reduce_area_rasters(rasters, transforms)

    return _asset


built_area_zone = built_area_factory(
    "zone", prefix="agebs", partitions_def=zone_partitions
)
built_area_mun = built_area_factory("mun", prefix="muns", partitions_def=mun_partitions)
