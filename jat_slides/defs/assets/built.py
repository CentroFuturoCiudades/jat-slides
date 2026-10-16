import geopandas as gpd
import numpy as np
import shapely
from affine import Affine
from cfc_dagster_utils.partitions import zone_partitions

import dagster as dg
from jat_slides.defs.partitions import mun_partitions
from jat_slides.defs.resources import EarthEngineResource
from jat_slides.ghsl import normalize_geometries


@dg.op(out=dg.Out(io_manager_key="raster_manager"))
def load_built_raster(
    earthengine_resource: EarthEngineResource,
    bounds: list[shapely.Geometry],
) -> tuple[np.ndarray, Affine]:
    return earthengine_resource.first_built(bounds)


@dg.op
def get_total_bounds(
    agebs_1990: gpd.GeoDataFrame,
    agebs_2000: gpd.GeoDataFrame,
    agebs_2010: gpd.GeoDataFrame,
    agebs_2020: gpd.GeoDataFrame,
) -> list:
    geoms = np.concatenate(
        [
            normalize_geometries(agebs_1990),
            normalize_geometries(agebs_2000),
            normalize_geometries(agebs_2010),
            normalize_geometries(agebs_2020),
        ],
    )
    return [shapely.union_all(geoms)]


@dg.graph
def built_graph(
    agebs_1990: gpd.GeoDataFrame,
    agebs_2000: gpd.GeoDataFrame,
    agebs_2010: gpd.GeoDataFrame,
    agebs_2020: gpd.GeoDataFrame,
) -> tuple[np.ndarray, Affine]:
    bounds = get_total_bounds(agebs_1990, agebs_2000, agebs_2010, agebs_2020)

    return load_built_raster(bounds)


@dg.graph_asset(
    name="built_zone",
    ins={
        "agebs_1990": dg.AssetIn(key=["agebs", "1990"]),
        "agebs_2000": dg.AssetIn(key=["agebs", "2000"]),
        "agebs_2010": dg.AssetIn(key=["agebs", "2010"]),
        "agebs_2020": dg.AssetIn(key=["agebs", "2020"]),
    },
    partitions_def=zone_partitions,
    group_name="built_rasters_base",
)
def built(
    agebs_1990: gpd.GeoDataFrame,
    agebs_2000: gpd.GeoDataFrame,
    agebs_2010: gpd.GeoDataFrame,
    agebs_2020: gpd.GeoDataFrame,
) -> tuple[np.ndarray, Affine]:
    return built_graph(agebs_1990, agebs_2000, agebs_2010, agebs_2020)  # ty: ignore[invalid-return-type]


@dg.graph_asset(
    name="built_mun",
    ins={
        "agebs_1990": dg.AssetIn(key=["muns", "1990"]),
        "agebs_2000": dg.AssetIn(key=["muns", "2000"]),
        "agebs_2010": dg.AssetIn(key=["muns", "2010"]),
        "agebs_2020": dg.AssetIn(key=["muns", "2020"]),
    },
    partitions_def=mun_partitions,
    group_name="built_rasters_mun",
)
def built_mun(
    agebs_1990: gpd.GeoDataFrame,
    agebs_2000: gpd.GeoDataFrame,
    agebs_2010: gpd.GeoDataFrame,
    agebs_2020: gpd.GeoDataFrame,
) -> tuple[np.ndarray, Affine]:
    return built_graph(agebs_1990, agebs_2000, agebs_2010, agebs_2020)  # ty: ignore[invalid-return-type]
