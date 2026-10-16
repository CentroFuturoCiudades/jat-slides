import geopandas as gpd
import pandas as pd
from cfc_dagster_utils.partitions import zone_partitions

import dagster as dg
from jat_slides.defs.partitions import mun_partitions
from jat_slides.defs.resources import EarthEngineResource
from jat_slides.ghsl import normalize_geometries

YEARS = (1990, 2000, 2010, 2020)


def get_year_area_factory(year: int) -> dg.OpDefinition:
    @dg.op(
        name=f"get_year_area_{year}",
    )
    def _op(
        earthengine_resource: EarthEngineResource,
        bounds: dict[int, list],
    ) -> float:
        return earthengine_resource.surface_area(year, bounds[year])

    return _op


get_year_area_ops = {year: get_year_area_factory(year) for year in YEARS}


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
        bounds[year] = normalize_geometries(agebs)
    return bounds


@dg.op(out=dg.Out(io_manager_key="dataframe_manager"))
def concat_areas(areas: list[float]) -> pd.DataFrame:
    return (
        pd.DataFrame([areas], index=["area"], columns=YEARS)
        .transpose()
        .reset_index(names="year")
    )


def built_area_factory(
    suffix: str,
    *,
    prefix: str,
    partitions_def: dg.PartitionsDefinition,
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

        areas = []
        for year in YEARS:
            area_op = get_year_area_ops[year]
            areas.append(area_op(bounds))
        return concat_areas(areas)

    return _asset


built_area_zone = built_area_factory(
    "zone",
    prefix="agebs",
    partitions_def=zone_partitions,
)
built_area_mun = built_area_factory("mun", prefix="muns", partitions_def=mun_partitions)
