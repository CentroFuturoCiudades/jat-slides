from typing import assert_never

import geopandas as gpd
import pandas as pd

from dagster import AssetIn, asset
from jat_slides.partitions import mun_partitions, zone_partitions


def calculate_built_urban_area(
    agebs_1990: gpd.GeoDataFrame,
    agebs_2000: gpd.GeoDataFrame,
    agebs_2010: gpd.GeoDataFrame,
    agebs_2020: gpd.GeoDataFrame,
) -> pd.DataFrame:
    out = []
    for year, agebs in zip(
        (1990, 2000, 2010, 2020),
        (agebs_1990, agebs_2000, agebs_2010, agebs_2020),
    ):
        area = agebs.to_crs("EPSG:6372").area.sum()
        out.append(dict(year=year, area=area))
    out = pd.DataFrame(out)
    return out


def built_urban_area_factory(suffix: str):
    if suffix == "zone":
        prefix = "agebs"
        partitions_def = zone_partitions
    elif suffix == "mun":
        prefix = "muns"
        partitions_def = mun_partitions
    elif suffix == "trimmed":
        prefix = "agebs_trimmed"
        partitions_def = zone_partitions
    else:
        assert_never(suffix)

    @asset(
        name="built_urban_area",
        key_prefix=f"stats_{suffix}",
        ins={
            "agebs_1990": AssetIn(key=[prefix, "1990"]),
            "agebs_2000": AssetIn(key=[prefix, "2000"]),
            "agebs_2010": AssetIn(key=[prefix, "2010"]),
            "agebs_2020": AssetIn(key=[prefix, "2020"]),
        },
        partitions_def=partitions_def,
        group_name=f"stats_{suffix}",
        io_manager_key="csv_manager",
    )
    def _asset(
        agebs_1990: gpd.GeoDataFrame,
        agebs_2000: gpd.GeoDataFrame,
        agebs_2010: gpd.GeoDataFrame,
        agebs_2020: gpd.GeoDataFrame,
    ) -> pd.DataFrame:
        return calculate_built_urban_area(
            agebs_1990,
            agebs_2000,
            agebs_2010,
            agebs_2020,
        )

    return _asset


dassets = [built_urban_area_factory(suffix) for suffix in ("zone", "mun", "trimmed")]
