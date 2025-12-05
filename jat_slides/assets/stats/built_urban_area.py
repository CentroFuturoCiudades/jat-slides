import geopandas as gpd
import pandas as pd

import dagster as dg
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
        strict=False,
    ):
        area = agebs.to_crs("EPSG:6372").area.sum()
        out.append({"year": year, "area": area})

    return pd.DataFrame(out)


def built_urban_area_factory(
    suffix: str, *, prefix: str, partitions_def: dg.PartitionsDefinition
) -> dg.AssetsDefinition:
    @dg.asset(
        name="built_urban_area",
        key_prefix=f"stats_{suffix}",
        ins={
            "agebs_1990": dg.AssetIn(key=[prefix, "1990"]),
            "agebs_2000": dg.AssetIn(key=[prefix, "2000"]),
            "agebs_2010": dg.AssetIn(key=[prefix, "2010"]),
            "agebs_2020": dg.AssetIn(key=[prefix, "2020"]),
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


built_urban_area_zone = built_urban_area_factory(
    "zone", prefix="agebs", partitions_def=zone_partitions
)
built_urban_area_mun = built_urban_area_factory(
    "mun", prefix="muns", partitions_def=mun_partitions
)
