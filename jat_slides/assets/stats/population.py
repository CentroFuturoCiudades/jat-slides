import geopandas as gpd
import pandas as pd

import dagster as dg
from jat_slides.partitions import mun_partitions, zone_partitions


def calculate_lost_pop(
    agebs_1990: gpd.GeoDataFrame,
    agebs_2000: gpd.GeoDataFrame,
    agebs_2010: gpd.GeoDataFrame,
    agebs_2020: gpd.GeoDataFrame,
) -> pd.DataFrame:
    pops = []
    for year, agebs in zip(
        (1990, 2000, 2010, 2020),
        (agebs_1990, agebs_2000, agebs_2010, agebs_2020),
        strict=True,
    ):
        pop = agebs["POBTOT"].sum()
        pops.append({"year": year, "pop": pop})
    return pd.DataFrame(pops)


def population_factory(
    suffix: str, *, prefix: str, partitions_def: dg.PartitionsDefinition
) -> dg.AssetsDefinition:
    @dg.asset(
        ins={
            "agebs_1990": dg.AssetIn(key=[prefix, "1990"]),
            "agebs_2000": dg.AssetIn(key=[prefix, "2000"]),
            "agebs_2010": dg.AssetIn(key=[prefix, "2010"]),
            "agebs_2020": dg.AssetIn(key=[prefix, "2020"]),
        },
        name="population",
        key_prefix=f"stats_{suffix}",
        partitions_def=partitions_def,
        io_manager_key="csv_manager",
        group_name=f"stats_{suffix}",
    )
    def _asset(
        agebs_1990: gpd.GeoDataFrame,
        agebs_2000: gpd.GeoDataFrame,
        agebs_2010: gpd.GeoDataFrame,
        agebs_2020: gpd.GeoDataFrame,
    ) -> pd.DataFrame:
        return calculate_lost_pop(agebs_1990, agebs_2000, agebs_2010, agebs_2020)

    return _asset


population_zone = population_factory(
    "zone", prefix="agebs", partitions_def=zone_partitions
)
population_mun = population_factory("mun", prefix="muns", partitions_def=mun_partitions)
