import geopandas as gpd

import dagster as dg
from jat_slides.partitions import mun_partitions, zone_partitions


def total_jobs_factory(
    level: str,
    *,
    partitions_def: dg.PartitionsDefinition,
) -> dg.AssetsDefinition:
    @dg.asset(
        name="total_jobs",
        key_prefix=f"stats_{level}",
        ins={"df_jobs": dg.AssetIn([f"jobs_{level}", "reprojected"])},
        partitions_def=partitions_def,
        io_manager_key="text_manager",
        group_name=f"stats_{level}",
    )
    def _asset(df_jobs: gpd.GeoDataFrame) -> float:
        return df_jobs["jobs"].sum()

    return _asset


dassets = [
    total_jobs_factory(level, partitions_def=partitions_def)
    for level, partitions_def in zip(
        ("zone", "mun"),
        (zone_partitions, mun_partitions),
        strict=False,
    )
]
