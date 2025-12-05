import geopandas as gpd

import dagster as dg
from jat_slides.partitions import mun_partitions, zone_partitions


def lost_pop_after_2000_factory(suffix: str) -> dg.AssetsDefinition:
    if suffix == "zone":
        partitions_def = zone_partitions
    elif suffix == "mun":
        partitions_def = mun_partitions
    else:
        err = f"Suffix {suffix} is not supported. Supported suffixes are 'zone' and 'mun'."  # noqa: E501
        raise ValueError(err)

    @dg.asset(
        name="lost_pop_after_2000",
        key_prefix=f"stats_{suffix}",
        ins={"df": dg.AssetIn(["cells", suffix])},
        partitions_def=partitions_def,
        group_name=f"stats_{suffix}",
        io_manager_key="text_manager",
    )
    def _asset(df: gpd.GeoDataFrame) -> float:
        return (df["difference"] < 0).sum() / len(df)

    return _asset


dassets = [lost_pop_after_2000_factory(suffix) for suffix in ("zone", "mun")]
