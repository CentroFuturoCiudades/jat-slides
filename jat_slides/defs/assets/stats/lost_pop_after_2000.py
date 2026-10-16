import geopandas as gpd
from cfc_dagster_utils.partitions import zone_partitions

import dagster as dg
from jat_slides.defs.partitions import mun_partitions


def lost_pop_factory(
    suffix: str, *, start_year: int = 2000, cells_key: dg.AssetKey
) -> dg.AssetsDefinition:
    if suffix == "zone":
        partitions_def = zone_partitions
    elif suffix == "mun":
        partitions_def = mun_partitions
    else:
        err = f"Suffix {suffix} is not supported. Supported suffixes are 'zone' and 'mun'."  # noqa: E501
        raise ValueError(err)

    @dg.asset(
        name=f"lost_pop_after_{start_year}",
        key_prefix=f"stats_{suffix}",
        ins={"df": dg.AssetIn(cells_key)},
        partitions_def=partitions_def,
        group_name=f"stats_{suffix}",
        io_manager_key="text_manager",
    )
    def _asset(df: gpd.GeoDataFrame) -> float:
        return (df["difference"] < 0).sum() / len(df)

    return _asset


dassets = [
    lost_pop_factory(suffix, cells_key=dg.AssetKey(["cells", suffix]))
    for suffix in ("zone", "mun")
]
dassets.append(
    lost_pop_factory(
        "zone", start_year=1990, cells_key=dg.AssetKey(["cells", "zone_1990_2020"])
    )
)
