import numpy as np
from affine import Affine
from cfc_dagster_utils.partitions import zone_partitions

import dagster as dg
from jat_slides.defs.partitions import mun_partitions


def built_after_factory(
    suffix: str,
    partitions_def: dg.PartitionsDefinition,
    *,
    start_year: int = 2000,
) -> dg.AssetsDefinition:
    @dg.asset(
        name=f"built_after_{start_year}",
        key_prefix=f"stats_{suffix}",
        ins={"built_data": dg.AssetIn(f"built_{suffix}")},
        partitions_def=partitions_def,
        io_manager_key="text_manager",
        group_name=f"stats_{suffix}",
    )
    def _asset(built_data: tuple[np.ndarray, Affine]) -> float:
        arr, _ = built_data
        return (arr >= start_year).sum() / (arr > 0).sum()

    return _asset


dassets = [
    built_after_factory(suffix, partitions_def=partitions_def)
    for suffix, partitions_def in zip(
        ("zone", "mun"),
        (zone_partitions, mun_partitions),
        strict=True,
    )
]

dassets.append(built_after_factory("zone", zone_partitions, start_year=1990))
