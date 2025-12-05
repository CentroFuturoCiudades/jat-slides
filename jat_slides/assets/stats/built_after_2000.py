import numpy as np
from affine import Affine

import dagster as dg
from jat_slides.partitions import mun_partitions, zone_partitions


def built_after_2000_factory(
    suffix: str,
    partitions_def: dg.PartitionsDefinition,
) -> dg.AssetsDefinition:
    @dg.asset(
        name="built_after_2000",
        key_prefix=f"stats_{suffix}",
        ins={"built_data": dg.AssetIn(f"built_{suffix}")},
        partitions_def=partitions_def,
        io_manager_key="text_manager",
        group_name=f"stats_{suffix}",
    )
    def _asset(built_data: tuple[np.ndarray, Affine]) -> float:
        arr, _ = built_data
        return (arr >= 2000).sum() / (arr > 0).sum()

    return _asset


dassets = [
    built_after_2000_factory(suffix, partitions_def=partitions_def)
    for suffix, partitions_def in zip(
        ("zone", "mun"),
        (zone_partitions, mun_partitions),
        strict=True,
    )
]
