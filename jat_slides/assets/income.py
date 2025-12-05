import json
from pathlib import Path

import geopandas as gpd
import pandas as pd

import dagster as dg
from jat_slides.partitions import mun_partitions, zone_partitions
from jat_slides.resources import PathResource


@dg.asset(
    name="zone",
    key_prefix="income",
    partitions_def=zone_partitions,
    io_manager_key="gpkg_manager",
    group_name="income",
)
def income(
    context: dg.AssetExecutionContext,
    path_resource: PathResource,
) -> gpd.GeoDataFrame:
    segregation_path = Path(path_resource.segregation_path)

    short_to_long_map_path = segregation_path / "short_to_long_map.json"
    with short_to_long_map_path.open(encoding="utf8") as f:
        long_to_short_map = {value: key for key, value in json.load(f).items()}

    if context.partition_key in long_to_short_map:
        return (
            gpd.read_file(
                segregation_path
                / "incomes"
                / f"{long_to_short_map[context.partition_key]}.gpkg",
            )
            .dropna(subset=["income_pc"])
            .to_crs("EPSG:4326")
        )

    return gpd.GeoDataFrame(geometry=[])


@dg.asset(
    name="mun",
    key_prefix="income",
    partitions_def=mun_partitions,
    io_manager_key="gpkg_manager",
    group_name="income_mun",
)
def load_state_income_df(
    context: dg.OpExecutionContext,
    path_resource: PathResource,
) -> gpd.GeoDataFrame:
    if len(context.partition_key) == 4:
        ent = f"0{context.partition_key[0]}"
    elif len(context.partition_key) == 5:
        ent = context.partition_key[:2]
    else:
        err = "Invalid partition key length"
        raise ValueError(err)

    income_path = Path(path_resource.segregation_path) / "incomes"

    df = [
        gpd.read_file(path).dropna(subset=["income_pc"])
        for path in income_path.glob(f"M{ent}*.gpkg")
    ]

    return (
        gpd.GeoDataFrame(pd.concat(df, ignore_index=True))
        .to_crs("EPSG:4326")
        .query("cvegeo.str.startswith(@context.partition_key)")
    )
