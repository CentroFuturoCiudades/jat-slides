from pathlib import Path

import geopandas as gpd
import pandas as pd

import dagster as dg
from jat_slides.partitions import mun_partitions, zone_partitions
from jat_slides.resources import PathResource


@dg.asset(
    name="zone",
    key_prefix="cells",
    partitions_def=zone_partitions,
    io_manager_key="gpkg_manager",
    group_name="cells_base",
)
def cells_base(
    context: dg.AssetExecutionContext,
    path_resource: PathResource,
) -> gpd.GeoDataFrame:
    fpath = (
        Path(path_resource.pg_path)
        / "final"
        / "differences"
        / "2000_2020"
        / f"{context.partition_key}.gpkg"
    )
    return gpd.read_file(fpath)


@dg.asset(
    name="mun",
    key_prefix="cells",
    ins={"agebs": dg.AssetIn(["muns", "2020"])},
    partitions_def=mun_partitions,
    io_manager_key="gpkg_manager",
    group_name="cells_mun",
)
def cells_mun(
    context: dg.AssetExecutionContext,
    path_resource: PathResource,
    agebs: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    if len(context.partition_key) == 4:
        ent = context.partition_key[0].rjust(2, "0")
    else:
        ent = context.partition_key[:2]

    diff_path = Path(path_resource.pg_path) / "final" / "differences" / "2000_2020"
    df = gpd.GeoDataFrame(
        pd.concat([gpd.read_file(path) for path in diff_path.glob(f"{ent}.*.gpkg")]),
    )

    joined = df.sjoin(agebs.to_crs("EPSG:6372"), how="inner", predicate="intersects")[
        "codigo"
    ].unique()

    return df.loc[df["codigo"].isin(joined)]
