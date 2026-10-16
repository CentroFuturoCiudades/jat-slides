from pathlib import Path

import geopandas as gpd
import pandas as pd
from cfc_dagster_utils.partitions import zone_partitions

import dagster as dg
from jat_slides.defs.partitions import mun_partitions
from jat_slides.defs.resources import PathResource


def cells_zone_factory(start_year: int = 2000) -> dg.AssetsDefinition:
    @dg.asset(
        name="zone" if start_year == 2000 else f"zone_{start_year}_2020",
        key_prefix="cells",
        partitions_def=zone_partitions,
        io_manager_key="geodataframe_manager",
        group_name="cells_zone",
    )
    def _asset(
        context: dg.AssetExecutionContext,
        path_resource: PathResource,
    ) -> gpd.GeoDataFrame:
        fpath = (
            Path(path_resource.pg_path)
            / "differences"
            / f"{start_year}_2020"
            / f"{context.partition_key}.geoparquet"
        )
        return gpd.read_parquet(fpath)

    return _asset


cells_base = cells_zone_factory()
cells_zone_1990_2020 = cells_zone_factory(1990)


@dg.asset(
    name="mun",
    key_prefix="cells",
    ins={"agebs": dg.AssetIn(["muns", "2020"])},
    partitions_def=mun_partitions,
    io_manager_key="geodataframe_manager",
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
        pd.concat(
            [gpd.read_parquet(path) for path in diff_path.glob(f"{ent}.*.geoparquet")]
        ),
    )

    joined = df.sjoin(agebs.to_crs("EPSG:6372"), how="inner", predicate="intersects")[
        "codigo"
    ].unique()

    return df.loc[df["codigo"].isin(joined)]
