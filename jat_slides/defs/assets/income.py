from pathlib import Path

import geopandas as gpd
import pandas as pd
from cfc_dagster_utils.partitions import zone_partitions
from cfc_dagster_utils.resources import PostgresResource

import dagster as dg
from jat_slides.defs.partitions import mun_partitions


@dg.asset(
    key=["income", "zone"],
    partitions_def=zone_partitions,
    io_manager_key="geodataframe_manager",
    group_name="income",
)
def income_zone(
    context: dg.AssetExecutionContext, postgis_resource: PostgresResource
) -> gpd.GeoDataFrame:
    zone = context.partition_key

    with postgis_resource.connect() as conn:
        return (
            gpd.read_postgis(
                """
            SELECT income_2020.income_pc, census_2020_ageb.geometry
                FROM income_2020
            INNER JOIN census_2020_ageb
                ON census_2020_ageb.cvegeo = income_2020.cvegeo
            INNER JOIN census_2020_mun
                ON census_2020_ageb.cve_mun = census_2020_mun.cvegeo
            WHERE census_2020_mun.cve_met = %(zone)s
            """,
                conn,
                params={"zone": zone},
                geom_col="geometry",
            )
            .to_crs("EPSG:4326")
            .assign(geometry=lambda df: df["geometry"].make_valid())
        )


@dg.asset(
    name="mun",
    key_prefix="income",
    partitions_def=mun_partitions,
    io_manager_key="geodataframe_manager",
    group_name="income_mun",
)
def load_state_income_df(
    context: dg.OpExecutionContext,
    postgis_resource: PostgresResource,
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
