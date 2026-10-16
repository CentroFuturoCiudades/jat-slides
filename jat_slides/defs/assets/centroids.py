import geopandas as gpd
from cfc_dagster_utils.partitions import zone_partitions
from cfc_dagster_utils.resources import PostgresResource

import dagster as dg


@dg.asset(
    key=["centroids_zone"],
    partitions_def=zone_partitions,
    io_manager_key="geodataframe_manager",
    group_name="centroids",
)
def centroids(
    context: dg.AssetExecutionContext, postgis_resource: PostgresResource
) -> gpd.GeoDataFrame:
    with postgis_resource.connect() as conn:
        return gpd.read_postgis(
            """
            SELECT * FROM centroids_historical
            WHERE cve_met = %(cve_met)s
            """,
            con=conn,
            geom_col="geometry",
            params={"cve_met": context.partition_key},
        ).set_index("cve_met")
