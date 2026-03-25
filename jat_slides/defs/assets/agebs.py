import geopandas as gpd
from dagster_components.partitions import zone_partitions
from dagster_components.resources import PostGISResource

import dagster as dg


def agebs_factory(year: int) -> dg.AssetsDefinition:
    @dg.asset(
        name=str(year),
        key_prefix="agebs",
        partitions_def=zone_partitions,
        io_manager_key="gpkg_manager",
        group_name="agebs",
    )
    def _asset(
        context: dg.AssetExecutionContext,
        postgis_resource: PostGISResource,
    ) -> gpd.GeoDataFrame:
        zone = context.partition_key

        with postgis_resource.connect() as conn:
            return (
                gpd.read_postgis(
                    """
                SELECT census_2020_ageb.geometry, census_2020_ageb."POBTOT"
                    FROM census_2020_ageb
                INNER JOIN census_2020_mun
                    ON census_2020_ageb."CVE_MUN" = census_2020_mun."CVEGEO"
                WHERE census_2020_mun."CVE_MET" = %(zone)s
                """,
                    conn,
                    params={"zone": zone},
                    geom_col="geometry",
                )
                .to_crs("ESRI:54009")
                .assign(geometry=lambda df: df["geometry"].make_valid())
            )

    return _asset


agebs = [agebs_factory(year) for year in (1990, 2000, 2010, 2020)]
