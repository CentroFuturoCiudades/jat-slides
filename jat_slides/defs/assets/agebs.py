import geopandas as gpd
from cfc_dagster_utils.partitions import zone_partitions
from cfc_dagster_utils.resources import PostgresResource

import dagster as dg

CENSUS_QUERIES = {
    1990: """
        SELECT pob1 AS pobtot, geometry
        FROM geo_unit_1990_ageb
        INNER JOIN scince_1990_ageb USING (cvegeo)
        INNER JOIN geometry_1990_ageb USING (cvegeo)
        WHERE cve_met = %(zone)s
    """,
    2000: """
        SELECT pob1 AS pobtot, geometry
        FROM geo_unit_2000_ageb
        INNER JOIN scince_2000_ageb USING (cvegeo)
        INNER JOIN geometry_2000_ageb USING (cvegeo)
        WHERE cve_met = %(zone)s
    """,
    2010: """
        SELECT pobtot, geometry FROM census_2010_ageb
        WHERE cve_met = %(zone)s
    """,
    2020: """
        SELECT census_2020_ageb.pobtot, census_2020_ageb.geometry
        FROM census_2020_ageb
        INNER JOIN census_2020_loc
            ON census_2020_ageb.cve_loc = census_2020_loc.cvegeo
        INNER JOIN census_2020_mun
            ON census_2020_loc.cve_mun = census_2020_mun.cvegeo
        WHERE census_2020_mun.cve_met = %(zone)s
    """,
}


def agebs_factory(year: int) -> dg.AssetsDefinition:
    @dg.asset(
        name=str(year),
        key_prefix="agebs",
        partitions_def=zone_partitions,
        io_manager_key="geodataframe_manager",
        group_name="agebs",
    )
    def _asset(
        context: dg.AssetExecutionContext,
        postgis_resource: PostgresResource,
    ) -> gpd.GeoDataFrame:
        zone = context.partition_key

        with postgis_resource.connect() as conn:
            return (
                gpd.read_postgis(
                    CENSUS_QUERIES[year],
                    conn,
                    params={"zone": zone},
                    geom_col="geometry",
                )
                .to_crs("ESRI:54009")
                .assign(geometry=lambda df: df["geometry"].make_valid())
            )

    return _asset


agebs = [agebs_factory(year) for year in (1990, 2000, 2010, 2020)]
