from pathlib import Path

import geopandas as gpd
import pandas as pd

import dagster as dg
from jat_slides.partitions import mun_partitions, zone_partitions
from jat_slides.resources import PathResource


@dg.asset(name="geo", key_prefix="jobs", io_manager_key="gpkg_manager")
def jobs_geo(path_resource: PathResource) -> gpd.GeoDataFrame:
    jobs_path = Path(path_resource.jobs_path) / "denue_2023_estimaciones.csv"

    return gpd.GeoDataFrame(
        (
            pd.read_csv(
                jobs_path,
                usecols=["num_empleos_esperados", "longitud", "latitud"],
            )
            .assign(geometry=lambda x: gpd.points_from_xy(x["longitud"], x["latitud"]))
            .drop(columns=["longitud", "latitud"])
        ),
        crs="EPSG:4326",
        geometry="geometry",
    ).to_crs("EPSG:6372")


def jobs_reprojected_factory(
    level: str,
    *,
    partitions_def: dg.PartitionsDefinition,
    unit_asset_key: list[str],
    index_col: str,
) -> dg.AssetsDefinition:
    @dg.asset(
        name=level,
        key_prefix="jobs",
        ins={
            "jobs": dg.AssetIn(["jobs", "geo"]),
            "units": dg.AssetIn(unit_asset_key),
        },
        partitions_def=partitions_def,
        io_manager_key="gpkg_manager",
    )
    def _asset(
        jobs: gpd.GeoDataFrame,
        units: gpd.GeoDataFrame,
    ) -> gpd.GeoDataFrame:
        jobs = jobs.to_crs("EPSG:6372")
        units = units.to_crs("EPSG:6372")

        joined = (
            units[[index_col, "geometry"]]
            .sjoin(jobs, how="inner", predicate="contains")
            .drop(
                columns=["index_right"],
            )
        )
        job_count = joined.groupby(index_col)["num_empleos_esperados"].sum()

        return (
            units.set_index(index_col)
            .assign(jobs=job_count)
            .dropna(subset=["jobs"])
            .reset_index()
        )

    return _asset


jobs_reprojected_zone = jobs_reprojected_factory(
    "zone",
    partitions_def=zone_partitions,
    unit_asset_key=["cells", "zone"],
    index_col="codigo",
)

jobs_reprojected_mun = jobs_reprojected_factory(
    "mun",
    partitions_def=mun_partitions,
    unit_asset_key=["cells", "mun"],
    index_col="codigo",
)

jobs_reprojected_ageb = jobs_reprojected_factory(
    "ageb",
    partitions_def=zone_partitions,
    unit_asset_key=["agebs", "2020"],
    index_col="CVEGEO",
)
