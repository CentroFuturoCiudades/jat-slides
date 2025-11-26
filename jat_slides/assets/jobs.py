from pathlib import Path

import geopandas as gpd
import pandas as pd

import dagster as dg
from jat_slides.partitions import zone_partitions, mun_partitions
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
    level: str, *, partitions_def: dg.PartitionsDefinition
) -> dg.AssetsDefinition:
    @dg.asset(
        name="reprojected",
        key_prefix=f"jobs_{level}",
        ins={
            "jobs": dg.AssetIn(["jobs", "geo"]),
            "cells": dg.AssetIn(["cells", level]),
        },
        partitions_def=partitions_def,
        io_manager_key="gpkg_manager",
    )
    def _asset(
        jobs: gpd.GeoDataFrame,
        cells: gpd.GeoDataFrame,
    ) -> gpd.GeoDataFrame:
        joined = (
            cells[["codigo", "geometry"]]
            .sjoin(jobs, how="inner", predicate="contains")
            .drop(
                columns=["index_right"],
            )
        )
        job_count = joined.groupby("codigo")["num_empleos_esperados"].sum()

        return (
            cells.set_index("codigo")
            .assign(jobs=job_count)
            .dropna(subset=["jobs"])
            .reset_index()
        )

    return _asset


dassets = [
    jobs_reprojected_factory(level, partitions_def=partitions_def)
    for level, partitions_def in zip(("zone", "mun"), (zone_partitions, mun_partitions))
]
