from pathlib import Path

import geopandas as gpd
import pandas as pd

import dagster as dg
from jat_slides.partitions import mun_partitions
from jat_slides.resources import PathResource


def muns_factory(year: int) -> dg.AssetsDefinition:
    @dg.asset(
        name=str(year),
        key_prefix="muns",
        partitions_def=mun_partitions,
        io_manager_key="gpkg_manager",
    )
    def _asset(
        context: dg.AssetExecutionContext,
        path_resource: PathResource,
    ) -> gpd.GeoDataFrame:
        total_chars = len(context.partition_key)
        if total_chars == 4:
            ent = context.partition_key[0].rjust(2, "0")
        else:
            ent = context.partition_key[:2]

        agebs_dir_path = (
            Path(path_resource.pg_path) / "final" / "zone_agebs" / "shaped" / str(year)
        )

        df = pd.concat(
            [
                gpd.read_file(path).to_crs("ESRI:54009")
                for path in agebs_dir_path.glob(f"{ent}.*.gpkg")
            ],
        ).assign(geometry=lambda df: df["geometry"].make_valid())

        code_prefix = context.partition_key.rjust(5, "0")

        return gpd.GeoDataFrame(
            df.loc[df["CVEGEO"].str.startswith(code_prefix), ["POBTOT", "geometry"]],
        )

    return _asset


agebs = [muns_factory(year) for year in (1990, 2000, 2010, 2020)]
