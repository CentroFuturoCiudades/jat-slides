from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from cfc_dagster_utils.partitions import zone_partitions

import dagster as dg
from jat_slides.defs.resources import PathResource

RADIUS_COLUMNS = ("r_3", "r_5", "r_9.3")


def build_remoteness_circles(
    brackets: pd.DataFrame, centroid: gpd.GeoDataFrame, zone: str
) -> gpd.GeoDataFrame:
    required = {"CVE_MET", *RADIUS_COLUMNS}
    if not required.issubset(brackets.columns):
        missing = required - set(brackets.columns)
        msg = f"Zone {zone}: missing remoteness columns {missing}"
        raise ValueError(msg)
    rows = brackets.loc[brackets["CVE_MET"] == zone, list(RADIUS_COLUMNS)]
    if len(rows) != 1:
        msg = f"Zone {zone}: expected one remoteness row, found {len(rows)}"
        raise ValueError(msg)
    radii = rows.iloc[0].apply(pd.to_numeric, errors="coerce").to_numpy(dtype=float)
    if not np.all(np.isfinite(radii) & (radii > 0)):
        msg = f"Zone {zone}: radii must be finite positive kilometers"
        raise ValueError(msg)
    if (
        centroid.crs is None
        or len(centroid) != 1
        or centroid.index.tolist() != [zone]
        or not centroid.geometry.geom_type.eq("Point").all()
        or centroid.geometry.is_empty.any()
        or not centroid.geometry.is_valid.all()
    ):
        msg = (
            f"Zone {zone}: expected one nonempty centroid point "
            "with a CRS and matching index"
        )
        raise ValueError(msg)

    point = centroid.to_crs("EPSG:4326").geometry.iloc[0]
    local_crs = (
        f"+proj=aeqd +lat_0={point.y} +lon_0={point.x} +datum=WGS84 +units=m +no_defs"
    )
    center = centroid.to_crs(local_crs).geometry.iloc[0]
    boundaries = [
        center.buffer(radius * 1000, quad_segs=90).boundary for radius in radii
    ]
    return gpd.GeoDataFrame(
        {"radius_column": RADIUS_COLUMNS, "radius_km": radii},
        geometry=boundaries,
        crs=local_crs,
    ).to_crs("EPSG:4326")


@dg.asset(
    partitions_def=zone_partitions,
    ins={"centroid": dg.AssetIn("centroids_zone")},
    io_manager_key="geodataframe_manager",
    group_name="remoteness",
)
def remoteness_circles_zone(
    context: dg.AssetExecutionContext,
    path_resource: PathResource,
    centroid: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    brackets = pd.read_csv(
        Path(path_resource.in_path) / "remoteness_brackets.csv",
        dtype={"CVE_MET": str},
    )
    return build_remoteness_circles(brackets, centroid, context.partition_key)
