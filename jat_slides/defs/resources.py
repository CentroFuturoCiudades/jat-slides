from collections.abc import Sequence

import ee
import numpy as np
import shapely
from affine import Affine

from dagster import ConfigurableResource, InitResourceContext
from jat_slides import ghsl


class EarthEngineResource(ConfigurableResource):
    project: str

    def setup_for_execution(self, context: InitResourceContext) -> None:
        if not self.project.strip():
            msg = "Set EARTHENGINE_PROJECT to an Earth Engine-enabled Cloud project."
            raise ValueError(msg)
        ee.Initialize(project=self.project)
        if context.log is not None:
            context.log.debug("Earth Engine initialized for GHSL queries.")

    def first_built(
        self, geometries: Sequence[shapely.Geometry]
    ) -> tuple[np.ndarray, Affine]:
        return ghsl.first_built_raster(geometries)

    def surface_area(self, year: int, geometries: Sequence[shapely.Geometry]) -> float:
        return ghsl.built_surface_area(year, geometries)


class PathResource(ConfigurableResource):
    in_path: str
    out_path: str
    pg_path: str
    job_estimation_path: str


class ZonesListResource(ConfigurableResource):
    zones: list[str]


class ZonesMapListResource(ConfigurableResource):
    zones: dict[str, list[float]]


class ZonesMapStrResource(ConfigurableResource):
    zones: dict[str, str]


class ZonesMapFloatResource(ConfigurableResource):
    zones: dict[str, float]


class ConfigResource(ConfigurableResource):
    bounds: dict[str, list[float]]
    names: dict[str, str] | None = None
    linewidths: dict[str, float] | None = None
    legend_pos: dict[str, str] | None = None
    add_labels: dict[str, list[str]] | None = None
    overlays: dict[str, dict[str, dict]] | None = None
