import os
from pathlib import Path
from typing import assert_never

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import rasterio as rio
from affine import Affine
from matplotlib.figure import Figure
from pptx.presentation import Presentation

import dagster as dg
from dagster import (
    ConfigurableIOManager,
    InputContext,
    OutputContext,
    ResourceDependency,
)
from jat_slides.resources import PathResource, path_resource


class BaseManager(ConfigurableIOManager):
    path_resource: ResourceDependency[PathResource]
    extension: str

    def _get_path(
        self,
        context: InputContext | OutputContext,
    ) -> Path | dict[str, Path]:
        out_path = Path(self.path_resource.data_path) / "generated"
        fpath = out_path / "/".join(context.asset_key.path)

        if context.has_asset_partitions:
            if len(context.asset_partition_keys) == 1:
                final_path = fpath / context.asset_partition_key
                final_path = final_path.with_suffix(final_path.suffix + self.extension)
            else:
                final_path = {}
                for key in context.asset_partition_keys:
                    temp_path = fpath / key
                    temp_path = temp_path.with_suffix(temp_path.suffix + self.extension)
                    final_path[key] = temp_path
        else:
            final_path = fpath.with_suffix(fpath.suffix + self.extension)

        return final_path


class DataFrameIOManager(BaseManager):
    def _is_geodataframe(self) -> bool:
        return self.extension in (".gpkg", ".geojson")

    def handle_output(self, context: OutputContext, obj: gpd.GeoDataFrame) -> None:
        out_path = self._get_path(context)

        if isinstance(out_path, dict):
            err = "Saving multiple files is not implemented for DataFrameIOManager."
            raise NotImplementedError(err)

        out_path.parent.mkdir(exist_ok=True, parents=True)

        if self._is_geodataframe():
            obj.to_file(out_path, mode="w")
        else:
            obj.to_csv(out_path, index=False)

    def load_input(self, context: InputContext) -> gpd.GeoDataFrame | pd.DataFrame:
        path = self._get_path(context)
        if isinstance(path, os.PathLike):
            if self._is_geodataframe():
                return gpd.read_file(path)
            return pd.read_csv(path)

        if isinstance(path, dict):
            out_dict = {}
            for key, fpath in path.items():
                if fpath.exists():
                    if self._is_geodataframe():
                        out_dict[key] = gpd.read_file(fpath)
                    else:
                        out_dict[key] = pd.read_csv(fpath)
                else:
                    out_dict[key] = None
            return out_dict

        err = "Loading multiple files is not implemented for DataFrameIOManager."
        raise NotImplementedError(err)


class RasterIOManager(BaseManager):
    def _get_raster_and_transform(self, fpath: Path) -> tuple[np.ndarray, Affine]:
        with rio.open(fpath, "r") as ds:
            data = ds.read(1)
            transform = ds.transform
        return data, transform

    def handle_output(
        self,
        context: OutputContext,
        obj: tuple[np.ndarray, Affine],
    ) -> None:
        fpath = self._get_path(context)
        fpath.parent.mkdir(exist_ok=True, parents=True)

        arr, transform = obj
        with rio.open(
            fpath,
            "w",
            driver="GTiff",
            count=1,
            height=arr.shape[0],
            width=arr.shape[1],
            dtype="uint16",
            compress="w",
            crs="ESRI:54009",
            transform=transform,
        ) as ds:
            ds.write(arr, 1)

    def load_input(self, context: InputContext) -> tuple[np.ndarray, Affine]:
        path = self._get_path(context)
        if isinstance(path, os.PathLike):
            data, transform = self._get_raster_and_transform(path)
            return data, transform

        if isinstance(path, dict):
            out_dict = {}
            for key, fpath in path.items():
                out_dict[key] = self._get_raster_and_transform(fpath)
            return out_dict

        assert_never(type(path))


class ReprojectedRasterIOManager(RasterIOManager):
    crs: str

    def _get_raster_and_transform(self, fpath: Path) -> tuple[np.ndarray, Affine]:
        with rio.open(fpath) as ds:
            transform, width, height = rio.warp.calculate_default_transform(
                ds.crs,
                self.crs,
                ds.width,
                ds.height,
                *ds.bounds,
            )

            data = np.zeros((height, width), dtype=int)
            rio.warp.reproject(
                ds.read(1),
                data,
                src_transform=ds.transform,
                src_crs=ds.crs,
                dst_transform=transform,
                dst_crs=self.crs,
                resampling=rio.warp.Resampling.nearest,
            )

        return data, transform


class PresentationIOManager(BaseManager):
    def handle_output(self, context: OutputContext, obj: Presentation) -> None:
        fpath = self._get_path(context)

        if isinstance(fpath, dict):
            err = "Saving multiple files is not implemented for PresentationIOManager."
            raise NotImplementedError(err)

        fpath.parent.mkdir(exist_ok=True, parents=True)
        obj.save(str(fpath))

    def load_input(self, context: InputContext) -> None:
        raise NotImplementedError


class PlotFigIOManager(BaseManager):
    def handle_output(self, context: OutputContext, obj: Figure) -> None:
        fpath = self._get_path(context)
        fpath.parent.mkdir(exist_ok=True, parents=True)

        obj.savefig(fpath, dpi=250)
        obj.clf()
        plt.close(obj)

    def load_input(self, context: InputContext) -> None:
        raise NotImplementedError


class PathIOManager(BaseManager):
    def handle_output(self, context: OutputContext, obj) -> None:  # noqa: ANN001
        raise NotImplementedError

    def load_input(self, context: InputContext) -> Path | dict[str, Path]:
        return self._get_path(context)


class TextIOManager(BaseManager):
    def handle_output(self, context: OutputContext, obj: float) -> None:
        fpath = self._get_path(context)

        if isinstance(fpath, dict):
            err = "Saving multiple files is not implemented for TextIOManager."
            raise NotImplementedError(err)

        fpath.parent.mkdir(exist_ok=True, parents=True)

        with fpath.open("w", encoding="utf8") as f:
            f.write(f"{obj:.10f}")

    def load_input(
        self,
        context: InputContext,
    ) -> float | dict[str, float | None]:
        fpath = self._get_path(context)
        if isinstance(fpath, os.PathLike):
            with fpath.open(encoding="utf8") as f:
                out = float(f.readline().strip("\n"))
        else:
            out = {}
            for key, subpath in fpath.items():
                if subpath.exists():
                    with subpath.open(encoding="utf8") as f:
                        out[key] = float(f.readline().strip("\n"))
                else:
                    out[key] = None
        return out


# Init

csv_manager = DataFrameIOManager(path_resource=path_resource, extension=".csv")
gpkg_manager = DataFrameIOManager(path_resource=path_resource, extension=".gpkg")
memory_manager = dg.InMemoryIOManager()
raster_manager = RasterIOManager(path_resource=path_resource, extension=".tif")
reprojected_raster_manager = ReprojectedRasterIOManager(
    path_resource=path_resource,
    extension=".tif",
    crs="EPSG:4326",
)
presentation_manger = PresentationIOManager(
    path_resource=path_resource,
    extension=".pptx",
)
plot_manager = PlotFigIOManager(path_resource=path_resource, extension=".jpg")
path_manager = PathIOManager(path_resource=path_resource, extension=".jpg")
text_manager = TextIOManager(path_resource=path_resource, extension=".txt")


defs = dg.Definitions(
    resources={
        "csv_manager": csv_manager,
        "gpkg_manager": gpkg_manager,
        "memory_manager": memory_manager,
        "presentation_manager": presentation_manger,
        "raster_manager": raster_manager,
        "reprojected_raster_manager": reprojected_raster_manager,
        "plot_manager": plot_manager,
        "path_manager": path_manager,
        "text_manager": text_manager,
    },
)
