import os
from pathlib import Path
from typing import assert_never

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import rasterio as rio
import rasterio.warp as rio_warp
from affine import Affine
from matplotlib.figure import Figure
from pptx.presentation import Presentation

from dagster import (
    ConfigurableIOManager,
    InputContext,
    OutputContext,
    ResourceDependency,
)
from jat_slides.defs.resources import PathResource


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

    def _get_single_path(self, context: InputContext | OutputContext) -> Path:
        path = self._get_path(context)
        if isinstance(path, dict):
            err = "Multiple paths found. Use _get_path instead of _get_single_path."
            raise TypeError(err)
        return path


class DataFrameIOManager(BaseManager):
    def _is_geodataframe(self) -> bool:
        return self.extension in (".gpkg", ".geojson")

    def handle_output(self, context: OutputContext, obj: gpd.GeoDataFrame) -> None:
        out_path = self._get_single_path(context)
        out_path.parent.mkdir(exist_ok=True, parents=True)

        if self._is_geodataframe():
            obj.to_file(out_path, mode="w")
        else:
            obj.to_csv(out_path, index=False)

    def load_input(
        self, context: InputContext
    ) -> pd.DataFrame | dict[str, pd.DataFrame | None]:
        path = self._get_path(context)

        if isinstance(path, Path):
            if self._is_geodataframe():
                return gpd.read_file(path)
            return pd.read_csv(path)

        if isinstance(path, dict):
            out_dict: dict[str, pd.DataFrame | None] = {}
            for key, fpath in path.items():
                if fpath.exists():
                    if self._is_geodataframe():
                        out_dict[key] = gpd.read_file(fpath)
                    else:
                        out_dict[key] = pd.read_csv(fpath)
                else:
                    out_dict[key] = None
            return out_dict

        assert_never(type(path))


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
        fpath = self._get_single_path(context)
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

    def load_input(
        self, context: InputContext
    ) -> tuple[np.ndarray, Affine] | dict[str, tuple[np.ndarray, Affine]]:
        path = self._get_path(context)
        if isinstance(path, Path):
            data, transform = self._get_raster_and_transform(path)
            return data, transform

        if isinstance(path, dict):
            out_dict: dict[str, tuple[np.ndarray, Affine]] = {}
            for key, fpath in path.items():
                out_dict[key] = self._get_raster_and_transform(fpath)
            return out_dict

        assert_never(type(path))


class ReprojectedRasterIOManager(RasterIOManager):
    crs: str

    def _get_raster_and_transform(self, fpath: Path) -> tuple[np.ndarray, Affine]:
        with rio.open(fpath) as ds:
            transform, width, height = rio_warp.calculate_default_transform(
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
        fpath = self._get_single_path(context)
        fpath.parent.mkdir(exist_ok=True, parents=True)
        obj.save(str(fpath))

    def load_input(self, context: InputContext) -> None:
        raise NotImplementedError


class PlotFigIOManager(BaseManager):
    def handle_output(self, context: OutputContext, obj: Figure) -> None:
        fpath = self._get_single_path(context)
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
        fpath = self._get_single_path(context)
        fpath.parent.mkdir(exist_ok=True, parents=True)

        with fpath.open("w", encoding="utf8") as f:
            f.write(f"{obj:.10f}")

    def load_input(
        self,
        context: InputContext,
    ) -> float | dict[str, float | None]:
        fpath = self._get_single_path(context)
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
