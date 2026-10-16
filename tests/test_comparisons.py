from pathlib import Path
from typing import TYPE_CHECKING, cast
from unittest.mock import patch

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
from affine import Affine
from cfc_dagster_utils.resources import PostgresResource
from PIL import Image
from pptx import Presentation
from pptx.shapes.autoshape import Shape
from pptx.shapes.picture import Picture
from pptx.slide import Slide
from shapely.geometry import box

import dagster as dg
from jat_slides.definitions import definitions
from jat_slides.defs.assets.agebs import agebs_factory
from jat_slides.defs.assets.cells import cells_zone_factory
from jat_slides.defs.assets.maps.population_grid import (
    population_grid_1990_2020_plot_zone,
)
from jat_slides.defs.assets.slides import slides_mun, slides_zone
from jat_slides.defs.assets.stats.built_after_2000 import built_after_factory
from jat_slides.defs.assets.stats.lost_pop_after_2000 import lost_pop_factory
from jat_slides.defs.partitions import mun_partitions
from jat_slides.defs.resources import ConfigResource, PathResource

if TYPE_CHECKING:
    from pptx.presentation import Presentation as PresentationType

ZONE = "01.1.01"


@pytest.mark.parametrize("year", [1990, 2000, 2010, 2020])
def test_census_year_query(tmp_path: Path, year: int) -> None:
    resource = PostgresResource(
        host="unused",
        port=5432,
        user="unused",
        password=str(tmp_path),
        database="unused",
    )
    frame = gpd.GeoDataFrame(
        {"pobtot": [year]}, geometry=[box(-103, 21, -102, 22)], crs=4326
    )
    with (
        patch.object(PostgresResource, "connect"),
        patch("geopandas.read_postgis", return_value=frame) as read,
        dg.build_asset_context(partition_key=ZONE) as context,
    ):
        result = agebs_factory(year)(context, resource)
    result = cast("gpd.GeoDataFrame", result)
    query = read.call_args.args[0]
    np.testing.assert_equal(read.call_args.kwargs["params"], {"zone": ZONE})
    np.testing.assert_equal(f"{year}_ageb" in query, desired=True)
    np.testing.assert_equal("pob1 AS pobtot" in query, desired=year < 2010)
    for other in {1990, 2000, 2010, 2020} - {year}:
        np.testing.assert_equal(f"{other}_ageb" in query, desired=False)
    np.testing.assert_equal(result["pobtot"].tolist(), [year])
    np.testing.assert_equal(str(result.crs), "ESRI:54009")
    np.testing.assert_equal(result.is_valid.all(), desired=True)


@pytest.mark.parametrize("year", [1990, 2000])
def test_period_cells_and_loss(tmp_path: Path, year: int) -> None:
    paths = PathResource(
        in_path=str(tmp_path),
        out_path=str(tmp_path),
        pg_path=str(tmp_path),
        job_estimation_path="unused",
    )
    for baseline, values in [(1990, [-2, -1, 0, 3]), (2000, [-1, 0, 2, 3])]:
        directory = tmp_path / "differences" / f"{baseline}_2020"
        directory.mkdir(parents=True)
        gpd.GeoDataFrame(
            {"difference": values},
            geometry=[box(i, 0, i + 1, 1) for i in range(4)],
            crs=6372,
        ).to_parquet(directory / f"{ZONE}.geoparquet")
    cells = cells_zone_factory(year)
    with dg.build_asset_context(partition_key=ZONE) as context:
        frame = cells(context, paths)
    loss = lost_pop_factory("zone", start_year=year, cells_key=cells.key)
    np.testing.assert_equal(loss(frame), 0.5 if year == 1990 else 0.25)


@pytest.mark.parametrize(("year", "expected"), [(1990, 0.75), (2000, 0.5)])
def test_construction_threshold(year: int, expected: float) -> None:
    asset = built_after_factory("mun", mun_partitions, start_year=year)
    data = np.array([[0, 1975, 1990, 2000, 2020]])
    np.testing.assert_equal(asset((data, Affine.identity())), expected)


def test_asset_dependencies() -> None:
    defs = definitions()
    dg.Definitions.validate_loadable(defs)
    graph = defs.resolve_asset_graph()
    cells_key = dg.AssetKey(["cells", "zone_1990_2020"])
    np.testing.assert_equal(
        graph.get(dg.AssetKey(["stats_zone", "lost_pop_after_1990"])).parent_keys,
        {cells_key},
    )
    np.testing.assert_equal(
        graph.get(dg.AssetKey(["stats_zone", "built_after_1990"])).parent_keys,
        {dg.AssetKey("built_zone")},
    )
    asset = population_grid_1990_2020_plot_zone
    np.testing.assert_equal(
        asset.asset_deps[asset.key],
        {cells_key, dg.AssetKey("remoteness_circles_zone")},
    )
    historical_inputs = {
        "lost_pop_after_1990",
        "built_after_1990",
        "pg_1990_figure_path",
    }
    np.testing.assert_equal(
        historical_inputs <= slides_zone.keys_by_input_name.keys(), desired=True
    )
    np.testing.assert_equal(
        historical_inputs.isdisjoint(slides_mun.keys_by_input_name), desired=True
    )
    for key in graph.get_all_asset_keys():
        if "1990" in key.path[-1] and key.path[0] in {
            "cells",
            "plot_zone",
            "stats_zone",
        }:
            np.testing.assert_equal(
                graph.get(key).partitions_def, slides_zone.partitions_def
            )
    np.testing.assert_equal(
        slides_zone.get_io_manager_key_for_asset_key(slides_zone.key),
        "presentation_manager",
    )
    np.testing.assert_equal(
        slides_zone.op.input_def_named("pg_1990_figure_path").input_manager_key,
        "path_manager",
    )


def slide_text(slide: Slide) -> str:
    return " ".join(shape.text for shape in slide.shapes if isinstance(shape, Shape))


def slide_image(slide: Slide) -> bytes:
    return next(
        shape.image.blob for shape in slide.shapes if isinstance(shape, Picture)
    )


@pytest.mark.parametrize("level", ["zone", "mun"])
@pytest.mark.parametrize("with_income", [True, False])
def test_slideshow(tmp_path: Path, level: str, *, with_income: bool) -> None:
    images = {}
    for name, color in [("built", "blue"), ("2000", "red"), ("1990", "green")]:
        path = tmp_path / f"{name}.png"
        Image.new("RGB", (100, 100), color).save(path)
        images[name] = path
    historical = (
        {
            "lost_pop_after_1990": 0.25,
            "built_after_1990": 0.75,
            "pg_1990_figure_path": images["1990"],
        }
        if level == "zone"
        else {}
    )
    # Exercise the asset adapter as well as the renderer, including conditional inputs.
    asset = slides_zone if level == "zone" else slides_mun
    with dg.build_asset_context(
        partition_key=ZONE if level == "zone" else "01001",
        resources={
            f"{level}_config_resource": ConfigResource(
                bounds={}, names={ZONE: "Aguascalientes", "01001": "Aguascalientes"}
            )
        },
    ) as context:
        pres = asset(
            context,
            lost_pop_after_2000=0.5,
            built_after_2000=0.25,
            total_jobs=1234.0,
            pop_df=pd.DataFrame({"year": [1990, 2000, 2020], "pop": [1e6, 2e6, 4e6]}),
            built_df=pd.DataFrame(
                {"year": [1990, 2000, 2020], "area": [2e6, 4e6, 8e6]}
            ),
            built_urban_df=pd.DataFrame(
                {"year": [1990, 2000, 2020], "area": [3e6, 6e6, 12e6]}
            ),
            built_figure_path=images["built"],
            pg_figure_path=images["2000"],
            income_figure_path=images["built"] if with_income else None,
            jobs_figure_path=images["built"],
            **historical,
        )
    path = tmp_path / "slides.pptx"
    pres = cast("PresentationType", pres)
    pres.save(str(path))
    slides = Presentation(str(path)).slides
    np.testing.assert_equal(len(slides), 4 + int(with_income) + 2 * (level == "zone"))
    np.testing.assert_equal("2000 a 2020" in slide_text(slides[1]), desired=True)
    np.testing.assert_equal("50%" in slide_text(slides[1]), desired=True)
    np.testing.assert_equal(slide_image(slides[1]), images["2000"].read_bytes())
    for index, year, factor in [
        (2, "2000", "x2.00"),
        *([(4, "1990", "x4.00")] if level == "zone" else []),
    ]:
        table = next(shape.table for shape in slides[index].shapes if shape.has_table)
        np.testing.assert_equal(table.cell(1, 0).text, year)
        np.testing.assert_equal(table.cell(2, 0).text, "2020")
        np.testing.assert_equal(
            [table.cell(1, col).text for col in range(1, 4)],
            ["1.00", "2.0", "3.0"] if year == "1990" else ["2.00", "4.0", "6.0"],
        )
        np.testing.assert_equal(
            [table.cell(2, col).text for col in range(1, 4)],
            ["4.00", "8.0", "12.0"],
        )
        np.testing.assert_equal(
            [table.cell(3, col).text for col in range(1, 4)], [factor] * 3
        )
        np.testing.assert_equal(
            slide_image(slides[index]), images["built"].read_bytes()
        )
    if level == "zone":
        np.testing.assert_equal("1990 a 2020" in slide_text(slides[3]), desired=True)
        np.testing.assert_equal("25%" in slide_text(slides[3]), desired=True)
        np.testing.assert_equal("75%" in slide_text(slides[4]), desired=True)
        np.testing.assert_equal(slide_image(slides[3]), images["1990"].read_bytes())
    np.testing.assert_equal("Empleos totales" in slide_text(slides[-1]), desired=True)
