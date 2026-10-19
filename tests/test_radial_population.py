from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pytest
from cfc_dagster_utils.managers.dataframe import DataFrameFileManager
from cfc_dagster_utils.partitions import zone_partitions
from PIL import Image

import dagster as dg
from jat_slides.definitions import definitions
from jat_slides.defs.assets.radial_population import (
    REMOTENESS_LABELS,
    plot_radial_change,
    prepare_radial_change,
    radial_population_factory,
    summarize_radial_change,
)
from jat_slides.defs.assets.slides import (
    slides_mun,
    slides_zone_1990_2020,
    slides_zone_2000_2020,
)
from jat_slides.defs.managers import PathIOManager, PlotFigIOManager
from jat_slides.defs.resources import PathResource

ZONE = "01.1.01"


def radial_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "r": [0, 1000, 2000, 3000, 4000, 5000],
            "ring_pop_1990": [20, 10, 10, 10, 0, 15],
            "ring_pop_2000": [10, 10, 10, 10, 10, 10],
            "ring_pop_2020": [10, 30, 30, 25, 0, 5],
        }
    )


def bracket_frame() -> pd.DataFrame:
    return pd.DataFrame({"CVE_MET": [ZONE], "r_3": [1], "r_5": [2], "r_9.3": [4]})


@pytest.mark.parametrize(
    ("year", "totals"), [(1990, [10, 20, 15, -10]), (2000, [20, 20, 5, -5])]
)
def test_calculation(year: int, totals: list[int]) -> None:
    prepared, radii, xmax = prepare_radial_change(
        radial_frame().iloc[::-1], bracket_frame(), ZONE, year
    )
    result = summarize_radial_change(prepared, year)
    np.testing.assert_equal(result.remoteness_cat.tolist(), list(REMOTENESS_LABELS))
    np.testing.assert_allclose(result.pop_diff, totals)
    expected_pct = (
        [100 / 3, 200, 150, -200 / 3] if year == 1990 else [100, 200, 25, -50]
    )
    np.testing.assert_allclose(result.pop_diff_pct, expected_pct)
    np.testing.assert_equal(xmax, 3)
    np.testing.assert_allclose(radii, [1, 2, 4])
    np.testing.assert_equal(
        prepared.remoteness_cat.tolist(),
        ["Central", "Central", "Intermedia", "Distante", "Distante", "Periurbana"],
    )
    fig = plot_radial_change(prepared, radii, xmax)
    try:
        ax = fig.axes[0]
        np.testing.assert_allclose(ax.get_xlim(), [0, 3])
        np.testing.assert_equal(
            [t.get_text() for t in ax.texts], list(REMOTENESS_LABELS[:3])
        )
        np.testing.assert_allclose(
            [t.get_position()[0] for t in ax.texts], [0.5, 1.5, 2.5]
        )
        np.testing.assert_allclose(
            np.asarray(ax.lines[0].get_ydata()), prepared.pop_diff.to_numpy()
        )
        np.testing.assert_equal(len(ax.collections), 2)
    finally:
        plt.close(fig)


def test_empty_categories() -> None:
    prepared, _, _ = prepare_radial_change(
        radial_frame().iloc[:2], bracket_frame(), ZONE, 1990
    )
    np.testing.assert_allclose(
        summarize_radial_change(prepared, 1990).pop_diff, [10, 0, 0, 0]
    )


@pytest.mark.parametrize(
    "problem",
    [
        "missing_column",
        "negative",
        "nan",
        "infinite",
        "empty",
        "zero_total",
        "missing_zone",
        "duplicate_zone",
        "unordered_brackets",
        "zero_bracket",
    ],
)
def test_invalid_inputs(problem: str) -> None:
    radial, brackets = radial_frame(), bracket_frame()
    if problem == "missing_column":
        radial = radial.drop(columns="ring_pop_1990")
    elif problem in {"negative", "nan", "infinite"}:
        radial = radial.astype({"r": float})
        radial.loc[0, "r"] = {"negative": -1, "nan": np.nan, "infinite": np.inf}[
            problem
        ]
    elif problem == "empty":
        radial = radial.iloc[:0]
    elif problem == "zero_total":
        radial["ring_pop_2020"] = 0
    elif problem == "missing_zone":
        brackets = brackets.iloc[:0]
    elif problem == "duplicate_zone":
        brackets = pd.concat([brackets, brackets])
    elif problem == "unordered_brackets":
        brackets["r_5"] = 1
    elif problem == "zero_bracket":
        brackets["r_3"] = 0
    with pytest.raises(ValueError, match=ZONE):
        prepare_radial_change(radial, brackets, ZONE, 1990)


@pytest.mark.parametrize("year", [1990, 2000])
def test_materialization(tmp_path: Path, year: int) -> None:
    radial_dir = tmp_path / "radial_f"
    radial_dir.mkdir()
    radial_frame().to_csv(radial_dir / f"radial_functions_{ZONE}_rho.csv", index=False)
    bracket_frame().to_csv(tmp_path / "remoteness_brackets.csv", index=False)
    paths = PathResource(
        in_path=str(tmp_path),
        out_path=str(tmp_path / "out"),
        pg_path="unused",
        job_estimation_path="unused",
    )
    table_manager = DataFrameFileManager(path_resource=paths, extension=".parquet")
    path_manager = PathIOManager(path_resource=paths, extension=".jpg")
    name = f"radial_population_change_{year}_2020"

    @dg.asset(
        partitions_def=zone_partitions,
        ins={
            "table": dg.AssetIn(key=["stats_zone", name]),
            "picture": dg.AssetIn(
                key=["plot_zone", name], input_manager_key="path_manager"
            ),
        },
    )
    def consumer(table: pd.DataFrame, picture: Path) -> None:
        expected = [10, 20, 15, -10] if year == 1990 else [20, 20, 5, -5]
        np.testing.assert_allclose(table.pop_diff, expected)
        expected_pct = (
            [100 / 3, 200, 150, -200 / 3] if year == 1990 else [100, 200, 25, -50]
        )
        np.testing.assert_allclose(table.pop_diff_pct, expected_pct)
        np.testing.assert_equal(
            picture, tmp_path / "out" / "plot_zone" / name / f"{ZONE}.jpg"
        )
        with Image.open(picture) as image:
            np.testing.assert_equal(image.size, (2500, 1250))

    result = dg.materialize(
        [radial_population_factory(year), consumer],
        partition_key=ZONE,
        resources={
            "path_resource": paths,
            "dataframe_manager": table_manager,
            "plot_manager": PlotFigIOManager(path_resource=paths, extension=".jpg"),
            "path_manager": path_manager,
        },
    )
    np.testing.assert_equal(result.success, desired=True)
    saved = pd.read_parquet(tmp_path / "out" / "stats_zone" / name / f"{ZONE}.parquet")
    np.testing.assert_equal(saved.remoteness_cat.tolist(), list(REMOTENESS_LABELS))


def test_definitions() -> None:
    defs = definitions()
    dg.Definitions.validate_loadable(defs)
    graph = defs.resolve_asset_graph()
    for year, slides_zone in [
        (1990, slides_zone_1990_2020),
        (2000, slides_zone_2000_2020),
    ]:
        name = f"radial_population_change_{year}_2020"
        for prefix, manager in [
            ("plot_zone", "plot_manager"),
            ("stats_zone", "dataframe_manager"),
        ]:
            key = dg.AssetKey([prefix, name])
            np.testing.assert_equal(graph.get(key).partitions_def, zone_partitions)
            np.testing.assert_equal(
                key in slides_zone.asset_deps[slides_zone.key], desired=True
            )
            np.testing.assert_equal(
                key in slides_mun.asset_deps[slides_mun.key], desired=False
            )
            np.testing.assert_equal(
                radial_population_factory(year).get_io_manager_key_for_asset_key(key),
                manager,
            )
        np.testing.assert_equal(
            slides_zone.op.input_def_named("radial_figure_path").input_manager_key,
            "path_manager",
        )


@pytest.mark.parametrize("scenario", ["mixed", "growth", "decline", "unchanged"])
def test_periods_share_axes_and_layout(scenario: str) -> None:
    radial = radial_frame()
    if scenario == "mixed":
        radial.loc[0, "ring_pop_1990"] = 100
    elif scenario == "growth":
        radial["ring_pop_1990"] = radial["ring_pop_2020"] * 0.2
        radial["ring_pop_2000"] = radial["ring_pop_2020"] * 0.8
    elif scenario == "decline":
        radial["ring_pop_1990"] = radial["ring_pop_2020"] * 3
        radial["ring_pop_2000"] = radial["ring_pop_2020"] * 2
    else:
        radial["ring_pop_1990"] = radial["ring_pop_2020"]
        radial["ring_pop_2000"] = radial["ring_pop_2020"]
    figures = []
    try:
        for year in (1990, 2000):
            prepared, radii, xmax = prepare_radial_change(
                radial, bracket_frame(), ZONE, year
            )
            figure = plot_radial_change(prepared, radii, xmax)
            figures.append(figure)
            figure.canvas.draw()
        first, second = [figure.axes[0] for figure in figures]
        np.testing.assert_allclose(first.get_ylim(), second.get_ylim())
        np.testing.assert_allclose(first.get_yticks(), second.get_yticks())
        np.testing.assert_allclose(first.get_xlim(), second.get_xlim())
        np.testing.assert_allclose(
            first.get_position().bounds, second.get_position().bounds
        )
        lower, upper = first.get_ylim()
        for year in (1990, 2000):
            changes = radial["ring_pop_2020"] - radial[f"ring_pop_{year}"]
            np.testing.assert_equal(lower < min(0, changes.min()), desired=True)
            np.testing.assert_equal(upper > max(0, changes.max()), desired=True)
    finally:
        for figure in figures:
            plt.close(figure)


@pytest.mark.parametrize("year", [1990, 2000])
def test_zero_baseline_percentage(year: int) -> None:
    radial = radial_frame().iloc[:3].copy()
    radial[f"ring_pop_{year}"] = 0
    prepared, _, _ = prepare_radial_change(radial, bracket_frame(), ZONE, year)
    summary = summarize_radial_change(prepared, year)
    np.testing.assert_allclose(summary.pop_diff, [40, 30, 0, 0])
    np.testing.assert_equal(summary.pop_diff_pct.isna().all(), desired=True)
