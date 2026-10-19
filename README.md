# JAT slides

## Radial population-change slides

Zone slides depend on the `plot_zone/radial_population_change_2000_2020`
and `plot_zone/radial_population_change_1990_2020` assets and their corresponding
`stats_zone` table assets. Each period materializes its plot and table together.
Supply `data/input/radial_f/radial_functions_<CVE_MET>_rho.csv` and
`data/input/remoteness_brackets.csv` before materialization. The radial CSVs are
local inputs and are not currently tracked by DVC.

Plots are saved as partitioned JPGs and tables as Parquet under `data/output`,
using the existing plot and DataFrame managers. The plot shows the radius
containing 90% of the 2020 population; the table sums changes over all radii.
Tables include absolute change (`pop_diff`) and percentage change (`pop_diff_pct`):
100 times the category's population change divided by its total population in
the starting year. A zero starting population produces a null percentage,
displayed as N/D on the slide. Rematerialize the radial assets before rebuilding
slides that previously used the two-column tables.

Zone presentations are materialized separately as `slides_zone_1990_2020` and
`slides_zone_2000_2020`, replacing the combined `slides_zone` asset. Each contains
its period's population, urbanization, and radial-change slides, plus copies of
the zone section slide, income (when available), and jobs. Outputs are saved to
`data/output/slides_zone_1990_2020/<CVE_MET>.pptx` and
`data/output/slides_zone_2000_2020/<CVE_MET>.pptx`. Select the new asset names in
Dagster; previously generated files under `data/output/slides_zone/` are retained
but are no longer updated. The municipal asset remains `slides_mun` (2000–2020).
Municipal presentations do not require these inputs.

## Verification

Run `uv run ruff format .`, `uv run ruff check .`, and `uv run ty check`.
Run the full test suite with coverage using:

```sh
uv run pytest --cov=jat_slides --cov-report=xml
```

The Earth Engine integration test requires credentials and is opt-in via
`GHSL_LIVE_TEST=1` and `EARTHENGINE_PROJECT`.
