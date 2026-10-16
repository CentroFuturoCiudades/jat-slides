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
The zone presentation groups population, urbanization, and radial-change slides
by period: 1990–2020 first, then 2000–2020, followed by income (when available)
and jobs.
Municipal presentations do not require these inputs.

## Verification

Run `uv run ruff format .`, `uv run ruff check .`, and `uv run ty check`.
Run the full test suite with coverage using:

```sh
uv run pytest --cov=jat_slides --cov-report=xml
```

The Earth Engine integration test requires credentials and is opt-in via
`GHSL_LIVE_TEST=1` and `EARTHENGINE_PROJECT`.
