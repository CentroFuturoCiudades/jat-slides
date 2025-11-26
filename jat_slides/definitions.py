import dagster as dg
from jat_slides import assets, managers, resources

# Definitions
defs = dg.Definitions.merge(
    dg.Definitions(
        assets=(
            list(dg.load_assets_from_modules([assets.agebs], group_name="agebs"))
            + list(dg.load_assets_from_modules([assets.muns], group_name="muns"))
            + list(
                dg.load_assets_from_modules(
                    [assets.built, assets.cells, assets.income],
                ),
            )
            + list(dg.load_assets_from_modules([assets.slides]))
            + list(dg.load_assets_from_modules([assets.jobs], group_name="jobs"))
        ),
    ),
    assets.agebs.defs,
    assets.maps.defs,
    assets.stats.defs,
    managers.defs,
    resources.defs,
)
