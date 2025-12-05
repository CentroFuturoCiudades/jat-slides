import dagster as dg
from jat_slides.assets.agebs import base

defs = dg.Definitions(
    assets=(list(dg.load_assets_from_modules([base], group_name="agebs_base"))),
)
