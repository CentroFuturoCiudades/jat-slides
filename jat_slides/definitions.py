from pathlib import Path

import toml
from cfc_dagster_utils.managers.dataframe import DataFrameFileManager
from cfc_dagster_utils.managers.geodataframe import GeoDataFrameFileManager
from cfc_dagster_utils.resources import PostgresResource

import dagster as dg
from jat_slides.defs.managers import (
    PathIOManager,
    PlotFigIOManager,
    PresentationIOManager,
    RasterIOManager,
    ReprojectedRasterIOManager,
    TextIOManager,
)
from jat_slides.defs.resources import ConfigResource, PathResource


@dg.definitions
def definitions() -> dg.Definitions:
    project_root = Path(__file__).parent.parent

    main_defs = dg.load_from_defs_folder(project_root=project_root)

    # Resources
    data_path = project_root / "data"
    path_resource = PathResource(
        in_path=str(data_path / "input"),
        out_path=str(data_path / "output"),
        pg_path="/Users/rodolfofigueroa/Documents/ageb-alignment/data/output",
    )

    zone_config_path = Path("./config/zone.toml")
    with zone_config_path.open(encoding="utf8") as f:
        config = toml.load(f)

    zone_config = ConfigResource(
        names=config.get("names"),
        bounds=config["bounds"],
        linewidths=config.get("linewidths"),
        legend_pos=config.get("legend_pos"),
        add_labels=config.get("add_labels"),
        overlays=config.get("overlays"),
    )

    mun_config_path = Path("./config/mun.toml")
    with mun_config_path.open(encoding="utf8") as f:
        config = toml.load(f)

    mun_config = ConfigResource(
        names=config.get("names"),
        bounds=config["bounds"],
        linewidths=config.get("linewidths"),
        legend_pos=config.get("legend_pos"),
        add_labels=config.get("add_labels"),
        overlays=config.get("overlays"),
    )

    postgis_resource = PostgresResource(
        host=dg.EnvVar("POSTGRES_HOST"),
        port=dg.EnvVar.int("POSTGRES_PORT"),
        user=dg.EnvVar("POSTGRES_USER"),
        password=dg.EnvVar("POSTGRES_PASSWORD"),
        database=dg.EnvVar("POSTGRES_DB"),
    )

    # Managers
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

    # Out
    extra_defs = dg.Definitions(
        resources={
            "path_resource": path_resource,
            "zone_config_resource": zone_config,
            "mun_config_resource": mun_config,
            "dataframe_manager": DataFrameFileManager(
                path_resource=path_resource, extension=".parquet"
            ),
            "geodataframe_manager": GeoDataFrameFileManager(
                path_resource=path_resource, extension=".geoparquet"
            ),
            "memory_manager": memory_manager,
            "presentation_manager": presentation_manger,
            "raster_manager": raster_manager,
            "reprojected_raster_manager": reprojected_raster_manager,
            "plot_manager": plot_manager,
            "path_manager": path_manager,
            "text_manager": text_manager,
            "postgis_resource": postgis_resource,
        },
    )
    return dg.Definitions.merge(main_defs, extra_defs)
