from dagster import ConfigurableResource


class PathResource(ConfigurableResource):
    pg_path: str
    ghsl_path: str
    segregation_path: str
    jobs_path: str
    data_path: str


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
