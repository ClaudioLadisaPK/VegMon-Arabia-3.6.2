from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


REGION_NAMES = {
    "R01": "Ar Riyad",
    "R02": "Makkah Al Mukarramah",
    "R03": "Al Madinah Al Munawwarah",
    "R04": "Al Qaseem",
    "R05": "Eastern Region",
    "R06": "Aseer",
    "R07": "Tabuk",
    "R08": "Hail",
    "R09": "Northern Borders",
    "R10": "Jazan",
    "R11": "Najran",
    "R12": "Al Bahah",
    "R13": "Al Jawf",
    "R14": "Region 14",
}

PRODUCTION_REGIONS = tuple(f"R{i:02d}" for i in range(1, 14))


@dataclass(slots=True)
class Settings:
    project_root: Path
    inputs_dir_override: Path | None = None
    outputs_dir_override: Path | None = None
    work_dir_override: Path | None = None
    grid_file_override: Path | None = None
    final_mosaic_crs: str = "EPSG:3857"
    inputs_dir: Path = field(init=False)
    regions_dir: Path = field(init=False)
    grids_dir: Path = field(init=False)
    grid_file: Path = field(init=False)
    outputs_dir: Path = field(init=False)
    work_dir: Path = field(init=False)
    stack_dir: Path = field(init=False)
    ndvi_dir: Path = field(init=False)
    stats_dir: Path = field(init=False)
    logs_dir: Path = field(init=False)
    state_dir: Path = field(init=False)
    reports_dir: Path = field(init=False)
    ndvi_nodata: int = -32768
    gdal_timeout: int = 14400
    stac_url: str = "https://planetarycomputer.microsoft.com/api/stac/v1"
    resolution: int = 10
    max_items: int = 10
    initial_max_items: int = 6
    tile_parallelism: int = 1
    intermediate_compression: str = "none"
    min_valid_ratio: float = 0.95
    coverage_threshold: float = 0.99
    final_ndvi_valid_ratio: float = 0.98
    max_coverage_loops: int = 5
    seasonal_fallback_coverage_threshold: float = 0.98
    seasonal_fallback_years: int = 2
    cloud_cover_lt: int = 80
    ndvi_threshold: float = 0.2
    valid_scl_classes: tuple[int, ...] = (2, 4, 5, 6, 7)
    dask_workers: int = 16
    dask_threads_per_worker: int = 2
    dask_memory_limit: str = "12GB"
    gdal_threads: str = "16"
    gdal_warp_memory_mb: int = 16384
    gdal_cache_mb: int = 4096
    final_gdal_threads: str = ""
    final_parallel_mosaics: int = 1
    use_scl: bool = True
    aoi_simplify_m: float = 0.0
    tile_retries: int = 4
    retry_delay_seconds: int = 30
    network_wait_max_seconds: int = 1800
    http_timeout: int = 60
    http_max_retry: int = 8
    http_retry_delay: int = 2
    check_disk_space: bool = True

    def __post_init__(self) -> None:
        if self.max_items < 1:
            raise ValueError("max_items deve essere >= 1")
        if self.initial_max_items < 1 or self.initial_max_items > self.max_items:
            self.initial_max_items = self.max_items
        self.tile_parallelism = max(1, self.tile_parallelism)
        self.final_gdal_threads = str(self.final_gdal_threads or self.gdal_threads)
        self.final_parallel_mosaics = max(1, min(2, self.final_parallel_mosaics))
        self.tile_retries = max(1, self.tile_retries)
        self.aoi_simplify_m = max(0.0, self.aoi_simplify_m)
        self.intermediate_compression = self.intermediate_compression.upper()
        if self.intermediate_compression not in {"NONE", "DEFLATE", "LZW"}:
            raise ValueError("intermediate_compression deve essere uno tra: none, deflate, lzw")
        self.inputs_dir = (self.inputs_dir_override or (self.project_root / "inputs")).resolve()
        self.regions_dir = self.inputs_dir / "regions"
        self.grids_dir = self.inputs_dir / "grids"
        self.outputs_dir = (self.outputs_dir_override or (self.project_root / "outputs")).resolve()
        self.grid_file = (
            self.grid_file_override.resolve()
            if self.grid_file_override is not None
            else (self.grids_dir / "ARAB_GRIGLIA.geojson").resolve()
        )
        self.work_dir = (self.work_dir_override or (self.outputs_dir / "working_s2")).resolve()
        self.stack_dir = self.outputs_dir / "S2" / "STACK"
        self.ndvi_dir = self.outputs_dir / "S2" / "NDVI"
        self.stats_dir = self.outputs_dir / "S2" / "STATS"
        self.logs_dir = self.outputs_dir / "logs"
        self.state_dir = self.outputs_dir / "state"
        self.reports_dir = self.outputs_dir / "reports"

    def ensure_directories(self) -> None:
        for path in (
            self.inputs_dir,
            self.regions_dir,
            self.grids_dir,
            self.outputs_dir,
            self.work_dir,
            self.stack_dir,
            self.ndvi_dir,
            self.stats_dir,
            self.logs_dir,
            self.state_dir,
            self.reports_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)

    def resolve_aoi_file(self, region_code: str) -> Path:
        preferred = self.regions_dir / f"{region_code}.geojson"
        legacy = self.project_root / f"{region_code}.geojson"
        return preferred if preferred.exists() else legacy

    def resolve_grid_file(self) -> Path:
        return self.grid_file
