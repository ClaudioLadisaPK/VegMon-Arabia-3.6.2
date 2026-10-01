from pathlib import Path

import geopandas as gpd
import numpy as np
import rasterio
import shapely
from rasterio.transform import from_origin

from msimne.cli import build_parser, resolve_performance
from msimne.config import Settings
from msimne.pipeline import simplify_aoi
from msimne.quality import ndvi_valid_ratio
from msimne.utils import call_with_retries, is_network_error


def _parse(argv, monkeypatch):
    for name in ("MSIMNE_WORKERS", "MSIMNE_TILE_PARALLELISM", "MSIMNE_PROFILE", "MSIMNE_FINAL_GDAL_THREADS", "MSIMNE_TILE_GDAL_CACHE_MB", "MSIMNE_FINAL_GDAL_CACHE_MB", "MSIMNE_GDAL_WARP_MEMORY_MB"):
        monkeypatch.delenv(name, raising=False)
    args = build_parser().parse_args(argv)
    resolve_performance(args)
    return args


def test_profile_wsl_values(monkeypatch):
    args = _parse(["--profile", "wsl"], monkeypatch)
    assert args.workers == 3
    assert args.tile_parallelism == 3
    assert args.final_gdal_threads == "4"


def test_explicit_argument_overrides_profile(monkeypatch):
    args = _parse(["--profile", "vm", "--tile-parallelism", "5"], monkeypatch)
    assert args.tile_parallelism == 5
    assert args.workers == 12


def test_no_profile_keeps_historic_defaults(monkeypatch):
    args = _parse([], monkeypatch)
    assert args.workers == 16
    assert args.gdal_threads == "16"
    assert args.final_gdal_threads is None


def test_env_overrides_profile(monkeypatch):
    monkeypatch.setenv("MSIMNE_WORKERS", "7")
    args = build_parser().parse_args(["--profile", "vm"])
    resolve_performance(args)
    assert args.workers == 7


def test_final_gdal_threads_defaults_to_gdal_threads(tmp_path: Path):
    settings = Settings(project_root=tmp_path, gdal_threads="4")
    assert settings.final_gdal_threads == "4"


def _write_raster(path: Path, data: np.ndarray, transform) -> None:
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=data.shape[1],
        height=data.shape[0],
        count=1,
        dtype="int16",
        crs="EPSG:3857",
        transform=transform,
        nodata=-32768,
        tiled=True,
        blockxsize=16,
        blockysize=16,
    ) as dst:
        dst.write(data, 1)


def test_ndvi_valid_ratio_counts_only_inside_aoi(tmp_path: Path):
    data = np.full((64, 64), 1000, dtype=np.int16)
    data[:, 32:] = -32768  # meta' destra senza dati
    transform = from_origin(0, 640, 10, 10)
    path = tmp_path / "ndvi.tif"
    _write_raster(path, data, transform)
    # AOI = quadrato centrale 320x320 m: meta' dentro la zona valida, meta' nella zona nodata
    aoi = gpd.GeoDataFrame(geometry=[shapely.box(160, 160, 480, 480)], crs="EPSG:3857")
    assert abs(ndvi_valid_ratio(path, aoi, -32768) - 0.5) < 1e-9
    # AOI tutta nella zona valida
    aoi_left = gpd.GeoDataFrame(geometry=[shapely.box(0, 0, 320, 640)], crs="EPSG:3857")
    assert ndvi_valid_ratio(path, aoi_left, -32768) == 1.0


def test_simplify_aoi_keeps_area(tmp_path: Path):
    circle = shapely.Point(0, 0).buffer(10000, quad_segs=512)
    aoi = gpd.GeoDataFrame(geometry=[circle], crs="EPSG:3857")
    settings = Settings(project_root=tmp_path, aoi_simplify_m=5)
    simplified = simplify_aoi(aoi, settings).geometry.iloc[0]
    assert shapely.get_num_coordinates(simplified) < shapely.get_num_coordinates(circle)
    assert circle.hausdorff_distance(simplified) <= 5.0 + 1e-6


def test_is_network_error_detects_dns_failure():
    try:
        try:
            raise OSError("Temporary failure in name resolution")
        except OSError as inner:
            raise RuntimeError("STAC search fallita") from inner
    except RuntimeError as exc:
        assert is_network_error(exc)
    assert not is_network_error(ValueError("Copertura NDVI sotto soglia"))


def test_call_with_retries_succeeds_after_failure():
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] < 2:
            raise ValueError("errore temporaneo")
        return "ok"

    assert call_with_retries(flaky, attempts=3, delay_seconds=0, label="test") == "ok"
    assert calls["n"] == 2


def test_phase_specific_gdal_cache(monkeypatch):
    args = _parse(["--profile", "wsl", "--final-gdal-cache-mb", "2048"], monkeypatch)
    assert args.tile_gdal_cache_mb == 1024
    assert args.final_gdal_cache_mb == 2048


def test_gdal_caches_default_to_common_value(tmp_path: Path):
    settings = Settings(project_root=tmp_path, gdal_cache_mb=3000)
    assert settings.tile_gdal_cache_mb == 3000
    assert settings.final_gdal_cache_mb == 3000


def test_final_warp_memory_alias(monkeypatch):
    args = _parse(["--final-warp-memory-mb", "1024"], monkeypatch)
    assert args.gdal_warp_memory_mb == 1024
