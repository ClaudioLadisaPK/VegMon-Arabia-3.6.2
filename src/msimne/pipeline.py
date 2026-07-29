from __future__ import annotations

import logging
import shutil
from dataclasses import dataclass
from pathlib import Path

import dask
import geopandas as gpd
import odc.stac
from dask.distributed import Client
try:
    from tqdm.auto import tqdm
except Exception:  # pragma: no cover
    tqdm = None

from .composite import compute_sentinel2_composite, ensure_full_grid_coverage
from .config import REGION_NAMES, Settings
from .mosaic import mosaic_export
from .quality import NdviQuality, validate_and_fill_ndvi
from .stac import open_catalog
from .stats import classify_stats
from .utils import VALID_CODES, get_tile_id, monthly_windows

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class WindowResult:
    region: str
    suffix: str
    ndvi_output: str
    stack_output: str
    stats_output: str
    quality: NdviQuality


def _progress(iterable, total: int | None = None, desc: str = ""):
    if tqdm is None:
        return iterable
    return tqdm(iterable, total=total, desc=desc)


def load_aoi_and_grid(settings: Settings, code: str):
    aoi_file = settings.resolve_aoi_file(code)
    if not aoi_file.exists():
        raise FileNotFoundError(f"File AOI non trovato: {aoi_file}")
    aoi = gpd.read_file(aoi_file)
    aoi = aoi.set_crs(settings.final_mosaic_crs) if aoi.crs is None else aoi.to_crs(settings.final_mosaic_crs)

    grid = gpd.read_file(settings.resolve_grid_file())
    grid = grid.set_crs(settings.final_mosaic_crs) if grid.crs is None else grid.to_crs(settings.final_mosaic_crs)
    return aoi, grid


def month_outputs_exist(settings: Settings, code: str, wstart, wend, same_month: bool) -> bool:
    ndvi_mosaic, stack_mosaic, _ = output_paths(settings, code, wstart, wend, same_month)
    return ndvi_mosaic.exists() and stack_mosaic.exists()


def output_suffix(code: str, wstart, wend, same_month: bool) -> str:
    return f"{code}_{wstart:%Y-%m-%d}_{wend:%Y-%m-%d}" if same_month else f"{code}_{wstart:%Y-%m}"


def output_paths(settings: Settings, code: str, wstart, wend, same_month: bool):
    suffix = output_suffix(code, wstart, wend, same_month)
    ndvi_mosaic = settings.ndvi_dir / f"S2_ndvi_{suffix}.tif"
    stack_mosaic = settings.stack_dir / f"S2_stack_{suffix}.tif"
    stats_csv = settings.stats_dir / f"S2_stats_{suffix}.csv"
    return ndvi_mosaic, stack_mosaic, stats_csv


def run_pipeline(settings: Settings, code: str, start_dt, end_dt) -> list[WindowResult]:
    if code not in VALID_CODES:
        raise ValueError(f"Codice '{code}' non valido. Usa uno tra: {', '.join(sorted(VALID_CODES))}")

    same_month = start_dt.year == end_dt.year and start_dt.month == end_dt.month
    windows = [(start_dt, end_dt)] if same_month else monthly_windows(start_dt, end_dt)
    aoi, grid = load_aoi_and_grid(settings, code)
    area_km2 = aoi.to_crs("EPSG:3857").geometry.area.sum() / 1e6
    LOGGER.info("Area AOI %.2f km2", area_km2)

    aoi_buff = gpd.GeoDataFrame(geometry=aoi.geometry.buffer(100), crs=aoi.crs)
    tiles = gpd.sjoin(grid, aoi_buff, how="inner", predicate="intersects").drop(columns=["index_right"])
    LOGGER.info("Tile count %s", len(tiles))

    client = Client(
        processes=True,
        n_workers=settings.dask_workers,
        threads_per_worker=settings.dask_threads_per_worker,
        memory_limit=settings.dask_memory_limit,
    )
    dask.config.set(
        {
            "distributed.worker.memory.target": 0.85,
            "distributed.worker.memory.spill": 0.90,
            "distributed.scheduler.worker-saturation": 1.0,
        }
    )
    odc.stac.configure_rio(cloud_defaults=True, client=client)
    catalog = open_catalog(settings)
    results: list[WindowResult] = []

    try:
        for widx, (wstart, wend) in _progress(list(enumerate(windows, start=1)), total=len(windows), desc="Finestre"):
            if month_outputs_exist(settings, code, wstart, wend, same_month):
                LOGGER.info("Output gia presenti per finestra %s/%s", widx, len(windows))
                results.append(validate_existing_window(settings, aoi, code, wstart, wend, same_month))
                continue
            LOGGER.info("Finestra %s/%s %s -> %s", widx, len(windows), wstart.date(), wend.date())
            tile_rows = _progress(list(tiles.iterrows()), total=len(tiles), desc=f"Tile {wstart:%Y-%m}")
            for pos, (_, tile) in enumerate(tile_rows, start=1):
                tid = get_tile_id(tile.geometry)
                LOGGER.info("Tile %s (%s/%s)", tid, pos, len(tiles))
                compute_sentinel2_composite(
                    tile.geometry,
                    tid,
                    wstart.date(),
                    wend.date(),
                    settings,
                    use_scl=True,
                    catalog=catalog,
                )
            ensure_full_grid_coverage(tiles, aoi, wstart, wend, settings, catalog=catalog)
            results.append(build_outputs_for_window(settings, aoi, tiles, code, wstart, wend, same_month))
    finally:
        client.close()

    cleanup_workdir(settings)
    return results


def validate_existing_window(settings: Settings, aoi, code: str, wstart, wend, same_month: bool) -> WindowResult:
    suffix = output_suffix(code, wstart, wend, same_month)
    ndvi_fp, stack_fp, stats_fp = output_paths(settings, code, wstart, wend, same_month)
    quality = validate_and_fill_ndvi(ndvi_fp, aoi, settings)
    if not quality.passed:
        raise ValueError(f"Copertura NDVI sotto soglia per {suffix}: {quality.valid_ratio_after:.2%}")
    if not stats_fp.exists():
        classify_stats(aoi, ndvi_fp.name, settings, out_csv_name=stats_fp.name)
    return WindowResult(
        region=code,
        suffix=suffix,
        ndvi_output=str(ndvi_fp),
        stack_output=str(stack_fp),
        stats_output=str(stats_fp),
        quality=quality,
    )


def tile_source_files(settings: Settings, tiles: gpd.GeoDataFrame, base: str, name_prefix: str) -> list[Path]:
    files: list[Path] = []
    seen: set[Path] = set()
    for _, tile in tiles.iterrows():
        path = settings.work_dir / get_tile_id(tile.geometry) / f"{name_prefix}_{base}.tif"
        if path.exists() and path not in seen:
            files.append(path)
            seen.add(path)
    return files


def build_outputs_for_window(
    settings: Settings,
    aoi,
    tiles: gpd.GeoDataFrame,
    code: str,
    wstart,
    wend,
    same_month: bool,
) -> WindowResult:
    suffix = output_suffix(code, wstart, wend, same_month)
    base = f"{wstart:%Y%m%d}_{wend:%Y%m%d}"
    stack_files = tile_source_files(settings, tiles, base, "stack")
    ndvi_files = tile_source_files(settings, tiles, base, "ndvi")
    if not ndvi_files:
        raise ValueError(f"Nessuna composite per {suffix}")

    stack_name = f"S2_stack_{suffix}.tif"
    ndvi_name = f"S2_ndvi_{suffix}.tif"
    stats_name = f"S2_stats_{suffix}.csv"
    mosaic_export(
        f"*/stack_{base}.tif",
        settings.stack_dir,
        stack_name,
        settings,
        dtype="Int16",
        aoi=aoi,
        source_files=stack_files,
    )
    mosaic_export(
        f"*/ndvi_{base}.tif",
        settings.ndvi_dir,
        ndvi_name,
        settings,
        dtype="Int16",
        aoi=aoi,
        scale_forced=1 / 10000.0,
        source_files=ndvi_files,
    )
    ndvi_fp = settings.ndvi_dir / ndvi_name
    stack_fp = settings.stack_dir / stack_name
    stats_fp = settings.stats_dir / stats_name
    if not ndvi_fp.exists() or not stack_fp.exists():
        raise ValueError(f"Output incompleti per {suffix}")

    quality = validate_and_fill_ndvi(ndvi_fp, aoi, settings)
    if not quality.passed:
        raise ValueError(f"Copertura NDVI sotto soglia per {suffix}: {quality.valid_ratio_after:.2%}")
    classify_stats(aoi, ndvi_name, settings, out_csv_name=stats_name)
    return WindowResult(
        region=code,
        suffix=suffix,
        ndvi_output=str(ndvi_fp),
        stack_output=str(stack_fp),
        stats_output=str(stats_fp),
        quality=quality,
    )


def cleanup_workdir(settings: Settings) -> None:
    if not settings.work_dir.exists():
        return
    for path in settings.work_dir.iterdir():
        try:
            if path.is_dir():
                shutil.rmtree(path)
            else:
                path.unlink()
        except Exception:
            LOGGER.warning("Pulizia fallita per %s", path)
