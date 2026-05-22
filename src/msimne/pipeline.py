from __future__ import annotations

import logging
import shutil
from glob import glob
from multiprocessing import cpu_count

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
from .stats import classify_stats
from .utils import VALID_CODES, get_tile_id, monthly_windows

LOGGER = logging.getLogger(__name__)


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
    suffix = f"{code}_{wstart:%Y-%m-%d}_{wend:%Y-%m-%d}" if same_month else f"{code}_{wstart:%Y-%m}"
    ndvi_mosaic = settings.ndvi_dir / f"S2_ndvi_{suffix}.tif"
    stack_mosaic = settings.stack_dir / f"S2_stack_{suffix}.tif"
    return ndvi_mosaic.exists() and stack_mosaic.exists()


def run_pipeline(settings: Settings, code: str, start_dt, end_dt) -> None:
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

    client = Client(processes=True, n_workers=cpu_count(), threads_per_worker=1, memory_limit="auto")
    dask.config.set(
        {
            "distributed.worker.memory.target": 0.85,
            "distributed.worker.memory.spill": 0.90,
            "distributed.scheduler.worker-saturation": 1.0,
        }
    )
    odc.stac.configure_rio(cloud_defaults=True, client=client)

    try:
        for widx, (wstart, wend) in _progress(list(enumerate(windows, start=1)), total=len(windows), desc="Finestre"):
            if month_outputs_exist(settings, code, wstart, wend, same_month):
                LOGGER.info("Output gia presenti per finestra %s/%s", widx, len(windows))
                continue
            LOGGER.info("Finestra %s/%s %s -> %s", widx, len(windows), wstart.date(), wend.date())
            for idx, tile in _progress(list(tiles.iterrows()), total=len(tiles), desc=f"Tile {wstart:%Y-%m}"):
                tid = get_tile_id(tile.geometry)
                LOGGER.info("Tile %s (%s/%s)", tid, idx + 1, len(tiles))
                compute_sentinel2_composite(tile.geometry, tid, wstart.date(), wend.date(), settings, use_scl=True)
            ensure_full_grid_coverage(tiles, aoi, wstart, wend, settings)
    finally:
        client.close()

    if same_month:
        build_outputs_for_window(settings, aoi, code, windows[0][0], windows[0][1], same_month=True)
    else:
        for wstart, wend in windows:
            build_outputs_for_window(settings, aoi, code, wstart, wend, same_month=False)

    cleanup_workdir(settings)


def build_outputs_for_window(settings: Settings, aoi, code: str, wstart, wend, same_month: bool) -> None:
    suffix = f"{code}_{wstart:%Y-%m-%d}_{wend:%Y-%m-%d}" if same_month else f"{code}_{wstart:%Y-%m}"
    base = f"{wstart:%Y%m%d}_{wend:%Y%m%d}"
    if len(glob(str(settings.work_dir / f"*/ndvi_{base}.tif"))) == 0:
        LOGGER.warning("Nessuna composite per %s", suffix)
        return

    stack_name = f"S2_stack_{suffix}.tif"
    ndvi_name = f"S2_ndvi_{suffix}.tif"
    stats_name = f"S2_stats_{suffix}.csv"
    mosaic_export(f"*/stack_{base}.tif", settings.stack_dir, stack_name, settings, dtype="Int16", aoi=aoi)
    mosaic_export(
        f"*/ndvi_{base}.tif",
        settings.ndvi_dir,
        ndvi_name,
        settings,
        dtype="Int16",
        aoi=aoi,
        scale_forced=1 / 10000.0,
    )
    if (settings.ndvi_dir / ndvi_name).exists() and (settings.stack_dir / stack_name).exists():
        classify_stats(aoi, ndvi_name, settings, out_csv_name=stats_name)


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
