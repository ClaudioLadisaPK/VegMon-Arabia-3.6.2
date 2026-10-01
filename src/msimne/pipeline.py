from __future__ import annotations

import gc
import logging
import os
import shutil
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

import dask
import geopandas as gpd
import odc.stac
import rasterio
import shapely
from dask.distributed import Client
try:
    from tqdm.auto import tqdm
except Exception:  # pragma: no cover
    tqdm = None

from .composite import build_tile_paths, compute_tile, ensure_full_grid_coverage
from .config import REGION_NAMES, Settings
from .mosaic import mosaic_export
from .quality import NdviQuality, validate_ndvi_coverage
from .stac import StacItemCache, gdal_http_options, open_catalog
from .stats import classify_stats
from .utils import VALID_CODES, get_tile_id, monthly_windows

LOGGER = logging.getLogger(__name__)

# Stime di spazio misurate sulla run R04 2026-07 (200 tile, mosaico 1,92 Gpx):
# ~55 MB per tile intermedia non compressa; temporanei + COG finali ~6,2 byte per pixel del riquadro.
TILE_WORK_BYTES = 60 * 1024**2
FINAL_BYTES_PER_BBOX_PIXEL = 6.5


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


@contextmanager
def _phase(timings: dict[str, float], name: str):
    tic = time.perf_counter()
    try:
        yield
    finally:
        timings[name] = timings.get(name, 0.0) + time.perf_counter() - tic


def _log_timings(code: str, timings: dict[str, float], wall_seconds: float) -> None:
    parts = ", ".join(f"{name}={seconds / 60:.1f} min" for name, seconds in timings.items())
    LOGGER.info("Tempi fasi %s: %s | durata totale=%.1f min", code, parts or "-", wall_seconds / 60)


def simplify_aoi(aoi: gpd.GeoDataFrame, settings: Settings) -> gpd.GeoDataFrame:
    if settings.aoi_simplify_m <= 0:
        return aoi
    union = shapely.union_all([geom for geom in aoi.geometry if geom is not None and not geom.is_empty])
    simplified = union.simplify(settings.aoi_simplify_m, preserve_topology=True)
    LOGGER.info(
        "AOI semplificata (tolleranza %.1f m): vertici %s -> %s",
        settings.aoi_simplify_m,
        shapely.get_num_coordinates(union),
        shapely.get_num_coordinates(simplified),
    )
    return gpd.GeoDataFrame(geometry=[simplified], crs=aoi.crs)


def _running_in_wsl() -> bool:
    try:
        return "microsoft" in Path("/proc/version").read_text().lower()
    except OSError:
        return False


def check_disk_space(settings: Settings, aoi: gpd.GeoDataFrame, pending_tiles: int) -> None:
    minx, miny, maxx, maxy = aoi.total_bounds
    bbox_pixels = ((maxx - minx) / settings.resolution) * ((maxy - miny) / settings.resolution)
    need_work = pending_tiles * TILE_WORK_BYTES
    need_final = bbox_pixels * FINAL_BYTES_PER_BBOX_PIXEL
    work_usage = shutil.disk_usage(settings.work_dir)
    out_usage = shutil.disk_usage(settings.outputs_dir)
    same_disk = os.stat(settings.work_dir).st_dev == os.stat(settings.outputs_dir).st_dev
    gib = 1024**3
    LOGGER.info(
        "Spazio stimato: tile=%.1f GB, mosaici=%.1f GB; libero work=%.1f GB, output=%.1f GB",
        need_work / gib,
        need_final / gib,
        work_usage.free / gib,
        out_usage.free / gib,
    )
    if same_disk:
        problems = work_usage.free < (need_work + need_final) * 1.1
    else:
        problems = work_usage.free < (need_work + need_final * 0.4) * 1.1 or out_usage.free < need_final * 0.6 * 1.1
    # In WSL il disco Linux e' un file .vhdx su Windows (di norma C:): lo spazio reale e' quello di C:.
    host_drive = Path("/mnt/c")
    if _running_in_wsl() and host_drive.exists() and not str(settings.work_dir).startswith("/mnt/"):
        host_free = shutil.disk_usage(host_drive).free
        LOGGER.info("WSL: spazio libero reale su Windows C: %.1f GB", host_free / gib)
        problems = problems or host_free < (need_work + need_final) * 1.1
    if problems:
        raise RuntimeError(
            "Spazio disco probabilmente insufficiente per completare la regione "
            f"(servono ~{(need_work + need_final) / gib:.0f} GB). Libera spazio o usa --skip-disk-check."
        )


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

    # Selezione tile sempre sull'AOI originale (stesso insieme di tile); l'AOI semplificata
    # serve a ritaglio del mosaico e controlli di copertura.
    aoi_buff = gpd.GeoDataFrame(geometry=aoi.geometry.buffer(100), crs=aoi.crs)
    tiles = gpd.sjoin(grid, aoi_buff, how="inner", predicate="intersects").drop(columns=["index_right"])
    LOGGER.info("Tile count %s", len(tiles))
    aoi = simplify_aoi(aoi, settings)
    timings: dict[str, float] = {}
    run_started = time.perf_counter()

    dask.config.set(
        {
            "distributed.worker.memory.target": 0.85,
            "distributed.worker.memory.spill": 0.90,
            "distributed.scheduler.worker-saturation": 1.0,
        }
    )
    client: Client | None = None
    catalog = open_catalog(settings)
    item_cache = StacItemCache(catalog, settings, tiles.to_crs("EPSG:4326").total_bounds)
    results: list[WindowResult] = []

    try:
        for widx, (wstart, wend) in _progress(list(enumerate(windows, start=1)), total=len(windows), desc="Finestre"):
            if month_outputs_exist(settings, code, wstart, wend, same_month):
                LOGGER.info("Output gia presenti per finestra %s/%s", widx, len(windows))
                with _phase(timings, "validazione_esistenti"):
                    results.append(validate_existing_window(settings, aoi, code, wstart, wend, same_month))
                continue
            LOGGER.info("Finestra %s/%s %s -> %s", widx, len(windows), wstart.date(), wend.date())
            base = f"{wstart:%Y%m%d}_{wend:%Y%m%d}"
            tile_jobs = [
                (pos, tile.geometry)
                for pos, (_, tile) in enumerate(tiles.iterrows(), start=1)
                if not tile_outputs_exist(settings, get_tile_id(tile.geometry), base)
            ]
            LOGGER.info("Tile da elaborare %s su %s", len(tile_jobs), len(tiles))
            if settings.check_disk_space:
                check_disk_space(settings, aoi, len(tile_jobs))
            failed_tiles: list[str] = []
            with _phase(timings, "tile"):
                if settings.tile_parallelism <= 1:
                    tile_rows = _progress(tile_jobs, total=len(tile_jobs), desc=f"Tile {wstart:%Y-%m}")
                    if tile_jobs:
                        client = ensure_dask_client(client, settings)
                    for pos, geom in tile_rows:
                        try:
                            run_tile_job(settings, catalog, item_cache, geom, wstart, wend, pos, len(tiles))
                        except Exception:
                            failed_tiles.append(get_tile_id(geom))
                            LOGGER.exception("Tile %s fallita: verra' ritentata nella fase di copertura", get_tile_id(geom))
                        flush_dask_memory(client)
                else:
                    LOGGER.info("Elaborazione tile in parallelo: parallelism=%s", settings.tile_parallelism)
                    if tile_jobs:
                        client = ensure_dask_client(client, settings)
                    with ThreadPoolExecutor(max_workers=settings.tile_parallelism) as executor:
                        futures = {
                            executor.submit(run_tile_job, settings, catalog, item_cache, geom, wstart, wend, pos, len(tiles)): geom
                            for pos, geom in tile_jobs
                        }
                        for future in _progress(as_completed(futures), total=len(futures), desc=f"Tile {wstart:%Y-%m}"):
                            try:
                                future.result()
                            except Exception:
                                tid = get_tile_id(futures[future])
                                failed_tiles.append(tid)
                                LOGGER.exception("Tile %s fallita: verra' ritentata nella fase di copertura", tid)
                    flush_dask_memory(client)
            if failed_tiles:
                LOGGER.warning("Tile fallite nella fase principale: %s (%s)", len(failed_tiles), ", ".join(failed_tiles))
            with _phase(timings, "copertura_tile"):
                if settings.seasonal_fallback_coverage_threshold > 0 or failed_tiles:
                    client = ensure_dask_client(client, settings)
                ensure_full_grid_coverage(tiles, aoi, wstart, wend, settings, catalog=catalog, item_cache=item_cache)
            client = close_dask_client(client)
            results.append(build_outputs_for_window(settings, aoi, tiles, code, wstart, wend, same_month, timings))
    finally:
        client = close_dask_client(client)
        _log_timings(code, timings, time.perf_counter() - run_started)

    cleanup_workdir(settings)
    return results


def ensure_dask_client(client: Client | None, settings: Settings) -> Client:
    if client is not None:
        return client
    # I worker Dask ereditano l'ambiente al momento dell'avvio: le opzioni HTTP di GDAL
    # (timeout/retry verso Planetary Computer) vanno impostate prima.
    os.environ.update(gdal_http_options(settings))
    client = Client(
        processes=True,
        n_workers=settings.dask_workers,
        threads_per_worker=settings.dask_threads_per_worker,
        memory_limit=settings.dask_memory_limit,
    )
    odc.stac.configure_rio(cloud_defaults=True, client=client, **gdal_http_options(settings))
    return client


def close_dask_client(client: Client | None) -> Client | None:
    if client is None:
        return None
    try:
        LOGGER.info("Chiusura client Dask prima delle fasi GDAL finali")
        flush_dask_memory(client)
        client.close(timeout=30)
    except Exception:
        LOGGER.warning("Chiusura client Dask non riuscita; gli output prodotti restano validi", exc_info=True)
    finally:
        gc.collect()
    return None


def flush_dask_memory(client: Client | None) -> None:
    if client is None:
        return
    try:
        client.run(gc.collect)
    except Exception:
        LOGGER.debug("GC sui worker Dask non riuscito", exc_info=True)
    gc.collect()


def tile_outputs_exist(settings: Settings, tile_id: str, base: str) -> bool:
    paths = build_tile_paths(settings, tile_id, base)
    return paths.stack.exists() and paths.ndvi.exists()


def run_tile_job(
    settings: Settings,
    catalog,
    item_cache: StacItemCache,
    geom,
    wstart,
    wend,
    pos: int,
    total: int,
) -> None:
    tid = get_tile_id(geom)
    LOGGER.info("Tile %s (%s/%s)", tid, pos, total)
    compute_tile(
        geom,
        tid,
        wstart.date(),
        wend.date(),
        settings,
        use_scl=settings.use_scl,
        catalog=catalog,
        item_cache=item_cache,
    )


def validate_existing_window(settings: Settings, aoi, code: str, wstart, wend, same_month: bool) -> WindowResult:
    suffix = output_suffix(code, wstart, wend, same_month)
    ndvi_fp, stack_fp, stats_fp = output_paths(settings, code, wstart, wend, same_month)
    if not ndvi_fp.exists() or not stack_fp.exists():
        raise FileNotFoundError(f"Output NDVI/stack mancanti per {suffix}")
    quality = validate_ndvi_coverage(ndvi_fp, aoi, settings)
    if not quality.passed:
        raise ValueError(f"Copertura NDVI sotto soglia per {suffix}: {quality.valid_ratio_after:.2%}")
    if not stats_fp.exists():
        LOGGER.info("Statistiche mancanti per %s: calcolo il CSV", suffix)
        classify_stats(aoi, ndvi_fp.name, settings, out_csv_name=stats_fp.name)
    if not stats_fp.exists():
        raise FileNotFoundError(f"Statistiche mancanti per {suffix}")
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


def _raster_is_readable(path: Path) -> bool:
    if not path.exists():
        return False
    try:
        with rasterio.open(path) as src:
            return src.width > 0 and src.height > 0
    except Exception:
        LOGGER.warning("Output esistente non leggibile, verra' rigenerato: %s", path)
        return False


def build_outputs_for_window(
    settings: Settings,
    aoi,
    tiles: gpd.GeoDataFrame,
    code: str,
    wstart,
    wend,
    same_month: bool,
    timings: dict[str, float] | None = None,
) -> WindowResult:
    timings = timings if timings is not None else {}
    suffix = output_suffix(code, wstart, wend, same_month)
    base = f"{wstart:%Y%m%d}_{wend:%Y%m%d}"
    stack_files = tile_source_files(settings, tiles, base, "stack")
    ndvi_files = tile_source_files(settings, tiles, base, "ndvi")
    if not ndvi_files:
        raise ValueError(f"Nessuna composite per {suffix}")

    stack_name = f"S2_stack_{suffix}.tif"
    ndvi_name = f"S2_ndvi_{suffix}.tif"
    stats_name = f"S2_stats_{suffix}.csv"
    ndvi_fp = settings.ndvi_dir / ndvi_name
    stack_fp = settings.stack_dir / stack_name
    stats_fp = settings.stats_dir / stats_name

    mosaic_jobs = []
    if _raster_is_readable(stack_fp):
        LOGGER.info("Mosaico STACK gia presente, salto: %s", stack_fp.name)
    else:
        mosaic_jobs.append(
            (
                "mosaico_stack",
                dict(
                    pattern=f"*/stack_{base}.tif",
                    output_dir=settings.stack_dir,
                    output_name=stack_name,
                    settings=settings,
                    dtype="Int16",
                    aoi=aoi,
                    source_files=stack_files,
                ),
            )
        )
    if _raster_is_readable(ndvi_fp):
        LOGGER.info("Mosaico NDVI gia presente, salto: %s", ndvi_fp.name)
    else:
        mosaic_jobs.append(
            (
                "mosaico_ndvi",
                dict(
                    pattern=f"*/ndvi_{base}.tif",
                    output_dir=settings.ndvi_dir,
                    output_name=ndvi_name,
                    settings=settings,
                    dtype="Int16",
                    aoi=aoi,
                    scale_forced=1 / 10000.0,
                    source_files=ndvi_files,
                ),
            )
        )

    def run_mosaic(name: str, kwargs: dict) -> None:
        LOGGER.info("Avvio %s (%s thread GDAL)", name, settings.final_gdal_threads)
        with _phase(timings, name):
            mosaic_export(**kwargs)
        LOGGER.info("Completato %s", name)

    if settings.final_parallel_mosaics > 1 and len(mosaic_jobs) > 1:
        with ThreadPoolExecutor(max_workers=len(mosaic_jobs)) as executor:
            for future in [executor.submit(run_mosaic, name, kwargs) for name, kwargs in mosaic_jobs]:
                future.result()
    else:
        for name, kwargs in mosaic_jobs:
            run_mosaic(name, kwargs)

    if not ndvi_fp.exists() or not stack_fp.exists():
        raise ValueError(f"Output incompleti per {suffix}")

    with _phase(timings, "controllo_copertura"):
        quality = validate_ndvi_coverage(ndvi_fp, aoi, settings)
    if not quality.passed:
        raise ValueError(f"Copertura NDVI sotto soglia per {suffix}: {quality.valid_ratio_after:.2%}")
    with _phase(timings, "statistiche"):
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
