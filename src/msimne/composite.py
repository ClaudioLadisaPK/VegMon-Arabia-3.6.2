from __future__ import annotations

import gc
import logging
import os
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

import geopandas as gpd
import numpy as np
import rasterio
import rasterio.mask
import rioxarray  # noqa: F401 - registers the .rio accessor on xarray objects
import xarray as xr
from dateutil.relativedelta import relativedelta
from rasterio.features import geometry_mask
from rasterio.warp import Resampling
from pystac_client import Client as StacClient

from .config import Settings
from .io import intermediate_raster_options, save_stack_int16, set_scale_offset
from .stac import InsufficientSclCoverage, StacItemCache, load_s2_median, open_catalog
from .utils import call_with_retries, get_tile_id

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class TilePaths:
    stack: Path
    ndvi: Path


class TileBuildStatus(str, Enum):
    BUILT = "built"
    NO_DATA = "no_data"
    LOW_SCL = "low_scl"


def q16_signed(a, scale=10000.0, nodata=-999):
    out = np.clip(a, -1.0, 1.0) * scale
    out = np.rint(out)
    out[~np.isfinite(a)] = nodata
    return out.astype(np.int16)


def build_tile_paths(settings: Settings, tile_id: str, base: str) -> TilePaths:
    out_dir = settings.work_dir / tile_id
    out_dir.mkdir(parents=True, exist_ok=True)
    return TilePaths(
        stack=out_dir / f"stack_{base}.tif",
        ndvi=out_dir / f"ndvi_{base}.tif",
    )


def raster_coverage_ratio(path: Path, nodata_val: int) -> float:
    with rasterio.open(path) as src:
        scale = max(int(max(src.width, src.height) / 2000), 1)
        out_h = max(src.height // scale, 1)
        out_w = max(src.width // scale, 1)
        arr = src.read(1, out_shape=(out_h, out_w), resampling=Resampling.nearest)
        return float(np.count_nonzero(arr != nodata_val)) / arr.size


def validate_raster_coverage(path: Path, nodata_val: int, min_valid_ratio: float) -> bool:
    return raster_coverage_ratio(path, nodata_val) >= min_valid_ratio


def _partial_path(path: Path) -> Path:
    return path.with_name(f"{path.stem}.partial{path.suffix}")


def fallback_marker_path(paths: TilePaths) -> Path:
    return paths.ndvi.with_name(f"{paths.ndvi.stem}.fallback_noscl")


def build_tile_products(med: xr.Dataset, paths: TilePaths, settings: Settings, crs_to_use, crs_str: str) -> None:
    # Scrittura su file .partial e rinomina finale: una tile interrotta a meta' non viene
    # mai scambiata per completa alla ripresa (tile_outputs_exist controlla i nomi finali).
    stack_tmp = _partial_path(paths.stack)
    ndvi_tmp = _partial_path(paths.ndvi)
    save_stack_int16(
        med,
        ["B04", "B03", "B02", "B08"],
        stack_tmp,
        nodata=settings.ndvi_nodata,
        settings=settings,
        crs=crs_to_use,
    )

    red = med["B04"].astype("float32")
    nir = med["B08"].astype("float32")
    den = nir + red
    ndvi = xr.where(den > 0, (nir - red) / den, np.nan).clip(-1.0, 1.0)

    ndvi_q = xr.apply_ufunc(
        q16_signed,
        ndvi,
        dask="parallelized",
        kwargs=dict(scale=10000.0, nodata=settings.ndvi_nodata),
        output_dtypes=[np.int16],
    )
    ndvi_q.rio.write_nodata(settings.ndvi_nodata, encoded=True, inplace=True)
    ndvi_q.rio.write_crs(crs_to_use, inplace=True)
    ndvi_q.rio.to_raster(ndvi_tmp, dtype="int16", **intermediate_raster_options(settings))
    set_scale_offset(ndvi_tmp, scale=1 / 10000.0, offset=0.0)
    os.replace(stack_tmp, paths.stack)
    os.replace(ndvi_tmp, paths.ndvi)


def release_dask_collection(collection) -> None:
    try:
        collection.close()
    except Exception:
        pass
    gc.collect()


def remove_tile_products(paths: TilePaths) -> None:
    for path in (
        paths.stack,
        paths.ndvi,
        _partial_path(paths.stack),
        _partial_path(paths.ndvi),
        fallback_marker_path(paths),
    ):
        if path.exists():
            path.unlink()


def build_tile_for_range(
    geom,
    paths: TilePaths,
    start,
    end,
    settings: Settings,
    use_scl: bool,
    catalog: StacClient,
    item_cache: StacItemCache | None = None,
) -> TileBuildStatus:
    return build_tile_for_ranges(
        geom,
        paths,
        [(start.isoformat(), end.isoformat())],
        settings,
        use_scl=use_scl,
        catalog=catalog,
        item_cache=item_cache,
    )


def build_tile_for_ranges(
    geom,
    paths: TilePaths,
    ranges: list[tuple[str, str]],
    settings: Settings,
    use_scl: bool,
    catalog: StacClient,
    item_cache: StacItemCache | None = None,
) -> TileBuildStatus:
    geom_wgs84 = gpd.GeoSeries([geom], crs=settings.final_mosaic_crs).to_crs("EPSG:4326").iloc[0]
    try:
        med = load_s2_median(
            catalog,
            settings,
            geom_wgs84,
            ranges,
            settings.final_mosaic_crs,
            use_scl=use_scl,
            item_cache=item_cache,
        )
    except InsufficientSclCoverage:
        return TileBuildStatus.LOW_SCL
    if med is None:
        return TileBuildStatus.NO_DATA

    med = med.persist()
    try:
        crs_to_use = med.rio.crs
        crs_str = rasterio.crs.CRS.from_user_input(crs_to_use).to_string()
    except Exception:
        crs_to_use = settings.final_mosaic_crs
        crs_str = rasterio.crs.CRS.from_user_input(settings.final_mosaic_crs).to_string()

    try:
        build_tile_products(med, paths, settings, crs_to_use, crs_str)
    finally:
        release_dask_collection(med)
    return TileBuildStatus.BUILT


def seasonal_fallback_ranges(start, end, years: int) -> list[tuple[str, str]]:
    return [
        ((start - relativedelta(years=year)).isoformat(), (end - relativedelta(years=year)).isoformat())
        for year in range(1, years + 1)
    ]


def compute_tile(
    geom,
    tile_id: str,
    start,
    end,
    settings: Settings,
    use_scl: bool = True,
    catalog: StacClient | None = None,
    item_cache: StacItemCache | None = None,
    force: bool = False,
) -> None:
    call_with_retries(
        compute_sentinel2_composite,
        geom,
        tile_id,
        start,
        end,
        settings,
        use_scl=use_scl,
        catalog=catalog,
        item_cache=item_cache,
        force=force,
        attempts=settings.tile_retries,
        delay_seconds=settings.retry_delay_seconds,
        label=f"tile {tile_id}",
        network_url=settings.stac_url,
        network_wait_seconds=settings.network_wait_max_seconds,
    )


def compute_sentinel2_composite(
    geom,
    tile_id: str,
    start,
    end,
    settings: Settings,
    use_scl: bool = True,
    catalog: StacClient | None = None,
    item_cache: StacItemCache | None = None,
    force: bool = False,
) -> None:
    base = f"{start:%Y%m%d}_{end:%Y%m%d}"
    paths = build_tile_paths(settings, tile_id, base)
    if not force and all(path.exists() for path in (paths.stack, paths.ndvi)) and use_scl:
        LOGGER.info("Tile %s gia elaborato per %s", tile_id, base)
        return

    catalog = catalog or open_catalog(settings)
    status = build_tile_for_range(
        geom,
        paths,
        start,
        end,
        settings,
        use_scl=use_scl,
        catalog=catalog,
        item_cache=item_cache,
    )
    if status == TileBuildStatus.LOW_SCL and use_scl:
        LOGGER.info("Copertura SCL insufficiente per %s, rigenero direttamente senza SCL", tile_id)
        remove_tile_products(paths)
        compute_sentinel2_composite(
            geom,
            tile_id,
            start,
            end,
            settings,
            use_scl=False,
            catalog=catalog,
            item_cache=item_cache,
        )
        return
    if status != TileBuildStatus.BUILT:
        LOGGER.warning("Nessun dataset valido per %s %s", tile_id, base)
        return

    coverage = raster_coverage_ratio(paths.ndvi, settings.ndvi_nodata)
    if use_scl and coverage < settings.min_valid_ratio:
        LOGGER.info("Copertura insufficiente per %s, rigenero senza SCL", tile_id)
        remove_tile_products(paths)
        compute_sentinel2_composite(
            geom,
            tile_id,
            start,
            end,
            settings,
            use_scl=False,
            catalog=catalog,
            item_cache=item_cache,
        )
        return

    if coverage >= settings.seasonal_fallback_coverage_threshold:
        return

    fallback_ranges = seasonal_fallback_ranges(start, end, settings.seasonal_fallback_years)
    LOGGER.warning(
        "Tile %s copertura %.2f%% sotto soglia %.2f%%: fallback stagionale anni precedenti %s senza SCL",
        tile_id,
        coverage * 100,
        settings.seasonal_fallback_coverage_threshold * 100,
        ", ".join(f"{rng[0]}->{rng[1]}" for rng in fallback_ranges),
    )
    remove_tile_products(paths)
    status = build_tile_for_ranges(
        geom,
        paths,
        fallback_ranges,
        settings,
        use_scl=False,
        catalog=catalog,
        item_cache=item_cache,
    )
    if status != TileBuildStatus.BUILT:
        LOGGER.warning("Fallback stagionale senza dataset valido per %s %s", tile_id, base)
        return
    fallback_coverage = raster_coverage_ratio(paths.ndvi, settings.ndvi_nodata)
    LOGGER.info("Tile %s copertura dopo fallback stagionale: %.2f%%", tile_id, fallback_coverage * 100)
    if not use_scl:
        # Il ricalcolo "critico" in ensure_full_grid_coverage (senza SCL, force) ripeterebbe
        # esattamente questo percorso con gli stessi dati: il marker permette di saltarlo.
        fallback_marker_path(paths).touch()


def tile_coverage_in_aoi(ndvi_fp: Path, tile_geom, aoi_union, nodata_val: int) -> float:
    inter = tile_geom.intersection(aoi_union)
    if inter.is_empty:
        return 1.0
    with rasterio.open(ndvi_fp) as src:
        arr, transform = rasterio.mask.mask(src, [inter], crop=True, filled=True, nodata=nodata_val)
    data = arr[0]
    inside = geometry_mask([inter], transform=transform, invert=True, out_shape=data.shape)
    total = int(inside.sum())
    if total == 0:
        return 0.0
    valid = (data != nodata_val) & inside
    return float(valid.sum()) / float(total)


def ensure_full_grid_coverage(
    tiles_gdf: gpd.GeoDataFrame,
    aoi: gpd.GeoDataFrame,
    wstart,
    wend,
    settings: Settings,
    catalog: StacClient | None = None,
    item_cache: StacItemCache | None = None,
) -> None:
    base = f"{wstart:%Y%m%d}_{wend:%Y%m%d}"
    aoi_union = aoi.geometry.unary_union
    attempted_critical: set[str] = set()
    for _, tile in tiles_gdf.iterrows():
        tid = get_tile_id(tile.geometry)
        if fallback_marker_path(build_tile_paths(settings, tid, base)).exists():
            attempted_critical.add(tid)
    for loop_idx in range(1, settings.max_coverage_loops + 1):
        missing = []
        critical_coverage = []
        unresolved_critical = []
        low_coverage = []
        for _, tile in tiles_gdf.iterrows():
            tid = get_tile_id(tile.geometry)
            ndvi_fp = settings.work_dir / tid / f"ndvi_{base}.tif"
            if not ndvi_fp.exists():
                missing.append((tid, tile.geometry, 0.0))
                continue
            ratio = tile_coverage_in_aoi(ndvi_fp, tile.geometry, aoi_union, settings.ndvi_nodata)
            if ratio < settings.seasonal_fallback_coverage_threshold:
                if tid in attempted_critical:
                    unresolved_critical.append((tid, ratio))
                else:
                    critical_coverage.append((tid, tile.geometry, ratio))
            elif ratio < settings.coverage_threshold:
                low_coverage.append((tid, ratio))
        if critical_coverage:
            for tid, _, ratio in critical_coverage:
                LOGGER.warning("Tile %s sotto soglia critica copertura: %.2f%%", tid, ratio * 100)
        if unresolved_critical:
            for tid, ratio in unresolved_critical:
                LOGGER.warning("Tile %s resta sotto soglia critica dopo fallback: %.2f%%", tid, ratio * 100)
        if low_coverage:
            for tid, ratio in low_coverage:
                LOGGER.warning("Tile %s sotto soglia copertura: %.2f%%", tid, ratio * 100)
        if not missing and not critical_coverage:
            return
        if loop_idx == settings.max_coverage_loops:
            for tid, _, ratio in missing:
                LOGGER.warning("Tile %s mancante dopo retry copertura: %.2f%%", tid, ratio * 100)
            for tid, _, ratio in critical_coverage:
                LOGGER.warning("Tile %s resta sotto soglia critica dopo retry copertura: %.2f%%", tid, ratio * 100)
            return
        for tid, geom, ratio in critical_coverage:
            LOGGER.info("Ricalcolo tile critica %s con fallback stagionale (copertura %.2f%%)", tid, ratio * 100)
            attempted_critical.add(tid)
            remove_tile_products(build_tile_paths(settings, tid, base))
            compute_tile(
                geom,
                tid,
                wstart.date(),
                wend.date(),
                settings,
                use_scl=False,
                catalog=catalog,
                item_cache=item_cache,
                force=True,
            )
        for tid, geom, ratio in missing:
            LOGGER.info("Ricalcolo tile mancante %s senza SCL (copertura %.2f%%)", tid, ratio * 100)
            compute_tile(
                geom,
                tid,
                wstart.date(),
                wend.date(),
                settings,
                use_scl=False,
                catalog=catalog,
                item_cache=item_cache,
            )
