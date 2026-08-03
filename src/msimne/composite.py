from __future__ import annotations

import logging
from dataclasses import dataclass
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
from .io import save_stack_int16, set_scale_offset
from .stac import load_s2_median, open_catalog
from .utils import get_tile_id, retry

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class TilePaths:
    stack: Path
    ndvi: Path
    binary: Path


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
        binary=out_dir / f"ndvi_bin_{base}.tif",
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


def build_tile_products(med: xr.Dataset, paths: TilePaths, settings: Settings, crs_to_use, crs_str: str) -> None:
    save_stack_int16(med, ["B04", "B03", "B02", "B08"], paths.stack, nodata=settings.ndvi_nodata, crs=crs_to_use)

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
    ndvi_q.rio.to_raster(paths.ndvi, compress="DEFLATE", predictor=2, dtype="int16")
    set_scale_offset(paths.ndvi, scale=1 / 10000.0, offset=0.0)

    veg = xr.where(np.isfinite(ndvi), xr.where(ndvi >= settings.ndvi_threshold, 1, 0), settings.ndvi_nodata).astype(
        np.int16
    )
    veg.rio.write_nodata(settings.ndvi_nodata, encoded=True, inplace=True)
    veg.rio.write_crs(crs_to_use, inplace=True)
    veg.rio.to_raster(paths.binary, compress="DEFLATE", predictor=2, dtype="int16")


def remove_tile_products(paths: TilePaths) -> None:
    for path in (paths.stack, paths.ndvi, paths.binary):
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
) -> bool:
    return build_tile_for_ranges(
        geom,
        paths,
        [(start.isoformat(), end.isoformat())],
        settings,
        use_scl=use_scl,
        catalog=catalog,
    )


def build_tile_for_ranges(
    geom,
    paths: TilePaths,
    ranges: list[tuple[str, str]],
    settings: Settings,
    use_scl: bool,
    catalog: StacClient,
) -> bool:
    geom_wgs84 = gpd.GeoSeries([geom], crs=settings.final_mosaic_crs).to_crs("EPSG:4326").iloc[0]
    med = load_s2_median(catalog, settings, geom_wgs84, ranges, settings.final_mosaic_crs, use_scl=use_scl)
    if med is None:
        return False

    med = med.persist()
    try:
        crs_to_use = med.rio.crs
        crs_str = rasterio.crs.CRS.from_user_input(crs_to_use).to_string()
    except Exception:
        crs_to_use = settings.final_mosaic_crs
        crs_str = rasterio.crs.CRS.from_user_input(settings.final_mosaic_crs).to_string()

    build_tile_products(med, paths, settings, crs_to_use, crs_str)
    return True


def seasonal_fallback_ranges(start, end, years: int) -> list[tuple[str, str]]:
    return [
        ((start - relativedelta(years=year)).isoformat(), (end - relativedelta(years=year)).isoformat())
        for year in range(1, years + 1)
    ]


@retry(6, 30)
def compute_sentinel2_composite(
    geom,
    tile_id: str,
    start,
    end,
    settings: Settings,
    use_scl: bool = True,
    catalog: StacClient | None = None,
    force: bool = False,
) -> None:
    base = f"{start:%Y%m%d}_{end:%Y%m%d}"
    paths = build_tile_paths(settings, tile_id, base)
    if not force and all(path.exists() for path in (paths.stack, paths.ndvi, paths.binary)) and use_scl:
        LOGGER.info("Tile %s gia elaborato per %s", tile_id, base)
        return

    catalog = catalog or open_catalog(settings)
    if not build_tile_for_range(geom, paths, start, end, settings, use_scl=use_scl, catalog=catalog):
        LOGGER.warning("Nessun dataset valido per %s %s", tile_id, base)
        return

    coverage = raster_coverage_ratio(paths.ndvi, settings.ndvi_nodata)
    if use_scl and coverage < settings.min_valid_ratio:
        LOGGER.info("Copertura insufficiente per %s, rigenero senza SCL", tile_id)
        remove_tile_products(paths)
        compute_sentinel2_composite(geom, tile_id, start, end, settings, use_scl=False, catalog=catalog)
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
    if not build_tile_for_ranges(geom, paths, fallback_ranges, settings, use_scl=False, catalog=catalog):
        LOGGER.warning("Fallback stagionale senza dataset valido per %s %s", tile_id, base)
        return
    fallback_coverage = raster_coverage_ratio(paths.ndvi, settings.ndvi_nodata)
    LOGGER.info("Tile %s copertura dopo fallback stagionale: %.2f%%", tile_id, fallback_coverage * 100)


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
) -> None:
    base = f"{wstart:%Y%m%d}_{wend:%Y%m%d}"
    aoi_union = aoi.geometry.unary_union
    attempted_critical: set[str] = set()
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
            compute_sentinel2_composite(
                geom,
                tid,
                wstart.date(),
                wend.date(),
                settings,
                use_scl=False,
                catalog=catalog,
                force=True,
            )
        for tid, geom, ratio in missing:
            LOGGER.info("Ricalcolo tile mancante %s senza SCL (copertura %.2f%%)", tid, ratio * 100)
            compute_sentinel2_composite(geom, tid, wstart.date(), wend.date(), settings, use_scl=False, catalog=catalog)
