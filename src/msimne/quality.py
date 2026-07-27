from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path

import geopandas as gpd
import numpy as np
import rasterio
from rasterio.features import geometry_mask
from rasterio.fill import fillnodata

from .config import Settings
from .io import convert_to_cog, set_scale_offset

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class NdviQuality:
    valid_ratio_before: float
    valid_ratio_after: float
    interpolated_ratio: float
    filled: bool
    passed: bool


def _aoi_geometries_for_raster(aoi: gpd.GeoDataFrame, crs) -> list:
    aoi_out = aoi.to_crs(crs) if aoi.crs else aoi.set_crs(crs)
    return [geom for geom in aoi_out.geometry if geom is not None and not geom.is_empty]


def ndvi_valid_ratio(path: Path, aoi: gpd.GeoDataFrame, nodata: int) -> float:
    with rasterio.open(path) as src:
        geometries = _aoi_geometries_for_raster(aoi, src.crs)
        valid = 0
        total = 0
        for _, window in src.block_windows(1):
            arr = src.read(1, window=window)
            inside = geometry_mask(
                geometries,
                transform=src.window_transform(window),
                invert=True,
                out_shape=arr.shape,
            )
            inside_count = int(np.count_nonzero(inside))
            if inside_count == 0:
                continue
            total += inside_count
            valid += int(np.count_nonzero((arr != nodata) & inside))
    if total == 0:
        return 0.0
    return float(valid) / float(total)


def gap_fill_ndvi_inplace(path: Path, aoi: gpd.GeoDataFrame, settings: Settings) -> float:
    nodata = settings.ndvi_nodata
    temp_tif = path.with_name(f"{path.stem}.gapfill_work.tif")

    with rasterio.open(path) as src:
        profile = src.profile.copy()
        scale = src.scales[0] if getattr(src, "scales", None) else 1.0
        offset = src.offsets[0] if getattr(src, "offsets", None) else 0.0
        arr = src.read(1)
        geometries = _aoi_geometries_for_raster(aoi, src.crs)
        inside = geometry_mask(geometries, transform=src.transform, invert=True, out_shape=arr.shape)

    missing_inside = (arr == nodata) & inside
    total_inside = int(np.count_nonzero(inside))
    valid_inside = int(np.count_nonzero((arr != nodata) & inside))
    missing_count = int(np.count_nonzero(missing_inside))
    if total_inside == 0 or missing_count == 0:
        return 0.0
    if valid_inside == 0:
        LOGGER.warning("Gap filling impossibile per %s: nessun pixel NDVI valido dentro AOI", path.name)
        return 0.0

    valid_mask = ((arr != nodata) & inside).astype("uint8")
    image = np.where(arr == nodata, 0, arr).astype("float32", copy=False)
    filled = fillnodata(
        image,
        mask=valid_mask,
        max_search_distance=settings.gap_fill_max_search_distance,
        smoothing_iterations=0,
    )
    out = arr.copy()
    fillable = missing_inside & np.isfinite(filled)
    out[fillable] = np.rint(np.clip(filled[fillable], -10000, 10000)).astype(np.int16)
    out[~inside] = nodata

    profile.update(driver="GTiff", count=1, dtype="int16", nodata=nodata, compress="DEFLATE", predictor=2)
    with rasterio.open(temp_tif, "w", **profile) as dst:
        dst.write(out, 1)
        dst.scales = (scale,)
        dst.offsets = (offset,)

    crs = profile.get("crs")
    convert_to_cog(temp_tif, settings, nodata=nodata, dtype="Int16", crs=crs.to_string() if crs else None)
    os.replace(temp_tif, path)
    set_scale_offset(path, scale=scale, offset=offset)
    return float(missing_count) / float(total_inside)


def validate_and_fill_ndvi(path: Path, aoi: gpd.GeoDataFrame, settings: Settings) -> NdviQuality:
    before = ndvi_valid_ratio(path, aoi, settings.ndvi_nodata)
    LOGGER.info("NDVI coverage before gap filling %s: %.2f%%", path.name, before * 100)
    interpolated_ratio = 0.0
    filled = False
    after = before
    if before < settings.final_ndvi_valid_ratio:
        interpolated_ratio = gap_fill_ndvi_inplace(path, aoi, settings)
        filled = interpolated_ratio > 0
        after = ndvi_valid_ratio(path, aoi, settings.ndvi_nodata)
        LOGGER.info(
            "NDVI coverage after gap filling %s: %.2f%% interpolated=%.2f%%",
            path.name,
            after * 100,
            interpolated_ratio * 100,
        )
    return NdviQuality(
        valid_ratio_before=before,
        valid_ratio_after=after,
        interpolated_ratio=interpolated_ratio,
        filled=filled,
        passed=after >= settings.final_ndvi_valid_ratio,
    )
