from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import geopandas as gpd
import numpy as np
import rasterio
import shapely
from rasterio.features import geometry_mask
from rasterio.windows import bounds as window_bounds

from .config import Settings

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class NdviQuality:
    valid_ratio_before: float
    valid_ratio_after: float
    interpolated_ratio: float
    filled: bool
    passed: bool


def _aoi_union_for_raster(aoi: gpd.GeoDataFrame, crs):
    aoi_out = aoi.to_crs(crs) if aoi.crs else aoi.set_crs(crs)
    geoms = [geom for geom in aoi_out.geometry if geom is not None and not geom.is_empty]
    return shapely.union_all(geoms) if geoms else None


def ndvi_valid_ratio(path: Path, aoi: gpd.GeoDataFrame, nodata: int) -> float:
    """Quota di pixel NDVI validi dentro l'AOI.

    Il poligono viene rasterizzato solo sui blocchi a cavallo del confine e solo nella parte
    che li interseca: i blocchi interamente dentro o fuori l'AOI si decidono con un test
    geometrico, cosi' il costo non esplode con AOI grandi e ricche di vertici (es. R05).
    """
    with rasterio.open(path) as src:
        aoi_union = _aoi_union_for_raster(aoi, src.crs)
        if aoi_union is None:
            return 0.0
        shapely.prepare(aoi_union)
        valid = 0
        total = 0
        for _, window in src.block_windows(1):
            left, bottom, right, top = window_bounds(window, src.transform)
            block_box = shapely.box(left, bottom, right, top)
            if not aoi_union.intersects(block_box):
                continue
            arr = src.read(1, window=window)
            if aoi_union.contains(block_box):
                inside_count = arr.size
                valid_count = int(np.count_nonzero(arr != nodata))
            else:
                clipped = shapely.clip_by_rect(aoi_union, left, bottom, right, top)
                if clipped.is_empty:
                    continue
                inside = geometry_mask(
                    [clipped],
                    transform=src.window_transform(window),
                    invert=True,
                    out_shape=arr.shape,
                )
                inside_count = int(np.count_nonzero(inside))
                valid_count = int(np.count_nonzero((arr != nodata) & inside))
            total += inside_count
            valid += valid_count
    if total == 0:
        return 0.0
    return float(valid) / float(total)


def validate_ndvi_coverage(path: Path, aoi: gpd.GeoDataFrame, settings: Settings) -> NdviQuality:
    ratio = ndvi_valid_ratio(path, aoi, settings.ndvi_nodata)
    passed = ratio >= settings.final_ndvi_valid_ratio
    LOGGER.info(
        "Copertura NDVI %s: %.2f%% (soglia %.2f%%)",
        path.name,
        ratio * 100,
        settings.final_ndvi_valid_ratio * 100,
    )
    if not passed:
        LOGGER.warning("Copertura NDVI sotto soglia per %s: nessuna interpolazione applicata", path.name)
    return NdviQuality(
        valid_ratio_before=ratio,
        valid_ratio_after=ratio,
        interpolated_ratio=0.0,
        filled=False,
        passed=passed,
    )
